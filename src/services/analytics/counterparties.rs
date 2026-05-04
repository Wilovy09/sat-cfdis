use super::summary::{dl_type_filter, parse_ym, rfc_column};
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct CounterpartiesResponse {
    pub top: Vec<CounterpartyRow>,
    pub total_counterparties: i64,
    pub hhi: f64,       // Herfindahl-Hirschman Index (concentration)
    pub top10_pct: f64, // % of total from top 10
}

#[derive(Debug, Serialize)]
pub struct CounterpartyRow {
    pub rfc: String,
    pub nombre: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub avg_invoice_mxn: f64,
    pub first_invoice: String,
    pub last_invoice: String,
    pub pct_of_total: f64,
    pub months_active: i64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
    limit: i64,
) -> anyhow::Result<CounterpartiesResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_col = if dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };
    let cp_name_col = if dl_type == "recibidos" {
        "nombre_emisor"
    } else {
        "nombre_receptor"
    };

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            {cp_col}                                                AS cp_rfc,
            MAX({cp_name_col})                                      AS cp_nombre,
            SUM(COALESCE(total_mxn,0)::float8)::float8                             AS total,
            COUNT(*)                                               AS cnt,
            MIN(fecha_emision)                                     AS first_inv,
            MAX(fecha_emision)                                     AS last_inv,
            COUNT(DISTINCT year * 100 + month)                     AS months_active
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY {cp_col}
        ORDER BY total DESC
        LIMIT $6
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    // Total for percentage calc
    let total_row = sqlx::query(&format!(
        r#"
        SELECT SUM(COALESCE(total_mxn,0)::float8)::float8 AS total, COUNT(DISTINCT {cp_col}) AS cp_count
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_one(pool)
    .await?;

    let grand_total: f64 = total_row.try_get("total").unwrap_or(0.0);
    let cp_count: i64 = total_row.try_get("cp_count").unwrap_or(0);

    let top: Vec<CounterpartyRow> = rows
        .iter()
        .map(|r| {
            let total: f64 = r.try_get("total").unwrap_or(0.0);
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            CounterpartyRow {
                rfc: r.try_get("cp_rfc").unwrap_or_default(),
                nombre: r.try_get("cp_nombre").unwrap_or_default(),
                total_mxn: total,
                invoice_count: cnt,
                avg_invoice_mxn: if cnt > 0 { total / cnt as f64 } else { 0.0 },
                first_invoice: r.try_get("first_inv").unwrap_or_default(),
                last_invoice: r.try_get("last_inv").unwrap_or_default(),
                pct_of_total: if grand_total > 0.0 {
                    total / grand_total * 100.0
                } else {
                    0.0
                },
                months_active: r.try_get("months_active").unwrap_or(0),
            }
        })
        .collect();

    // HHI: sum of (share_i)^2 — use top results as approximation
    let hhi: f64 = top
        .iter()
        .map(|r| (r.pct_of_total / 100.0).powi(2))
        .sum::<f64>()
        * 10_000.0;

    let top10_total: f64 = top.iter().take(10).map(|r| r.total_mxn).sum();
    let top10_pct = if grand_total > 0.0 {
        top10_total / grand_total * 100.0
    } else {
        0.0
    };

    Ok(CounterpartiesResponse {
        top,
        total_counterparties: cp_count,
        hhi,
        top10_pct,
    })
}
