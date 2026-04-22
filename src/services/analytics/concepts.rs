use super::summary::{dl_type_filter, parse_ym, rfc_column};
/// Concepts: top products/services by clave_prod_serv and descripcion.
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct ConceptsResponse {
    pub top_by_amount: Vec<ConceptRow>,
    pub top_by_count: Vec<ConceptRow>,
    pub by_clave: Vec<ClaveRow>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ConceptRow {
    pub descripcion: String,
    pub clave_prod_serv: String,
    pub total_importe: f64,
    pub invoice_count: i64,
    pub avg_precio: f64,
}

#[derive(Debug, Serialize)]
pub struct ClaveRow {
    pub clave_prod_serv: String,
    pub total_importe: f64,
    pub invoice_count: i64,
    pub pct_of_total: f64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<ConceptsResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);

    // By description (top 50 by amount)
    let desc_rows = sqlx::query(&format!(
        r#"
        SELECT
            UPPER(TRIM(COALESCE(cc.descripcion, '')))   AS desc,
            COALESCE(cc.clave_prod_serv, '')             AS clave,
            SUM(COALESCE(cc.importe, 0))                 AS total,
            COUNT(DISTINCT c.uuid)                       AS cnt,
            AVG(COALESCE(cc.valor_unitario, 0))          AS avg_precio
        FROM pulso.cfdi_concepts cc
        JOIN pulso.cfdis c ON c.uuid = cc.uuid
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.tipo_comprobante NOT IN ('P','N')
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
          AND cc.descripcion IS NOT NULL
        GROUP BY desc, clave
        ORDER BY total DESC
        LIMIT 50
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let top_by_amount: Vec<ConceptRow> = desc_rows
        .iter()
        .map(|r| {
            let total: f64 = r.try_get("total").unwrap_or(0.0);
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            ConceptRow {
                descripcion: r.try_get("desc").unwrap_or_default(),
                clave_prod_serv: r.try_get("clave").unwrap_or_default(),
                total_importe: total,
                invoice_count: cnt,
                avg_precio: r.try_get("avg_precio").unwrap_or(0.0),
            }
        })
        .collect();

    // Top by count
    let mut top_by_count = top_by_amount.clone();
    top_by_count.sort_by(|a, b| b.invoice_count.cmp(&a.invoice_count));
    top_by_count.truncate(20);

    // By clave_prod_serv
    let clave_rows = sqlx::query(&format!(
        r#"
        SELECT
            COALESCE(cc.clave_prod_serv, 'SIN_CLAVE') AS clave,
            SUM(COALESCE(cc.importe, 0))               AS total,
            COUNT(DISTINCT c.uuid)                     AS cnt
        FROM pulso.cfdi_concepts cc
        JOIN pulso.cfdis c ON c.uuid = cc.uuid
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.tipo_comprobante NOT IN ('P','N')
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY clave
        ORDER BY total DESC
        LIMIT 30
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let grand_total: f64 = clave_rows
        .iter()
        .map(|r| r.try_get::<f64, _>("total").unwrap_or(0.0))
        .sum();

    let by_clave: Vec<ClaveRow> = clave_rows
        .iter()
        .map(|r| {
            let total: f64 = r.try_get("total").unwrap_or(0.0);
            ClaveRow {
                clave_prod_serv: r.try_get("clave").unwrap_or_default(),
                total_importe: total,
                invoice_count: r.try_get("cnt").unwrap_or(0),
                pct_of_total: if grand_total > 0.0 {
                    total / grand_total * 100.0
                } else {
                    0.0
                },
            }
        })
        .collect();

    Ok(ConceptsResponse {
        top_by_amount,
        top_by_count,
        by_clave,
    })
}
