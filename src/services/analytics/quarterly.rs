use super::summary::{dl_type_filter, parse_ym, rfc_column};
/// Quarterly aggregation — groups months into fiscal quarters with YoY comparison.
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct QuarterlyResponse {
    pub quarters: Vec<QuarterRow>,
    pub yoy: Vec<QuarterYoyRow>,
}

#[derive(Debug, Serialize)]
pub struct QuarterRow {
    pub year: i64,
    pub quarter: i64,       // 1–4
    pub period: String,     // e.g. "2024-Q1"
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub is_complete: bool,  // all 3 months present in data
}

#[derive(Debug, Serialize)]
pub struct QuarterYoyRow {
    pub quarter: i64,
    pub period: String,     // e.g. "Q1"
    pub current_year: i64,
    pub prior_year: i64,
    pub current_mxn: f64,
    pub prior_mxn: f64,
    pub delta_pct: Option<f64>,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<QuarterlyResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);

    // Aggregate by year + quarter + count distinct months present
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            year,
            CASE
                WHEN month BETWEEN 1 AND 3 THEN 1
                WHEN month BETWEEN 4 AND 6 THEN 2
                WHEN month BETWEEN 7 AND 9 THEN 3
                ELSE 4
            END AS quarter,
            SUM(COALESCE(total_neto_mxn,0)::float8)::float8 AS total,
            COUNT(*) AS cnt,
            COUNT(DISTINCT month) AS months_present
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.normalization_rules nr
              WHERE nr.owner_rfc = $1 AND nr.action = 'exclude'
                AND (
                  (nr.cfdi_uuid IS NOT NULL AND UPPER(nr.cfdi_uuid) = UPPER(uuid))
                  OR (nr.cfdi_uuid IS NULL AND (
                      (nr.dl_type IN ('emitidos','ambos') AND nr.source_rfc = rfc_receptor)
                      OR (nr.dl_type IN ('recibidos','ambos') AND nr.source_rfc = rfc_emisor)
                  ))
                )
          )
        GROUP BY year, quarter
        ORDER BY year, quarter
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let quarters: Vec<QuarterRow> = rows
        .iter()
        .map(|r| {
            let year: i64 = r.try_get("year").unwrap_or(0);
            let quarter: i64 = r.try_get("quarter").unwrap_or(0);
            let months_present: i64 = r.try_get("months_present").unwrap_or(0);
            QuarterRow {
                year,
                quarter,
                period: format!("{year}-Q{quarter}"),
                total_mxn: r.try_get("total").unwrap_or(0.0),
                invoice_count: r.try_get("cnt").unwrap_or(0),
                is_complete: months_present == 3,
            }
        })
        .collect();

    // YoY: pair each quarter with the same quarter in the prior year
    // Find the two most recent years with data
    let mut years_seen: Vec<i64> = quarters.iter().map(|q| q.year).collect();
    years_seen.dedup();
    years_seen.sort();
    years_seen.dedup();

    let yoy = if years_seen.len() >= 2 {
        let current_year = *years_seen.last().unwrap();
        let prior_year = years_seen[years_seen.len() - 2];

        let q_map: std::collections::HashMap<(i64, i64), &QuarterRow> =
            quarters.iter().map(|q| ((q.year, q.quarter), q)).collect();

        (1i64..=4)
            .filter_map(|q| {
                let curr = q_map.get(&(current_year, q))?;
                let prior = q_map.get(&(prior_year, q))?;
                let delta_pct = if curr.is_complete && prior.is_complete && prior.total_mxn != 0.0 {
                    Some((curr.total_mxn - prior.total_mxn) / prior.total_mxn * 100.0)
                } else {
                    None
                };
                Some(QuarterYoyRow {
                    quarter: q,
                    period: format!("Q{q}"),
                    current_year,
                    prior_year,
                    current_mxn: curr.total_mxn,
                    prior_mxn: prior.total_mxn,
                    delta_pct,
                })
            })
            .collect()
    } else {
        vec![]
    };

    Ok(QuarterlyResponse { quarters, yoy })
}
