use std::collections::HashMap;
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;
use super::summary::{dl_type_filter, rfc_column};

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct PeriodComparisonResponse {
    pub per_year: Vec<PeriodYearRow>,
    pub monthly_matrix: Vec<MonthMatrixRow>,
    pub top_cp_by_year: Vec<CpPeriodRow>,
    pub bridges: Vec<BridgeEntry>,
}

#[derive(Debug, Serialize)]
pub struct PeriodYearRow {
    pub year: i32,
    pub period_label: String,
    pub total_mxn: f64,
    pub cp_count: i64,
    pub invoice_count: i64,
    pub avg_ticket: f64,
    pub top10_pct: f64,
    pub yoy_pct: Option<f64>,
    pub fy_total_mxn: f64,
    pub pct_of_fy: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct MonthMatrixRow {
    pub year: i32,
    pub month: i32,
    pub total_mxn: f64,
    pub cumulative_mxn: f64,
}

#[derive(Debug, Serialize)]
pub struct CpPeriodRow {
    pub year: i32,
    pub rank: i64,
    pub rfc: String,
    pub nombre: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub share_pct: f64,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct BridgeEntry {
    pub year_current: i32,
    pub year_prev: i32,
    pub rows: Vec<BridgeRow>,
    pub top_expansions: Vec<BridgeRow>,
    pub top_contractions: Vec<BridgeRow>,
    pub new_relevant: Vec<BridgeRow>,
    pub lost_relevant: Vec<BridgeRow>,
}

#[derive(Debug, Serialize, Clone)]
pub struct BridgeRow {
    pub rfc: String,
    pub nombre: String,
    pub current_mxn: f64,
    pub prev_mxn: f64,
    pub delta_mxn: f64,
    pub delta_pct: Option<f64>,
    pub status: String,
}

// ---------------------------------------------------------------------------
// Main function
// ---------------------------------------------------------------------------

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from_month: i32,
    to_month: i32,
    years: &[i32],
    limit: i64,
) -> anyhow::Result<PeriodComparisonResponse> {
    let owner_col = rfc_column(dl_type);
    let dl_filter = dl_type_filter(dl_type);
    let cp_col = if dl_type == "recibidos" { "rfc_emisor" } else { "rfc_receptor" };
    let cp_name_col = if dl_type == "recibidos" { "nombre_emisor" } else { "nombre_receptor" };

    let years_vec: Vec<i32> = years.iter().copied().collect();

    // Month abbreviations in Spanish
    const MONTHS: [&str; 12] = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];
    let fm_idx = ((from_month - 1).clamp(0, 11)) as usize;
    let tm_idx = ((to_month - 1).clamp(0, 11)) as usize;
    let period_label = format!("{}–{}", MONTHS[fm_idx], MONTHS[tm_idx]);

    // -----------------------------------------------------------------------
    // Query 1 – Per year period summary
    // -----------------------------------------------------------------------
    let q1 = format!(
        r#"
        SELECT year,
               SUM(COALESCE(total_mxn, 0)::float8)::float8 AS total,
               COUNT(DISTINCT {cp_col}) AS cp_count,
               COUNT(*) AS invoice_count
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P', 'N')
          AND year = ANY($2)
          AND month >= $3 AND month <= $4
        GROUP BY year
        ORDER BY year
        "#
    );

    let period_rows = sqlx::query(&q1)
        .bind(rfc)
        .bind(&years_vec as &[i32])
        .bind(from_month)
        .bind(to_month)
        .fetch_all(pool)
        .await?;

    // Map year -> (total, cp_count, invoice_count)
    let mut period_map: HashMap<i32, (f64, i64, i64)> = HashMap::new();
    for r in &period_rows {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let total: f64 = r.try_get("total").unwrap_or(0.0);
        let cp_count: i64 = r.try_get("cp_count").unwrap_or(0);
        let invoice_count: i64 = r.try_get("invoice_count").unwrap_or(0);
        period_map.insert(year, (total, cp_count, invoice_count));
    }

    // -----------------------------------------------------------------------
    // Query 2 – Full year totals (for % del FY)
    // -----------------------------------------------------------------------
    let q2 = format!(
        r#"
        SELECT year,
               SUM(COALESCE(total_mxn, 0)::float8)::float8 AS fy_total
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P', 'N')
          AND year = ANY($2)
        GROUP BY year
        "#
    );

    let fy_rows = sqlx::query(&q2)
        .bind(rfc)
        .bind(&years_vec as &[i32])
        .fetch_all(pool)
        .await?;

    let mut fy_map: HashMap<i32, f64> = HashMap::new();
    for r in &fy_rows {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let fy_total: f64 = r.try_get("fy_total").unwrap_or(0.0);
        fy_map.insert(year, fy_total);
    }

    // -----------------------------------------------------------------------
    // Query 3 – Top N counterparties per year (ranked)
    // -----------------------------------------------------------------------
    let q3 = format!(
        r#"
        WITH ranked AS (
            SELECT year,
                   {cp_col} AS cp_rfc,
                   MAX({cp_name_col}) AS cp_nombre,
                   SUM(COALESCE(total_mxn, 0)::float8)::float8 AS total,
                   COUNT(*) AS invoice_count,
                   ROW_NUMBER() OVER (PARTITION BY year ORDER BY SUM(COALESCE(total_mxn, 0)) DESC) AS rnk
            FROM pulso.cfdis
            WHERE {owner_col} = $1
              AND {dl_filter}
              AND tipo_comprobante NOT IN ('P', 'N')
              AND year = ANY($2)
              AND month >= $3 AND month <= $4
            GROUP BY year, {cp_col}
        )
        SELECT year, cp_rfc, cp_nombre, total, invoice_count, rnk
        FROM ranked
        WHERE rnk <= $5
        ORDER BY year, rnk
        "#
    );

    let top_rows = sqlx::query(&q3)
        .bind(rfc)
        .bind(&years_vec as &[i32])
        .bind(from_month)
        .bind(to_month)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    // Build a map: year -> Vec<(rfc, total)> for top10_pct and CpPeriodRow status
    let mut top_by_year: HashMap<i32, Vec<(String, f64)>> = HashMap::new();
    for r in &top_rows {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let cp_rfc: String = r.try_get("cp_rfc").unwrap_or_default();
        let total: f64 = r.try_get("total").unwrap_or(0.0);
        top_by_year.entry(year).or_default().push((cp_rfc, total));
    }

    // -----------------------------------------------------------------------
    // Query 4 – Monthly matrix
    // -----------------------------------------------------------------------
    let q4 = format!(
        r#"
        SELECT year, month,
               SUM(COALESCE(total_mxn, 0)::float8)::float8 AS total
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P', 'N')
          AND year = ANY($2)
          AND month >= $3 AND month <= $4
        GROUP BY year, month
        ORDER BY year, month
        "#
    );

    let matrix_raw = sqlx::query(&q4)
        .bind(rfc)
        .bind(&years_vec as &[i32])
        .bind(from_month)
        .bind(to_month)
        .fetch_all(pool)
        .await?;

    // Compute cumulative per year
    let mut monthly_matrix: Vec<MonthMatrixRow> = Vec::new();
    let mut cumulative_by_year: HashMap<i32, f64> = HashMap::new();
    for r in &matrix_raw {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let month: i32 = r.try_get::<i64, _>("month").unwrap_or(0) as i32;
        let total: f64 = r.try_get("total").unwrap_or(0.0);
        let cum = cumulative_by_year.entry(year).or_insert(0.0);
        *cum += total;
        monthly_matrix.push(MonthMatrixRow {
            year,
            month,
            total_mxn: total,
            cumulative_mxn: *cum,
        });
    }

    // -----------------------------------------------------------------------
    // Build per_year
    // -----------------------------------------------------------------------
    let mut sorted_years: Vec<i32> = years_vec.clone();
    sorted_years.sort();

    let mut per_year: Vec<PeriodYearRow> = Vec::new();
    for (i, &year) in sorted_years.iter().enumerate() {
        let (period_total, cp_count, invoice_count) = period_map.get(&year).copied().unwrap_or((0.0, 0, 0));
        let fy_total = fy_map.get(&year).copied().unwrap_or(0.0);

        // YoY %: compare with previous year in sorted list
        let yoy_pct = if i > 0 {
            let prev_year = sorted_years[i - 1];
            let (prev_total, _, _) = period_map.get(&prev_year).copied().unwrap_or((0.0, 0, 0));
            if prev_total != 0.0 {
                Some(((period_total - prev_total) / prev_total) * 100.0)
            } else {
                None
            }
        } else {
            None
        };

        // pct_of_fy
        let pct_of_fy = if fy_total != 0.0 {
            Some(period_total / fy_total * 100.0)
        } else {
            None
        };

        // avg_ticket
        let avg_ticket = if invoice_count > 0 {
            period_total / invoice_count as f64
        } else {
            0.0
        };

        // top10_pct: sum of top min(10, limit) cp shares for this year
        let top10_pct = if period_total > 0.0 {
            let tops = top_by_year.get(&year).map(|v| v.as_slice()).unwrap_or(&[]);
            let take = (10usize).min(tops.len());
            let top_sum: f64 = tops[..take].iter().map(|(_, t)| t).sum();
            top_sum / period_total * 100.0
        } else {
            0.0
        };

        per_year.push(PeriodYearRow {
            year,
            period_label: period_label.clone(),
            total_mxn: period_total,
            cp_count,
            invoice_count,
            avg_ticket,
            top10_pct,
            yoy_pct,
            fy_total_mxn: fy_total,
            pct_of_fy,
        });
    }

    // -----------------------------------------------------------------------
    // Build top_cp_by_year
    // -----------------------------------------------------------------------
    // For status: compare cp total with previous year's top data
    // Build a map: year -> HashMap<rfc, total>
    let mut year_cp_totals: HashMap<i32, HashMap<String, f64>> = HashMap::new();
    for r in &top_rows {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let cp_rfc: String = r.try_get("cp_rfc").unwrap_or_default();
        let total: f64 = r.try_get("total").unwrap_or(0.0);
        year_cp_totals.entry(year).or_default().insert(cp_rfc, total);
    }

    let mut top_cp_by_year: Vec<CpPeriodRow> = Vec::new();
    for r in &top_rows {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let rank: i64 = r.try_get("rnk").unwrap_or(0);
        let cp_rfc: String = r.try_get("cp_rfc").unwrap_or_default();
        let cp_nombre: String = r.try_get("cp_nombre").unwrap_or_default();
        let total: f64 = r.try_get("total").unwrap_or(0.0);
        let invoice_count: i64 = r.try_get("invoice_count").unwrap_or(0);

        let (period_total, _, _) = period_map.get(&year).copied().unwrap_or((0.0, 0, 0));
        let share_pct = if period_total > 0.0 { total / period_total * 100.0 } else { 0.0 };

        // Status: compare with previous year in sorted list
        let status = {
            let prev_year_idx = sorted_years.iter().position(|&y| y == year).and_then(|i| i.checked_sub(1));
            let status_str = if let Some(pi) = prev_year_idx {
                let prev_year = sorted_years[pi];
                let prev_total = year_cp_totals
                    .get(&prev_year)
                    .and_then(|m| m.get(&cp_rfc))
                    .copied()
                    .unwrap_or(0.0);
                if prev_total == 0.0 {
                    "Nuevo"
                } else if total > prev_total * 1.05 {
                    "Expansión"
                } else if total < prev_total * 0.95 {
                    "Contracción"
                } else {
                    "Estable"
                }
            } else {
                "Estable"
            };
            status_str.to_string()
        };

        top_cp_by_year.push(CpPeriodRow {
            year,
            rank,
            rfc: cp_rfc,
            nombre: cp_nombre,
            total_mxn: total,
            invoice_count,
            share_pct,
            status,
        });
    }

    // -----------------------------------------------------------------------
    // Query 5 – Bridge per year pair
    // -----------------------------------------------------------------------
    let mut bridges: Vec<BridgeEntry> = Vec::new();

    for i in 1..sorted_years.len() {
        let year_current = sorted_years[i];
        let year_prev = sorted_years[i - 1];

        let q5 = format!(
            r#"
            WITH curr AS (
                SELECT {cp_col} AS cp_rfc, MAX({cp_name_col}) AS cp_nombre,
                       SUM(COALESCE(total_mxn,0)::float8)::float8 AS total
                FROM pulso.cfdis
                WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
                  AND year = $2 AND month >= $3 AND month <= $4
                GROUP BY {cp_col}
            ),
            prev AS (
                SELECT {cp_col} AS cp_rfc, MAX({cp_name_col}) AS cp_nombre,
                       SUM(COALESCE(total_mxn,0)::float8)::float8 AS total
                FROM pulso.cfdis
                WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
                  AND year = $5 AND month >= $3 AND month <= $4
                GROUP BY {cp_col}
            )
            SELECT COALESCE(c.cp_rfc, p.cp_rfc) AS cp_rfc,
                   COALESCE(c.cp_nombre, p.cp_nombre) AS cp_nombre,
                   COALESCE(c.total, 0.0) AS curr_total,
                   COALESCE(p.total, 0.0) AS prev_total
            FROM curr c FULL OUTER JOIN prev p ON c.cp_rfc = p.cp_rfc
            ORDER BY ABS(COALESCE(c.total, 0.0) - COALESCE(p.total, 0.0)) DESC
            LIMIT $6
            "#
        );

        let bridge_limit = limit * 4;
        let bridge_raw = sqlx::query(&q5)
            .bind(rfc)
            .bind(year_current)
            .bind(from_month)
            .bind(to_month)
            .bind(year_prev)
            .bind(bridge_limit)
            .fetch_all(pool)
            .await?;

        let mut all_rows: Vec<BridgeRow> = Vec::new();
        for r in &bridge_raw {
            let cp_rfc: String = r.try_get("cp_rfc").unwrap_or_default();
            let cp_nombre: String = r.try_get("cp_nombre").unwrap_or_default();
            let curr_total: f64 = r.try_get("curr_total").unwrap_or(0.0);
            let prev_total: f64 = r.try_get("prev_total").unwrap_or(0.0);
            let delta_mxn = curr_total - prev_total;

            let status = if prev_total == 0.0 {
                "Nuevo"
            } else if curr_total == 0.0 {
                "Perdido"
            } else if delta_mxn > prev_total * 0.05 {
                "Expansión"
            } else if delta_mxn < -(prev_total * 0.05) {
                "Contracción"
            } else {
                "Estable"
            };

            let delta_pct = if prev_total != 0.0 {
                Some((delta_mxn / prev_total) * 100.0)
            } else {
                None
            };

            all_rows.push(BridgeRow {
                rfc: cp_rfc,
                nombre: cp_nombre,
                current_mxn: curr_total,
                prev_mxn: prev_total,
                delta_mxn,
                delta_pct,
                status: status.to_string(),
            });
        }

        // top_expansions: Expansión, sorted by delta DESC, top 5
        let mut top_expansions: Vec<BridgeRow> = all_rows
            .iter()
            .filter(|r| r.status == "Expansión")
            .cloned()
            .collect();
        top_expansions.sort_by(|a, b| b.delta_mxn.partial_cmp(&a.delta_mxn).unwrap_or(std::cmp::Ordering::Equal));
        top_expansions.truncate(5);

        // top_contractions: Contracción, sorted by delta ASC (most negative), top 5
        let mut top_contractions: Vec<BridgeRow> = all_rows
            .iter()
            .filter(|r| r.status == "Contracción")
            .cloned()
            .collect();
        top_contractions.sort_by(|a, b| a.delta_mxn.partial_cmp(&b.delta_mxn).unwrap_or(std::cmp::Ordering::Equal));
        top_contractions.truncate(5);

        // new_relevant: Nuevo, sorted by curr_total DESC, top 5
        let mut new_relevant: Vec<BridgeRow> = all_rows
            .iter()
            .filter(|r| r.status == "Nuevo")
            .cloned()
            .collect();
        new_relevant.sort_by(|a, b| b.current_mxn.partial_cmp(&a.current_mxn).unwrap_or(std::cmp::Ordering::Equal));
        new_relevant.truncate(5);

        // lost_relevant: Perdido, sorted by prev_total DESC, top 5
        let mut lost_relevant: Vec<BridgeRow> = all_rows
            .iter()
            .filter(|r| r.status == "Perdido")
            .cloned()
            .collect();
        lost_relevant.sort_by(|a, b| b.prev_mxn.partial_cmp(&a.prev_mxn).unwrap_or(std::cmp::Ordering::Equal));
        lost_relevant.truncate(5);

        // main rows: Nuevo, Expansión, Contracción only (not Perdido), sorted by ABS(delta) DESC, top limit
        let mut rows: Vec<BridgeRow> = all_rows
            .iter()
            .filter(|r| r.status == "Nuevo" || r.status == "Expansión" || r.status == "Contracción")
            .cloned()
            .collect();
        rows.sort_by(|a, b| b.delta_mxn.abs().partial_cmp(&a.delta_mxn.abs()).unwrap_or(std::cmp::Ordering::Equal));
        rows.truncate(limit as usize);

        bridges.push(BridgeEntry {
            year_current,
            year_prev,
            rows,
            top_expansions,
            top_contractions,
            new_relevant,
            lost_relevant,
        });
    }

    Ok(PeriodComparisonResponse {
        per_year,
        monthly_matrix,
        top_cp_by_year,
        bridges,
    })
}
