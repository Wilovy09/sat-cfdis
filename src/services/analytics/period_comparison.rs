use super::summary::{cp_key_expr, cp_nombre_expr, dl_type_filter, get_f64, rfc_column};
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct PeriodComparisonResponse {
    pub per_year: Vec<PeriodYearRow>,
    pub monthly_matrix: Vec<MonthMatrixRow>,
    pub top_cp_by_year: Vec<CpPeriodRow>,
    pub bridges: Vec<BridgeEntry>,
    pub effective_from_month: i32,
    pub effective_to_month: i32,
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
    // L11-16 / AUD-111: a real bridge -- periodo_anterior_mxn + expansion_mxn +
    // contraction_mxn + new_mxn + lost_mxn + estable_mxn == periodo_actual_mxn, exactly
    // (trap 1). `estable_mxn` isn't one of the four highlighted drivers but still has to be
    // in the sum -- an "Estable" counterparty's delta is small (within the +-5% band) but
    // not literally zero, and leaving it out would make the four components not close.
    pub periodo_anterior_mxn: f64,
    pub periodo_actual_mxn: f64,
    pub expansion_mxn: f64,
    pub contraction_mxn: f64,
    pub new_mxn: f64,
    pub lost_mxn: f64,
    pub estable_mxn: f64,
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
    let cp_key_expr = cp_key_expr(cp_col, cp_name_col);
    let cp_nombre_expr = cp_nombre_expr(cp_col, cp_name_col);

    let years_vec: Vec<i32> = years.to_vec();

    // L7-01: if -- and only if -- the compared years include the year of the last closed
    // calendar month, the effective to_month drops to min(to_month, that month), applied to
    // ALL compared years so the comparison stays over the same period for every year. Must
    // land before period_label is built below, or CMP02's "Periodo" column would keep saying
    // the untopped range while the queries already use the topped one.
    let last_closed_yyyymm = crate::routes::analytics::current_month_yyyymm();
    let last_closed_year = (last_closed_yyyymm / 100) as i32;
    let last_closed_month = (last_closed_yyyymm % 100) as i32;
    let to_month = if years_vec.contains(&last_closed_year) {
        to_month.min(last_closed_month)
    } else {
        to_month
    };

    // Month abbreviations in Spanish
    const MONTHS: [&str; 12] = [
        "Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic",
    ];
    let fm_idx = ((from_month - 1).clamp(0, 11)) as usize;
    let tm_idx = ((to_month - 1).clamp(0, 11)) as usize;
    let period_label = format!("{}–{}", MONTHS[fm_idx], MONTHS[tm_idx]);

    // -----------------------------------------------------------------------
    // Query 1 – Per year period summary
    // -----------------------------------------------------------------------
    let q1 = format!(
        r#"
        SELECT year,
               SUM(COALESCE(total_neto_mxn_ajustado, 0)::float8)::float8 AS total,
               COUNT(DISTINCT ({cp_key_expr})) AS cp_count,
               COUNT(*) AS invoice_count
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P', 'N', 'T')
          AND NOT is_cancelled
          AND year = ANY($2)
          AND month >= $3 AND month <= $4
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
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
        let total: f64 = get_f64(r, "total");
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
               SUM(COALESCE(total_neto_mxn_ajustado, 0)::float8)::float8 AS fy_total
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P', 'N', 'T')
          AND NOT is_cancelled
          AND year = ANY($2)
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
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
        let fy_total: f64 = get_f64(r, "fy_total");
        fy_map.insert(year, fy_total);
    }

    // -----------------------------------------------------------------------
    // Query 3 – Top N counterparties per year (ranked)
    // -----------------------------------------------------------------------
    let q3 = format!(
        r#"
        WITH ranked AS (
            SELECT year,
                   ({cp_key_expr}) AS cp_rfc,
                   {cp_nombre_expr} AS cp_nombre,
                   SUM(COALESCE(total_neto_mxn_ajustado, 0)::float8)::float8 AS total,
                   COUNT(*) AS invoice_count,
                   ROW_NUMBER() OVER (PARTITION BY year ORDER BY SUM(COALESCE(total_neto_mxn_ajustado, 0)) DESC) AS rnk
            FROM pulso.cfdis_ajustado c
            WHERE {owner_col} = $1
              AND {dl_filter}
              AND tipo_comprobante NOT IN ('P', 'N', 'T')
          AND NOT is_cancelled
              AND year = ANY($2)
              AND month >= $3 AND month <= $4
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY year, ({cp_key_expr})
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

    // -----------------------------------------------------------------------
    // Query 3b – Top 10 counterparties per year, for the "Concentración Top 10" KPI only.
    // C13-02/AUD-134: `limit` (the caller's Top-N selector) binds Query 3 above, whose
    // `top_rows` also feeds `top_cp_by_year` -- which must keep returning exactly `limit`
    // rows, selector-driven, unrelated to this item. Reusing that same query and just
    // raising its bind to 10 would inflate `top_cp_by_year` past what the selector asked
    // for, so the concentration metric needs its own query, fixed at 10 regardless of
    // `limit` (with the selector at 5, Query 3 alone can never have more than 5 rows to
    // sum, so "Top 10" silently became "Top 5").
    // -----------------------------------------------------------------------
    let q3b = format!(
        r#"
        WITH ranked AS (
            SELECT year,
                   ({cp_key_expr}) AS cp_rfc,
                   SUM(COALESCE(total_neto_mxn_ajustado, 0)::float8)::float8 AS total,
                   ROW_NUMBER() OVER (PARTITION BY year ORDER BY SUM(COALESCE(total_neto_mxn_ajustado, 0)) DESC) AS rnk
            FROM pulso.cfdis_ajustado c
            WHERE {owner_col} = $1
              AND {dl_filter}
              AND tipo_comprobante NOT IN ('P', 'N', 'T')
              AND NOT is_cancelled
              AND year = ANY($2)
              AND month >= $3 AND month <= $4
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY year, ({cp_key_expr})
        )
        SELECT year, total FROM ranked WHERE rnk <= 10
        "#
    );

    let top10_rows = sqlx::query(&q3b)
        .bind(rfc)
        .bind(&years_vec as &[i32])
        .bind(from_month)
        .bind(to_month)
        .fetch_all(pool)
        .await?;

    let mut top10_sum_by_year: HashMap<i32, f64> = HashMap::new();
    for r in &top10_rows {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let total: f64 = get_f64(r, "total");
        *top10_sum_by_year.entry(year).or_insert(0.0) += total;
    }

    // -----------------------------------------------------------------------
    // Query 4 – Monthly matrix
    // -----------------------------------------------------------------------
    let q4 = format!(
        r#"
        SELECT year, month,
               SUM(COALESCE(total_neto_mxn_ajustado, 0)::float8)::float8 AS total
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P', 'N', 'T')
          AND NOT is_cancelled
          AND year = ANY($2)
          AND month >= $3 AND month <= $4
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
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
        let total: f64 = get_f64(r, "total");
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
        let (period_total, cp_count, invoice_count) =
            period_map.get(&year).copied().unwrap_or((0.0, 0, 0));
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

        // top10_pct: sum of the top 10 cp shares for this year (C13-02/AUD-134 -- always
        // 10, independent of the caller's `limit` selector; see Query 3b above).
        let top10_pct = if period_total > 0.0 {
            let top_sum = top10_sum_by_year.get(&year).copied().unwrap_or(0.0);
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
        let total: f64 = get_f64(r, "total");
        year_cp_totals
            .entry(year)
            .or_default()
            .insert(cp_rfc, total);
    }

    let mut top_cp_by_year: Vec<CpPeriodRow> = Vec::new();
    for r in &top_rows {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let rank: i64 = r.try_get("rnk").unwrap_or(0);
        let cp_rfc: String = r.try_get("cp_rfc").unwrap_or_default();
        let cp_nombre: String = r.try_get("cp_nombre").unwrap_or_default();
        let total: f64 = get_f64(r, "total");
        let invoice_count: i64 = r.try_get("invoice_count").unwrap_or(0);

        let (period_total, _, _) = period_map.get(&year).copied().unwrap_or((0.0, 0, 0));
        let share_pct = if period_total > 0.0 {
            total / period_total * 100.0
        } else {
            0.0
        };

        // L11-15 / AUD-110: this used to be its own classification (>1.05x / <0.95x of
        // prev_total) -- a different formula than CMP04's bridge (delta > prev*0.05 /
        // delta < -prev*0.05, plus an explicit "Perdido" case for curr_total==0), so the
        // SAME counterparty in the SAME year could be "Expansión" here and "Nuevo" there.
        // CMP04 is the one that wins (compares against the same adjacent year, same
        // exclusions, same tipo_comprobante filter as year_cp_totals below -- verified the
        // two totals match). Not a shared computation (this stays a separate query, Query 3
        // vs CMP04's Query 5), but the exact same formula against the exact same numbers
        // gives the exact same answer, which is the point -- CMP06 has no classification
        // logic of its own left to disagree with.
        let status = {
            let prev_year_idx = sorted_years
                .iter()
                .position(|&y| y == year)
                .and_then(|i| i.checked_sub(1));
            let status_str = if let Some(pi) = prev_year_idx {
                let prev_year = sorted_years[pi];
                let prev_total = year_cp_totals
                    .get(&prev_year)
                    .and_then(|m| m.get(&cp_rfc))
                    .copied()
                    .unwrap_or(0.0);
                let delta = total - prev_total;
                if prev_total == 0.0 {
                    "Nuevo"
                } else if total == 0.0 {
                    "Perdido"
                } else if delta > prev_total * 0.05 {
                    "Expansión"
                } else if delta < -(prev_total * 0.05) {
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
                SELECT ({cp_key_expr}) AS cp_rfc, {cp_nombre_expr} AS cp_nombre,
                       SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS total
                FROM pulso.cfdis_ajustado c
                WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N','T') AND NOT is_cancelled
                  AND year = $2 AND month >= $3 AND month <= $4
                  AND NOT EXISTS (
                      SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
                  )
                GROUP BY ({cp_key_expr})
            ),
            prev AS (
                SELECT ({cp_key_expr}) AS cp_rfc, {cp_nombre_expr} AS cp_nombre,
                       SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS total
                FROM pulso.cfdis_ajustado c
                WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N','T') AND NOT is_cancelled
                  AND year = $5 AND month >= $3 AND month <= $4
                  AND NOT EXISTS (
                      SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
                  )
                GROUP BY ({cp_key_expr})
            )
            SELECT COALESCE(c.cp_rfc, p.cp_rfc) AS cp_rfc,
                   COALESCE(c.cp_nombre, p.cp_nombre) AS cp_nombre,
                   COALESCE(c.total, 0.0) AS curr_total,
                   COALESCE(p.total, 0.0) AS prev_total
            FROM curr c FULL OUTER JOIN prev p ON c.cp_rfc = p.cp_rfc
            "#
        );

        // L11-16 trap 1: the four bridge components have to sum to the EXACT variación --
        // the old `LIMIT $6` (top `limit*4` counterparties by |delta|) cut off small-delta
        // ("Estable") counterparties before they ever reached the aggregate, so the four
        // displayed buckets never actually summed to periodo_actual - periodo_anterior. No
        // limit here now; `rows`/top_expansions/etc. below still truncate in Rust for
        // display, but the aggregate totals are computed from this full universe first.
        let bridge_raw = sqlx::query(&q5)
            .bind(rfc)
            .bind(year_current)
            .bind(from_month)
            .bind(to_month)
            .bind(year_prev)
            .fetch_all(pool)
            .await?;

        let mut all_rows: Vec<BridgeRow> = Vec::new();
        for r in &bridge_raw {
            let cp_rfc: String = r.try_get("cp_rfc").unwrap_or_default();
            let cp_nombre: String = r.try_get("cp_nombre").unwrap_or_default();
            let curr_total: f64 = get_f64(r, "curr_total");
            let prev_total: f64 = get_f64(r, "prev_total");
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
        top_expansions.sort_by(|a, b| {
            b.delta_mxn
                .partial_cmp(&a.delta_mxn)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        top_expansions.truncate(5);

        // top_contractions: Contracción, sorted by delta ASC (most negative), top 5
        let mut top_contractions: Vec<BridgeRow> = all_rows
            .iter()
            .filter(|r| r.status == "Contracción")
            .cloned()
            .collect();
        top_contractions.sort_by(|a, b| {
            a.delta_mxn
                .partial_cmp(&b.delta_mxn)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        top_contractions.truncate(5);

        // new_relevant: Nuevo, sorted by curr_total DESC, top 5
        let mut new_relevant: Vec<BridgeRow> = all_rows
            .iter()
            .filter(|r| r.status == "Nuevo")
            .cloned()
            .collect();
        new_relevant.sort_by(|a, b| {
            b.current_mxn
                .partial_cmp(&a.current_mxn)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        new_relevant.truncate(5);

        // lost_relevant: Perdido, sorted by prev_total DESC, top 5
        let mut lost_relevant: Vec<BridgeRow> = all_rows
            .iter()
            .filter(|r| r.status == "Perdido")
            .cloned()
            .collect();
        lost_relevant.sort_by(|a, b| {
            b.prev_mxn
                .partial_cmp(&a.prev_mxn)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        lost_relevant.truncate(5);

        // main rows: Nuevo, Expansión, Contracción only (not Perdido), sorted by ABS(delta) DESC, top limit
        let mut rows: Vec<BridgeRow> = all_rows
            .iter()
            .filter(|r| r.status == "Nuevo" || r.status == "Expansión" || r.status == "Contracción")
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            b.delta_mxn
                .abs()
                .partial_cmp(&a.delta_mxn.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        rows.truncate(limit as usize);

        // L11-16 trap 2: a counterparty that drops to zero is "Perdido", not "Contracción" --
        // already enforced by the status classification above (curr_total == 0.0 check
        // comes before the delta-threshold checks), so these sums-by-status don't double
        // up a departing counterparty into both buckets.
        let periodo_anterior_mxn: f64 = all_rows.iter().map(|r| r.prev_mxn).sum();
        let periodo_actual_mxn: f64 = all_rows.iter().map(|r| r.current_mxn).sum();
        let sum_by = |status: &str| -> f64 {
            all_rows
                .iter()
                .filter(|r| r.status == status)
                .map(|r| r.delta_mxn)
                .sum()
        };
        let expansion_mxn = sum_by("Expansión");
        let contraction_mxn = sum_by("Contracción");
        let new_mxn = sum_by("Nuevo");
        let lost_mxn = sum_by("Perdido");
        let estable_mxn = sum_by("Estable");

        bridges.push(BridgeEntry {
            year_current,
            year_prev,
            rows,
            top_expansions,
            top_contractions,
            new_relevant,
            lost_relevant,
            periodo_anterior_mxn,
            periodo_actual_mxn,
            expansion_mxn,
            contraction_mxn,
            new_mxn,
            lost_mxn,
            estable_mxn,
        });
    }

    Ok(PeriodComparisonResponse {
        per_year,
        monthly_matrix,
        top_cp_by_year,
        bridges,
        effective_from_month: from_month,
        effective_to_month: to_month,
    })
}
