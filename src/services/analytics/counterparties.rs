use super::summary::{
    LABEL_EXTRANJERO_GENERICO, LABEL_PUBLICO_GENERAL, RFC_EXTRANJERO_GENERICO, RFC_PUBLICO_GENERAL,
    cp_key_expr, cp_nombre_expr, current_month_yyyymm, dl_type_filter, get_f64, get_f64_opt,
    normalized_name_expr, parse_ym, rfc_column,
};
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub struct CounterpartiesResponse {
    pub top: Vec<CounterpartyRow>,
    pub total_counterparties: i64,
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
    let cp_key_expr = cp_key_expr(cp_col, cp_name_col);
    let cp_nombre_expr = cp_nombre_expr(cp_col, cp_name_col);

    // L11-33 / DEC-069: no counterparty is excluded by default here -- neither IMSS,
    // Infonavit, nor the generic RFCs, on either clientes or proveedores. Reverts L10-10's
    // resolution (which excluded them by exact RFC match). Two things changed the decision:
    // a prefix-based version of that filter (`isRegulatory` on the frontend, before L10-10)
    // was already misclassifying a real company (`IMS2003263P4`, a manufacturing supplier)
    // as a regulatory authority just for its RFC prefix, and the symmetric risk was worse --
    // Nubarium's single largest supplier (18.85% of spend) has an RFC starting with `SAT`,
    // so any future "for consistency" prefix rule would silently disappear it. Product
    // decision on top of that risk: a real counterparty with real weight not showing up in
    // a Top 10 is worse for an analyst than seeing it there -- the money left the company
    // either way, and the analyst decides what to do with that row. No filter at all is
    // simpler than any filter, and cannot misclassify anyone.
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ({cp_key_expr})                                        AS cp_rfc,
            {cp_nombre_expr}                                       AS cp_nombre,
            SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8                          AS total,
            COUNT(*)                                               AS cnt,
            MIN(fecha_emision)                                     AS first_inv,
            MAX(fecha_emision)                                     AS last_inv,
            COUNT(DISTINCT year * 100 + month)                     AS months_active,
            SUM(SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)) OVER ()::float8 AS grand_total,
            COUNT(*) OVER ()                                       AS cp_count
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        GROUP BY ({cp_key_expr})
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

    let grand_total: f64 = rows.first().map_or(0.0, |r| get_f64(r, "grand_total"));
    let cp_count: i64 = rows
        .first()
        .map_or(0, |r| r.try_get("cp_count").unwrap_or(0));

    let top: Vec<CounterpartyRow> = rows
        .iter()
        .map(|r| {
            let total: f64 = get_f64(r, "total");
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

    let top10_total: f64 = top.iter().take(10).map(|r| r.total_mxn).sum();
    let top10_pct = if grand_total > 0.0 {
        top10_total / grand_total * 100.0
    } else {
        0.0
    };

    Ok(CounterpartiesResponse {
        top,
        total_counterparties: cp_count,
        top10_pct,
    })
}

// ---------------------------------------------------------------------------
// Evolution
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct EvolutionResponse {
    pub rows: Vec<CpEvolutionRow>,
    pub years: Vec<i32>,
}

#[derive(Debug, Serialize)]
pub struct CpEvolutionRow {
    pub rfc: String,
    pub nombre: String,
    pub years: HashMap<String, f64>,
    pub total_acumulado: f64,
    pub cagr_pct: Option<f64>,
    // L11-09 / AUD-106: None when there's no complete year to compare against (a
    // counterparty whose only activity is the in-progress current year) -- not a label
    // guessed by omission. This was the C8-03 bug this item traces back to.
    pub tendencia: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CounterpartySelectorRow {
    pub rfc: String,
    pub nombre: String,
    pub total_mxn: f64,
}

// L11-08 / AUD-105: the CNT07 selector used to be fed from evolution()'s own Top-20-and-
// exclusion-filtered rows, so a normalized counterparty could never be opened in the
// individual view -- exactly the screen that would explain why it's normalized -- and only
// 20 of (say) 306 counterparties were reachable at all. This is the full universe: no
// exclusion filter (a normalized counterparty is a real counterparty, just one whose
// figures don't count toward the aggregate) and no Top-N cap.
pub async fn list_selector(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<Vec<CounterpartySelectorRow>> {
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
    let cp_key_expr = cp_key_expr(cp_col, cp_name_col);
    let cp_nombre_expr = cp_nombre_expr(cp_col, cp_name_col);

    let rows = sqlx::query(&format!(
        r#"
        SELECT ({cp_key_expr}) AS cp_rfc,
               {cp_nombre_expr} AS cp_nombre,
               SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS total
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY ({cp_key_expr})
        ORDER BY total DESC
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| CounterpartySelectorRow {
            rfc: r.try_get("cp_rfc").unwrap_or_default(),
            nombre: r.try_get("cp_nombre").unwrap_or_default(),
            total_mxn: get_f64(r, "total"),
        })
        .collect())
}

pub async fn get_evolution(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<EvolutionResponse> {
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
    let cp_key_expr = cp_key_expr(cp_col, cp_name_col);
    let cp_nombre_expr = cp_nombre_expr(cp_col, cp_name_col);

    // L8-08: two measures per year -- yr_total (full year, what gets painted) and
    // yr_total_capped (months 1..M, where M is the last closed month's month number, fed
    // to CAGR/tendencia only). Computed here with one extra FILTER instead of a second
    // query.
    let current_ym = current_month_yyyymm();
    let cap_month = (current_ym % 100) as i32;
    let rows = sqlx::query(&format!(
        r#"
        SELECT ({cp_key_expr}) AS cp_rfc,
               {cp_nombre_expr} AS cp_nombre,
               year,
               SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS yr_total,
               SUM(COALESCE(total_neto_mxn_ajustado,0)::float8) FILTER (WHERE month <= $6)::float8 AS yr_total_capped
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        GROUP BY ({cp_key_expr}), year
        ORDER BY ({cp_key_expr}), year
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .bind(cap_month as i64)
    .fetch_all(pool)
    .await?;

    // Group by cp_rfc. Per-year value is (full total, capped-to-month total) -- L8-08.
    type YearTotals = HashMap<i32, (f64, f64)>;
    let mut cp_map: HashMap<String, (String, YearTotals)> = HashMap::new();
    let mut all_years: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();

    for row in &rows {
        let cp_rfc: String = row.try_get("cp_rfc").unwrap_or_default();
        let cp_nombre: String = row.try_get("cp_nombre").unwrap_or_default();
        let year: i32 = row.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let yr_total: f64 = get_f64(row, "yr_total");
        let yr_total_capped: f64 = get_f64(row, "yr_total_capped");

        all_years.insert(year);
        let entry = cp_map
            .entry(cp_rfc.clone())
            .or_insert_with(|| (cp_nombre.clone(), HashMap::new()));
        entry.0 = cp_nombre;
        entry.1.insert(year, (yr_total, yr_total_capped));
    }

    let years_sorted: Vec<i32> = all_years.into_iter().collect();

    // L8-08: capping only activates when the series' last year is the current calendar
    // year (otherwise every year in view is already closed, so full-year totals already
    // compare fairly) and M >= 3 (a one- or two-month partial year isn't a usable CAGR
    // base). "Todos los años" get capped to 1..M, not just the current one -- comparing
    // Jan-Ago in every year, not eight months against twelve.
    let current_year = (current_ym / 100) as i32;
    let last_year_is_current = years_sorted.last() == Some(&current_year);
    let cap_active = last_year_is_current && cap_month >= 3;
    // Piso: with fewer than 3 months in the current year, a capped comparison isn't a rate
    // (one month against one month), so the current year is dropped from CAGR/tendencia
    // entirely instead of capped -- it still appears in the painted `years` column.
    let exclude_current_from_cagr = last_year_is_current && cap_month < 3;

    // Build rows
    let mut evolution_rows: Vec<CpEvolutionRow> = cp_map
        .into_iter()
        .map(|(cp_rfc, (cp_nombre, year_map))| {
            let total_acumulado: f64 = year_map.values().map(|&(full, _)| full).sum();

            // C8-03 / AUD-067: two separate vectors, not one shared between them. The full
            // (uncapped) series decides the tendencia label -- a counterparty that billed
            // Sep-Dec 2025 and something in 2026 has real history even though L8-08's
            // capped (Jan-M) series sees only one year of it. The capped series feeds CAGR
            // only, same as L8-08 -- a counterparty can end up with a tendencia label and
            // no CAGR (fewer than 2 years in the capped series but 2+ in the full one);
            // that's correct, not a bug to paper over by computing CAGR on the full series.
            //
            // L11-09 / AUD-106: this series now also excludes the current (partial) year
            // entirely, same principle as CAGR's own capping -- comparing a complete 2023
            // against an eight-month 2026 labeled a real +48.5% CAGR client "Deterioro"
            // because $1,955,000-annualized-equivalent still undercuts a full year's total.
            // Unlike CAGR (which caps the current year to Jan-M so it stays comparable),
            // tendencia just drops it -- there's no "capped label", only complete-year
            // history or no label at all.
            let mut nonzero_full: Vec<(i32, f64)> = year_map
                .iter()
                .filter(|&(&y, _)| y != current_year)
                .map(|(&y, &(full, _))| (y, full))
                .filter(|&(_, v)| v > 0.0)
                .collect();
            nonzero_full.sort_by_key(|(y, _)| *y);

            let mut nonzero_capped: Vec<(i32, f64)> = year_map
                .iter()
                .filter(|&(&y, _)| !(exclude_current_from_cagr && y == current_year))
                .map(|(&y, &(full, capped))| {
                    let v = if cap_active { capped } else { full };
                    (y, v)
                })
                .filter(|&(_, v)| v > 0.0)
                .collect();
            nonzero_capped.sort_by_key(|(y, _)| *y);

            let cagr_pct = if nonzero_capped.len() >= 2 {
                let first_val = nonzero_capped.first().unwrap().1;
                let last_val = nonzero_capped.last().unwrap().1;
                let n_years =
                    (nonzero_capped.last().unwrap().0 - nonzero_capped.first().unwrap().0) as f64;
                if first_val > 0.0 && n_years > 0.0 {
                    Some(((last_val / first_val).powf(1.0 / n_years) - 1.0) * 100.0)
                } else {
                    None
                }
            } else {
                None
            };

            // L8-11: "Nuevo" requires the single year of activity to BE the most recent
            // COMPLETE year in the series -- a counterparty whose only invoice was years
            // ago, with nothing since (up to and including the last complete year), is a
            // dead account, not a new one. An EMPTY series is not "Nuevo" either (is_some_and,
            // not is_none_or -- that was the bug: on an empty capped series it returned true
            // unconditionally, labeling dead accounts with zero Jan-M activity as "Nuevo").
            //
            // L11-09 / AUD-106 trap 2: a counterparty with NO complete-year history at all
            // (only ever active in the still-partial current year) gets no tendencia label,
            // not "Nuevo" by omission -- there's nothing to compare it against yet.
            let last_complete_year = years_sorted.iter().filter(|&&y| y != current_year).max();
            let tendencia = if nonzero_full.is_empty() {
                None
            } else if nonzero_full.len() == 1 {
                let is_most_recent = nonzero_full
                    .first()
                    .is_some_and(|&(y, _)| Some(&y) == last_complete_year);
                Some(if is_most_recent {
                    "Nuevo".to_string()
                } else {
                    "↓ En declive".to_string()
                })
            } else {
                let first_val = nonzero_full.first().unwrap().1;
                let last_val = nonzero_full.last().unwrap().1;
                Some(if last_val > first_val {
                    "↑ Crecimiento".to_string()
                } else if last_val < first_val * 0.5 {
                    "↓ En declive".to_string()
                } else if last_val < first_val * 0.95 {
                    "↓ Deterioro".to_string()
                } else {
                    "Estable".to_string()
                })
            };

            // L8-08: the painted column stays the full-year total -- only CAGR/tendencia
            // above read the capped measure.
            let years_str: HashMap<String, f64> = year_map
                .into_iter()
                .map(|(y, (full, _))| (y.to_string(), full))
                .collect();

            CpEvolutionRow {
                rfc: cp_rfc,
                nombre: cp_nombre,
                years: years_str,
                total_acumulado,
                cagr_pct,
                tendencia,
            }
        })
        .collect();

    evolution_rows.sort_by(|a, b| {
        b.total_acumulado
            .partial_cmp(&a.total_acumulado)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    evolution_rows.truncate(20);

    Ok(EvolutionResponse {
        rows: evolution_rows,
        years: years_sorted,
    })
}

// ---------------------------------------------------------------------------
// LTM Comparison
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct LtmComparisonResponse {
    // Display only -- capped to the top 20 by ltm_mxn. The quick-read fields below are
    // computed over the full universe, before this cap.
    pub rows: Vec<LtmRow>,
    pub ltm_total: f64,
    pub ltm_prev_total: f64,
    // L11-11 / AUD-108: full-universe counterparty movement, not just what survived the
    // top-20 display cap. `new_mxn`/`lost_mxn` let the frontend report the net, not just
    // the counts, in monto.
    pub new_count: i64,
    pub new_mxn: f64,
    pub lost_count: i64,
    pub lost_mxn: f64,
}

#[derive(Debug, Serialize)]
pub struct LtmRow {
    pub rfc: String,
    pub nombre: String,
    pub ltm_mxn: f64,
    pub prev_ltm_mxn: f64,
    pub delta_mxn: f64,
    pub delta_pct: Option<f64>,
    pub share_ltm_pct: f64,
    pub months_active: i64,
    pub invoice_count: i64,
    pub status: String,
}

pub async fn get_ltm_comparison(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    to: &str,
) -> anyhow::Result<LtmComparisonResponse> {
    let (req_to_y, req_to_m): (i64, i64) = parse_ym(to);
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
    let cp_key_expr = cp_key_expr(cp_col, cp_name_col);
    let cp_nombre_expr = cp_nombre_expr(cp_col, cp_name_col);

    // L8-09: no longer clamped to the last month with any data. The KPI above this table
    // and this table's own totals used to read the window two different ways -- one
    // clamped here, one not -- so the same RFC could show one figure in the KPI and a
    // different one in the table for what's supposed to be the same LTM window.
    let (actual_to_y, actual_to_m): (i64, i64) = (req_to_y, req_to_m);

    // Compute LTM window: [to - 11 months ... to]
    let ltm_end_y = actual_to_y;
    let ltm_end_m = actual_to_m;
    let ltm_start_total_months = (actual_to_y * 12 + actual_to_m - 1) - 11;
    let ltm_start_y = ltm_start_total_months / 12;
    let ltm_start_m = ltm_start_total_months % 12 + 1;

    // PrevLTM window: [to_y/to_m - 23 months ... to_y/to_m - 12 months]
    let prev_end_total_months = ltm_start_total_months - 1;
    let prev_end_y = prev_end_total_months / 12;
    let prev_end_m = prev_end_total_months % 12 + 1;
    let prev_start_total_months = prev_end_total_months - 11;
    let prev_start_y = prev_start_total_months / 12;
    let prev_start_m = prev_start_total_months % 12 + 1;

    let ltm_rows = sqlx::query(&format!(
        r#"
        SELECT ({cp_key_expr}) AS cp_rfc,
               {cp_nombre_expr} AS cp_nombre,
               SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS ltm_total,
               COUNT(DISTINCT year * 100 + month) AS months_active,
               COUNT(*) AS invoice_count
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        GROUP BY ({cp_key_expr})
        "#
    ))
    .bind(rfc)
    .bind(ltm_start_y)
    .bind(ltm_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_all(pool)
    .await?;

    let prev_rows = sqlx::query(&format!(
        r#"
        SELECT ({cp_key_expr}) AS cp_rfc,
               {cp_nombre_expr} AS cp_nombre,
               SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS prev_total
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        GROUP BY ({cp_key_expr})
        "#
    ))
    .bind(rfc)
    .bind(prev_start_y)
    .bind(prev_start_m)
    .bind(prev_end_y)
    .bind(prev_end_m)
    .fetch_all(pool)
    .await?;

    // (name, total) for prev LTM counterparties
    let mut prev_map: HashMap<String, (String, f64)> = HashMap::new();
    for row in &prev_rows {
        let cp_rfc: String = row.try_get("cp_rfc").unwrap_or_default();
        let cp_nombre: String = row.try_get("cp_nombre").unwrap_or_default();
        let prev_total: f64 = get_f64(row, "prev_total");
        prev_map.insert(cp_rfc, (cp_nombre, prev_total));
    }

    let ltm_grand_total: f64 = ltm_rows.iter().map(|r| get_f64(r, "ltm_total")).sum();
    let prev_grand_total: f64 = prev_map.values().map(|(_, v)| *v).sum();

    let mut rows: Vec<LtmRow> = ltm_rows
        .iter()
        .map(|r| {
            let cp_rfc: String = r.try_get("cp_rfc").unwrap_or_default();
            let cp_nombre: String = r.try_get("cp_nombre").unwrap_or_default();
            let ltm_mxn: f64 = get_f64(r, "ltm_total");
            let prev_ltm_mxn: f64 = prev_map.get(&cp_rfc).map(|(_, v)| *v).unwrap_or(0.0);
            let delta_mxn = ltm_mxn - prev_ltm_mxn;
            // L11-10 / AUD-107: a percentage against a near-zero base is unreadable, not
            // informative -- a client's LTM previo of $4,000 turned a real $1.58M swing into
            // "+39,599.2%". Below this floor the cell shows "n/m" (frontend renders None as
            // "—") instead of a five-figure percentage; declared here, not implied.
            const MATERIALITY_FLOOR_MXN: f64 = 10_000.0;
            let delta_pct = if prev_ltm_mxn > MATERIALITY_FLOOR_MXN {
                Some(delta_mxn / prev_ltm_mxn * 100.0)
            } else {
                None
            };
            let share_ltm_pct = if ltm_grand_total > 0.0 {
                ltm_mxn / ltm_grand_total * 100.0
            } else {
                0.0
            };
            let months_active: i64 = r.try_get("months_active").unwrap_or(0);
            let invoice_count: i64 = r.try_get("invoice_count").unwrap_or(0);
            let status = if prev_ltm_mxn == 0.0 {
                "Nueva en LTM".to_string()
            } else {
                "Retenida".to_string()
            };

            LtmRow {
                rfc: cp_rfc,
                nombre: cp_nombre,
                ltm_mxn,
                prev_ltm_mxn,
                delta_mxn,
                delta_pct,
                share_ltm_pct,
                months_active,
                invoice_count,
                status,
            }
        })
        .collect();

    // Add "Perdida vs LTM previo" entries for counterparties present in prev LTM but not current
    let ltm_rfcs: std::collections::HashSet<String> = rows.iter().map(|r| r.rfc.clone()).collect();
    for (cp_rfc, (cp_nombre, prev_total)) in &prev_map {
        if !ltm_rfcs.contains(cp_rfc) && *prev_total > 0.0 {
            rows.push(LtmRow {
                rfc: cp_rfc.clone(),
                nombre: cp_nombre.clone(),
                ltm_mxn: 0.0,
                prev_ltm_mxn: *prev_total,
                delta_mxn: -*prev_total,
                delta_pct: Some(-100.0),
                share_ltm_pct: 0.0,
                months_active: 0,
                invoice_count: 0,
                status: "Perdida vs LTM previo".to_string(),
            });
        }
    }

    rows.sort_by(|a, b| {
        b.ltm_mxn
            .partial_cmp(&a.ltm_mxn)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // L11-11 / AUD-108: computed over the FULL universe, before the display truncate below.
    // "Perdida" rows always carry ltm_mxn=0.0, so they always sort to the bottom -- with
    // more than 20 counterparties active in the current LTM, truncate(20) silently dropped
    // every lost counterparty (and any small "new" one ranked below the top 20 by LTM
    // total), which is exactly why the quick-read's own count only ever matched what was
    // left in the truncated `rows` instead of the whole portfolio.
    let new_count = rows.iter().filter(|r| r.status == "Nueva en LTM").count() as i64;
    let new_mxn: f64 = rows
        .iter()
        .filter(|r| r.status == "Nueva en LTM")
        .map(|r| r.ltm_mxn)
        .sum();
    let lost_count = rows
        .iter()
        .filter(|r| r.status == "Perdida vs LTM previo")
        .count() as i64;
    let lost_mxn: f64 = rows
        .iter()
        .filter(|r| r.status == "Perdida vs LTM previo")
        .map(|r| r.prev_ltm_mxn)
        .sum();

    rows.truncate(20);

    Ok(LtmComparisonResponse {
        rows,
        ltm_total: ltm_grand_total,
        ltm_prev_total: prev_grand_total,
        new_count,
        new_mxn,
        lost_count,
        lost_mxn,
    })
}

// ---------------------------------------------------------------------------
// Payments Detail
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct PaymentsDetailResponse {
    pub rows: Vec<CpPaymentRow>,
}

#[derive(Debug, Serialize)]
pub struct CpPaymentRow {
    pub rfc: String,
    pub nombre: String,
    pub facturado_mxn: f64,
    pub cobrado_mxn: f64,
    pub saldo_pendiente_mxn: f64,
    // L11-13 / AUD-109: facturado - cobrado - notas_credito_mxn = saldo_pendiente_mxn,
    // always -- see the comment where this is computed for why it's a residual, not an
    // independent query.
    pub notas_credito_mxn: f64,
    pub pct_cobrado: f64,
    pub facturas_ppd: i64,
    pub facturas_abiertas: i64,
    pub dias_cobro_ppd: f64,
    pub monto_riesgo_180d: f64,
    // L9-04 / DEC-045: true when there's an active exclude rule *on this counterparty*
    // (normalization_rules.source_rfc, not a single cfdi_uuid) -- the cartera itself never
    // applies exclusions (DEC-044: it's a balance), so this only labels rows, it never
    // changes any amount.
    pub normalizada: bool,
}

pub async fn get_payments_detail(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<PaymentsDetailResponse> {
    let (_from_y, _from_m) = parse_ym(from);
    let (_to_y, _to_m) = parse_ym(to);
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
    // Two qualification variants: `all_inv` (bare column names) vs the CTEs that
    // reference it through the `inv` alias. All four CTEs below MUST use the
    // matching variant consistently, or the joins between them (on cp_rfc) will
    // silently stop matching for split (generic-RFC) counterparties.
    let cp_key_bare = cp_key_expr(cp_col, cp_name_col);
    let cp_nombre_bare = cp_nombre_expr(cp_col, cp_name_col);
    let cp_key_inv = cp_key_expr(&format!("inv.{cp_col}"), &format!("inv.{cp_name_col}"));

    // L2-01/L2-03: universe and per-invoice state both come from the shared base
    // (pulso.cfdi_cobro_estado) instead of re-deriving pagado/saldo here. Full universe
    // (PUE + PPD), no date filter — cartera is a balance, not a period flow.
    //
    // L9-04 / DEC-045: two different universes live in this one table, not one. Facturado,
    // Cobrado, facturas_ppd and dias_cobro are ventas/P&L concepts -- L7-06's exclusion
    // filter stays correct for those. Saldo, facturas_abiertas, monto_riesgo and the
    // "normalizada" flag are cartera -- a balance -- and DEC-045 says a balance doesn't
    // drop a counterparty just because sales excluded them: "el cliente sigue debiendo lo
    // que debe." `all_inv` below carries both; each downstream CTE opts into the
    // exclusion filter or not depending on which side of that line it's on.
    let rows = sqlx::query(&format!(
        r#"
        WITH all_inv AS (
            SELECT c.{cp_col} AS {cp_col}, c.{cp_name_col} AS {cp_name_col},
                   b.uuid, b.fecha_emision, b.total_mxn AS inv_total, b.metodo_pago,
                   b.saldo_mxn, b.pagado_mxn, b.dias_antiguedad, b.ultimo_pago_fecha,
                   NOT EXISTS (
                       SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = b.uuid
                   ) AS included
            FROM pulso.cfdi_cobro_estado b
            JOIN pulso.cfdis c ON c.uuid = b.uuid
            WHERE b.{owner_col} = $1 AND b.{dl_filter}
        ),
        inv_base AS (
            -- Ventas: facturado / facturas_ppd, exclusion-filtered (L7-06 unchanged).
            SELECT ({cp_key_bare}) AS cp_rfc,
                   SUM(inv_total) AS facturado,
                   COUNT(DISTINCT CASE WHEN metodo_pago = 'PPD' THEN uuid END) AS facturas_ppd
            FROM all_inv
            WHERE included
            GROUP BY ({cp_key_bare})
        ),
        cobrado_by_cp AS (
            -- L7-04 / DEC-040: real collection, not saldo's derived "paid" (which nets out
            -- credit notes applied to the invoice). LEAST guards the same overpayment edge
            -- case saldo's own clamp-to-zero already protects against on the other side.
            -- Ventas: exclusion-filtered, same as facturado above.
            SELECT ({cp_key_inv}) AS cp_rfc,
                   SUM(LEAST(inv.pagado_mxn, inv.inv_total))::float8 AS cobrado
            FROM all_inv inv
            WHERE inv.included
            GROUP BY ({cp_key_inv})
        ),
        dias_by_cp AS (
            -- Ventas-adjacent (collection speed): exclusion-filtered, unchanged.
            SELECT ({cp_key_inv})                                                       AS cp_rfc,
                   AVG((inv.ultimo_pago_fecha - inv.fecha_emision::date)::float8) AS dias_cobro
            FROM all_inv inv
            WHERE inv.included AND inv.metodo_pago = 'PPD' AND inv.ultimo_pago_fecha IS NOT NULL
            GROUP BY ({cp_key_inv})
        ),
        -- Cartera: saldo, facturas_abiertas, monto_riesgo and "normalizada" -- the FULL
        -- universe (no `included` filter). C8-01 / AUD-065 already fixed saldo to read
        -- straight from saldo_mxn (not facturado - cobrado); this keeps that and drops the
        -- exclusion on top of it. Grouped with cp_key_bare so a 100%-excluded counterparty
        -- still gets a cp_rfc/cp_nombre here even though it has no row in inv_base.
        cartera_by_cp AS (
            SELECT ({cp_key_bare}) AS cp_rfc,
                   {cp_nombre_bare} AS cp_nombre,
                   SUM(saldo_mxn)::float8 AS saldo,
                   COUNT(DISTINCT CASE WHEN metodo_pago = 'PPD' AND saldo_mxn > 1.0 THEN uuid END) AS facturas_abiertas,
                   COALESCE(SUM(CASE
                       WHEN metodo_pago = 'PPD'
                        AND saldo_mxn > 1000.0
                        AND dias_antiguedad > 180
                        AND saldo_mxn / NULLIF(inv_total, 0) >= 0.03
                       THEN saldo_mxn
                   END)::float8, 0) AS monto_riesgo,
                   -- L9-04 / DEC-045: an active rule ON THIS COUNTERPARTY (source_rfc), not
                   -- whether any one of its invoices got excluded -- cfdi_uuid IS NULL
                   -- excludes single-invoice rules, which don't say anything about the
                   -- counterparty as a whole.
                   BOOL_OR(EXISTS (
                       SELECT 1 FROM pulso.normalization_rules nr
                       WHERE nr.owner_rfc = $1
                         AND nr.action = 'exclude'
                         AND nr.cfdi_uuid IS NULL
                         AND nr.source_rfc = {cp_col}
                         AND nr.{dl_filter}
                   )) AS normalizada
            FROM all_inv
            GROUP BY ({cp_key_bare})
        ),
        ranked AS (
            SELECT cb2.cp_rfc,
                   cb2.cp_nombre,
                   COALESCE(ib.facturado, 0)         AS facturado,
                   cb2.saldo,
                   COALESCE(co.cobrado, 0)            AS cobrado,
                   COALESCE(ib.facturas_ppd, 0)       AS facturas_ppd,
                   cb2.facturas_abiertas,
                   COALESCE(dc.dias_cobro, 0)         AS dias_cobro,
                   cb2.monto_riesgo,
                   cb2.normalizada
            -- Cartera (cartera_by_cp) drives the row set: it's the broader universe, so
            -- every cp_rfc that has an inv_base row also has one here, but not vice versa
            -- -- a 100%-excluded counterparty (zero facturado) still needs a row to carry
            -- its real saldo and its "normalizada" tag.
            FROM cartera_by_cp cb2
            LEFT JOIN inv_base ib     ON ib.cp_rfc = cb2.cp_rfc
            LEFT JOIN cobrado_by_cp co ON co.cp_rfc = cb2.cp_rfc
            LEFT JOIN dias_by_cp dc    ON dc.cp_rfc = cb2.cp_rfc
        )
        -- Top 50 by facturado (unchanged ranking for the normal case), UNIONed with any
        -- normalizada row that didn't make that cut -- "no ocultar" (L9-04's own trap #2)
        -- extends to the row limit, not just to a missing filter toggle: a counterparty
        -- excluded from sales entirely would otherwise sort to the bottom of a 400+ row
        -- table and never surface.
        SELECT * FROM (
            (SELECT * FROM ranked ORDER BY facturado DESC LIMIT 50)
            UNION
            (SELECT * FROM ranked WHERE normalizada)
        ) combined
        ORDER BY facturado DESC
        "#
    ))
    .bind(rfc)
    .fetch_all(pool)
    .await?;

    let payment_rows: Vec<CpPaymentRow> = rows
        .iter()
        .map(|r| {
            let facturado: f64 = get_f64(r, "facturado");
            let cobrado: f64 = get_f64(r, "cobrado");
            // C8-01: read, not derived -- facturado - cobrado is no longer the same number
            // now that "cobrado" excludes credit notes (L7-04); saldo_mxn already accounts
            // for them directly, same source get_individual/CNT07 already reads from.
            let saldo_pendiente: f64 = get_f64(r, "saldo");
            let pct_cobrado = if facturado > 0.0 {
                cobrado / facturado * 100.0
            } else {
                0.0
            };
            CpPaymentRow {
                rfc: r.try_get("cp_rfc").unwrap_or_default(),
                nombre: r.try_get("cp_nombre").unwrap_or_default(),
                facturado_mxn: facturado,
                cobrado_mxn: cobrado,
                saldo_pendiente_mxn: saldo_pendiente,
                // L11-13 / AUD-109: closes the subtraction on screen instead of leaving it
                // to look like an error. facturado - cobrado - notas_credito = saldo,
                // exactly, by construction (the residual of the same three numbers already
                // computed above, not a separately-queried figure that could drift from them).
                notas_credito_mxn: facturado - cobrado - saldo_pendiente,
                pct_cobrado,
                facturas_ppd: r.try_get("facturas_ppd").unwrap_or(0),
                facturas_abiertas: r.try_get("facturas_abiertas").unwrap_or(0),
                dias_cobro_ppd: get_f64(r, "dias_cobro"),
                monto_riesgo_180d: get_f64(r, "monto_riesgo"),
                normalizada: r.try_get("normalizada").unwrap_or(false),
            }
        })
        .collect();

    Ok(PaymentsDetailResponse { rows: payment_rows })
}

// ---------------------------------------------------------------------------
// Atypical
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct AtypicalResponse {
    pub rows: Vec<AtypicalRow>,
}

#[derive(Debug, Serialize)]
pub struct AtypicalRow {
    pub rfc: String,
    pub nombre: String,
    pub period: String,
    pub total_mxn: f64,
    pub median_mxn: f64,
    pub multiple: f64,
    pub pct_of_cp_total: f64,
}

pub async fn get_atypical(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<AtypicalResponse> {
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
    let cp_key_expr = cp_key_expr(cp_col, cp_name_col);
    let cp_nombre_expr = cp_nombre_expr(cp_col, cp_name_col);

    let rows = sqlx::query(&format!(
        r#"
        WITH monthly AS (
            SELECT ({cp_key_expr}) AS cp_rfc, {cp_nombre_expr} AS cp_nombre,
                   year, month,
                   year::text || '-' || LPAD(month::text, 2, '0') AS period,
                   SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS mo_total
            FROM pulso.cfdis_ajustado c
            WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N','T')
              AND NOT is_cancelled
              AND (year > $2 OR (year = $2 AND month >= $3))
              AND (year < $4 OR (year = $4 AND month <= $5))
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY ({cp_key_expr}), year, month
        ),
        stats AS (
            SELECT cp_rfc,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY mo_total) AS median_amt,
                   SUM(mo_total) AS cp_total
            FROM monthly
            GROUP BY cp_rfc
            HAVING COUNT(*) >= 3
        )
        SELECT m.cp_rfc, m.cp_nombre, m.period, m.mo_total,
               s.median_amt, s.cp_total,
               m.mo_total / NULLIF(s.median_amt, 0) AS multiple
        FROM monthly m
        JOIN stats s ON s.cp_rfc = m.cp_rfc
        WHERE s.median_amt > 0 AND m.mo_total > s.median_amt * 2.5
          AND m.mo_total / NULLIF(s.cp_total, 0) >= 0.03
        ORDER BY multiple DESC
        LIMIT 20
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let atypical_rows: Vec<AtypicalRow> = rows
        .iter()
        .map(|r| {
            let mo_total: f64 = get_f64(r, "mo_total");
            let cp_total: f64 = get_f64(r, "cp_total");
            let pct_of_cp_total = if cp_total > 0.0 {
                mo_total / cp_total * 100.0
            } else {
                0.0
            };
            AtypicalRow {
                rfc: r.try_get("cp_rfc").unwrap_or_default(),
                nombre: r.try_get("cp_nombre").unwrap_or_default(),
                period: r.try_get("period").unwrap_or_default(),
                total_mxn: mo_total,
                median_mxn: get_f64(r, "median_amt"),
                multiple: get_f64(r, "multiple"),
                pct_of_cp_total,
            }
        })
        .collect();

    Ok(AtypicalResponse {
        rows: atypical_rows,
    })
}

// ---------------------------------------------------------------------------
// Individual
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct CpIndividualResponse {
    pub rfc: String,
    pub nombre: String,
    pub yearly_totals: Vec<CpYearRow>,
    pub by_month_by_year: Vec<CpMonthRow>,
    pub top_concepts: Vec<CpConceptRow>,
    pub pct_of_year: HashMap<String, f64>,
    // Cobranza (full universe, no date filter)
    pub cobrado_mxn: f64,
    pub facturado_ppd_mxn: f64,
    pub saldo_pendiente_mxn: f64,
    pub pct_cobrado: f64,
    pub dias_cobro_ppd: Option<f64>,
    // L11-08 / AUD-105: true when this counterparty has at least one normalization-excluded
    // invoice -- drives the "cliente excluido de los totales" warning pill. This view's own
    // figures above are NOT filtered by that exclusion (see yearly_rows' comment); the
    // aggregate screens the pill points to are.
    pub is_excluded: bool,
}

#[derive(Debug, Serialize)]
pub struct CpYearRow {
    pub year: i32,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub crecimiento_pct: Option<f64>,
    pub cagr_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct CpMonthRow {
    pub year: i32,
    pub month: i32,
    pub period: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
}

#[derive(Debug, Serialize)]
pub struct CpConceptRow {
    pub descripcion: String,
    pub year_amounts: HashMap<String, f64>,
    pub year_counts: HashMap<String, i64>,
    pub total_mxn: f64,
}

/// Per-concept accumulator while aggregating `concept_rows` below: (year -> amount, year ->
/// count, running total_mxn), keyed by concept description.
type ConceptAccumulator = HashMap<String, (HashMap<String, f64>, HashMap<String, i64>, f64)>;

pub async fn get_individual(
    pool: &DbPool,
    owner_rfc: &str,
    cp_rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<CpIndividualResponse> {
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

    // `cp_rfc` may be a composite "GENERIC_RFC||NORMALIZED_NAME" key produced by the
    // top-list/evolution/LTM endpoints (see cp_key_expr). Split it back apart so
    // drill-down filters down to exactly the one real counterparty that key
    // represents, instead of every invoice sharing the generic RFC. Ordinary RFCs
    // never contain "||", so `name_filter` is empty and every filter below becomes
    // a no-op — identical to the pre-fix behavior.
    let (base_rfc, name_filter): (&str, &str) = cp_rfc.split_once("||").unwrap_or((cp_rfc, ""));
    let name_filter_expr = normalized_name_expr(cp_name_col);
    let name_filter_expr_c = normalized_name_expr(&format!("c.{cp_name_col}"));
    let name_filter_expr_inv = normalized_name_expr(&format!("inv.{cp_name_col}"));

    // L11-07 / AUD-104: CAGR needs the SAME capped-current-year series L8-08 already gave
    // evolution() (CNT03) -- yr_total_capped, months 1..cap_month, fed to CAGR only. This
    // used to compare full years unconditionally, 8 points off CNT03 for the same
    // counterparty on the same screen.
    let current_ym = current_month_yyyymm();
    let cap_month = (current_ym % 100) as i32;
    let current_year_i32 = (current_ym / 100) as i32;

    // 1. Yearly totals for this counterparty
    // L11-08 / AUD-105: unlike every OTHER counterparty query in this file, this one does
    // NOT filter cfdi_exclusion. This is the drill-down for a single, specifically-selected
    // counterparty -- if they're 100% normalization-excluded, filtering here would zero out
    // exactly the client the analyst most wants to inspect. `is_excluded` below tells the
    // frontend to show the warning pill; `owner_yearly_rows` (the denominator for
    // pct_of_year) stays filtered, so the aggregate-vs-this-client comparison is still
    // apples-to-apples with what every other screen shows.
    let yearly_rows = sqlx::query(&format!(
        r#"
        SELECT year,
               SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS yr_total,
               SUM(COALESCE(total_neto_mxn_ajustado,0)::float8) FILTER (WHERE month <= $8)::float8 AS yr_total_capped,
               COUNT(*) AS cnt
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND {cp_col} = $2 AND ($3 = '' OR {name_filter_expr} = $3)
          AND (year > $4 OR (year = $4 AND month >= $5))
          AND (year < $6 OR (year = $6 AND month <= $7))
        GROUP BY year
        ORDER BY year
        "#
    ))
    .bind(owner_rfc)
    .bind(base_rfc)
    .bind(name_filter)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .bind(cap_month as i64)
    .fetch_all(pool)
    .await?;

    // Resolve nombre in Rust for generic RFCs (no extra DB round-trip needed); for
    // ordinary RFCs, fall back to the pre-existing lookup query.
    let cp_nombre: String = match base_rfc {
        RFC_PUBLICO_GENERAL if !name_filter.is_empty() => name_filter.to_string(),
        RFC_PUBLICO_GENERAL => LABEL_PUBLICO_GENERAL.to_string(),
        RFC_EXTRANJERO_GENERICO if !name_filter.is_empty() => name_filter.to_string(),
        RFC_EXTRANJERO_GENERICO => LABEL_EXTRANJERO_GENERICO.to_string(),
        _ => {
            let cp_nombre_row = sqlx::query(&format!(
                r#"
                SELECT MAX({cp_name_col}) AS cp_nombre
                FROM pulso.cfdis
                WHERE {owner_col} = $1 AND {cp_col} = $2 AND {dl_filter}
                  AND NOT is_cancelled
                "#
            ))
            .bind(owner_rfc)
            .bind(base_rfc)
            .fetch_optional(pool)
            .await?;

            cp_nombre_row
                .as_ref()
                .and_then(|r| r.try_get("cp_nombre").ok())
                .unwrap_or_else(|| cp_rfc.to_string())
        }
    };

    let mut raw_years: Vec<(i32, f64, i64, f64)> = yearly_rows
        .iter()
        .map(|r| {
            let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
            let total: f64 = get_f64(r, "yr_total");
            let capped: f64 = get_f64(r, "yr_total_capped");
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            (year, total, cnt, capped)
        })
        .collect();
    raw_years.sort_by_key(|(y, _, _, _)| *y);

    // L11-07 / AUD-104: value used for CAGR only -- the current (partial) year
    // contributes its capped (Jan-M) total instead of the full year, the same rule L8-08
    // already applies to CNT03/evolution(). None when the current year has fewer than 3
    // closed months (not a usable CAGR base), matching evolution()'s own floor. `total_mxn`
    // and `crecimiento_pct` (year-over-year growth) are untouched by this -- they stay the
    // real full-year totals; only `cagr_pct` reads the capped value.
    let cagr_value = |year: i32, total: f64, capped: f64| -> Option<f64> {
        if year == current_year_i32 {
            if cap_month < 3 { None } else { Some(capped) }
        } else {
            Some(total)
        }
    };

    let yearly_totals: Vec<CpYearRow> = raw_years
        .iter()
        .enumerate()
        .map(|(i, &(year, total_mxn, invoice_count, capped_mxn))| {
            let crecimiento_pct = if i > 0 {
                let prev_total = raw_years[i - 1].1;
                if prev_total > 0.0 {
                    Some((total_mxn - prev_total) / prev_total * 100.0)
                } else {
                    None
                }
            } else {
                None
            };

            // CAGR from first year to this year, capped-current-year aware (cagr_value).
            let cagr_pct = if i > 0 {
                let first_val = cagr_value(raw_years[0].0, raw_years[0].1, raw_years[0].3);
                let this_val = cagr_value(year, total_mxn, capped_mxn);
                let n_years = i as f64;
                match (first_val, this_val) {
                    (Some(f), Some(t)) if f > 0.0 && t > 0.0 => {
                        Some(((t / f).powf(1.0 / n_years) - 1.0) * 100.0)
                    }
                    _ => None,
                }
            } else {
                None
            };

            CpYearRow {
                year,
                total_mxn,
                invoice_count,
                crecimiento_pct,
                cagr_pct,
            }
        })
        .collect();

    // 2. Monthly breakdown
    let monthly_rows = sqlx::query(&format!(
        r#"
        SELECT year, month,
               year::text || '-' || LPAD(month::text, 2, '0') AS period,
               SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS mo_total,
               COUNT(*) AS cnt
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND {cp_col} = $2 AND ($3 = '' OR {name_filter_expr} = $3)
          AND (year > $4 OR (year = $4 AND month >= $5))
          AND (year < $6 OR (year = $6 AND month <= $7))
        GROUP BY year, month
        ORDER BY year, month
        "#
    ))
    .bind(owner_rfc)
    .bind(base_rfc)
    .bind(name_filter)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let by_month_by_year: Vec<CpMonthRow> = monthly_rows
        .iter()
        .map(|r| CpMonthRow {
            year: r.try_get::<i64, _>("year").unwrap_or(0) as i32,
            month: r.try_get::<i64, _>("month").unwrap_or(0) as i32,
            period: r.try_get("period").unwrap_or_default(),
            total_mxn: get_f64(r, "mo_total"),
            invoice_count: r.try_get("cnt").unwrap_or(0),
        })
        .collect();

    // 3. Top concepts
    let concept_rows = sqlx::query(&format!(
        r#"
        SELECT SUBSTRING(cc.descripcion, 1, 80) AS desc_key,
               c.year,
               SUM(COALESCE(cc.importe, 0)::float8)::float8 AS yr_amount,
               COUNT(*) AS yr_count
        FROM pulso.cfdi_concepts cc
        JOIN pulso.cfdis c ON c.uuid = cc.uuid
        WHERE c.{owner_col} = $1 AND c.{dl_filter} AND c.tipo_comprobante NOT IN ('P','N','T')
          AND NOT c.is_cancelled
          AND c.{cp_col} = $2 AND ($3 = '' OR {name_filter_expr_c} = $3)
          AND (c.year > $4 OR (c.year = $4 AND c.month >= $5))
          AND (c.year < $6 OR (c.year = $6 AND c.month <= $7))
        GROUP BY SUBSTRING(cc.descripcion, 1, 80), c.year
        "#
    ))
    .bind(owner_rfc)
    .bind(base_rfc)
    .bind(name_filter)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Aggregate concepts
    let mut concept_map: ConceptAccumulator = HashMap::new();
    for row in &concept_rows {
        let desc: String = row.try_get("desc_key").unwrap_or_default();
        let year: i32 = row.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let yr_amount: f64 = get_f64(row, "yr_amount");
        let yr_count: i64 = row.try_get("yr_count").unwrap_or(0);
        let year_key = year.to_string();

        let entry = concept_map
            .entry(desc)
            .or_insert_with(|| (HashMap::new(), HashMap::new(), 0.0));
        *entry.0.entry(year_key.clone()).or_insert(0.0) += yr_amount;
        *entry.1.entry(year_key).or_insert(0) += yr_count;
        entry.2 += yr_amount;
    }

    let mut top_concepts: Vec<CpConceptRow> = concept_map
        .into_iter()
        .map(
            |(desc, (year_amounts, year_counts, total_mxn))| CpConceptRow {
                descripcion: desc,
                year_amounts,
                year_counts,
                total_mxn,
            },
        )
        .collect();
    top_concepts.sort_by(|a, b| {
        b.total_mxn
            .partial_cmp(&a.total_mxn)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    top_concepts.truncate(10);

    // 4. pct_of_year: for each year, what % of owner's total does this cp represent
    let owner_yearly_rows = sqlx::query(&format!(
        r#"
        SELECT year,
               SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS yr_total
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        GROUP BY year
        "#
    ))
    .bind(owner_rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let owner_year_map: HashMap<i32, f64> = owner_yearly_rows
        .iter()
        .map(|r| {
            let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
            let total: f64 = get_f64(r, "yr_total");
            (year, total)
        })
        .collect();

    let pct_of_year: HashMap<String, f64> = raw_years
        .iter()
        .map(|&(year, cp_total, _, _)| {
            let owner_total = *owner_year_map.get(&year).unwrap_or(&0.0);
            let pct = if owner_total > 0.0 {
                cp_total / owner_total * 100.0
            } else {
                0.0
            };
            (year.to_string(), pct)
        })
        .collect();

    // Cobranza for this specific counterparty — full universe (no date filter).
    // L2-01/L2-03: shared base instead of re-deriving pagado/saldo.
    // L11-08 / AUD-105: facturado/cobrado no longer exclusion-filtered either (L7-06's
    // filter removed here) -- same reasoning as yearly_rows above, this drill-down shows
    // this one counterparty's real activity regardless of normalization status. saldo
    // (cartera, a balance) was already unfiltered before this item, for the same reason
    // get_payments_detail's saldo is: a 100%-excluded counterparty still owes what it owes.
    let cobranza_row = sqlx::query(&format!(
        r#"
        WITH ppd_detail AS (
            SELECT b.uuid, b.total_mxn AS inv_total, b.pagado_mxn
            FROM pulso.cfdi_cobro_estado b
            JOIN pulso.cfdis inv ON inv.uuid = b.uuid
            WHERE b.{owner_col} = $1 AND b.{dl_filter}
              AND b.{cp_col} = $2 AND ($3 = '' OR {name_filter_expr_inv} = $3)
              AND b.metodo_pago = 'PPD'
        )
        -- L7-04 / DEC-040: "cobrado" is real collection (LEAST(pagado_mxn, inv_total)), not
        -- saldo's derived "paid" -- see cobrado_by_cp in get_payments_detail above for why.
        SELECT
            SUM(inv_total)::float8                      AS facturado,
            SUM(LEAST(pagado_mxn, inv_total))::float8    AS cobrado
        FROM ppd_detail
        "#
    ))
    .bind(owner_rfc)
    .bind(base_rfc)
    .bind(name_filter)
    .fetch_one(pool)
    .await?;

    let facturado_ppd: f64 = get_f64(&cobranza_row, "facturado");
    let cobrado_mxn: f64 = get_f64(&cobranza_row, "cobrado");
    let pct_cobrado = if facturado_ppd > 0.0 {
        cobrado_mxn / facturado_ppd * 100.0
    } else {
        0.0
    };

    // L9-04 / DEC-045: cartera -- no exclusion filter, unlike facturado/cobrado above.
    let saldo_row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(SUM(b.saldo_mxn), 0)::float8 AS saldo
        FROM pulso.cfdi_cobro_estado b
        JOIN pulso.cfdis inv ON inv.uuid = b.uuid
        WHERE b.{owner_col} = $1 AND b.{dl_filter}
          AND b.{cp_col} = $2 AND ($3 = '' OR {name_filter_expr_inv} = $3)
          AND b.metodo_pago = 'PPD'
        "#
    ))
    .bind(owner_rfc)
    .bind(base_rfc)
    .bind(name_filter)
    .fetch_one(pool)
    .await?;
    let saldo: f64 = get_f64(&saldo_row, "saldo");

    let dias_row = sqlx::query(&format!(
        r#"
        SELECT AVG((b.ultimo_pago_fecha - b.fecha_emision::date)::float8) AS dias
        FROM pulso.cfdi_cobro_estado b
        JOIN pulso.cfdis inv ON inv.uuid = b.uuid
        WHERE b.{owner_col} = $1 AND b.{dl_filter}
          AND b.{cp_col} = $2 AND ($3 = '' OR {name_filter_expr_inv} = $3)
          AND b.metodo_pago = 'PPD'
          AND b.ultimo_pago_fecha IS NOT NULL
        "#
    ))
    .bind(owner_rfc)
    .bind(base_rfc)
    .bind(name_filter)
    .fetch_one(pool)
    .await?;

    let dias_cobro_ppd: Option<f64> = get_f64_opt(&dias_row, "dias");

    // L11-08 / AUD-105: tells the frontend to show the "cliente excluido de los totales"
    // warning pill -- true when at least one of this counterparty's invoices is subject to
    // a normalization exclusion rule (the same relation every OTHER screen filters through,
    // checked here instead of filtered out).
    let is_excluded_row = sqlx::query(&format!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM pulso.cfdis_ajustado c
            JOIN pulso.cfdi_exclusion ex ON ex.owner_rfc = $1 AND ex.uuid = c.uuid
            WHERE {owner_col} = $1 AND {cp_col} = $2 AND ($3 = '' OR {name_filter_expr_c} = $3)
        ) AS is_excluded
        "#
    ))
    .bind(owner_rfc)
    .bind(base_rfc)
    .bind(name_filter)
    .fetch_one(pool)
    .await?;
    let is_excluded: bool = is_excluded_row.try_get("is_excluded").unwrap_or(false);

    Ok(CpIndividualResponse {
        rfc: cp_rfc.to_string(),
        nombre: cp_nombre,
        yearly_totals,
        by_month_by_year,
        top_concepts,
        pct_of_year,
        cobrado_mxn,
        facturado_ppd_mxn: facturado_ppd,
        saldo_pendiente_mxn: saldo,
        pct_cobrado,
        dias_cobro_ppd,
        is_excluded,
    })
}
