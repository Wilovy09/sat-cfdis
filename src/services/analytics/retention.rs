use super::summary::{dl_type_filter, parse_ym, rfc_column};
/// Retention: cohort analysis — counterparties by their first invoice month.
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct RetentionResponse {
    pub cohorts: Vec<CohortRow>,
    pub overall_retention_pct: f64,
    pub avg_lifespan_months: f64,
    pub churned_last_3m: i64, // counterparties not seen in last 3 months
    pub new_last_3m: i64,
}

#[derive(Debug, Serialize)]
pub struct CohortRow {
    pub cohort_period: String, // YYYY-MM of first invoice
    pub cohort_size: i64,
    pub total_mxn: f64,
    pub months_retained: Vec<MonthRetained>,
}

#[derive(Debug, Serialize)]
pub struct MonthRetained {
    pub offset: i64, // months since first invoice
    pub period: String,
    pub active_count: i64,
    pub retention_pct: f64,
    pub total_mxn: f64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<RetentionResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_col = if dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };

    // First invoice month per counterparty
    let first_rows = sqlx::query(&format!(
        r#"
        SELECT {cp_col} AS cp_rfc,
               MIN(year * 100 + month) AS first_ym,
               MAX(year * 100 + month) AS last_ym
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY {cp_col}
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Activity by counterparty by month
    let activity_rows = sqlx::query(&format!(
        r#"
        SELECT {cp_col} AS cp_rfc,
               year * 100 + month AS ym,
               SUM(COALESCE(total_mxn,0))::float8 AS total
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY {cp_col}, year, month
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Build activity map: cp_rfc → Vec<(ym, total)>
    let mut activity: std::collections::HashMap<String, Vec<(i64, f64)>> = Default::default();
    for r in &activity_rows {
        let cp: String = r.try_get("cp_rfc").unwrap_or_default();
        let ym: i64 = r.try_get("ym").unwrap_or(0);
        let total: f64 = r.try_get("total").unwrap_or(0.0);
        activity.entry(cp).or_default().push((ym, total));
    }

    let last_ym = to_y * 100 + to_m;
    let cur_ym = last_ym;

    // Group counterparties by cohort (first month)
    let mut cohort_map: std::collections::BTreeMap<i64, Vec<String>> = Default::default();
    let mut lifespans = Vec::new();
    let mut churned = 0i64;
    let mut new_last_3m = 0i64;

    for r in &first_rows {
        let cp: String = r.try_get("cp_rfc").unwrap_or_default();
        let first: i64 = r.try_get("first_ym").unwrap_or(0);
        let last: i64 = r.try_get("last_ym").unwrap_or(0);

        cohort_map.entry(first).or_default().push(cp);

        let lifespan = ym_diff(first, last);
        lifespans.push(lifespan);

        // Churned: last activity > 3 months ago
        if ym_diff(last, cur_ym) > 3 {
            churned += 1;
        }

        // New: first invoice in last 3 months
        if ym_diff(first, cur_ym) <= 2 {
            new_last_3m += 1;
        }
    }

    let avg_lifespan = if lifespans.is_empty() {
        0.0
    } else {
        lifespans.iter().sum::<i64>() as f64 / lifespans.len() as f64
    };

    let total_cps = first_rows.len() as i64;
    let retained = total_cps - churned;
    let overall_retention = if total_cps > 0 {
        retained as f64 / total_cps as f64 * 100.0
    } else {
        0.0
    };

    // Build cohort rows (limit to 24 cohorts for display)
    let mut cohorts = Vec::new();
    for (cohort_ym, cps) in cohort_map.iter().rev().take(24) {
        let cohort_period = ym_to_str(*cohort_ym);
        let cohort_size = cps.len() as i64;

        // For each offset month, count active counterparties
        let max_offset = ym_diff(*cohort_ym, last_ym).min(12);
        let mut months_retained = Vec::new();
        let mut total_cohort_mxn = 0.0f64;

        for offset in 0..=max_offset {
            let check_ym = add_months(*cohort_ym, offset);
            let check_period = ym_to_str(check_ym);
            let mut active_count = 0i64;
            let mut month_total = 0.0f64;

            for cp in cps {
                if let Some(acts) = activity.get(cp) {
                    if let Some(&(_, t)) = acts.iter().find(|(ym, _)| *ym == check_ym) {
                        active_count += 1;
                        month_total += t;
                    }
                }
            }

            total_cohort_mxn += month_total;
            months_retained.push(MonthRetained {
                offset,
                period: check_period,
                active_count,
                retention_pct: if cohort_size > 0 {
                    active_count as f64 / cohort_size as f64 * 100.0
                } else {
                    0.0
                },
                total_mxn: month_total,
            });
        }

        cohorts.push(CohortRow {
            cohort_period,
            cohort_size,
            total_mxn: total_cohort_mxn,
            months_retained,
        });
    }

    Ok(RetentionResponse {
        cohorts,
        overall_retention_pct: overall_retention,
        avg_lifespan_months: avg_lifespan,
        churned_last_3m: churned,
        new_last_3m,
    })
}

fn ym_diff(from: i64, to: i64) -> i64 {
    let fy = from / 100;
    let fm = from % 100;
    let ty = to / 100;
    let tm = to % 100;
    (ty - fy) * 12 + (tm - fm)
}

fn add_months(ym: i64, n: i64) -> i64 {
    let y = ym / 100;
    let m = ym % 100;
    let total = y * 12 + m - 1 + n;
    (total / 12) * 100 + (total % 12) + 1
}

fn ym_to_str(ym: i64) -> String {
    format!("{:04}-{:02}", ym / 100, ym % 100)
}
