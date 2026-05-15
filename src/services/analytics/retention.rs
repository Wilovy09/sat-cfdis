use super::summary::{dl_type_filter, rfc_column};
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize)]
pub struct RetentionResponse {
    pub years: Vec<RetentionYearRow>,
    pub top_lost_by_year: Vec<TopLostCp>,
    pub incomplete_years: Vec<IncompleteYear>,
}

#[derive(Debug, Serialize)]
pub struct RetentionYearRow {
    pub year: i32,
    pub total_cp: i64,
    pub new_cp: Option<i64>,
    pub retained_cp: Option<i64>,
    pub lost_cp: Option<i64>,
    pub total_mxn: f64,
    pub new_mxn: Option<f64>,
    pub retained_mxn: Option<f64>,
    pub lost_mxn: Option<f64>,
    pub pct_new_mxn: Option<f64>,
    pub pct_retained_mxn: Option<f64>,
    pub churn_vs_prev_pct: Option<f64>,
    pub churn_vs_curr_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct TopLostCp {
    pub year_lost: i32,
    pub rfc: String,
    pub nombre: String,
    pub last_active_mxn: f64,
}

#[derive(Debug, Serialize)]
pub struct IncompleteYear {
    pub year: i32,
    pub months: i32,
}

pub async fn get(pool: &DbPool, rfc: &str, dl_type: &str) -> anyhow::Result<RetentionResponse> {
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

    // Q1: distinct months per year (for incomplete detection)
    let q1 = format!(
        "SELECT year, COUNT(DISTINCT month)::bigint AS month_count \
         FROM pulso.cfdis \
         WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N') \
         GROUP BY year ORDER BY year"
    );
    let rows1 = sqlx::query(&q1).bind(rfc).fetch_all(pool).await?;
    let mut months_per_year: HashMap<i32, i32> = HashMap::new();
    for r in &rows1 {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let cnt: i32 = r.try_get::<i64, _>("month_count").unwrap_or(0) as i32;
        months_per_year.insert(year, cnt);
    }

    // Q2: per (year, cp_rfc) totals
    let q2 = format!(
        "SELECT year, {cp_col} AS rfc, MAX({cp_name_col}) AS nombre, \
                SUM(COALESCE(total_mxn,0)::float8)::float8 AS total_mxn \
         FROM pulso.cfdis \
         WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N') \
         GROUP BY year, {cp_col} \
         ORDER BY year"
    );
    let rows2 = sqlx::query(&q2).bind(rfc).fetch_all(pool).await?;

    // Build: year -> HashMap<rfc, (nombre, total_mxn)>
    let mut year_clients: HashMap<i32, HashMap<String, (String, f64)>> = HashMap::new();
    for r in &rows2 {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let cp_rfc: String = r.try_get("rfc").unwrap_or_default();
        let nombre: String = r.try_get("nombre").unwrap_or_default();
        let total_mxn: f64 = r.try_get("total_mxn").unwrap_or(0.0);
        year_clients
            .entry(year)
            .or_default()
            .insert(cp_rfc, (nombre, total_mxn));
    }

    let mut sorted_years: Vec<i32> = year_clients.keys().copied().collect();
    sorted_years.sort();

    // Q3: year totals
    let q3 = format!(
        "SELECT year, SUM(COALESCE(total_mxn,0)::float8)::float8 AS total_mxn \
         FROM pulso.cfdis \
         WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N') \
         GROUP BY year ORDER BY year"
    );
    let rows3 = sqlx::query(&q3).bind(rfc).fetch_all(pool).await?;
    let mut year_totals: HashMap<i32, f64> = HashMap::new();
    for r in &rows3 {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let total: f64 = r.try_get("total_mxn").unwrap_or(0.0);
        year_totals.insert(year, total);
    }

    // Build retention rows and top lost
    let mut years: Vec<RetentionYearRow> = Vec::new();
    let mut top_lost_by_year: Vec<TopLostCp> = Vec::new();

    for (i, &year) in sorted_years.iter().enumerate() {
        let curr_clients = year_clients.get(&year).cloned().unwrap_or_default();
        let total_mxn = year_totals.get(&year).copied().unwrap_or(0.0);
        let total_cp = curr_clients.len() as i64;

        if i == 0 {
            years.push(RetentionYearRow {
                year,
                total_cp,
                new_cp: None,
                retained_cp: None,
                lost_cp: None,
                total_mxn,
                new_mxn: None,
                retained_mxn: None,
                lost_mxn: None,
                pct_new_mxn: None,
                pct_retained_mxn: None,
                churn_vs_prev_pct: None,
                churn_vs_curr_pct: None,
            });
        } else {
            let prev_year = sorted_years[i - 1];
            let prev_clients = year_clients.get(&prev_year).cloned().unwrap_or_default();
            let prev_total_mxn = year_totals.get(&prev_year).copied().unwrap_or(0.0);

            let curr_rfcs: HashSet<&str> = curr_clients.keys().map(|s| s.as_str()).collect();
            let prev_rfcs: HashSet<&str> = prev_clients.keys().map(|s| s.as_str()).collect();

            let new_rfcs: Vec<&str> = curr_rfcs.difference(&prev_rfcs).copied().collect();
            let retained_rfcs: Vec<&str> = curr_rfcs.intersection(&prev_rfcs).copied().collect();
            let lost_rfcs: Vec<&str> = prev_rfcs.difference(&curr_rfcs).copied().collect();

            let new_mxn: f64 = new_rfcs
                .iter()
                .filter_map(|r| curr_clients.get(*r))
                .map(|(_, m)| m)
                .sum();
            let retained_mxn: f64 = retained_rfcs
                .iter()
                .filter_map(|r| curr_clients.get(*r))
                .map(|(_, m)| m)
                .sum();
            let lost_mxn: f64 = lost_rfcs
                .iter()
                .filter_map(|r| prev_clients.get(*r))
                .map(|(_, m)| m)
                .sum();

            let new_cp = new_rfcs.len() as i64;
            let retained_cp = retained_rfcs.len() as i64;
            let lost_cp = lost_rfcs.len() as i64;

            let pct_new_mxn = if total_mxn > 0.0 {
                Some(new_mxn / total_mxn * 100.0)
            } else {
                None
            };
            let pct_retained_mxn = if total_mxn > 0.0 {
                Some(retained_mxn / total_mxn * 100.0)
            } else {
                None
            };
            let churn_vs_prev_pct = if prev_total_mxn > 0.0 {
                Some(lost_mxn / prev_total_mxn * 100.0)
            } else {
                None
            };
            let churn_vs_curr_pct = if total_mxn > 0.0 {
                Some(lost_mxn / total_mxn * 100.0)
            } else {
                None
            };

            // Top 5 lost clients for this year (sorted by last active mxn desc)
            let mut lost_vec: Vec<(String, String, f64)> = lost_rfcs
                .iter()
                .filter_map(|r| {
                    prev_clients
                        .get(*r)
                        .map(|(n, m)| (r.to_string(), n.clone(), *m))
                })
                .collect();
            lost_vec.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
            for (cp_rfc, nombre, last_active_mxn) in lost_vec.into_iter().take(5) {
                top_lost_by_year.push(TopLostCp {
                    year_lost: year,
                    rfc: cp_rfc,
                    nombre,
                    last_active_mxn,
                });
            }

            years.push(RetentionYearRow {
                year,
                total_cp,
                new_cp: Some(new_cp),
                retained_cp: Some(retained_cp),
                lost_cp: Some(lost_cp),
                total_mxn,
                new_mxn: Some(new_mxn),
                retained_mxn: Some(retained_mxn),
                lost_mxn: Some(lost_mxn),
                pct_new_mxn,
                pct_retained_mxn,
                churn_vs_prev_pct,
                churn_vs_curr_pct,
            });
        }
    }

    let mut incomplete_years: Vec<IncompleteYear> = months_per_year
        .iter()
        .filter(|(_, m)| **m < 12)
        .map(|(y, m)| IncompleteYear {
            year: *y,
            months: *m,
        })
        .collect();
    incomplete_years.sort_by_key(|y| y.year);

    Ok(RetentionResponse {
        years,
        top_lost_by_year,
        incomplete_years,
    })
}
