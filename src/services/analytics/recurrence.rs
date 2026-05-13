use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;
use super::summary::{dl_type_filter, rfc_column};

#[derive(Debug, Serialize)]
pub struct RecurrenceResponse {
    pub window_months: i32,
    pub rec_threshold: i32,
    pub from_period: String,
    pub to_period: String,
    pub by_active_months: Vec<ActiveMonthsBucket>,
    pub scores_by_year: Vec<YearScore>,
    pub top_recurrent: Vec<RecurrentCp>,
}

#[derive(Debug, Serialize)]
pub struct ActiveMonthsBucket {
    pub months_active: i64,
    pub cp_count: i64,
    pub total_mxn: f64,
    pub pct_of_total: f64,
}

#[derive(Debug, Serialize)]
pub struct YearScore {
    pub year: i32,
    pub score: f64,
}

#[derive(Debug, Serialize)]
pub struct RecurrentCp {
    pub rfc: String,
    pub nombre: String,
    pub months_active: i64,
    pub total_mxn: f64,
    pub pct_of_total: f64,
    pub avg_monthly_mxn: f64,
    pub invoice_count: i64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    window_months: i32,
) -> anyhow::Result<RecurrenceResponse> {
    let owner_col = rfc_column(dl_type);
    let dl_filter = dl_type_filter(dl_type);
    let cp_col = if dl_type == "recibidos" { "rfc_emisor" } else { "rfc_receptor" };
    let cp_name_col = if dl_type == "recibidos" { "nombre_emisor" } else { "nombre_receptor" };

    let max_q = format!(
        "SELECT MAX(year * 100 + month)::bigint AS max_period \
         FROM pulso.cfdis \
         WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P', 'N')"
    );
    let max_row = sqlx::query(&max_q).bind(rfc).fetch_one(pool).await?;
    let max_period: i64 = max_row.try_get("max_period").unwrap_or(0);
    if max_period == 0 {
        return Ok(RecurrenceResponse {
            window_months: 0, rec_threshold: 1, from_period: String::new(), to_period: String::new(),
            by_active_months: vec![], scores_by_year: vec![], top_recurrent: vec![],
        });
    }

    let max_year  = (max_period / 100) as i32;
    let max_month = (max_period % 100) as i32;
    let max_month_abs  = max_year * 12 + max_month - 1;
    let from_month_abs = max_month_abs - (window_months - 1);
    let from_year  = from_month_abs / 12;
    let from_month = from_month_abs % 12 + 1;
    let from_yyyymm = (from_year * 100 + from_month) as i64;

    let from_period = format!("{from_year}-{from_month:02}");
    let to_period   = format!("{max_year}-{max_month:02}");

    let actual_q = format!(
        "SELECT COUNT(DISTINCT year * 100 + month)::bigint AS cnt \
         FROM pulso.cfdis \
         WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P', 'N') \
           AND year * 100 + month >= $2 AND year * 100 + month <= $3"
    );
    let actual_row = sqlx::query(&actual_q)
        .bind(rfc).bind(from_yyyymm).bind(max_period).fetch_one(pool).await?;
    let actual_window: i64 = actual_row.try_get("cnt").unwrap_or(window_months as i64);
    let actual_window = actual_window.max(1);

    // Q1: distribution by active months
    let q1 = format!(r#"
        WITH cp_months AS (
            SELECT {cp_col},
                   COUNT(DISTINCT year * 100 + month)::bigint   AS months_active,
                   SUM(COALESCE(total_mxn,0)::float8)::float8   AS total_mxn
            FROM pulso.cfdis
            WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
              AND year * 100 + month >= $2 AND year * 100 + month <= $3
            GROUP BY {cp_col}
        ),
        wt AS (SELECT GREATEST(SUM(total_mxn), 1) AS total FROM cp_months)
        SELECT months_active,
               COUNT(*)::bigint                              AS cp_count,
               SUM(total_mxn)::float8                       AS total_mxn,
               SUM(total_mxn) / (SELECT total FROM wt) * 100 AS pct_of_total
        FROM cp_months GROUP BY months_active ORDER BY months_active DESC
    "#);
    let rows1 = sqlx::query(&q1).bind(rfc).bind(from_yyyymm).bind(max_period).fetch_all(pool).await?;
    let by_active_months: Vec<ActiveMonthsBucket> = rows1.iter().map(|r| ActiveMonthsBucket {
        months_active: r.try_get("months_active").unwrap_or(0),
        cp_count:      r.try_get("cp_count").unwrap_or(0),
        total_mxn:     r.try_get("total_mxn").unwrap_or(0.0),
        pct_of_total:  r.try_get::<f64, _>("pct_of_total").unwrap_or(0.0),
    }).collect();

    // Q2: recurrence score per year (revenue-weighted continuity)
    let q2 = format!(r#"
        WITH cp_window AS (
            SELECT {cp_col},
                   COUNT(DISTINCT year * 100 + month)::float8 AS months_in_window
            FROM pulso.cfdis
            WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
              AND year * 100 + month >= $2 AND year * 100 + month <= $3
            GROUP BY {cp_col}
        ),
        cp_year AS (
            SELECT year, {cp_col},
                   SUM(COALESCE(total_mxn,0)::float8)::float8 AS year_total
            FROM pulso.cfdis
            WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
              AND year * 100 + month >= $2 AND year * 100 + month <= $3
            GROUP BY year, {cp_col}
        ),
        year_totals AS (
            SELECT year, GREATEST(SUM(year_total), 1) AS yt FROM cp_year GROUP BY year
        )
        SELECT cy.year,
               LEAST(100.0,
                 SUM((cw.months_in_window / $4::float8) * (cy.year_total / yt.yt)) * 100
               )::float8 AS score
        FROM cp_year cy
        JOIN cp_window cw ON cw.{cp_col} = cy.{cp_col}
        JOIN year_totals yt ON yt.year = cy.year
        GROUP BY cy.year ORDER BY cy.year
    "#);
    let rows2 = sqlx::query(&q2).bind(rfc).bind(from_yyyymm).bind(max_period).bind(actual_window).fetch_all(pool).await?;
    let scores_by_year: Vec<YearScore> = rows2.iter().map(|r| YearScore {
        year:  r.try_get::<i64, _>("year").unwrap_or(0) as i32,
        score: r.try_get("score").unwrap_or(0.0),
    }).collect();

    // Q3: top recurrent counterparties (>= 75% of window, min 1, capped at 18)
    let min_months: i64 = ((actual_window * 3 / 4).max(1)).min(18);
    let q3 = format!(r#"
        WITH cp_data AS (
            SELECT {cp_col}                                           AS rfc,
                   MAX({cp_name_col})                                  AS nombre,
                   COUNT(DISTINCT year * 100 + month)::bigint          AS months_active,
                   SUM(COALESCE(total_mxn,0)::float8)::float8          AS total_mxn,
                   COUNT(*)::bigint                                    AS invoice_count
            FROM pulso.cfdis
            WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
              AND year * 100 + month >= $2 AND year * 100 + month <= $3
            GROUP BY {cp_col}
            HAVING COUNT(DISTINCT year * 100 + month) >= $4
        ),
        wt AS (
            SELECT GREATEST(SUM(COALESCE(total_mxn,0)::float8), 1) AS total
            FROM pulso.cfdis
            WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
              AND year * 100 + month >= $2 AND year * 100 + month <= $3
        )
        SELECT rfc, nombre, months_active, total_mxn,
               total_mxn / (SELECT total FROM wt) * 100     AS pct_of_total,
               total_mxn / months_active::float8             AS avg_monthly_mxn,
               invoice_count
        FROM cp_data
        ORDER BY months_active DESC, total_mxn DESC
        LIMIT 20
    "#);
    let rows3 = sqlx::query(&q3).bind(rfc).bind(from_yyyymm).bind(max_period).bind(min_months).fetch_all(pool).await?;
    let top_recurrent: Vec<RecurrentCp> = rows3.iter().map(|r| RecurrentCp {
        rfc:             r.try_get("rfc").unwrap_or_default(),
        nombre:          r.try_get("nombre").unwrap_or_default(),
        months_active:   r.try_get("months_active").unwrap_or(0),
        total_mxn:       r.try_get("total_mxn").unwrap_or(0.0),
        pct_of_total:    r.try_get::<f64, _>("pct_of_total").unwrap_or(0.0),
        avg_monthly_mxn: r.try_get("avg_monthly_mxn").unwrap_or(0.0),
        invoice_count:   r.try_get("invoice_count").unwrap_or(0),
    }).collect();

    Ok(RecurrenceResponse {
        window_months: actual_window as i32,
        rec_threshold: min_months as i32,
        from_period, to_period,
        by_active_months, scores_by_year, top_recurrent,
    })
}
