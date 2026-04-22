use super::summary::{dl_type_filter, parse_ym, rfc_column};
/// Recurrence: how often counterparties invoice each month.
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct RecurrenceResponse {
    pub by_frequency: Vec<FrequencyBucket>,
    pub recurring_pct: f64, // % of total MXN from recurring (>=3 months)
    pub one_time_pct: f64,
    pub top_recurring: Vec<RecurringCounterparty>,
}

#[derive(Debug, Serialize)]
pub struct FrequencyBucket {
    pub months_active: i64,
    pub counterparty_count: i64,
    pub total_mxn: f64,
}

#[derive(Debug, Serialize)]
pub struct RecurringCounterparty {
    pub rfc: String,
    pub nombre: String,
    pub months_active: i64,
    pub total_mxn: f64,
    pub avg_monthly_mxn: f64,
    pub consistency_pct: f64, // months_active / total_months_in_range * 100
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<RecurrenceResponse> {
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

    // Total months in range
    let total_months = month_span(from_y, from_m, to_y, to_m);

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            {cp_col}                                        AS cp_rfc,
            MAX({cp_name_col})                              AS cp_nombre,
            COUNT(DISTINCT year * 100 + month)              AS months_active,
            SUM(COALESCE(total_mxn,0))                     AS total,
            SUM(COALESCE(total_mxn,0)) /
                NULLIF(COUNT(DISTINCT year * 100 + month),0) AS avg_monthly
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY {cp_col}
        ORDER BY months_active DESC, total DESC
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let mut freq_map: std::collections::BTreeMap<i64, (i64, f64)> = Default::default();
    let mut top_recurring = Vec::new();
    let mut recurring_total = 0.0f64;
    let mut one_time_total = 0.0f64;
    let mut grand_total = 0.0f64;

    for r in &rows {
        let months: i64 = r.try_get("months_active").unwrap_or(0);
        let total: f64 = r.try_get("total").unwrap_or(0.0);
        let avg: f64 = r.try_get("avg_monthly").unwrap_or(0.0);
        let cp_rfc: String = r.try_get("cp_rfc").unwrap_or_default();
        let cp_name: String = r.try_get("cp_nombre").unwrap_or_default();

        grand_total += total;
        if months >= 3 {
            recurring_total += total;
        } else {
            one_time_total += total;
        }

        let e = freq_map.entry(months).or_insert((0, 0.0));
        e.0 += 1;
        e.1 += total;

        if months >= 3 && top_recurring.len() < 20 {
            top_recurring.push(RecurringCounterparty {
                rfc: cp_rfc,
                nombre: cp_name,
                months_active: months,
                total_mxn: total,
                avg_monthly_mxn: avg,
                consistency_pct: if total_months > 0 {
                    months as f64 / total_months as f64 * 100.0
                } else {
                    0.0
                },
            });
        }
    }

    let by_frequency = freq_map
        .into_iter()
        .map(|(months, (count, total))| FrequencyBucket {
            months_active: months,
            counterparty_count: count,
            total_mxn: total,
        })
        .collect();

    Ok(RecurrenceResponse {
        by_frequency,
        recurring_pct: if grand_total > 0.0 {
            recurring_total / grand_total * 100.0
        } else {
            0.0
        },
        one_time_pct: if grand_total > 0.0 {
            one_time_total / grand_total * 100.0
        } else {
            0.0
        },
        top_recurring,
    })
}

fn month_span(from_y: i64, from_m: i64, to_y: i64, to_m: i64) -> i64 {
    (to_y - from_y) * 12 + (to_m - from_m) + 1
}
