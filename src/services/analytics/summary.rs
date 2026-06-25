use crate::db::DbPool;
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryParams {
    pub dl_type: String, // emitidos|recibidos|ambos
    pub from: String,    // YYYY-MM
    pub to: String,      // YYYY-MM
}

#[derive(Debug, Serialize)]
pub struct SummaryResponse {
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub avg_monthly_mxn: f64,
    pub ltm_total_mxn: f64,
    pub ltm_months: i64,
    pub ltm_display_allowed: bool,
    pub by_month: Vec<MonthlyTotal>,
    pub by_year: Vec<YearlyTotal>,
    pub by_tipo: Vec<TipoTotal>,
    pub growth_pct_yoy: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct MonthlyTotal {
    pub year: i64,
    pub month: i64,
    pub period: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub net_mxn: f64, // ingreso minus egreso
}

#[derive(Debug, Serialize)]
pub struct YearlyTotal {
    pub year: i64,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub ingreso_mxn: f64,
    pub egreso_mxn: f64,
}

#[derive(Debug, Serialize)]
pub struct TipoTotal {
    pub tipo_comprobante: String,
    pub label: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
}

pub async fn get(pool: &DbPool, rfc: &str, p: &SummaryParams) -> anyhow::Result<SummaryResponse> {
    let (from_y, from_m) = parse_ym(&p.from);
    let (to_y, to_m) = parse_ym(&p.to);

    let dl_filter = dl_type_filter(&p.dl_type);
    let rfc_col = rfc_column(&p.dl_type);

    // Monthly breakdown
    let rows = sqlx::query(
        &format!(r#"
        WITH excluded AS (
            SELECT cfdi_uuid, source_rfc, dl_type
            FROM pulso.normalization_rules
            WHERE owner_rfc = $1 AND action = 'exclude'
        )
        SELECT year, month,
               SUM(CASE WHEN tipo_comprobante = 'I' THEN COALESCE(total_neto_mxn,0) ELSE 0 END)::float8  AS ingreso,
               SUM(CASE WHEN tipo_comprobante = 'E' THEN -COALESCE(total_neto_mxn,0) ELSE 0 END)::float8 AS egreso,
               SUM(COALESCE(total_neto_mxn,0))::float8 AS total,
               COUNT(*)                                  AS cnt
        FROM pulso.cfdis c
        LEFT JOIN excluded exc
            ON (exc.cfdi_uuid IS NOT NULL AND UPPER(exc.cfdi_uuid) = UPPER(c.uuid))
            OR (exc.cfdi_uuid IS NULL AND (
                (exc.dl_type IN ('emitidos','ambos') AND exc.source_rfc = c.rfc_receptor)
                OR (exc.dl_type IN ('recibidos','ambos') AND exc.source_rfc = c.rfc_emisor)
            ))
        WHERE c.{rfc_col} = $1
          AND c.{dl_filter}
          AND c.tipo_comprobante NOT IN ('P','N','T')
          AND NOT c.is_cancelled
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
          AND exc.cfdi_uuid IS NULL
        GROUP BY year, month
        ORDER BY year, month
        "#),
    )
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let mut by_month: Vec<MonthlyTotal> = rows
        .iter()
        .map(|r| {
            let year: i64 = r.try_get("year").unwrap_or(0);
            let month: i64 = r.try_get("month").unwrap_or(0);
            let ingreso: f64 = r.try_get("ingreso").unwrap_or(0.0);
            let egreso: f64 = r.try_get("egreso").unwrap_or(0.0);
            let total: f64 = r.try_get("total").unwrap_or(0.0);
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            MonthlyTotal {
                year,
                month,
                period: format!("{year}-{month:02}"),
                total_mxn: total,
                invoice_count: cnt,
                net_mxn: ingreso - egreso,
            }
        })
        .collect();

    // LTM = last 12 *calendar* months ending at to_y/to_m.
    // Do NOT use .take(12) — that grabs the last 12 data points and skips
    // gap months, producing inflated totals when data is sparse.
    by_month.sort_by(|a, b| (a.year, a.month).cmp(&(b.year, b.month)));
    let ltm_total_months = to_y * 12 + to_m; // absolute month index of the end
    let ltm_start_abs = ltm_total_months - 11; // 12-month window inclusive
    let ltm_start_y = (ltm_start_abs - 1) / 12;
    let ltm_start_m = ((ltm_start_abs - 1) % 12) + 1;
    let ltm_slice: Vec<&MonthlyTotal> = by_month.iter()
        .filter(|m| (m.year, m.month) >= (ltm_start_y, ltm_start_m))
        .collect();
    let ltm_total_mxn: f64 = ltm_slice.iter().map(|m| m.net_mxn).sum();
    let ltm_months = ltm_slice.len() as i64;

    let total_mxn: f64 = by_month.iter().map(|m| m.net_mxn).sum();
    let invoice_count: i64 = by_month.iter().map(|m| m.invoice_count).sum();
    let avg_monthly = if by_month.is_empty() {
        0.0
    } else {
        total_mxn / by_month.len() as f64
    };

    // Yearly
    let by_year = aggregate_yearly(&by_month);

    // YoY growth: last full year vs prior year
    let growth_pct_yoy = yoy_growth(&by_year);

    // LTM display gate: suppress in Jan/Feb of the year following the last complete FY.
    // Those two months carry so little data that LTM vs FY comparisons mislead.
    let ltm_display_allowed = {
        let last_full_year = by_year
            .iter()
            .filter(|y| {
                let n = by_month.iter().filter(|m| m.year == y.year).count();
                n == 12
            })
            .map(|y| y.year)
            .max();
        match last_full_year {
            Some(fy) => !(to_y == fy + 1 && to_m <= 2),
            None => true,
        }
    };

    // Derive by_tipo from the already-fetched by_month rows — no second DB round trip.
    // The monthly query filters OUT P/N/T, so by_tipo reflects only I and E types,
    // which is the relevant breakdown for this analytics surface.
    let by_tipo = {
        let ingreso_total: f64 = by_month.iter().map(|m| m.net_mxn.max(0.0)).sum();
        let egreso_total: f64 = by_month.iter().map(|m| (-m.net_mxn).max(0.0)).sum();
        // invoice_count is undifferentiated in the monthly rows; use proportional split
        // only if both sides are non-zero, otherwise assign all to whichever is non-zero.
        let ingreso_count: i64 = by_month
            .iter()
            .filter(|m| m.net_mxn >= 0.0)
            .map(|m| m.invoice_count)
            .sum();
        let egreso_count: i64 = by_month
            .iter()
            .filter(|m| m.net_mxn < 0.0)
            .map(|m| m.invoice_count)
            .sum();
        let mut tipos = Vec::new();
        if ingreso_total > 0.0 || ingreso_count > 0 {
            tipos.push(TipoTotal {
                tipo_comprobante: "I".to_string(),
                label: tipo_label("I").to_string(),
                total_mxn: ingreso_total,
                invoice_count: ingreso_count,
            });
        }
        if egreso_total > 0.0 || egreso_count > 0 {
            tipos.push(TipoTotal {
                tipo_comprobante: "E".to_string(),
                label: tipo_label("E").to_string(),
                total_mxn: egreso_total,
                invoice_count: egreso_count,
            });
        }
        tipos
    };

    Ok(SummaryResponse {
        total_mxn,
        invoice_count,
        avg_monthly_mxn: avg_monthly,
        ltm_total_mxn,
        ltm_months,
        ltm_display_allowed,
        by_month,
        by_year,
        by_tipo,
        growth_pct_yoy,
    })
}

fn aggregate_yearly(months: &[MonthlyTotal]) -> Vec<YearlyTotal> {
    let mut map: std::collections::BTreeMap<i64, YearlyTotal> = Default::default();
    for m in months {
        let e = map.entry(m.year).or_insert_with(|| YearlyTotal {
            year: m.year,
            total_mxn: 0.0,
            invoice_count: 0,
            ingreso_mxn: 0.0,
            egreso_mxn: 0.0,
        });
        e.total_mxn += m.net_mxn;
        e.invoice_count += m.invoice_count;
        e.ingreso_mxn += m.net_mxn.max(0.0);
        e.egreso_mxn += (-m.net_mxn).max(0.0);
    }
    map.into_values().collect()
}

fn yoy_growth(years: &[YearlyTotal]) -> Option<f64> {
    if years.len() < 2 {
        return None;
    }
    let last = years.last()?;
    let prior = years.get(years.len() - 2)?;
    if prior.total_mxn == 0.0 {
        return None;
    }
    Some((last.total_mxn - prior.total_mxn) / prior.total_mxn * 100.0)
}

fn tipo_label(t: &str) -> &str {
    match t {
        "I" => "Ingreso",
        "E" => "Egreso",
        "P" => "Pago",
        "N" => "Nómina",
        "T" => "Traslado",
        _ => "Otro",
    }
}

pub fn parse_ym(s: &str) -> (i64, i64) {
    let parts: Vec<&str> = s.splitn(2, '-').collect();
    let y = parts.first().and_then(|s| s.parse().ok()).unwrap_or(2020);
    let m = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
    (y, m)
}

pub fn rfc_column(dl_type: &str) -> &'static str {
    match dl_type {
        "recibidos" => "rfc_receptor",
        _ => "rfc_emisor",
    }
}

pub fn dl_type_filter(dl_type: &str) -> &'static str {
    match dl_type {
        "recibidos" => "dl_type IN ('recibidos', 'ambos')",
        "ambos" => "1=1",
        _ => "dl_type IN ('emitidos', 'ambos')",
    }
}
