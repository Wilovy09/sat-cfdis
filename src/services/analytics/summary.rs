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
        SELECT year, month,
               SUM(CASE WHEN tipo_comprobante IN ('I','T') THEN COALESCE(total_mxn,0) ELSE 0 END)::float8 AS ingreso,
               SUM(CASE WHEN tipo_comprobante = 'E' THEN COALESCE(total_mxn,0) ELSE 0 END)::float8        AS egreso,
               SUM(COALESCE(total_mxn,0))::float8  AS total,
               COUNT(*)                              AS cnt
        FROM pulso.cfdis
        WHERE {rfc_col} = $1
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

    // LTM = last 12 months in range
    by_month.sort_by(|a, b| (a.year, a.month).cmp(&(b.year, b.month)));
    let ltm_slice: Vec<&MonthlyTotal> = by_month.iter().rev().take(12).collect();
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

    // By tipo_comprobante
    let tipo_rows = sqlx::query(&format!(
        r#"
        SELECT tipo_comprobante, SUM(COALESCE(total_mxn,0))::float8 AS total, COUNT(*) AS cnt
        FROM pulso.cfdis
        WHERE {rfc_col} = $1
          AND {dl_filter}
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
        GROUP BY tipo_comprobante
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let by_tipo = tipo_rows
        .iter()
        .map(|r| {
            let tipo: String = r.try_get("tipo_comprobante").unwrap_or_default();
            let total: f64 = r.try_get("total").unwrap_or(0.0);
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            TipoTotal {
                label: tipo_label(&tipo).to_string(),
                tipo_comprobante: tipo,
                total_mxn: total,
                invoice_count: cnt,
            }
        })
        .collect();

    Ok(SummaryResponse {
        total_mxn,
        invoice_count,
        avg_monthly_mxn: avg_monthly,
        ltm_total_mxn,
        ltm_months,
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
        e.total_mxn += m.total_mxn;
        e.invoice_count += m.invoice_count;
        e.ingreso_mxn += m.total_mxn.max(0.0);
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
        "recibidos" => "dl_type = 'recibidos'",
        "ambos" => "1=1",
        _ => "dl_type = 'emitidos'",
    }
}
