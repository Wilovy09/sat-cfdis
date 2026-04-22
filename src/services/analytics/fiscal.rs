use super::summary::{dl_type_filter, parse_ym, rfc_column};
/// Fiscal & Currency: IVA/ISR breakdown, multi-currency analysis.
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct FiscalResponse {
    pub tax_summary: TaxSummary,
    pub by_currency: Vec<CurrencyRow>,
    pub effective_tax_rate: f64,
    pub iva_traslado_total: f64,
    pub iva_retenido_total: f64,
    pub isr_retenido_total: f64,
    pub ieps_total: f64,
    pub iva_neto: f64, // iva_traslado - iva_retenido
    pub by_month: Vec<FiscalMonth>,
}

#[derive(Debug, Serialize)]
pub struct TaxSummary {
    pub base_gravable: f64,
    pub iva_16_base: f64,
    pub iva_16_importe: f64,
    pub iva_8_base: f64,
    pub iva_8_importe: f64,
    pub iva_exento_base: f64,
    pub iva_cero_base: f64,
    pub isr_retenido: f64,
    pub iva_retenido: f64,
    pub ieps_total: f64,
}

#[derive(Debug, Serialize)]
pub struct CurrencyRow {
    pub moneda: String,
    pub invoice_count: i64,
    pub total_original: f64,
    pub total_mxn: f64,
    pub pct_of_total: f64,
    pub avg_tipo_cambio: f64,
}

#[derive(Debug, Serialize)]
pub struct FiscalMonth {
    pub period: String,
    pub subtotal: f64,
    pub iva_traslado: f64,
    pub iva_retenido: f64,
    pub isr_retenido: f64,
    pub total_mxn: f64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<FiscalResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);

    // Tax aggregates by impuesto + tipo_factor + is_retenido
    let tax_rows = sqlx::query(&format!(
        r#"
        SELECT
            t.impuesto,
            t.tipo_factor,
            t.tasa,
            t.is_retenido,
            SUM(COALESCE(t.base, 0))   AS base_sum,
            SUM(COALESCE(t.importe, 0)) AS importe_sum
        FROM pulso.cfdi_taxes t
        JOIN pulso.cfdis c ON c.uuid = t.uuid
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.tipo_comprobante NOT IN ('P','N')
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY t.impuesto, t.tipo_factor, t.tasa, t.is_retenido
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let mut tax = TaxSummary::default();
    let mut iva_traslado_total = 0.0f64;
    let mut iva_retenido_total = 0.0f64;
    let mut isr_retenido_total = 0.0f64;
    let mut ieps_total = 0.0f64;

    for r in &tax_rows {
        let impuesto: String = r.try_get("impuesto").unwrap_or_default();
        let tipo_factor: String = r.try_get("tipo_factor").unwrap_or_default();
        let tasa: f64 = r.try_get("tasa").unwrap_or(0.0);
        let is_ret: i64 = r.try_get("is_retenido").unwrap_or(0);
        let base: f64 = r.try_get("base_sum").unwrap_or(0.0);
        let importe: f64 = r.try_get("importe_sum").unwrap_or(0.0);

        match impuesto.as_str() {
            "002" => {
                // IVA
                if is_ret == 1 {
                    tax.iva_retenido += importe;
                    iva_retenido_total += importe;
                } else {
                    match tipo_factor.as_str() {
                        "Exento" => {
                            tax.iva_exento_base += base;
                        }
                        "Tasa" if (tasa - 0.0).abs() < 0.001 => {
                            tax.iva_cero_base += base;
                        }
                        "Tasa" if tasa > 0.07 && tasa < 0.10 => {
                            tax.iva_8_base += base;
                            tax.iva_8_importe += importe;
                            iva_traslado_total += importe;
                        }
                        _ => {
                            tax.iva_16_base += base;
                            tax.iva_16_importe += importe;
                            iva_traslado_total += importe;
                        }
                    }
                }
            }
            "001" => {
                // ISR
                if is_ret == 1 {
                    tax.isr_retenido += importe;
                    isr_retenido_total += importe;
                }
            }
            "003" => {
                // IEPS
                tax.ieps_total += importe;
                ieps_total += importe;
            }
            _ => {}
        }
    }

    tax.base_gravable = tax.iva_16_base + tax.iva_8_base;
    let effective_tax_rate = if tax.base_gravable > 0.0 {
        (tax.iva_16_importe + tax.iva_8_importe) / tax.base_gravable * 100.0
    } else {
        0.0
    };

    // Currency breakdown
    let currency_rows = sqlx::query(&format!(
        r#"
        SELECT
            COALESCE(moneda, 'MXN')     AS moneda,
            COUNT(*)                    AS cnt,
            SUM(COALESCE(total, 0))     AS total_orig,
            SUM(COALESCE(total_mxn, 0)) AS total_mxn_sum,
            AVG(COALESCE(tipo_cambio, 1.0)) AS avg_tc
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY moneda
        ORDER BY total_mxn_sum DESC
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let grand_total_mxn: f64 = currency_rows
        .iter()
        .map(|r| r.try_get::<f64, _>("total_mxn_sum").unwrap_or(0.0))
        .sum();

    let by_currency: Vec<CurrencyRow> = currency_rows
        .iter()
        .map(|r| {
            let total_mxn: f64 = r.try_get("total_mxn_sum").unwrap_or(0.0);
            CurrencyRow {
                moneda: r.try_get("moneda").unwrap_or_default(),
                invoice_count: r.try_get("cnt").unwrap_or(0),
                total_original: r.try_get("total_orig").unwrap_or(0.0),
                total_mxn,
                pct_of_total: if grand_total_mxn > 0.0 {
                    total_mxn / grand_total_mxn * 100.0
                } else {
                    0.0
                },
                avg_tipo_cambio: r.try_get("avg_tc").unwrap_or(1.0),
            }
        })
        .collect();

    // Monthly fiscal summary
    let month_rows = sqlx::query(
        &format!(r#"
        SELECT
            c.year, c.month,
            SUM(COALESCE(c.subtotal, 0))     AS subtotal,
            SUM(COALESCE(c.total_mxn, 0))    AS total_mxn,
            SUM(CASE WHEN t.impuesto='002' AND t.is_retenido=0 THEN COALESCE(t.importe,0) ELSE 0 END) AS iva_tras,
            SUM(CASE WHEN t.impuesto='002' AND t.is_retenido=1 THEN COALESCE(t.importe,0) ELSE 0 END) AS iva_ret,
            SUM(CASE WHEN t.impuesto='001' AND t.is_retenido=1 THEN COALESCE(t.importe,0) ELSE 0 END) AS isr_ret
        FROM pulso.cfdis c
        LEFT JOIN pulso.cfdi_taxes t ON t.uuid = c.uuid
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.tipo_comprobante NOT IN ('P','N')
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY c.year, c.month
        ORDER BY c.year, c.month
        "#),
    )
    .bind(rfc).bind(from_y).bind(from_m).bind(to_y).bind(to_m)
    .fetch_all(pool)
    .await?;

    let by_month: Vec<FiscalMonth> = month_rows
        .iter()
        .map(|r| {
            let year: i64 = r.try_get("year").unwrap_or(0);
            let month: i64 = r.try_get("month").unwrap_or(0);
            FiscalMonth {
                period: format!("{year}-{month:02}"),
                subtotal: r.try_get("subtotal").unwrap_or(0.0),
                iva_traslado: r.try_get("iva_tras").unwrap_or(0.0),
                iva_retenido: r.try_get("iva_ret").unwrap_or(0.0),
                isr_retenido: r.try_get("isr_ret").unwrap_or(0.0),
                total_mxn: r.try_get("total_mxn").unwrap_or(0.0),
            }
        })
        .collect();

    Ok(FiscalResponse {
        tax_summary: tax,
        by_currency,
        effective_tax_rate,
        iva_traslado_total,
        iva_retenido_total,
        isr_retenido_total,
        ieps_total,
        iva_neto: iva_traslado_total - iva_retenido_total,
        by_month,
    })
}

impl Default for TaxSummary {
    fn default() -> Self {
        Self {
            base_gravable: 0.0,
            iva_16_base: 0.0,
            iva_16_importe: 0.0,
            iva_8_base: 0.0,
            iva_8_importe: 0.0,
            iva_exento_base: 0.0,
            iva_cero_base: 0.0,
            isr_retenido: 0.0,
            iva_retenido: 0.0,
            ieps_total: 0.0,
        }
    }
}
