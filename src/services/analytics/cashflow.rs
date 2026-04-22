use super::summary::{dl_type_filter, parse_ym, rfc_column};
/// Cashflow: timeline of invoiced vs paid amounts, net cash position.
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct CashflowResponse {
    pub timeline: Vec<CashflowMonth>,
    pub cumulative_position: f64,
    pub avg_collection_days: f64,
    pub pue_total_mxn: f64,    // PUE = paid immediately
    pub ppd_invoiced_mxn: f64, // PPD = deferred
    pub ppd_paid_mxn: f64,
    pub ppd_outstanding_mxn: f64,
    pub payment_method_breakdown: Vec<PaymentMethodRow>,
}

#[derive(Debug, Serialize)]
pub struct CashflowMonth {
    pub period: String,
    pub year: i64,
    pub month: i64,
    pub ingreso_invoiced_mxn: f64, // I comprobantes emitidos
    pub egreso_invoiced_mxn: f64,  // E comprobantes
    pub pago_received_mxn: f64,    // P complementos received
    pub net_mxn: f64,
    pub cumulative_mxn: f64,
    pub ppd_outstanding_start: f64,
    pub new_ppd_mxn: f64,
    pub ppd_paid_this_month: f64,
}

#[derive(Debug, Serialize)]
pub struct PaymentMethodRow {
    pub forma_pago: String,
    pub label: String,
    pub total_mxn: f64,
    pub count: i64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<CashflowResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);

    // Monthly invoiced (I and E comprobantes)
    let invoiced_rows = sqlx::query(&format!(
        r#"
        SELECT year, month, tipo_comprobante,
               SUM(COALESCE(total_mxn,0)) AS total
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante IN ('I','E')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY year, month, tipo_comprobante
        ORDER BY year, month
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Monthly payments received/made via complemento P
    let pago_rows = sqlx::query(&format!(
        r#"
        SELECT c.year, c.month,
               SUM(COALESCE(p.monto, 0)) AS total_pagos
        FROM pulso.cfdi_payments p
        JOIN pulso.cfdis c ON c.uuid = p.payment_uuid
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.tipo_comprobante = 'P'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY c.year, c.month
        ORDER BY c.year, c.month
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Build month maps
    type Ym = (i64, i64);
    let mut ingreso_map: std::collections::HashMap<Ym, f64> = Default::default();
    let mut egreso_map: std::collections::HashMap<Ym, f64> = Default::default();
    let mut pago_map: std::collections::HashMap<Ym, f64> = Default::default();
    let mut pue_total = 0.0f64;
    let mut ppd_inv = 0.0f64;

    for r in &invoiced_rows {
        let y: i64 = r.try_get("year").unwrap_or(0);
        let m: i64 = r.try_get("month").unwrap_or(0);
        let tipo: String = r.try_get("tipo_comprobante").unwrap_or_default();
        let total: f64 = r.try_get("total").unwrap_or(0.0);
        match tipo.as_str() {
            "I" => {
                *ingreso_map.entry((y, m)).or_insert(0.0) += total;
            }
            "E" => {
                *egreso_map.entry((y, m)).or_insert(0.0) += total;
            }
            _ => {}
        }
    }

    for r in &pago_rows {
        let y: i64 = r.try_get("year").unwrap_or(0);
        let m: i64 = r.try_get("month").unwrap_or(0);
        let t: f64 = r.try_get("total_pagos").unwrap_or(0.0);
        *pago_map.entry((y, m)).or_insert(0.0) += t;
    }

    // PUE / PPD totals
    let metodo_row = sqlx::query(&format!(
        r#"
        SELECT metodo_pago, SUM(COALESCE(total_mxn,0)) AS total
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante = 'I'
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY metodo_pago
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    for r in &metodo_row {
        let m: String = r.try_get("metodo_pago").unwrap_or_default();
        let t: f64 = r.try_get("total").unwrap_or(0.0);
        match m.as_str() {
            "PUE" => pue_total += t,
            "PPD" => ppd_inv += t,
            _ => {}
        }
    }

    // PPD paid (from payment docs)
    let ppd_paid_row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(SUM(pd.imp_pagado), 0) AS paid
        FROM pulso.cfdi_payment_docs pd
        JOIN pulso.cfdis inv ON inv.uuid = pd.invoice_uuid
        WHERE inv.{owner_col} = $1
          AND inv.{dl_filter}
          AND inv.tipo_comprobante = 'I'
          AND inv.metodo_pago = 'PPD'
          AND (inv.year > $2 OR (inv.year = $2 AND inv.month >= $3))
          AND (inv.year < $4 OR (inv.year = $4 AND inv.month <= $5))
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_one(pool)
    .await?;
    let ppd_paid: f64 = ppd_paid_row.try_get("paid").unwrap_or(0.0);

    // Build timeline
    let mut all_yms: std::collections::BTreeSet<Ym> = Default::default();
    for &ym in ingreso_map.keys() {
        all_yms.insert(ym);
    }
    for &ym in egreso_map.keys() {
        all_yms.insert(ym);
    }
    for &ym in pago_map.keys() {
        all_yms.insert(ym);
    }

    let mut cumulative = 0.0f64;
    let mut timeline = Vec::new();

    for (y, m) in &all_yms {
        let ingreso = ingreso_map.get(&(*y, *m)).copied().unwrap_or(0.0);
        let egreso = egreso_map.get(&(*y, *m)).copied().unwrap_or(0.0);
        let pagos = pago_map.get(&(*y, *m)).copied().unwrap_or(0.0);
        let net = ingreso - egreso + pagos;
        cumulative += net;

        timeline.push(CashflowMonth {
            period: format!("{y}-{m:02}"),
            year: *y,
            month: *m,
            ingreso_invoiced_mxn: ingreso,
            egreso_invoiced_mxn: egreso,
            pago_received_mxn: pagos,
            net_mxn: net,
            cumulative_mxn: cumulative,
            ppd_outstanding_start: 0.0,
            new_ppd_mxn: 0.0,
            ppd_paid_this_month: 0.0,
        });
    }

    // Payment method breakdown from complementos
    let pm_rows = sqlx::query(&format!(
        r#"
        SELECT p.forma_pago, COUNT(*) AS cnt, SUM(COALESCE(p.monto,0)) AS total
        FROM pulso.cfdi_payments p
        JOIN pulso.cfdis c ON c.uuid = p.payment_uuid
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY p.forma_pago
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

    let payment_method_breakdown: Vec<PaymentMethodRow> = pm_rows
        .iter()
        .map(|r| {
            let forma: String = r.try_get("forma_pago").unwrap_or_default();
            PaymentMethodRow {
                label: super::payments::forma_label_str(&forma),
                forma_pago: forma,
                total_mxn: r.try_get("total").unwrap_or(0.0),
                count: r.try_get("cnt").unwrap_or(0),
            }
        })
        .collect();

    Ok(CashflowResponse {
        timeline,
        cumulative_position: cumulative,
        avg_collection_days: 0.0,
        pue_total_mxn: pue_total,
        ppd_invoiced_mxn: ppd_inv,
        ppd_paid_mxn: ppd_paid,
        ppd_outstanding_mxn: (ppd_inv - ppd_paid).max(0.0),
        payment_method_breakdown,
    })
}
