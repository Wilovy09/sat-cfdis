use super::summary::{dl_type_filter, parse_ym, rfc_column};
/// Payments: payment complement analysis — collection (emitidos) and payables (recibidos).
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct PaymentsResponse {
    pub total_invoiced_mxn: f64,
    pub total_paid_mxn: f64,
    pub total_outstanding_mxn: f64,
    pub collection_rate_pct: f64,
    pub avg_days_to_pay: f64,
    pub by_forma_pago: Vec<FormaRow>,
    pub by_metodo_pago: Vec<MetodoRow>,
    pub outstanding_invoices: Vec<OutstandingInvoice>,
    pub payment_timeline: Vec<PaymentMonth>,
}

#[derive(Debug, Serialize)]
pub struct FormaRow {
    pub forma_pago: String,
    pub label: String,
    pub invoice_count: i64,
    pub total_mxn: f64,
    pub pct_of_total: f64,
}

#[derive(Debug, Serialize)]
pub struct MetodoRow {
    pub metodo_pago: String,
    pub label: String,
    pub invoice_count: i64,
    pub total_mxn: f64,
}

#[derive(Debug, Serialize)]
pub struct OutstandingInvoice {
    pub uuid: String,
    pub rfc_cp: String,
    pub nombre_cp: String,
    pub fecha_emision: String,
    pub total_mxn: f64,
    pub paid_mxn: f64,
    pub outstanding_mxn: f64,
    pub days_outstanding: i64,
}

#[derive(Debug, Serialize)]
pub struct PaymentMonth {
    pub period: String,
    pub invoiced_mxn: f64,
    pub paid_mxn: f64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<PaymentsResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_rfc_col = if dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };
    let cp_name_col = if dl_type == "recibidos" {
        "nombre_emisor"
    } else {
        "nombre_receptor"
    };

    // Invoices in range (PUE = paid upfront, PPD = deferred)
    let inv_total_row = sqlx::query(&format!(
        r#"
        SELECT SUM(COALESCE(total_mxn,0)::float8)::float8 AS total
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante = 'I'
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_one(pool)
    .await?;
    let total_invoiced_mxn: f64 = inv_total_row.try_get("total").unwrap_or(0.0);

    // Total paid via payment complements linked to these invoices
    let paid_row = sqlx::query(&format!(
        r#"
        SELECT SUM(COALESCE(pd.imp_pagado, 0)::float8) AS paid
        FROM pulso.cfdi_payment_docs pd
        JOIN pulso.cfdis inv ON inv.uuid = pd.invoice_uuid
        JOIN pulso.cfdis pay ON pay.uuid = pd.payment_uuid
        WHERE inv.{owner_col} = $1
          AND inv.{dl_filter}
          AND inv.tipo_comprobante = 'I'
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
    let total_paid_mxn: f64 = paid_row.try_get("paid").unwrap_or(0.0);
    let total_outstanding = (total_invoiced_mxn - total_paid_mxn).max(0.0);
    let collection_rate = if total_invoiced_mxn > 0.0 {
        total_paid_mxn / total_invoiced_mxn * 100.0
    } else {
        0.0
    };

    // By forma_pago
    let forma_rows = sqlx::query(&format!(
        r#"
        SELECT
            COALESCE(forma_pago, '99')    AS forma,
            COUNT(*)                      AS cnt,
            SUM(COALESCE(total_mxn,0)::float8)::float8    AS total
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante = 'I'
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY forma
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

    let by_forma_pago: Vec<FormaRow> = forma_rows
        .iter()
        .map(|r| {
            let forma: String = r.try_get("forma").unwrap_or_default();
            let total: f64 = r.try_get("total").unwrap_or(0.0);
            FormaRow {
                label: forma_label(&forma).to_string(),
                pct_of_total: if total_invoiced_mxn > 0.0 {
                    total / total_invoiced_mxn * 100.0
                } else {
                    0.0
                },
                forma_pago: forma,
                invoice_count: r.try_get("cnt").unwrap_or(0),
                total_mxn: total,
            }
        })
        .collect();

    // By metodo_pago (PUE vs PPD)
    let metodo_rows = sqlx::query(&format!(
        r#"
        SELECT
            COALESCE(metodo_pago, 'PUE')  AS metodo,
            COUNT(*)                       AS cnt,
            SUM(COALESCE(total_mxn,0)::float8)::float8     AS total
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante = 'I'
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY metodo
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let by_metodo_pago: Vec<MetodoRow> = metodo_rows
        .iter()
        .map(|r| {
            let metodo: String = r.try_get("metodo").unwrap_or_default();
            MetodoRow {
                label: metodo_label(&metodo).to_string(),
                metodo_pago: metodo,
                invoice_count: r.try_get("cnt").unwrap_or(0),
                total_mxn: r.try_get("total").unwrap_or(0.0),
            }
        })
        .collect();

    // Outstanding invoices (PPD with remaining balance)
    let outstanding_rows = sqlx::query(&format!(
        r#"
        SELECT
            inv.uuid,
            inv.{cp_rfc_col}                         AS cp_rfc,
            inv.{cp_name_col}                        AS cp_nombre,
            inv.fecha_emision,
            inv.total_mxn,
            COALESCE(SUM(pd.imp_pagado)::float8, 0)          AS paid,
            inv.total_mxn - COALESCE(SUM(pd.imp_pagado)::float8, 0) AS outstanding
        FROM pulso.cfdis inv
        LEFT JOIN pulso.cfdi_payment_docs pd ON pd.invoice_uuid = inv.uuid
        WHERE inv.{owner_col} = $1
          AND inv.{dl_filter}
          AND inv.tipo_comprobante = 'I'
          AND inv.metodo_pago = 'PPD'
          AND (inv.year > $2 OR (inv.year = $2 AND inv.month >= $3))
          AND (inv.year < $4 OR (inv.year = $4 AND inv.month <= $5))
        GROUP BY inv.uuid
        HAVING (inv.total_mxn - COALESCE(SUM(pd.imp_pagado)::float8, 0)) > 1.0
        ORDER BY (inv.total_mxn - COALESCE(SUM(pd.imp_pagado)::float8, 0)) DESC
        LIMIT 50
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let outstanding_invoices: Vec<OutstandingInvoice> = outstanding_rows
        .iter()
        .map(|r| {
            OutstandingInvoice {
                uuid: r.try_get("uuid").unwrap_or_default(),
                rfc_cp: r.try_get("cp_rfc").unwrap_or_default(),
                nombre_cp: r.try_get("cp_nombre").unwrap_or_default(),
                fecha_emision: r.try_get("fecha_emision").unwrap_or_default(),
                total_mxn: r.try_get("total_mxn").unwrap_or(0.0),
                paid_mxn: r.try_get("paid").unwrap_or(0.0),
                outstanding_mxn: r.try_get("outstanding").unwrap_or(0.0),
                days_outstanding: 0, // TODO: compute from current date
            }
        })
        .collect();

    // Monthly payment timeline
    let timeline_rows = sqlx::query(&format!(
        r#"
        SELECT year, month,
               SUM(COALESCE(total_mxn,0)::float8)::float8 AS invoiced
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante = 'I'
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY year, month
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

    let payment_timeline: Vec<PaymentMonth> = timeline_rows
        .iter()
        .map(|r| {
            let year: i64 = r.try_get("year").unwrap_or(0);
            let month: i64 = r.try_get("month").unwrap_or(0);
            PaymentMonth {
                period: format!("{year}-{month:02}"),
                invoiced_mxn: r.try_get("invoiced").unwrap_or(0.0),
                paid_mxn: 0.0, // TODO: join payment complement dates per month
            }
        })
        .collect();

    Ok(PaymentsResponse {
        total_invoiced_mxn,
        total_paid_mxn,
        total_outstanding_mxn: total_outstanding,
        collection_rate_pct: collection_rate,
        avg_days_to_pay: 0.0, // requires date arithmetic over payment dates
        by_forma_pago,
        by_metodo_pago,
        outstanding_invoices,
        payment_timeline,
    })
}

pub fn forma_label_str(f: &str) -> String {
    forma_label(f).to_string()
}

fn forma_label(f: &str) -> &str {
    match f {
        "01" => "Efectivo",
        "02" => "Cheque nominativo",
        "03" => "Transferencia electrónica",
        "04" => "Tarjeta de crédito",
        "05" => "Monedero electrónico",
        "06" => "Dinero electrónico",
        "08" => "Vales de despensa",
        "12" => "Dación en pago",
        "13" => "Pago por subrogación",
        "14" => "Pago por consignación",
        "15" => "Condonación",
        "17" => "Compensación",
        "23" => "Novación",
        "24" => "Confusión",
        "25" => "Remisión de deuda",
        "26" => "Prescripción o caducidad",
        "27" => "A satisfacción del acreedor",
        "28" => "Tarjeta de débito",
        "29" => "Tarjeta de servicios",
        "30" => "Aplicación de anticipos",
        "31" => "Intermediario pagos",
        "99" => "Por definir",
        _ => f,
    }
}

fn metodo_label(m: &str) -> &str {
    match m {
        "PUE" => "Pago en una sola exhibición",
        "PPD" => "Pago en parcialidades o diferido",
        _ => m,
    }
}
