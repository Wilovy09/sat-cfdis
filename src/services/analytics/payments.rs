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
    pub exposure_180d_mxn: f64,
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

    // Collection totals use the FULL universe (no date filter) — "% cobrado del universo"
    // means all PPD invoices ever emitted, not just those in the selected period.
    // The date filter applies only to timeline / forma / metodo breakdowns below.
    // paid_raw = valid payment complements (excluding cancelled ones) + credit notes (tipo E, tipo_relacion=01).
    let totals_row = sqlx::query(&format!(
        r#"
        WITH pue_totals AS (
            SELECT SUM(COALESCE(total_mxn,0)::float8)::float8 AS pue_total
            FROM pulso.cfdis
            WHERE {owner_col} = $1
              AND {dl_filter}
              AND tipo_comprobante = 'I'
              AND COALESCE(metodo_pago,'PUE') != 'PPD'
              AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
        ),
        ppd_per_invoice AS (
            SELECT
                inv.uuid,
                COALESCE(inv.total_mxn, 0)::float8 AS inv_total,
                COALESCE((
                    SELECT SUM(pd.imp_pagado)
                    FROM pulso.cfdi_payment_docs pd
                    JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
                    WHERE pd.invoice_uuid = inv.uuid
                      AND UPPER(COALESCE(comp.estado_sat,'')) NOT LIKE '%CANCEL%'
                )::float8, 0) +
                COALESCE((
                    SELECT SUM(COALESCE(nc.total_mxn, 0)::float8)
                    FROM pulso.cfdi_relacionados cr
                    JOIN pulso.cfdis nc ON nc.uuid = cr.source_uuid
                    WHERE cr.related_uuid = inv.uuid
                      AND cr.tipo_relacion = '01'
                      AND nc.tipo_comprobante = 'E'
                      AND UPPER(COALESCE(nc.estado_sat,'')) NOT LIKE '%CANCEL%'
                ), 0) AS paid_raw
            FROM pulso.cfdis inv
            WHERE inv.{owner_col} = $1
              AND inv.{dl_filter}
              AND inv.tipo_comprobante = 'I'
              AND inv.metodo_pago = 'PPD'
              AND UPPER(COALESCE(inv.estado_sat,'')) NOT LIKE '%CANCEL%'
        ),
        ppd_agg AS (
            SELECT
                SUM(inv_total)::float8                                       AS ppd_total,
                SUM(LEAST(paid_raw, inv_total))::float8                      AS ppd_cobrado,
                SUM(GREATEST(inv_total - paid_raw, 0))::float8               AS ppd_outstanding
            FROM ppd_per_invoice
        )
        SELECT
            (pt.pue_total + pa.ppd_total)::float8                           AS total_invoiced,
            (pt.pue_total + pa.ppd_cobrado)::float8                         AS total_paid,
            pa.ppd_outstanding::float8                                       AS ppd_outstanding
        FROM pue_totals pt, ppd_agg pa
        "#
    ))
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let total_invoiced_mxn: f64 = totals_row.try_get("total_invoiced").unwrap_or(0.0);
    let total_paid_mxn: f64     = totals_row.try_get("total_paid").unwrap_or(0.0);
    let total_outstanding: f64  = totals_row.try_get("ppd_outstanding").unwrap_or(0.0);
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
          AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
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
          AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
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

    // Outstanding invoices — full universe (no date filter)
    let outstanding_rows = sqlx::query(&format!(
        r#"
        SELECT uuid, cp_rfc, cp_nombre, fecha_emision, total_mxn, days_out, paid
        FROM (
            SELECT
                inv.uuid,
                inv.{cp_rfc_col}                                 AS cp_rfc,
                inv.{cp_name_col}                                AS cp_nombre,
                inv.fecha_emision,
                inv.total_mxn,
                (CURRENT_DATE - inv.fecha_emision::date)::bigint AS days_out,
                COALESCE((
                    SELECT SUM(pd.imp_pagado)
                    FROM pulso.cfdi_payment_docs pd
                    JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
                    WHERE pd.invoice_uuid = inv.uuid
                      AND UPPER(COALESCE(comp.estado_sat,'')) NOT LIKE '%CANCEL%'
                )::float8, 0) +
                COALESCE((
                    SELECT SUM(COALESCE(nc.total_mxn, 0)::float8)
                    FROM pulso.cfdi_relacionados cr
                    JOIN pulso.cfdis nc ON nc.uuid = cr.source_uuid
                    WHERE cr.related_uuid = inv.uuid
                      AND cr.tipo_relacion = '01'
                      AND nc.tipo_comprobante = 'E'
                      AND UPPER(COALESCE(nc.estado_sat,'')) NOT LIKE '%CANCEL%'
                ), 0) AS paid
            FROM pulso.cfdis inv
            WHERE inv.{owner_col} = $1
              AND inv.{dl_filter}
              AND inv.tipo_comprobante = 'I'
              AND inv.metodo_pago = 'PPD'
              AND UPPER(COALESCE(inv.estado_sat,'')) NOT LIKE '%CANCEL%'
        ) sub
        WHERE (sub.total_mxn - sub.paid) > 1.0
        ORDER BY (sub.total_mxn - sub.paid) DESC
        LIMIT 50
        "#
    ))
    .bind(rfc)
    .fetch_all(pool)
    .await?;

    let outstanding_invoices: Vec<OutstandingInvoice> = outstanding_rows
        .iter()
        .map(|r| {
            let total: f64 = r.try_get("total_mxn").unwrap_or(0.0);
            let paid: f64 = r.try_get("paid").unwrap_or(0.0);
            OutstandingInvoice {
                uuid: r.try_get("uuid").unwrap_or_default(),
                rfc_cp: r.try_get("cp_rfc").unwrap_or_default(),
                nombre_cp: r.try_get("cp_nombre").unwrap_or_default(),
                fecha_emision: r.try_get("fecha_emision").unwrap_or_default(),
                total_mxn: total,
                paid_mxn: paid,
                outstanding_mxn: (total - paid).max(0.0),
                days_outstanding: r.try_get("days_out").unwrap_or(0),
            }
        })
        .collect();

    // Exposure >180d — full universe (no date filter)
    let exposure_row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(SUM(GREATEST(
            inv.total_mxn -
            COALESCE((
                SELECT SUM(pd.imp_pagado)
                FROM pulso.cfdi_payment_docs pd
                JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
                WHERE pd.invoice_uuid = inv.uuid
                  AND UPPER(COALESCE(comp.estado_sat,'')) NOT LIKE '%CANCEL%'
            )::float8, 0) -
            COALESCE((
                SELECT SUM(COALESCE(nc.total_mxn, 0)::float8)
                FROM pulso.cfdi_relacionados cr
                JOIN pulso.cfdis nc ON nc.uuid = cr.source_uuid
                WHERE cr.related_uuid = inv.uuid
                  AND cr.tipo_relacion = '01'
                  AND nc.tipo_comprobante = 'E'
                  AND UPPER(COALESCE(nc.estado_sat,'')) NOT LIKE '%CANCEL%'
            ), 0),
            0
        )::float8), 0) AS exposure
        FROM pulso.cfdis inv
        WHERE inv.{owner_col} = $1
          AND inv.{dl_filter}
          AND inv.tipo_comprobante = 'I'
          AND inv.metodo_pago = 'PPD'
          AND UPPER(COALESCE(inv.estado_sat,'')) NOT LIKE '%CANCEL%'
          AND (CURRENT_DATE - inv.fecha_emision::date) > 180
        "#
    ))
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let exposure_180d_mxn: f64 = exposure_row.try_get("exposure").unwrap_or(0.0);

    // Average days to pay — full universe (no date filter)
    let avg_days_row = sqlx::query(&format!(
        r#"
        SELECT AVG((cp.fecha_pago::date - inv.fecha_emision::date)::float8) AS avg_days
        FROM pulso.cfdis inv
        JOIN pulso.cfdi_payment_docs pd ON pd.invoice_uuid = inv.uuid
        JOIN pulso.cfdi_payments cp ON cp.payment_uuid = pd.payment_uuid
            AND cp.pago_num = pd.pago_num
        WHERE inv.{owner_col} = $1
          AND inv.{dl_filter}
          AND inv.tipo_comprobante = 'I'
          AND UPPER(COALESCE(inv.estado_sat,'')) NOT LIKE '%CANCEL%'
          AND cp.fecha_pago IS NOT NULL
        "#
    ))
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let avg_days_to_pay: f64 = avg_days_row.try_get("avg_days").unwrap_or(0.0);

    // Monthly timeline: invoiced = PUE+PPD emitted; paid = PUE (immediate) + PPD DR payments
    // grouped by invoice emission month. Avoids multiplying PUE totals via payment doc JOIN.
    let timeline_rows = sqlx::query(&format!(
        r#"
        WITH inv_by_month AS (
            SELECT year, month,
                   SUM(CASE WHEN COALESCE(metodo_pago,'PUE') != 'PPD'
                       THEN COALESCE(total_mxn,0)::float8 ELSE 0 END) AS pue_invoiced,
                   SUM(CASE WHEN metodo_pago = 'PPD'
                       THEN COALESCE(total_mxn,0)::float8 ELSE 0 END) AS ppd_invoiced
            FROM pulso.cfdis
            WHERE {owner_col} = $1
              AND {dl_filter}
              AND tipo_comprobante = 'I'
              AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
              AND (year > $2 OR (year = $2 AND month >= $3))
              AND (year < $4 OR (year = $4 AND month <= $5))
            GROUP BY year, month
        ),
        ppd_paid_by_month AS (
            SELECT inv.year, inv.month,
                   COALESCE(SUM(pd.imp_pagado)::float8, 0) AS ppd_paid
            FROM pulso.cfdis inv
            JOIN pulso.cfdi_payment_docs pd ON pd.invoice_uuid = inv.uuid
            WHERE inv.{owner_col} = $1
              AND inv.{dl_filter}
              AND inv.tipo_comprobante = 'I'
              AND inv.metodo_pago = 'PPD'
              AND UPPER(COALESCE(inv.estado_sat,'')) NOT LIKE '%CANCEL%'
              AND (inv.year > $2 OR (inv.year = $2 AND inv.month >= $3))
              AND (inv.year < $4 OR (inv.year = $4 AND inv.month <= $5))
            GROUP BY inv.year, inv.month
        )
        SELECT bm.year, bm.month,
               (bm.pue_invoiced + bm.ppd_invoiced)::float8 AS invoiced,
               (bm.pue_invoiced + COALESCE(pbm.ppd_paid, 0))::float8 AS paid
        FROM inv_by_month bm
        LEFT JOIN ppd_paid_by_month pbm ON pbm.year = bm.year AND pbm.month = bm.month
        ORDER BY bm.year, bm.month
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
                paid_mxn: r.try_get("paid").unwrap_or(0.0),
            }
        })
        .collect();

    Ok(PaymentsResponse {
        total_invoiced_mxn,
        total_paid_mxn,
        total_outstanding_mxn: total_outstanding,
        collection_rate_pct: collection_rate,
        avg_days_to_pay,
        exposure_180d_mxn,
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
