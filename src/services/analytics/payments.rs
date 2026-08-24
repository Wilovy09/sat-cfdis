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

    // Collection totals — universe capped at the latest "complete" month (as_of_cutoff),
    // now also capped at the last complete calendar month so the current month never
    // counts as "cartera" just because it hasn't finished yet (AUD-009). L2-01: pagado/
    // saldo per invoice comes from the shared base, which also folds in returns ('03',
    // AUD-008) that this query used to miss.
    let direccion = if dl_type == "recibidos" { "recibidos" } else { "emitidos" };
    let totals_row = sqlx::query(&format!(
        r#"
        WITH cutoff AS (
            SELECT COALESCE(
                (SELECT as_of_ym FROM pulso.rfc_as_of_cutoff WHERE owner_rfc = $1 AND direccion = $2),
                999912
            ) AS as_of_ym
        )
        SELECT
            COALESCE(SUM(c.total_mxn), 0)::float8               AS total_invoiced,
            COALESCE(SUM(c.total_mxn - c.saldo_mxn), 0)::float8 AS total_paid,
            COALESCE(SUM(CASE WHEN c.metodo_pago = 'PPD' THEN c.saldo_mxn ELSE 0 END), 0)::float8 AS ppd_outstanding
        FROM pulso.cfdi_cobro_estado c, cutoff
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND (c.year * 100 + c.month) <= cutoff.as_of_ym
        "#
    ))
    .bind(rfc)
    .bind(direccion)
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
          AND NOT is_cancelled
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
          AND NOT is_cancelled
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

    // Outstanding invoices — full universe (no date filter, L2-01: cartera is a balance).
    // days_out now comes from the base's dias_antiguedad (DEC-024 / L2-05: measured from
    // the last complete calendar month, not CURRENT_DATE, so the same query run on two
    // different days gives the same answer).
    let outstanding_rows = sqlx::query(&format!(
        r#"
        SELECT c.uuid,
               inv.{cp_rfc_col}  AS cp_rfc,
               inv.{cp_name_col} AS cp_nombre,
               c.fecha_emision,
               c.total_mxn,
               c.dias_antiguedad AS days_out,
               (c.total_mxn - c.saldo_mxn) AS paid
        FROM pulso.cfdi_cobro_estado c
        JOIN pulso.cfdis inv ON inv.uuid = c.uuid
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.metodo_pago = 'PPD'
          AND c.saldo_mxn > 1.0
        ORDER BY c.saldo_mxn DESC
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

    // Exposure >180d — full universe (no date filter), aged from the base's dias_antiguedad.
    let exposure_row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(SUM(c.saldo_mxn), 0)::float8 AS exposure
        FROM pulso.cfdi_cobro_estado c
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.metodo_pago = 'PPD'
          AND c.dias_antiguedad > 180
        "#
    ))
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let exposure_180d_mxn: f64 = exposure_row.try_get("exposure").unwrap_or(0.0);

    // Average days to pay — PPD invoices only, using the base's ultimo_pago_fecha (already
    // guarded against fecha_pago < fecha_emision data errors).
    let avg_days_row = sqlx::query(&format!(
        r#"
        SELECT AVG((c.ultimo_pago_fecha - c.fecha_emision::date)::float8) AS avg_days
        FROM pulso.cfdi_cobro_estado c
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.metodo_pago = 'PPD'
          AND c.ultimo_pago_fecha IS NOT NULL
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
              AND NOT is_cancelled
              AND (year > $2 OR (year = $2 AND month >= $3))
              AND (year < $4 OR (year = $4 AND month <= $5))
            GROUP BY year, month
        ),
        ppd_paid_by_month AS (
            SELECT c.year, c.month,
                   SUM(c.total_mxn - c.saldo_mxn)::float8 AS ppd_paid
            FROM pulso.cfdi_cobro_estado c
            WHERE c.{owner_col} = $1
              AND c.{dl_filter}
              AND c.metodo_pago = 'PPD'
              AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
              AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
            GROUP BY c.year, c.month
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
