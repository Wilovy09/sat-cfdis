use super::summary::{dl_type_filter, get_f64, parse_ym, rfc_column};
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
    // C-01: aging por cubetas, 0-30/31-60/61-90/91-180/>180, same universe as
    // total_outstanding_mxn (PPD, capped at the last closed month, no saldo floor) so the
    // five buckets sum to that figure exactly.
    pub aging_buckets: Vec<AgingBucket>,
}

#[derive(Debug, Serialize)]
pub struct AgingBucket {
    pub label: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub pct_of_total: f64,
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

    // Collection totals — universe capped at the last complete calendar month (L7-03 /
    // DEC-039), not the old "densest month" cutoff from pulso.rfc_as_of_cutoff. That view's
    // COALESCE(…, 999912) left 855 of 996 RFC emisores with no cap at all (the current
    // month counted as cartera), and froze another 111 at a stale month -- both wrong in
    // opposite directions. The current month never counts as "cartera" just because it
    // hasn't finished yet (AUD-009). L2-01: pagado/saldo per invoice comes from the shared
    // base, which also folds in returns ('03', AUD-008) that this query used to miss.
    let cutoff_yyyymm = crate::routes::analytics::current_month_yyyymm();
    let totals_row = sqlx::query(&format!(
        r#"
        SELECT
            COALESCE(SUM(c.total_mxn), 0)::float8               AS total_invoiced,
            COALESCE(SUM(c.total_mxn - c.saldo_mxn), 0)::float8 AS total_paid,
            COALESCE(SUM(LEAST(c.pagado_mxn, c.total_mxn)), 0)::float8 AS total_cobrado_real,
            COALESCE(SUM(CASE WHEN c.metodo_pago = 'PPD' THEN c.saldo_mxn ELSE 0 END), 0)::float8 AS ppd_outstanding
        FROM pulso.cfdi_cobro_estado c
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND (c.year * 100 + c.month) <= $2
        "#
    ))
    .bind(rfc)
    .bind(cutoff_yyyymm)
    .fetch_one(pool)
    .await?;
    let total_invoiced_mxn: f64 = get_f64(&totals_row, "total_invoiced");
    // total_paid_mxn keeps the old total_mxn - saldo_mxn arithmetic on purpose (L7-04: it's
    // not painted on any screen today, so it's declared out of scope rather than moved).
    let total_paid_mxn: f64 = get_f64(&totals_row, "total_paid");
    let total_outstanding: f64 = get_f64(&totals_row, "ppd_outstanding");
    // L7-04 / DEC-040: "% Cobrado/Pagado del universo" measures real collection, not
    // saldo's derived "paid" (which nets out credit notes applied to the invoice -- a
    // credit note isn't money that came in). LEAST guards the same overpayment edge case
    // saldo's own clamp-to-zero already protects against on the other side.
    let total_cobrado_real: f64 = get_f64(&totals_row, "total_cobrado_real");
    let collection_rate = if total_invoiced_mxn > 0.0 {
        total_cobrado_real / total_invoiced_mxn * 100.0
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
            let total: f64 = get_f64(r, "total");
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
                total_mxn: get_f64(r, "total"),
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
            let total: f64 = get_f64(r, "total_mxn");
            let paid: f64 = get_f64(r, "paid");
            OutstandingInvoice {
                uuid: r.try_get("uuid").unwrap_or_default(),
                rfc_cp: r.try_get("cp_rfc").unwrap_or_default(),
                nombre_cp: r.try_get("cp_nombre").unwrap_or_default(),
                fecha_emision: r.try_get("fecha_emision").unwrap_or_default(),
                total_mxn: total,
                paid_mxn: paid,
                outstanding_mxn: (total - paid).max(0.0),
                // L7-05: dias_antiguedad is a 4-byte Postgres integer -- decoding it
                // straight as i64 silently failed and always fell back to 0.
                days_outstanding: r.try_get::<i32, _>("days_out").unwrap_or(0) as i64,
            }
        })
        .collect();

    // Exposure >180d, aged from the base's dias_antiguedad. C-01: now capped at the same
    // last complete calendar month as total_outstanding_mxn above -- without that shared
    // cutoff the two queries don't share a universe, and the first RFC with a PPD invoice
    // issued after the cutoff would make exposure exceed the saldo pendiente card (which
    // *is* capped), an impossible result since exposure is supposed to be a subset of it.
    let exposure_row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(SUM(c.saldo_mxn), 0)::float8 AS exposure
        FROM pulso.cfdi_cobro_estado c
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.metodo_pago = 'PPD'
          AND c.dias_antiguedad > 180
          AND (c.year * 100 + c.month) <= $2
        "#
    ))
    .bind(rfc)
    .bind(cutoff_yyyymm)
    .fetch_one(pool)
    .await?;
    let exposure_180d_mxn: f64 = get_f64(&exposure_row, "exposure");

    // C-01: aging por cubetas -- exact same universe as total_outstanding_mxn (PPD, capped
    // at the same cutoff, NO saldo floor -- filtering saldo > 1 here would drop the
    // buckets' sum a few pesos short of the "Saldo pendiente" card, the kind of gap that
    // makes a verification never close). Antiguedad comes straight from the base's
    // dias_antiguedad (anchored to the last closed month's last day), not recomputed
    // against today -- two runs on different days must give the same buckets. Bucketed by
    // upper edge (<=30, <=60, ...); a negative antiguedad (invoice issued after the cutoff)
    // falls into the first bucket.
    let aging_row = sqlx::query(&format!(
        r#"
        -- C8-02 / AUD-066: the five COUNTs get their own `AND saldo_mxn > 0` -- otherwise
        -- they count every invoice in the universe, including ones already fully paid.
        -- The five SUMs stay unfiltered on purpose (same as C-01 already established: a
        -- saldo floor there loses $61.10 and breaks the tie-out with "Saldo pendiente").
        -- `> 0`, not `> 1`: the count must count exactly what the amount sums, and a
        -- fifty-cent saldo still sums into the amount.
        SELECT
            COALESCE(SUM(c.saldo_mxn) FILTER (WHERE c.dias_antiguedad <= 30), 0)::float8                                    AS b1_mxn,
            COUNT(*) FILTER (WHERE c.dias_antiguedad <= 30 AND c.saldo_mxn > 0)                                              AS b1_cnt,
            COALESCE(SUM(c.saldo_mxn) FILTER (WHERE c.dias_antiguedad > 30 AND c.dias_antiguedad <= 60), 0)::float8          AS b2_mxn,
            COUNT(*) FILTER (WHERE c.dias_antiguedad > 30 AND c.dias_antiguedad <= 60 AND c.saldo_mxn > 0)                   AS b2_cnt,
            COALESCE(SUM(c.saldo_mxn) FILTER (WHERE c.dias_antiguedad > 60 AND c.dias_antiguedad <= 90), 0)::float8          AS b3_mxn,
            COUNT(*) FILTER (WHERE c.dias_antiguedad > 60 AND c.dias_antiguedad <= 90 AND c.saldo_mxn > 0)                   AS b3_cnt,
            COALESCE(SUM(c.saldo_mxn) FILTER (WHERE c.dias_antiguedad > 90 AND c.dias_antiguedad <= 180), 0)::float8         AS b4_mxn,
            COUNT(*) FILTER (WHERE c.dias_antiguedad > 90 AND c.dias_antiguedad <= 180 AND c.saldo_mxn > 0)                  AS b4_cnt,
            COALESCE(SUM(c.saldo_mxn) FILTER (WHERE c.dias_antiguedad > 180), 0)::float8                                     AS b5_mxn,
            COUNT(*) FILTER (WHERE c.dias_antiguedad > 180 AND c.saldo_mxn > 0)                                              AS b5_cnt
        FROM pulso.cfdi_cobro_estado c
        WHERE c.{owner_col} = $1
          AND c.{dl_filter}
          AND c.metodo_pago = 'PPD'
          AND (c.year * 100 + c.month) <= $2
        "#
    ))
    .bind(rfc)
    .bind(cutoff_yyyymm)
    .fetch_one(pool)
    .await?;
    let bucket_defs: [(&str, &str, &str); 5] = [
        ("0-30 días", "b1_mxn", "b1_cnt"),
        ("31-60 días", "b2_mxn", "b2_cnt"),
        ("61-90 días", "b3_mxn", "b3_cnt"),
        ("91-180 días", "b4_mxn", "b4_cnt"),
        ("> 180 días", "b5_mxn", "b5_cnt"),
    ];
    let aging_total: f64 = bucket_defs
        .iter()
        .map(|(_, mxn_col, _)| get_f64(&aging_row, mxn_col))
        .sum();
    let aging_buckets: Vec<AgingBucket> = bucket_defs
        .iter()
        .map(|(label, mxn_col, cnt_col)| {
            let total_mxn = get_f64(&aging_row, mxn_col);
            AgingBucket {
                label: label.to_string(),
                total_mxn,
                invoice_count: aging_row.try_get(*cnt_col).unwrap_or(0),
                pct_of_total: if aging_total > 0.0 {
                    total_mxn / aging_total * 100.0
                } else {
                    0.0
                },
            }
        })
        .collect();

    // Average days to pay — PPD invoices only, using the base's ultimo_pago_fecha (already
    // guarded against fecha_pago < fecha_emision data errors). L9-06: shared with
    // cashflow.rs's identical query now, see avg_dias_a_cobro's own comment for why.
    let avg_days_to_pay: f64 = super::summary::avg_dias_a_cobro(pool, rfc, dl_type).await?;

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
                invoiced_mxn: get_f64(r, "invoiced"),
                paid_mxn: get_f64(r, "paid"),
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
        aging_buckets,
    })
}

// P-04 / DEC-042: only caller was cashflow.rs's payment_method_breakdown, paused (not
// deleted) since nothing on screen reads it -- kept, not removed, for when that's restored.
#[allow(dead_code)]
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
