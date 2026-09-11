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
    pub net_mxn: f64, // ingreso minus egreso, pre-IVA (total_neto_mxn)
    // AUD-006 Step 1: cash-flow measure, con IVA (total_mxn). Not net — the dashboard
    // needs ingreso and egreso as separate series, each a positive magnitude.
    pub ingreso_con_iva_mxn: f64,
    pub egreso_con_iva_mxn: f64,
}

#[derive(Debug, Serialize)]
pub struct YearlyTotal {
    pub year: i64,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub ingreso_mxn: f64,
    pub egreso_mxn: f64,
    pub ingreso_con_iva_mxn: f64,
    pub egreso_con_iva_mxn: f64,
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
               SUM(CASE WHEN tipo_comprobante = 'I' THEN COALESCE(total_neto_mxn_ajustado,0) ELSE 0 END)::float8  AS ingreso,
               SUM(CASE WHEN tipo_comprobante = 'E' THEN -COALESCE(total_neto_mxn_ajustado,0) ELSE 0 END)::float8 AS egreso,
               SUM(CASE WHEN tipo_comprobante = 'I' THEN COALESCE(total_mxn,0) ELSE 0 END)::float8       AS ingreso_iva,
               SUM(CASE WHEN tipo_comprobante = 'E' THEN COALESCE(total_mxn,0) ELSE 0 END)::float8       AS egreso_iva,
               SUM(COALESCE(total_neto_mxn_ajustado,0))::float8 AS total,
               COUNT(*)                                  AS cnt
        FROM pulso.cfdis_ajustado c
        WHERE c.{rfc_col} = $1
          AND c.{dl_filter}
          AND c.tipo_comprobante NOT IN ('P','N','T')
          AND NOT c.is_cancelled
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex
              WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
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
            let ingreso_iva: f64 = r.try_get("ingreso_iva").unwrap_or(0.0);
            let egreso_iva: f64 = r.try_get("egreso_iva").unwrap_or(0.0);
            let total: f64 = r.try_get("total").unwrap_or(0.0);
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            MonthlyTotal {
                year,
                month,
                period: format!("{year}-{month:02}"),
                total_mxn: total,
                invoice_count: cnt,
                net_mxn: ingreso - egreso,
                ingreso_con_iva_mxn: ingreso_iva,
                egreso_con_iva_mxn: egreso_iva,
            }
        })
        .collect();

    // LTM = last 12 *calendar* months ending at to_y/to_m.
    // Do NOT use .take(12) — that grabs the last 12 data points and skips
    // gap months, producing inflated totals when data is sparse.
    by_month.sort_by_key(|a| (a.year, a.month));
    let ltm_total_months = to_y * 12 + to_m; // absolute month index of the end
    let ltm_start_abs = ltm_total_months - 11; // 12-month window inclusive
    let ltm_start_y = (ltm_start_abs - 1) / 12;
    let ltm_start_m = ((ltm_start_abs - 1) % 12) + 1;
    let ltm_slice: Vec<&MonthlyTotal> = by_month
        .iter()
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

#[derive(Debug, Serialize)]
pub struct MonthContributor {
    pub rfc: String,
    pub nombre: String,
    pub month_total_mxn: f64,
    pub other_months_avg_mxn: f64,
    // Signed pesos this counterparty came in above (or, negative, below) its OWN average in
    // the other months of the query window. Deliberately not a percentage: the frontend
    // already knows the RFC's own month total and its own other-months average (same
    // `by_month` data that flagged the month as atypical in the first place), so it divides
    // this figure by that same RFC-level excess to get the share -- one definition of "the
    // month's excess" instead of two (this endpoint summing every counterparty's own excess
    // would count more than the RFC's actual excess, since counterparties can move in
    // opposite directions the same month and net out at the RFC level).
    pub excess_mxn: f64,
}

// L11-02 / DEC-057: RES04's quick-read names the counterparty that explains most of an
// atypical month's excess over its OWN average in the other months of the query window --
// factually, never causally (never "a new client's invoice moved the number", only what
// the numbers show). Called only for months the frontend has already flagged as atypical
// (rare), so this extra round trip isn't worth folding into `get` itself.
pub async fn month_top_contributor(
    pool: &DbPool,
    rfc: &str,
    p: &SummaryParams,
    target_year: i64,
    target_month: i64,
) -> anyhow::Result<Option<MonthContributor>> {
    let dl_filter = dl_type_filter(&p.dl_type);
    let rfc_col = rfc_column(&p.dl_type);
    let (from_y, from_m) = parse_ym(&p.from);
    let (to_y, to_m) = parse_ym(&p.to);

    let cp_col = if p.dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };
    let cp_name_col = if p.dl_type == "recibidos" {
        "nombre_emisor"
    } else {
        "nombre_receptor"
    };
    let cp_key = cp_key_expr(cp_col, cp_name_col);
    let cp_nombre = cp_nombre_expr(cp_col, cp_name_col);

    let row = sqlx::query(&format!(
        r#"
        WITH per_cp AS (
            SELECT
                ({cp_key}) AS cp_rfc,
                {cp_nombre} AS cp_nombre,
                SUM(CASE WHEN year = $6 AND month = $7 THEN COALESCE(total_neto_mxn_ajustado,0)::float8 ELSE 0 END) AS month_total,
                SUM(CASE WHEN NOT (year = $6 AND month = $7) THEN COALESCE(total_neto_mxn_ajustado,0)::float8 ELSE 0 END) AS other_total,
                COUNT(DISTINCT CASE WHEN NOT (year = $6 AND month = $7) THEN year * 100 + month END) AS other_months
            FROM pulso.cfdis_ajustado c
            WHERE {rfc_col} = $1
              AND {dl_filter}
              AND tipo_comprobante NOT IN ('P','N','T')
              AND NOT is_cancelled
              AND (year > $2 OR (year = $2 AND month >= $3))
              AND (year < $4 OR (year = $4 AND month <= $5))
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY ({cp_key})
        ),
        -- L11-02 trap 3: an atypical month can be low, not just high -- excess is signed
        -- (never floored at 0) and ranked by magnitude, so a client that did unusually
        -- LESS than its own norm can explain a below-average month the same way a client
        -- doing unusually more explains an above-average one.
        scored AS (
            SELECT cp_rfc, cp_nombre, month_total,
                   (other_total / NULLIF(other_months, 0))::float8 AS other_avg,
                   (month_total - (other_total / NULLIF(other_months, 0))::float8) AS excess
            FROM per_cp
            WHERE other_months > 0
        )
        SELECT cp_rfc, cp_nombre, month_total, other_avg, excess
        FROM scored
        ORDER BY ABS(excess) DESC
        LIMIT 1
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .bind(target_year)
    .bind(target_month)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };
    let excess = get_f64(&row, "excess");
    if excess == 0.0 {
        return Ok(None);
    }

    Ok(Some(MonthContributor {
        rfc: row.try_get("cp_rfc").unwrap_or_default(),
        nombre: row.try_get("cp_nombre").unwrap_or_default(),
        month_total_mxn: get_f64(&row, "month_total"),
        other_months_avg_mxn: get_f64(&row, "other_avg"),
        excess_mxn: excess,
    }))
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
            ingreso_con_iva_mxn: 0.0,
            egreso_con_iva_mxn: 0.0,
        });
        e.total_mxn += m.net_mxn;
        e.invoice_count += m.invoice_count;
        e.ingreso_mxn += m.net_mxn.max(0.0);
        e.egreso_mxn += (-m.net_mxn).max(0.0);
        e.ingreso_con_iva_mxn += m.ingreso_con_iva_mxn;
        e.egreso_con_iva_mxn += m.egreso_con_iva_mxn;
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
        "ambos" => "dl_type IN ('emitidos', 'recibidos', 'ambos')",
        _ => "dl_type IN ('emitidos', 'ambos')",
    }
}

/// L9-06 / AUD-073: `cashflow.rs` and `payments.rs` had this exact query written twice --
/// same SQL, same bind params, differing only in COALESCE-in-SQL vs unwrap_or-in-Rust for
/// the empty-population NULL case, which behave identically. One query, one name here;
/// `avg_collection_days` (cashflow) and `avg_days_to_pay` (payments) are the response field
/// names each caller already exposes and keep -- this is the single definition both read
/// from, so the four screens that show it (L9-03) can't drift again the way they almost did.
pub async fn avg_dias_a_cobro(pool: &DbPool, rfc: &str, dl_type: &str) -> anyhow::Result<f64> {
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(AVG((c.ultimo_pago_fecha - c.fecha_emision::date)::float8), 0.0) AS avg_days
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
    Ok(get_f64(&row, "avg_days"))
}

// ---------------------------------------------------------------------------
// L5-01: f64 column reads that don't silently swallow a decode failure
// ---------------------------------------------------------------------------
// sqlx's Rust `f64` only decodes a Postgres FLOAT8/double precision value -- a raw
// NUMERIC or REAL/FLOAT4 column read as f64 fails to decode. Every SELECT in this
// codebase is expected to `::float8`-cast such a column, but a `try_get` can still fail
// for other reasons, and a bare `.unwrap_or(0.0)`/`.ok()` used to turn any of those
// failures into a silent zero/null with nothing in the logs. These wrap the fallback
// with a warning naming the column, so a future case like this surfaces instead of
// shipping a wrong number to the frontend unnoticed.
pub fn get_f64_opt(row: &sqlx::postgres::PgRow, col: &str) -> Option<f64> {
    match row.try_get::<f64, _>(col) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!(column = col, error = %e, "failed to decode f64 column, falling back");
            None
        }
    }
}

pub fn get_f64(row: &sqlx::postgres::PgRow, col: &str) -> f64 {
    get_f64_opt(row, col).unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// Generic SAT RFCs (XAXX/XEXX) — counterparty grouping
// ---------------------------------------------------------------------------
// SAT overloads a handful of RFCs to mean "no real RFC available": XAXX010101000
// (Público en General, and other miscellaneous cases) and XEXX010101000 (foreign
// counterparties with no Mexican RFC). Grouping naively by RFC merges every distinct
// real counterparty hiding behind one of these into a single row and mislabels it
// with whichever name an aggregate happens to pick. The helpers below build a
// row-level (non-aggregate) composite key `RFC||NORMALIZED_NAME` so each real
// counterparty gets its own group, while leaving ordinary RFCs untouched.

pub const RFC_PUBLICO_GENERAL: &str = "XAXX010101000";
pub const RFC_EXTRANJERO_GENERICO: &str = "XEXX010101000";
pub const LABEL_PUBLICO_GENERAL: &str = "Público en General";
pub const LABEL_EXTRANJERO_GENERICO: &str = "Cliente extranjero (RFC genérico)";

// L8-07: exact RFC, not prefix -- confirmed by the team, not guessed (IMSS = IMS421231I45,
// Infonavit = INF7205011ZA). A prefix match ('IMS%'/'INF%') excluded a legitimate
// manufacturing supplier of the RFC de control ($4,072) whose RFC happened to start the
// same way. L10-10 initially proposed reintroducing prefix matching for the counterparties
// concentration filter; kept exact-match here instead and flagged the conflict, since it's
// the same false positive H8 already found and fixed once.
pub const RFC_IMSS: &str = "IMS421231I45";
pub const RFC_INFONAVIT: &str = "INF7205011ZA";

/// Row-level (non-aggregate) name normalization: upper, trim, collapse internal
/// whitespace, strip punctuation other than `&` and `-`. Mirrors the normalization
/// used by the Python reference implementation (xml-dashboard-mvp).
pub fn normalized_name_expr(name_col: &str) -> String {
    format!(
        r#"REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE({name_col}, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g')"#
    )
}

/// Row-level counterparty grouping key: the bare RFC for ordinary counterparties
/// (unchanged behavior), but for invoices carrying a generic SAT RFC (XAXX/XEXX)
/// with a non-blank name, a composite `RFC||NORMALIZED_NAME` so each distinct real
/// counterparty hiding behind the generic RFC gets its own group. Generic-RFC rows
/// with a genuinely blank name still collapse to the bare RFC.
///
/// Must be used identically (same `cp_col`/`cp_name_col` qualification) in every
/// query that groups by counterparty, or the same real-world counterparty will
/// produce different keys in different endpoints and silently fail to join.
pub fn cp_key_expr(cp_col: &str, cp_name_col: &str) -> String {
    let norm = normalized_name_expr(cp_name_col);
    format!(
        r#"CASE WHEN {cp_col} IN ('{RFC_PUBLICO_GENERAL}', '{RFC_EXTRANJERO_GENERICO}') AND {norm} <> ''
                THEN {cp_col} || '||' || {norm}
                ELSE {cp_col} END"#
    )
}

/// Companion aggregate display-name expression for a query already `GROUP BY`ed on
/// `cp_key_expr(...)`. Must be selected as an aggregate (it wraps everything in
/// `MAX(...)`) since Postgres can't select a bare column when grouping by a derived
/// expression.
pub fn cp_nombre_expr(cp_col: &str, cp_name_col: &str) -> String {
    let norm = normalized_name_expr(cp_name_col);
    format!(
        r#"CASE
             WHEN MAX({cp_col}) = '{RFC_PUBLICO_GENERAL}' THEN COALESCE(NULLIF(MAX({norm}), ''), '{LABEL_PUBLICO_GENERAL}')
             WHEN MAX({cp_col}) = '{RFC_EXTRANJERO_GENERICO}' THEN COALESCE(NULLIF(MAX({norm}), ''), '{LABEL_EXTRANJERO_GENERICO}')
             ELSE MAX({cp_name_col})
           END"#
    )
}

// ---------------------------------------------------------------------------
// L7-03: shared "last complete calendar month" cutoff
// ---------------------------------------------------------------------------
// Moved here from routes/analytics.rs. hallazgos.rs is mirrored into the lib crate root
// for consistency_invariants.rs (L6C-10 -- it needs to call the real hallazgos::get), but
// `routes` only exists in the binary crate root, so a caller inside hallazgos.rs can't
// reach `crate::routes::analytics::current_month_yyyymm()`. summary.rs is already the
// lib-visible home every analytics module depends on, so it holds the one definition both
// crate roots share -- routes/analytics.rs re-exports it so existing callers
// (recurrence.rs, period_comparison.rs) keep calling it the same way.
pub(crate) fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let mut y = 1970u64;
    let mut rem = days;
    loop {
        let leap = (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400);
        let dy = if leap { 366 } else { 365 };
        if rem < dy {
            break;
        }
        rem -= dy;
        y += 1;
    }
    let leap = (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400);
    let months = [
        31u64,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut mo = 1u64;
    for &dm in &months {
        if rem < dm {
            break;
        }
        rem -= dm;
        mo += 1;
    }
    (y, mo, rem + 1)
}

/// Returns the last fully-closed month (i.e. never the current in-progress month).
pub(crate) fn current_month() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86400;
    let (y, m, _) = days_to_ymd(days);
    // Step back one month to get the last complete month
    let total = y as i64 * 12 + m as i64 - 1 - 1;
    let ly = total / 12;
    let lm = total % 12 + 1;
    format!("{ly:04}-{lm:02}")
}

/// Same cutoff as `current_month()`, as YYYYMM — for callers that compare against
/// `year*100+month` integers instead of formatted strings (e.g. `recurrence.rs`,
/// `period_comparison.rs`, `hallazgos.rs`, `payments.rs`, `cashflow.rs`).
pub(crate) fn current_month_yyyymm() -> i64 {
    let s = current_month();
    let y: i64 = s[0..4].parse().unwrap_or(0);
    let m: i64 = s[5..7].parse().unwrap_or(0);
    y * 100 + m
}

#[cfg(test)]
mod generic_rfc_tests {
    use super::*;

    #[test]
    fn cp_key_expr_builds_case_with_composite_key() {
        let expr = cp_key_expr("rfc_receptor", "nombre_receptor");
        assert!(expr.contains("rfc_receptor IN ('XAXX010101000', 'XEXX010101000')"));
        assert!(expr.contains("rfc_receptor || '||' ||"));
        assert!(expr.contains("ELSE rfc_receptor END"));
    }

    #[test]
    fn cp_nombre_expr_falls_back_to_labels() {
        let expr = cp_nombre_expr("rfc_receptor", "nombre_receptor");
        assert!(expr.contains("'Público en General'"));
        assert!(expr.contains("'Cliente extranjero (RFC genérico)'"));
        assert!(expr.contains("MAX(nombre_receptor)"));
    }

    #[test]
    fn normalized_name_expr_collapses_whitespace_and_strips_punctuation() {
        let expr = normalized_name_expr("nombre_receptor");
        assert!(expr.contains(r"'\s+'"));
        assert!(expr.contains(r"'[^A-Z0-9 &\-]'"));
    }

    #[test]
    fn composite_key_splits_back_apart() {
        assert_eq!(
            "XAXX010101000||ACME CORP".split_once("||"),
            Some(("XAXX010101000", "ACME CORP"))
        );
        assert_eq!("REAL0101010AB1".split_once("||"), None);
    }
}
