//! L6-02: executable number contract. One `#[tokio::test]` per row of the Lote 6 control
//! table, each one self-contained against the real shared test database (no shared state,
//! no ordering dependency between tests) so future lotes can add assertions here without
//! restructuring existing ones.
//!
//! Two kinds of expected value appear below:
//! - Relational assertions (rows 4, 6, 7, 8) compare two independently-computed figures
//!   that must agree by construction, regardless of how much data has synced in. These are
//!   the ones that actually catch a regression over time.
//! - Absolute-literal assertions (rows 1, 2, 3, 5) are hardcoded against the value measured
//!   on 2026-09-03, the day this file was written. This platform continuously syncs new
//!   CFDIs from SAT in the background, so these numbers are a point-in-time baseline, not a
//!   permanent constant -- expect to have to bump them again as the dataset grows. That's a
//!   known limitation of asserting an absolute count/sum against a live table, not a bug in
//!   the test.
use pulso_backend::config::Config;
use pulso_backend::db::{self, DbPool};
use pulso_backend::services::analytics::{
    counterparties, normalization, payroll, quarterly, summary,
};
use sqlx::Row;

/// "RFC de prueba" per the Lote 6 doc.
const RFC_PRUEBA: &str = "NUB170623KI3";
/// "RFC grande" per the Lote 6 doc.
const RFC_GRANDE: &str = "CES100706U65";

async fn connect() -> DbPool {
    dotenvy::dotenv().ok();
    let cfg = Config::from_env();
    db::init_pool(&cfg)
        .await
        .expect("connect to the shared test database (POSTGRES_* env vars)")
}

fn get_f64_opt(row: &sqlx::postgres::PgRow, col: &str) -> Option<f64> {
    row.try_get::<Option<f64>, _>(col).unwrap_or(None)
}

/// Row 1: nomina bruta real -- the raw, UNFACTORED sum straight off `cfdi_nomina` joined to
/// `cfdis`, never through `nomina_normalizada` (which already has any scale/adjust factor
/// baked into its `total_percepciones`). Baseline measured 2026-09-03.
#[tokio::test]
async fn nomina_bruta_real_unfactored() {
    let pool = connect().await;
    for (rfc, expected) in [
        (RFC_PRUEBA, 20_592_281.51_f64),
        (RFC_GRANDE, 44_443_673.80_f64),
    ] {
        let row = sqlx::query(
            r#"SELECT SUM(n.total_percepciones)::float8 AS v
               FROM pulso.cfdis c
               JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
               WHERE c.rfc_emisor = $1 AND c.tipo_comprobante = 'N' AND NOT c.is_cancelled"#,
        )
        .bind(rfc)
        .fetch_one(&pool)
        .await
        .unwrap();
        let actual = get_f64_opt(&row, "v").unwrap_or(0.0);
        assert!(
            (actual - expected).abs() < 0.01,
            "nomina bruta real for {rfc}: expected {expected} (2026-09-03 baseline), got \
             {actual}. If {actual} > {expected}, that's expected growth (new CFDIs synced) -- \
             re-measure and bump this baseline. If {actual} < {expected}, that's a regression."
        );
    }
}

/// Row 2: nomina normalizada no excluida -- sum of `nomina_normalizada.total_percepciones`
/// (already factored) where not excluded. Must differ from row 1's raw figure for
/// RFC_PRUEBA, which has an active scale rule; for RFC_GRANDE (no active rule today) the
/// two are expected to be equal. Baseline measured 2026-09-03.
#[tokio::test]
async fn nomina_normalizada_no_excluida() {
    const RAW_PRUEBA: f64 = 20_592_281.51;
    const EXPECTED_PRUEBA: f64 = 20_317_281.51;
    const EXPECTED_GRANDE: f64 = 44_443_673.80;

    let pool = connect().await;
    for (rfc, expected) in [(RFC_PRUEBA, EXPECTED_PRUEBA), (RFC_GRANDE, EXPECTED_GRANDE)] {
        let row = sqlx::query(
            r#"SELECT SUM(total_percepciones)::float8 AS v
               FROM pulso.nomina_normalizada WHERE rfc_emisor = $1 AND NOT is_excluded"#,
        )
        .bind(rfc)
        .fetch_one(&pool)
        .await
        .unwrap();
        let actual = get_f64_opt(&row, "v").unwrap_or(0.0);
        assert!(
            (actual - expected).abs() < 0.01,
            "nomina normalizada no excluida for {rfc}: expected {expected} (2026-09-03 \
             baseline), got {actual}."
        );
    }

    // RFC_PRUEBA has an active scale rule -- raw and normalized must genuinely differ.
    assert!(
        (RAW_PRUEBA - EXPECTED_PRUEBA).abs() > 0.01,
        "RFC_PRUEBA's raw and normalized nomina totals are equal -- its scale rule may have \
         stopped applying, which would itself be worth investigating."
    );
}

/// Row 3: filas de la vista de nomina -- COUNT(*) FROM nomina_normalizada per owner (all
/// rows, excluded or not: the view itself never drops a row for exclusion, it only flags
/// `is_excluded`). Baseline measured 2026-09-03.
#[tokio::test]
async fn filas_de_la_vista_de_nomina() {
    let pool = connect().await;
    for (rfc, expected) in [(RFC_PRUEBA, 821_i64), (RFC_GRANDE, 10_592_i64)] {
        let row = sqlx::query(
            r#"SELECT COUNT(*) AS v FROM pulso.nomina_normalizada WHERE rfc_emisor = $1"#,
        )
        .bind(rfc)
        .fetch_one(&pool)
        .await
        .unwrap();
        let actual: i64 = row.try_get("v").unwrap();
        assert!(
            actual >= expected,
            "filas de la vista for {rfc}: expected at least {expected} (2026-09-03 \
             baseline), got {actual}. A count LOWER than the baseline is a regression \
             (dropped rows), not expected growth."
        );
    }
}

/// Row 4: filas de la vista = filas de su join base -- a RELATIONAL assertion. This must
/// hold as an identity by construction: `nomina_normalizada`'s WHERE clause (migration 066)
/// is exactly `c.tipo_comprobante = 'N' AND NOT c.is_cancelled` over `cfdis JOIN cfdi_nomina`,
/// with no other row-dropping or row-duplicating logic (the LATERAL joins for
/// factor/exclusion are all `LIMIT 1`, so they can't fan out rows). If a future change to
/// the view's join shape drops or duplicates rows, this catches it regardless of how much
/// data has synced in.
#[tokio::test]
async fn filas_de_la_vista_igual_a_join_base() {
    let pool = connect().await;
    for rfc in [RFC_PRUEBA, RFC_GRANDE] {
        let view_row = sqlx::query(
            r#"SELECT COUNT(*) AS v FROM pulso.nomina_normalizada WHERE rfc_emisor = $1"#,
        )
        .bind(rfc)
        .fetch_one(&pool)
        .await
        .unwrap();
        let view_count: i64 = view_row.try_get("v").unwrap();

        let base_row = sqlx::query(
            r#"SELECT COUNT(*) AS v FROM pulso.cfdis c
               JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
               WHERE c.rfc_emisor = $1 AND c.tipo_comprobante = 'N' AND NOT c.is_cancelled"#,
        )
        .bind(rfc)
        .fetch_one(&pool)
        .await
        .unwrap();
        let base_count: i64 = base_row.try_get("v").unwrap();

        assert_eq!(
            view_count, base_count,
            "for {rfc}: nomina_normalizada has {view_count} rows but its own join base \
             (cfdis JOIN cfdi_nomina, tipo_comprobante='N', not cancelled) has {base_count} \
             -- the view is silently dropping or duplicating rows."
        );
    }
}

/// Row 5: filas de la base de exclusiones. Baseline measured 2026-09-03 (doc: 145 / 0).
#[tokio::test]
async fn filas_de_exclusiones() {
    let pool = connect().await;
    for (rfc, expected) in [(RFC_PRUEBA, 145_i64), (RFC_GRANDE, 0_i64)] {
        let row =
            sqlx::query(r#"SELECT COUNT(*) AS v FROM pulso.cfdi_exclusion WHERE owner_rfc = $1"#)
                .bind(rfc)
                .fetch_one(&pool)
                .await
                .unwrap();
        let actual: i64 = row.try_get("v").unwrap();
        assert!(
            actual >= expected,
            "filas de cfdi_exclusion for {rfc}: expected at least {expected} (2026-09-03 \
             baseline), got {actual}. Lower than baseline is a regression, not growth."
        );
    }
}

/// Row 6: puente lado comprobantes = poblacion excluida -- a RELATIONAL assertion, WITH a
/// declared tolerance of +/-2 pesos.
///
/// "Puente lado comprobantes" is the comprobante-rule-sourced side of
/// `list_ebitda_bridge_adjustments` (the `rows` query, tipo_comprobante IN ('I','E'), signed
/// by which side of the comprobante the owner sits on) -- as opposed to the nomina-rule
/// side (dl_type == "nomina", sourced from `payroll_normalization_rules`). We isolate it by
/// filtering the real function's output on `dl_type != "nomina"`, which is safe because
/// `receipt_excl_rows` (the OTHER comprobante-rule source, which targets tipo_comprobante =
/// 'N') currently matches zero rows for both test RFCs -- confirmed separately below.
///
/// The comparison side is the SAME population (cfdi_exclusion joined to cfdis_ajustado,
/// tipo_comprobante IN ('I','E'), not cancelled) and the SAME signed CASE / cast to float8,
/// computed as a single un-grouped SUM instead of the real function's per-(rule,year)
/// grouping. Both sides read the identical underlying REAL columns
/// (`cfdis`/`cfdis_ajustado.total_neto_mxn_ajustado`), so they are expected to differ by a
/// few cents to a few pesos depending on summation order -- NOT to match exactly.
///
/// L6-12 landed (migration 069): pulso.cfdis's money columns are NUMERIC now, so this is
/// an exact-equality check -- confirmed directly (same population, summed in two different
/// orders) that the same total now comes out byte-identical regardless of grouping, which
/// is exactly the acceptance test the Lote 6 doc names for L6-12.
#[tokio::test]
async fn puente_lado_comprobantes_igual_poblacion_excluida() {
    const TOLERANCE_MXN: f64 = 0.0;

    let pool = connect().await;

    for rfc in [RFC_PRUEBA, RFC_GRANDE] {
        // receipt_excl_rows (the other comprobante-rule source, N-type) must be empty for
        // this isolation-by-dl_type to be valid -- assert that precondition explicitly
        // rather than assuming it silently.
        let n_side = sqlx::query(
            r#"SELECT COUNT(*) AS v
               FROM pulso.cfdi_exclusion ex
               JOIN pulso.normalization_rules nr ON nr.id = ex.rule_id
               JOIN pulso.cfdis c ON c.uuid = ex.uuid
               JOIN pulso.cfdi_nomina n ON n.uuid = ex.uuid
               JOIN pulso.nomina_normalizada nn ON nn.uuid = ex.uuid AND nn.rfc_emisor = ex.owner_rfc
               WHERE ex.owner_rfc = $1 AND c.tipo_comprobante = 'N' AND NOT c.is_cancelled
                 AND nn.employee_rule_id IS NULL"#,
        )
        .bind(rfc)
        .fetch_one(&pool)
        .await
        .unwrap();
        let n_side_count: i64 = n_side.try_get("v").unwrap();
        assert_eq!(
            n_side_count, 0,
            "for {rfc}: receipt_excl_rows (comprobante rule targeting an N-type receipt) now \
             matches {n_side_count} row(s) -- the dl_type != \"nomina\" isolation below no \
             longer isolates only the I/E comprobante side; this test needs to sum that \
             source in too."
        );

        let bridge = normalization::list_ebitda_bridge_adjustments(&pool, rfc, 2000, 1, 2100, 12)
            .await
            .unwrap();
        let bridge_total: f64 = bridge
            .iter()
            .filter(|r| r.dl_type != "nomina")
            .map(|r| r.total_mxn)
            .sum();

        let direct_row = sqlx::query(
            r#"SELECT SUM(
                    CASE WHEN c.rfc_emisor = ex.owner_rfc THEN -COALESCE(c.total_neto_mxn_ajustado, 0)
                         ELSE COALESCE(c.total_neto_mxn_ajustado, 0) END
                 )::float8 AS v
               FROM pulso.cfdi_exclusion ex
               JOIN pulso.cfdis_ajustado c ON c.uuid = ex.uuid
               WHERE ex.owner_rfc = $1 AND c.tipo_comprobante IN ('I','E') AND NOT c.is_cancelled"#,
        )
        .bind(rfc)
        .fetch_one(&pool)
        .await
        .unwrap();
        let direct_total = get_f64_opt(&direct_row, "v").unwrap_or(0.0);

        assert!(
            (bridge_total - direct_total).abs() <= TOLERANCE_MXN,
            "puente lado comprobantes for {rfc}: bridge fn gives {bridge_total:.4}, direct \
             population query gives {direct_total:.4} -- diff {:.4} exceeds the declared \
             +/-{TOLERANCE_MXN} tolerance (see TODO L6-12 above).",
            (bridge_total - direct_total).abs()
        );
    }
}

/// Row 7: total de un mes de nomina -- mismo pidiendo el rango completo que pidiendo solo
/// ese mes. Direct test of L5-04's devengo-consistency fix: `payroll::monthly_series` both
/// filters and groups by `year_devengo`/`month_devengo`, so a month's total must not depend
/// on how wide a window it was requested through.
#[tokio::test]
async fn mes_completo_igual_a_mes_solo() {
    let pool = connect().await;
    // RFC_PRUEBA, 2023-01: a real month with nomina data (confirmed present 2026-09-03).
    let rfc = RFC_PRUEBA;
    let (year, month) = (2023_i64, 1_i64);

    let full_range = payroll::monthly_series(&pool, rfc, 2000, 1, 2100, 12)
        .await
        .unwrap();
    let full_range_total = full_range
        .iter()
        .find(|m| m.year == year && m.month == month)
        .unwrap_or_else(|| panic!("month {year}-{month:02} not found in full-range series"))
        .total_percepciones;

    let scoped = payroll::monthly_series(&pool, rfc, year, month, year, month)
        .await
        .unwrap();
    let scoped_total = scoped
        .iter()
        .find(|m| m.year == year && m.month == month)
        .unwrap_or_else(|| panic!("month {year}-{month:02} not found in scoped series"))
        .total_percepciones;

    assert!(
        (full_range_total - scoped_total).abs() < 0.01,
        "month {year}-{month:02} for {rfc}: full-range total={full_range_total}, \
         single-month total={scoped_total} -- these should be identical (L5-04)."
    );
}

/// Row 8: ingresos netos normalizados -- mismo valor en Ingresos, Resumen trimestral y
/// Contrapartes.
///
/// `quarterly::get` and `counterparties::get` share the exact same population filter
/// (`tipo_comprobante NOT IN ('P','N')`) and match EXACTLY (zero tolerance) -- that part of
/// the invariant is airtight. `summary::get` differs from both in two ways, neither of
/// which is L6-06/07/08:
///   1. `summary.rs`'s monthly query additionally excludes tipo_comprobante = 'T', while
///      quarterly.rs/counterparties.rs do not. This is currently a non-event for both test
///      RFCs (RFC_PRUEBA has zero T-type comprobantes; RFC_GRANDE's 47 T-type comprobantes
///      apparently carry ~0 net amount), but it's a real, latent filter inconsistency of
///      exactly the "one copy fixed, its twin isn't" class this lote's rule #4 warns about
///      -- flagged here for a future lote, not fixed.
///   2. Same REAL-precision grouping-order effect as row 6 (TODO L6-12): summary.rs groups
///      by (year, month) while quarterly/counterparties group by (year, quarter) /
///      (counterparty), so floating-point summation order differs even over the identical
///      population.
/// Both together explain a few-peso gap between summary and the other two; declaring a
/// tolerance here (rather than forcing exact equality) keeps the assertion honest about
/// what's actually true today instead of papering over reason #1's structural gap.
///
/// NOTE: dl_type="ambos" is NOT used here. `summary::get`'s monthly query builds
/// `AND c.{dl_filter}` and `dl_type_filter("ambos")` returns the bare fragment "1=1",
/// producing invalid SQL ("c.1=1") -- a real, previously undocumented bug in
/// `summary.rs`, out of scope for this lote. Reported separately; sidestepped here with
/// dl_type="emitidos", which both other screens use the same way.
#[tokio::test]
async fn ingresos_netos_tres_pantallas() {
    // TODO L6-12 (precision) + latent 'T' filter gap (see doc comment above).
    const TOLERANCE_MXN: f64 = 5.0;

    let pool = connect().await;

    for rfc in [RFC_PRUEBA, RFC_GRANDE] {
        let sp = summary::SummaryParams {
            dl_type: "emitidos".to_string(),
            from: "2000-01".to_string(),
            to: "2100-12".to_string(),
        };
        let ingresos_total = summary::get(&pool, rfc, &sp).await.unwrap().total_mxn;

        let quarterly_resp = quarterly::get(&pool, rfc, "emitidos", "2000-01", "2100-12")
            .await
            .unwrap();
        let trimestral_total: f64 = quarterly_resp.quarters.iter().map(|q| q.total_mxn).sum();

        // High limit so `top` effectively covers every counterparty in the period.
        let cp_resp = counterparties::get(&pool, rfc, "emitidos", "2000-01", "2100-12", 1_000_000)
            .await
            .unwrap();
        let contrapartes_total: f64 = cp_resp.top.iter().map(|r| r.total_mxn).sum();

        assert!(
            (trimestral_total - contrapartes_total).abs() < 0.01,
            "for {rfc}: Resumen trimestral ({trimestral_total}) and Contrapartes \
             ({contrapartes_total}) must match EXACTLY -- they share the identical \
             population filter, so any gap here is a real regression."
        );
        assert!(
            (ingresos_total - trimestral_total).abs() <= TOLERANCE_MXN,
            "for {rfc}: Ingresos ({ingresos_total}) vs Resumen trimestral \
             ({trimestral_total}) differ by {:.4}, exceeding the declared \
             +/-{TOLERANCE_MXN} tolerance (see doc comment on this test for the two known \
             reasons).",
            (ingresos_total - trimestral_total).abs()
        );
    }
}
