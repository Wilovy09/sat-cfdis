//! L6-03 (items 1 and 2), redone per L6C-10.
//!
//! **First version** (this session, earlier): a DATA precondition -- does any nomina row
//! have emision != devengo? That's permanently true for real payroll data, so it stayed red
//! forever regardless of whether L6-06/07/08 got fixed.
//!
//! **Second version** (this session, after that): a source-text search -- does hallazgos.rs's
//! query contain `year_devengo`? That correctly detects the fix, but only by reading the SQL
//! string, never by calling the real code path.
//!
//! **This version** (L6C-10): the actual ask -- not against the data, and not against the
//! source text, but BETWEEN CONSUMERS. Two modules that compute the same number from the
//! same devengo-consistent view must agree, by calling the real functions and comparing
//! their real output. This passes in verde exactly when the underlying code is right,
//! regardless of how much data has synced in -- and it's the one design of these three that
//! would actually fail again if L6-06/07/08's fix were ever reverted.
//!
//! Both invariants below carry a non-vacuity guard: if the measured window has zero rows
//! where year <> year_devengo (or month <> month_devengo), the OLD (emision-grouped) and NEW
//! (devengo-grouped) definitions are indistinguishable on that data, and a passing assertion
//! would prove nothing. Confirmed directly against the RFC grande: year 2022 alone has zero
//! such rows (an old, buggy grouping would pass there by coincidence); year 2023 alone has
//! divergent rows that net to the same total (jun/nov cancel at the peso). Testing across
//! the FULL unbounded range, year by year, sidesteps both traps: any single year's
//! coincidental cancellation doesn't hide a real mismatch in a different year.
use pulso_backend::config::Config;
use pulso_backend::db::{self, DbPool};
use pulso_backend::services::analytics::{hallazgos, payroll};
use sqlx::Row;

const RFC_PRUEBA: &str = "NUB170623KI3";
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

/// Rows with year <> year_devengo anywhere in this RFC's (non-excluded) nomina -- the
/// non-vacuity guard both invariants below assert is nonzero before trusting a green result.
async fn has_devengo_divergence(pool: &DbPool, rfc: &str) -> bool {
    let row = sqlx::query(
        r#"SELECT COUNT(*) AS n FROM pulso.nomina_normalizada
           WHERE rfc_emisor = $1 AND NOT is_excluded
             AND (year <> year_devengo OR month <> month_devengo)"#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await
    .unwrap();
    row.try_get::<i64, _>("n").unwrap_or(0) > 0
}

/// Invariante 1: una sola definicion de costo de nomina -- H3 (hallazgos.rs) vs
/// payroll::monthly_series, year by year.
///
/// H3's own per-year nomina figure isn't exposed by hallazgos::get's public response (only a
/// margin-percentage delta appears in its `cuerpo` string) -- mirrored here verbatim from its
/// current query (see hallazgos.rs's own `nom_rows`) as the only way to test it end-to-end
/// short of refactoring H3 purely for testability, same technique this suite's own "puente"
/// test (number_contract.rs) already uses for a different private query. Comparison side
/// calls the real `payroll::monthly_series`.
#[tokio::test]
async fn invariante_una_sola_definicion_costo_nomina() {
    let pool = connect().await;
    for rfc in [RFC_PRUEBA, RFC_GRANDE] {
        assert!(
            has_devengo_divergence(&pool, rfc).await,
            "for {rfc}: zero rows have year <> year_devengo -- this invariant can't \
             distinguish the emision- and devengo-grouped definitions on today's data, so a \
             passing result here would prove nothing. Confirm the data is as expected before \
             trusting this test."
        );

        // Ensure hallazgos::get runs without erroring for this RFC (exercises the same code
        // path H3's real cuerpo is built from), even though its per-year figure isn't
        // independently recoverable from the response.
        hallazgos::get(&pool, rfc)
            .await
            .expect("hallazgos::get failed");

        let h3_nom_rows = sqlx::query(
            r#"SELECT n.year_devengo AS year, SUM(n.total_percepciones)::float8 AS nomina
               FROM pulso.nomina_normalizada n
               WHERE n.rfc_emisor = $1 AND NOT n.is_excluded
               GROUP BY n.year_devengo"#,
        )
        .bind(rfc)
        .fetch_all(&pool)
        .await
        .unwrap();

        let by_month = payroll::monthly_series(&pool, rfc, 2000, 1, 2100, 12)
            .await
            .expect("payroll::monthly_series failed");
        let mut by_year_from_monthly: std::collections::HashMap<i64, f64> =
            std::collections::HashMap::new();
        for m in &by_month {
            *by_year_from_monthly.entry(m.year).or_insert(0.0) += m.total_percepciones;
        }

        let mut checked = 0;
        for row in &h3_nom_rows {
            let year: i64 = row.try_get("year").unwrap_or(0);
            let h3_nomina = get_f64_opt(row, "nomina").unwrap_or(0.0);
            let Some(&monthly_total) = by_year_from_monthly.get(&year) else {
                continue;
            };
            checked += 1;
            assert!(
                (h3_nomina - monthly_total).abs() < 0.01,
                "for {rfc}, year {year}: H3's nomina total ({h3_nomina:.2}) != sum of \
                 payroll::monthly_series for that year ({monthly_total:.2})."
            );
        }
        assert!(
            checked > 0,
            "for {rfc}: no year appeared in both H3's grouping and monthly_series -- can't \
             verify the invariant"
        );
    }
}

/// Invariante 2: una sola definicion de mes de nomina -- by_year, by_month, summary_row
/// (windowed to exactly one calendar year), and headcount_by_month vs by_month's
/// employee_count, matched by period string (not by index -- the two series can have a
/// different number of months).
#[tokio::test]
async fn invariante_una_sola_definicion_mes_de_nomina() {
    let pool = connect().await;
    for rfc in [RFC_PRUEBA, RFC_GRANDE] {
        assert!(
            has_devengo_divergence(&pool, rfc).await,
            "for {rfc}: zero rows have year <> year_devengo or month <> month_devengo -- \
             this invariant can't distinguish the two definitions on today's data."
        );

        let full = payroll::get(&pool, rfc, "2000-01", "2100-12")
            .await
            .expect("payroll::get failed");

        // by_year vs sum of by_month, year by year (both from the SAME response, so no
        // extra round trip needed for this half).
        let mut by_month_year_totals: std::collections::HashMap<i64, f64> =
            std::collections::HashMap::new();
        for m in &full.by_month {
            *by_month_year_totals.entry(m.year).or_insert(0.0) += m.total_pagado;
        }
        let mut year_checked = 0;
        for y in &full.by_year {
            let Some(&monthly_sum) = by_month_year_totals.get(&y.year) else {
                continue;
            };
            year_checked += 1;
            assert!(
                (y.total_pagado - monthly_sum).abs() < 0.01,
                "for {rfc}, year {}: by_year total ({:.2}) != sum of by_month for that year \
                 ({monthly_sum:.2}).",
                y.year,
                y.total_pagado
            );
        }
        assert!(
            year_checked > 0,
            "for {rfc}: no year appeared in both by_year and by_month -- can't verify"
        );

        // headcount_by_month vs by_month's employee_count, matched by `period` string, not
        // by index -- the two series aren't guaranteed to have the same length.
        let hc_by_period: std::collections::HashMap<&str, i64> = full
            .headcount_by_month
            .iter()
            .map(|h| (h.period.as_str(), h.headcount))
            .collect();
        let mut period_checked = 0;
        for m in &full.by_month {
            let Some(&hc) = hc_by_period.get(m.period.as_str()) else {
                continue;
            };
            period_checked += 1;
            assert_eq!(
                hc, m.employee_count,
                "for {rfc}, period {}: headcount_by_month ({hc}) != by_month's employee_count \
                 ({}) -- matched by period string, not index.",
                m.period, m.employee_count
            );
        }
        assert!(
            period_checked > 0,
            "for {rfc}: no period appeared in both headcount_by_month and by_month -- can't \
             verify"
        );

        // summary_row windowed to EXACTLY one calendar year, against that same year's
        // by_year total. Pick a year with real devengo divergence (not just any year) so
        // this half of the invariant, too, is exercised against data that could actually
        // distinguish the two definitions -- one extra payroll::get call, deliberately
        // scoped to a single RFC-specific year rather than every year, since payroll::get
        // is its own multi-round-trip cost (see perf_budget.rs's MULTI_QUERY_BUDGET_GET).
        let divergent_year_row = sqlx::query(
            r#"SELECT year_devengo AS y FROM pulso.nomina_normalizada
               WHERE rfc_emisor = $1 AND NOT is_excluded
                 AND (year <> year_devengo OR month <> month_devengo)
               LIMIT 1"#,
        )
        .bind(rfc)
        .fetch_one(&pool)
        .await
        .expect("the has_devengo_divergence guard above already confirmed this row exists");
        let target_year: i64 = divergent_year_row.try_get("y").unwrap_or(0);

        let Some(year_total) = full
            .by_year
            .iter()
            .find(|y| y.year == target_year)
            .map(|y| y.total_pagado)
        else {
            panic!("for {rfc}: year {target_year} (confirmed divergent) doesn't appear in by_year");
        };

        let scoped = payroll::get(
            &pool,
            rfc,
            &format!("{target_year}-01"),
            &format!("{target_year}-12"),
        )
        .await
        .expect("payroll::get (scoped) failed");

        assert!(
            (scoped.summary.total_pagado_mxn - year_total).abs() < 0.01,
            "for {rfc}, year {target_year}: summary_row windowed to exactly that calendar \
             year ({:.2}) != by_year's total for the same year ({year_total:.2}).",
            scoped.summary.total_pagado_mxn
        );
    }
}
