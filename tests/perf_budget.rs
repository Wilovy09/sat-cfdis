//! L6-04: performance budget for the shared queries this project's Lote 5 work already
//! touches, timed for real (wall-clock, not `EXPLAIN`) against the largest RFC.
//!
//! `list_payroll_rules`'s per-rule factor-warning re-evaluation (L5-14) only runs its
//! extra query for rules in the `adjust_to_amount_mxn` family, and today there are none
//! platform-wide -- so the budget seeds 60 synthetic rows under a clearly fake owner RFC
//! to exercise that loop for real, and tears them down before any assertion that could
//! panic (see `list_payroll_rules_with_seeded_adjust_rules_stays_within_budget`).

use std::time::{Duration, Instant};

use pulso_backend::db::{self, DbPool};
use pulso_backend::services::analytics::{normalization, payroll, summary};
use sqlx::Row;

/// The RFC with the most data in the shared test database (per `PULSO_Correcciones_Lote6.md`).
const BIG_RFC: &str = "CES100706U65";

/// Doc's own ceiling: "deja margen y detiene una regresión ... entre 50 y 500 segundos."
const BUDGET: Duration = Duration::from_millis(1000);

/// Obviously fake, never a real client RFC -- checked against both `pulso.users` and
/// `pulso.payroll_normalization_rules` before seeding, below.
const SYNTHETIC_OWNER_RFC: &str = "TEST000101TST";

async fn connect() -> DbPool {
    dotenvy::dotenv().ok();
    let cfg = pulso_backend::config::Config::from_env();
    let pool = db::init_pool(&cfg)
        .await
        .expect("failed to connect to the shared test database");
    // `init_pool` runs the full migration check as its last step, which can hand the
    // migrator's connection back to the pool in a state that costs a fresh
    // connection/TLS handshake on whichever query runs next -- confirmed directly: a
    // pool's first query here took 788ms, its second 304ms, stable from then on. A
    // long-lived production pool pays this exactly once, ever; a per-test pool would pay
    // it on every single test, which is what the query itself costs, not what it costs to
    // reach the database. One throwaway round trip here settles the pool before any
    // budget-asserting test starts its clock.
    sqlx::query("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("failed to warm the connection pool");
    pool
}

#[tokio::test]
async fn payroll_monthly_series_stays_within_budget() {
    let pool = connect().await;
    let (from_y, from_m) = summary::parse_ym("2000-01");
    let (to_y, to_m) = summary::parse_ym("2030-12");

    let start = Instant::now();
    let months = payroll::monthly_series(&pool, BIG_RFC, from_y, from_m, to_y, to_m)
        .await
        .expect("payroll::monthly_series query failed");
    let elapsed = start.elapsed();

    println!(
        "[L6-04] payroll::monthly_series({BIG_RFC}) took {elapsed:?} ({} months)",
        months.len()
    );
    assert!(
        elapsed < BUDGET,
        "payroll monthly series exceeded the {BUDGET:?} budget: {elapsed:?}"
    );
}

#[tokio::test]
async fn payroll_employee_catalog_stays_within_budget() {
    let pool = connect().await;

    let start = Instant::now();
    let employees = normalization::list_payroll_employees(&pool, BIG_RFC)
        .await
        .expect("normalization::list_payroll_employees query failed");
    let elapsed = start.elapsed();

    println!(
        "[L6-04] list_payroll_employees({BIG_RFC}) took {elapsed:?} ({} employees)",
        employees.len()
    );
    assert!(
        elapsed < BUDGET,
        "payroll employee catalog exceeded the {BUDGET:?} budget: {elapsed:?}"
    );
}

/// Real employee RFCs from `BIG_RFC`'s own nómina population, so `compute_adjust_factor_
/// warnings`'s join runs against real percepciones shape rather than made-up strings. The
/// synthetic rules still bind to `SYNTHETIC_OWNER_RFC` as `rfc_emisor`, which no real CFDI
/// carries -- each of the 60 extra queries the loop fires resolves to zero rows, but the
/// dominant cost L5-14 introduced is the one-round-trip-per-rule shape itself, which this
/// preserves regardless of row count.
async fn sample_employee_rfcs(pool: &DbPool, count: i64) -> Vec<String> {
    let rows = sqlx::query(
        "SELECT DISTINCT rfc_receptor FROM pulso.nomina_normalizada
         WHERE rfc_emisor = $1 AND rfc_receptor IS NOT NULL AND rfc_receptor != ''
         LIMIT $2",
    )
    .bind(BIG_RFC)
    .bind(count)
    .fetch_all(pool)
    .await
    .expect("failed to sample employee RFCs from the big RFC's nómina population");

    rows.iter()
        .map(|r| r.try_get::<String, _>("rfc_receptor").unwrap_or_default())
        .collect()
}

async fn seed_adjust_rules(pool: &DbPool, employee_rfcs: &[String]) -> Result<(), sqlx::Error> {
    for (i, employee_rfc) in employee_rfcs.iter().enumerate() {
        sqlx::query(
            "INSERT INTO pulso.payroll_normalization_rules
                (id, owner_rfc, rule_family, employee_rfc, employee_name, action,
                 value_mxn, created_at, updated_at)
             VALUES ($1, $2, 'adjust_to_amount_mxn', $3, $4, 'adjust', $5, NOW()::text, NOW()::text)",
        )
        .bind(format!("l6-04-synthetic-{i}"))
        .bind(SYNTHETIC_OWNER_RFC)
        .bind(employee_rfc)
        .bind(format!("L6-04 synthetic employee {i}"))
        .bind(15_000.0_f64)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Deletes every synthetic row under `SYNTHETIC_OWNER_RFC`, regardless of how many made it
/// in. Called unconditionally before any assertion in the seeded test below, so a budget
/// failure (an `assert!` that panics) still leaves the shared test database clean.
async fn cleanup_adjust_rules(pool: &DbPool) {
    let result = sqlx::query("DELETE FROM pulso.payroll_normalization_rules WHERE owner_rfc = $1")
        .bind(SYNTHETIC_OWNER_RFC)
        .execute(pool)
        .await;
    if let Err(e) = result {
        // A cleanup failure must never be silent: it's the one thing this test promises.
        panic!("failed to clean up synthetic payroll_normalization_rules rows: {e}");
    }
}

#[tokio::test]
async fn list_payroll_rules_with_seeded_adjust_rules_stays_within_budget() {
    const RULE_COUNT: i64 = 60;
    // `usize::try_from` would need an `.expect` at every call site for a value that can
    // never fail (RULE_COUNT is a small compile-time literal) -- converted once here instead.
    #[allow(clippy::cast_possible_truncation)]
    const RULE_COUNT_USIZE: usize = RULE_COUNT as usize;

    let pool = connect().await;

    let already_used_as_owner: i64 = sqlx::query(
        "SELECT COUNT(*) AS n FROM pulso.payroll_normalization_rules WHERE owner_rfc = $1",
    )
    .bind(SYNTHETIC_OWNER_RFC)
    .fetch_one(&pool)
    .await
    .expect("failed to check for a pre-existing synthetic owner_rfc")
    .try_get("n")
    .unwrap_or(0);
    let already_a_real_user: i64 =
        sqlx::query("SELECT COUNT(*) AS n FROM pulso.users WHERE rfc = $1")
            .bind(SYNTHETIC_OWNER_RFC)
            .fetch_one(&pool)
            .await
            .expect("failed to check the synthetic RFC against pulso.users")
            .try_get("n")
            .unwrap_or(0);
    assert_eq!(
        already_used_as_owner + already_a_real_user,
        0,
        "{SYNTHETIC_OWNER_RFC} must be unused before seeding -- pick a different fake RFC"
    );

    let employee_rfcs = sample_employee_rfcs(&pool, RULE_COUNT).await;
    assert_eq!(
        employee_rfcs.len(),
        RULE_COUNT_USIZE,
        "expected {RULE_COUNT} distinct employee RFCs under {BIG_RFC} to seed against"
    );

    let seed_result = seed_adjust_rules(&pool, &employee_rfcs).await;

    let measurement = if seed_result.is_ok() {
        let start = Instant::now();
        let read = normalization::list_payroll_rules(&pool, SYNTHETIC_OWNER_RFC).await;
        Some((start.elapsed(), read))
    } else {
        None
    };

    // Teardown before any assertion below can panic.
    cleanup_adjust_rules(&pool).await;

    seed_result.expect("failed to seed synthetic adjust_to_amount_mxn rules");
    let (elapsed, read) = measurement.expect("measurement skipped: seeding failed");
    let rules = read.expect("list_payroll_rules failed while measuring the seeded-rule budget");

    assert_eq!(
        rules.len(),
        RULE_COUNT_USIZE,
        "expected all 60 seeded rules back"
    );
    let warned = rules
        .iter()
        .filter(|r| !r.factor_warnings.is_empty())
        .count();
    println!(
        "[L6-04] list_payroll_rules({SYNTHETIC_OWNER_RFC}) with {RULE_COUNT} adjust_to_amount_mxn \
         rules took {elapsed:?} ({warned} carrying a factor warning)"
    );
    assert!(
        elapsed < BUDGET,
        "list_payroll_rules with {RULE_COUNT} adjust_to_amount_mxn rules exceeded the \
         {BUDGET:?} budget: {elapsed:?} -- see PULSO_Correcciones_Lote6.md L6-04: batch \
         compute_adjust_factor_warnings into a single query instead of one per rule"
    );
}
