//! L6-04: performance budget for the shared queries this project's Lote 5 work already
//! touches, timed for real (wall-clock, not `EXPLAIN`) against the largest RFC.
//!
//! `list_payroll_rules`'s per-rule factor-warning re-evaluation (L5-14) only runs its
//! extra query for rules in the `adjust_to_amount_mxn` family, and today there are none
//! platform-wide -- so the budget seeds 60 synthetic rows under `BIG_RFC` itself (real
//! employees sampled from its own nómina, so the factor lookup hits real percepciones data
//! -- an earlier version seeded under a fake owner RFC instead, which made that lookup
//! resolve to nothing for all 60 rules every run) to exercise that loop for real, and tears
//! them down by a synthetic id prefix (never by owner_rfc -- BIG_RFC is a real, heavily-used
//! RFC) before any assertion that could panic (see
//! `list_payroll_rules_with_seeded_adjust_rules_stays_within_budget`).

use std::time::{Duration, Instant};

use pulso_backend::db::{self, DbPool};
use pulso_backend::services::analytics::{hallazgos, normalization, payroll, summary};
use sqlx::Row;

/// The RFC with the most data in the shared test database (per `PULSO_Correcciones_Lote6.md`).
const BIG_RFC: &str = "CES100706U65";

/// Doc's own ceiling: "deja margen y detiene una regresión ... entre 50 y 500 segundos."
const BUDGET: Duration = Duration::from_millis(1000);

/// `payroll::get` and `payroll::get_snapshot` don't fit `BUDGET` -- not because L6C-04/05's
/// devengo migration made them slow, but because they were ALREADY built as many (8-15)
/// separate round trips to `pulso.nomina_normalizada`, a view with three per-row LATERAL
/// joins (factor/exclusion). Nobody had ever timed either function before L6C-08 added
/// these two tests, so this is the first measurement either has ever had, not a regression
/// against a prior passing baseline.
///
/// Confirmed directly, both ways, before accepting this: (1) `get_snapshot`'s `emp_rows`
/// WAS a real bug -- `WHERE rfc_receptor IN (subquery)` against the view made Postgres
/// re-evaluate all ~10,592 rows once per active employee (41 loops), 12.47s, reproduced
/// identically with plain emisión columns instead of devengo -- fixed below with a
/// `MATERIALIZED` CTE (12.47s -> 347ms in isolation) and by folding four more of
/// `get_snapshot`'s round trips into one FILTER-based aggregate query, bringing the whole
/// function from ~15s to ~3.2s. (2) The remaining cost in both functions -- and all of
/// `payroll::get`'s ~15s -- is many round trips at 300ms-1.4s each, no single outlier: e.g.
/// `ded_rows` (full 2000-2030 range) measured 450ms with plain emisión columns and ~800ms
/// with devengo, the same order of magnitude, not the 10x+ jump `emp_rows` had. A real fix
/// (one held connection, a `CREATE TEMP TABLE` materializing the view once, every one of
/// the 15+8 queries reading that instead) is worth doing but is a properly-scoped follow-up
/// of its own, not a same-day fix alongside 27 other query rewrites.
///
/// Set well above today's measured numbers (with real margin) but nowhere near the
/// 50-500s incident-class thresholds this project has already hit twice -- high enough to
/// not be a false alarm today, low enough to still catch a genuine future regression.
const MULTI_QUERY_BUDGET_SNAPSHOT: Duration = Duration::from_secs(5);
const MULTI_QUERY_BUDGET_GET: Duration = Duration::from_secs(20);

/// `hallazgos::get` -- L6C-08 named this one explicitly and it went unmeasured through the
/// whole lote (found in a later review). Same "many round trips, no single outlier" shape
/// as `payroll::get`: it calls `payroll::get_snapshot` internally (H4) on top of its own
/// ~10 round trips (H1, H2/H3's annual data, H5A/H5B's several queries, H6). Measured
/// directly: 19.6s, no `IN (subquery)`-against-the-view pattern found (the specific shape
/// `get_snapshot`'s `emp_rows` bug was).
///
/// Per a later review: first landed sharing `MULTI_QUERY_BUDGET_GET` (20s) -- 19.6s against
/// a 20s ceiling is 2% margin, in a binary whose six perf_budget tests run in parallel
/// against the shared database (one of them seeding 60 rows) alongside it. That's a flaky
/// gate waiting to happen, and a job that cries wolf teaches people to ignore it. Given its
/// own budget with real margin instead -- a PROVISIONAL ceiling meant to catch a real
/// regression (going to minutes, the incident class this project has already hit twice),
/// not a target to optimize toward. The real fix (one held connection, materialize the view
/// once, every round trip reads that) is its own follow-up, same as `payroll::get`'s.
const MULTI_QUERY_BUDGET_HALLAZGOS: Duration = Duration::from_secs(40);

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

/// L6C-08: `payroll::get` -- the fifteen queries L6C-04 moved to devengo, none of which had
/// a timed regression guard before this. Same full-range window `payroll_monthly_series_
/// stays_within_budget` already uses, so this and that test are directly comparable.
/// Budget: see `MULTI_QUERY_BUDGET_GET`'s doc comment -- this is pre-existing cost, not a
/// devengo regression, confirmed directly against the same query with emisión columns.
#[tokio::test]
async fn payroll_get_stays_within_budget() {
    let pool = connect().await;

    let start = Instant::now();
    let response = payroll::get(&pool, BIG_RFC, "2000-01", "2030-12")
        .await
        .expect("payroll::get query failed");
    let elapsed = start.elapsed();

    println!(
        "[L6C-08] payroll::get({BIG_RFC}) took {elapsed:?} ({} months, {} employees)",
        response.by_month.len(),
        response.by_employee.len()
    );
    assert!(
        elapsed < MULTI_QUERY_BUDGET_GET,
        "payroll::get exceeded the {MULTI_QUERY_BUDGET_GET:?} budget: {elapsed:?}"
    );
}

/// L6C-08: `payroll::get_snapshot` -- Dashboard's most-viewed card (headcount, run-rate LTM,
/// YoY, pasivo laboral). L6C-05 moved its anchor (`period_row`) and all eight queries to
/// devengo; this is its first timed regression guard. Found and fixed a real pre-existing
/// bug while adding it -- see `MULTI_QUERY_BUDGET_SNAPSHOT`'s doc comment.
#[tokio::test]
async fn payroll_get_snapshot_stays_within_budget() {
    let pool = connect().await;

    let start = Instant::now();
    let snapshot = payroll::get_snapshot(&pool, BIG_RFC)
        .await
        .expect("payroll::get_snapshot query failed");
    let elapsed = start.elapsed();

    println!(
        "[L6C-08] payroll::get_snapshot({BIG_RFC}) took {elapsed:?} \
         (headcount_actual={}, months_of_data={})",
        snapshot.headcount_actual, snapshot.months_of_data
    );
    assert!(
        elapsed < MULTI_QUERY_BUDGET_SNAPSHOT,
        "payroll::get_snapshot exceeded the {MULTI_QUERY_BUDGET_SNAPSHOT:?} budget: {elapsed:?}"
    );
}

/// L6C-08 named this one by name; it went unmeasured until a later review caught it.
#[tokio::test]
async fn hallazgos_get_stays_within_budget() {
    let pool = connect().await;

    let start = Instant::now();
    let response = hallazgos::get(&pool, BIG_RFC)
        .await
        .expect("hallazgos::get query failed");
    let elapsed = start.elapsed();

    println!(
        "[L6C-08] hallazgos::get({BIG_RFC}) took {elapsed:?} ({} hallazgos)",
        response.all.len()
    );
    assert!(
        elapsed < MULTI_QUERY_BUDGET_HALLAZGOS,
        "hallazgos::get exceeded the {MULTI_QUERY_BUDGET_HALLAZGOS:?} budget: {elapsed:?}"
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

/// Real employee RFCs from `BIG_RFC`'s own nómina population, so the seeded rules bind to
/// employees who actually have real CFDIs to look up.
///
/// Per Rob's review: the previous version of this test bound the synthetic rules'
/// `owner_rfc` to `SYNTHETIC_OWNER_RFC` (a fake RFC with zero real CFDIs) instead of
/// `BIG_RFC` -- `batch_adjust_factor_sources`'s `WHERE c.rfc_emisor = $1` never matched a
/// single row regardless of which real `employee_rfc` a rule named, so the per-rule
/// percepciones lookup resolved to nothing for all 60 rules, every run. The budget measured
/// ~0ms of the actual cost path (confirmed: "0 carrying a factor warning" every time this
/// ran) -- exactly the kind of blind spot that let the real regression this same review
/// found (compute_adjust_factor_warnings/batch_adjust_factor_sources joining the whole view
/// for two columns, 385x slower) go uncaught. Rules now seed under `BIG_RFC` itself, so the
/// lookup hits real rows.
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

/// ID prefix for every synthetic rule this test seeds -- distinct enough that no real rule
/// (production IDs are UUIDs) could ever collide, and used as the ONLY key `cleanup_adjust_
/// rules` deletes by. Deliberately not `owner_rfc = BIG_RFC` (a real, heavily-used RFC) --
/// deleting by owner_rfc here would risk a real client rule if one existed at cleanup time.
const SYNTHETIC_RULE_ID_PREFIX: &str = "l6-04-synthetic-";

async fn seed_adjust_rules(pool: &DbPool, employee_rfcs: &[String]) -> Result<(), sqlx::Error> {
    for (i, employee_rfc) in employee_rfcs.iter().enumerate() {
        sqlx::query(
            "INSERT INTO pulso.payroll_normalization_rules
                (id, owner_rfc, rule_family, employee_rfc, employee_name, action,
                 value_mxn, created_at, updated_at)
             VALUES ($1, $2, 'adjust_to_amount_mxn', $3, $4, 'adjust', $5, NOW()::text, NOW()::text)",
        )
        .bind(format!("{SYNTHETIC_RULE_ID_PREFIX}{i}"))
        .bind(BIG_RFC)
        .bind(employee_rfc)
        .bind(format!("L6-04 synthetic employee {i}"))
        .bind(15_000.0_f64)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Deletes every synthetic row by id prefix, regardless of how many made it in. Called
/// unconditionally before any assertion in the seeded test below, so a budget failure (an
/// `assert!` that panics) still leaves the shared test database clean. By id, not by
/// `owner_rfc = BIG_RFC` -- BIG_RFC is a real, heavily-used RFC; deleting by owner_rfc would
/// risk a real client rule.
async fn cleanup_adjust_rules(pool: &DbPool) {
    let result = sqlx::query("DELETE FROM pulso.payroll_normalization_rules WHERE id LIKE $1")
        .bind(format!("{SYNTHETIC_RULE_ID_PREFIX}%"))
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

    let already_seeded: i64 =
        sqlx::query("SELECT COUNT(*) AS n FROM pulso.payroll_normalization_rules WHERE id LIKE $1")
            .bind(format!("{SYNTHETIC_RULE_ID_PREFIX}%"))
            .fetch_one(&pool)
            .await
            .expect("failed to check for pre-existing synthetic rule ids")
            .try_get("n")
            .unwrap_or(0);
    assert_eq!(
        already_seeded, 0,
        "rows with id LIKE '{SYNTHETIC_RULE_ID_PREFIX}%' already exist under {BIG_RFC} -- a \
         previous run's cleanup may have failed; clear them by hand before re-running"
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
        let read = normalization::list_payroll_rules(&pool, BIG_RFC).await;
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
    // Per Rob's review: the previous version of this assertion (implicitly, by never
    // checking) let every rule's factor lookup silently resolve to zero rows -- seeding
    // under a fake owner_rfc meant zero real percepciones could ever match, so "0 carrying
    // a factor warning" was indistinguishable from the real cost path never running at all.
    // Seeded against BIG_RFC's own real employees now (`sample_employee_rfcs`), so each of
    // the 60 lookups should find real percepciones data -- asserting that directly here,
    // not just hoping the elapsed time reflects real work.
    let checked_percepciones: i64 = sqlx::query(
        "SELECT COUNT(DISTINCT rfc_receptor) AS n FROM pulso.nomina_normalizada
         WHERE rfc_emisor = $1 AND rfc_receptor = ANY($2)",
    )
    .bind(BIG_RFC)
    .bind(&employee_rfcs)
    .fetch_one(&pool)
    .await
    .expect("failed to confirm the seeded employees have real nomina data")
    .try_get("n")
    .unwrap_or(0);
    assert_eq!(
        checked_percepciones, RULE_COUNT,
        "expected all {RULE_COUNT} seeded employee RFCs to have real nomina_normalizada rows \
         under {BIG_RFC} -- if this is 0, the factor lookup this budget measures resolves to \
         nothing again, same blind spot as before"
    );
    let warned = rules
        .iter()
        .filter(|r| !r.factor_warnings.is_empty())
        .count();
    println!(
        "[L6-04] list_payroll_rules({BIG_RFC}) with {RULE_COUNT} adjust_to_amount_mxn \
         rules took {elapsed:?} ({warned} carrying a factor warning, {checked_percepciones} \
         of {RULE_COUNT} employees confirmed to have real nomina data)"
    );
    assert!(
        elapsed < BUDGET,
        "list_payroll_rules with {RULE_COUNT} adjust_to_amount_mxn rules exceeded the \
         {BUDGET:?} budget: {elapsed:?} -- see PULSO_Correcciones_Lote6.md L6-04: batch \
         compute_adjust_factor_warnings into a single query instead of one per rule"
    );
}
