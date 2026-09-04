//! L6-01 evidence: the migration guard passes against the repo's current, checked-out
//! state, and fails (naming the missing version) against a synthetic "one behind"
//! scenario. The synthetic case is exercised through `check_migrations_current` directly,
//! with `applied_max` computed as `real_max - 1` — it never deletes a row from the shared
//! test database's `_sqlx_migrations`, which other work also reads.

use std::path::Path;

use pulso_backend::db;

async fn connect() -> db::DbPool {
    dotenvy::dotenv().ok();
    let cfg = pulso_backend::config::Config::from_env();
    db::init_pool(&cfg)
        .await
        .expect("failed to connect to the shared test database")
}

#[tokio::test]
async fn guard_passes_against_current_real_state() {
    let pool = connect().await;

    let result = db::migration_guard::ensure_current(&pool, Path::new("./migrations")).await;

    assert!(
        result.is_ok(),
        "migration guard should pass against the checked-out repo's current state: {result:?}"
    );
}

#[tokio::test]
async fn guard_fails_and_names_the_missing_version_when_one_behind() {
    let pool = connect().await;

    let real_max = db::migration_guard::applied_max_version(&pool)
        .await
        .expect("failed to read _sqlx_migrations");

    // Synthetic "one behind" scenario, via the pure function's arguments only -- the real
    // `_sqlx_migrations` table is shared infrastructure and is never touched here.
    let result = db::migration_guard::check_migrations_current(real_max - 1, real_max);

    let err = result.expect_err("guard must fail when the DB is one migration behind the repo");
    assert!(
        err.contains(&real_max.to_string()),
        "error message should name the missing version {real_max}: {err}"
    );
}
