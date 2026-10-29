//! Migration 096: one RFC in two accounts, one analysis each. Two owners of the same RFC
//! each see only their own adjustments -- exclusions, payroll rules, cached results -- and
//! the account that had the RFC first sees exactly what it saw before.
//!
//! Writes users, CFDIs and rules, so it runs only against a disposable database that
//! already has every migration applied, named by `PULSO_SCRATCH_DATABASE_URL`:
//!     cargo test --test multi_owner -- --ignored
//! Never point it at a shared database.

use pulso_backend::db::{self, DbPool};
use pulso_backend::services::analytics::{counterparties, normalization};
use pulso_backend::services::response_cache;
use sqlx::postgres::PgPoolOptions;

const RFC: &str = "MOT010101AB1";
const CLIENT_A: &str = "CLA020202AB2";
const CLIENT_B: &str = "CLB030303AB3";
const EMPLOYEE: &str = "EMPL800101AB1";

async fn scratch_pool() -> DbPool {
    let url = std::env::var("PULSO_SCRATCH_DATABASE_URL").unwrap_or_else(|_| {
        panic!("PULSO_SCRATCH_DATABASE_URL is not set -- this test writes data; point it at a disposable database")
    });
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to the scratch database")
}

async fn exec(pool: &DbPool, sql: &str) {
    sqlx::raw_sql(sql).execute(pool).await.expect(sql);
}

/// Two accounts, A (had the RFC first) and B (co-owner), both verified; the RFC billed
/// 1,000 to CLIENT_A and 500 to CLIENT_B, plus one payroll receipt of 100.
async fn seed(pool: &DbPool) -> (String, String) {
    exec(
        pool,
        &format!(
            "DELETE FROM pulso.endpoint_response_cache WHERE split_part(rfc, '#', 1) = '{RFC}';
             DELETE FROM pulso.normalization_rules WHERE split_part(owner_rfc, '#', 1) = '{RFC}';
             DELETE FROM pulso.payroll_normalization_rules WHERE split_part(owner_rfc, '#', 1) = '{RFC}';
             DELETE FROM pulso.cfdi_nomina WHERE uuid LIKE 'MO-%';
             DELETE FROM pulso.cfdis_raw WHERE uuid LIKE 'MO-%';
             DELETE FROM pulso.users WHERE rfc = '{RFC}';
             DELETE FROM pulso.sync_jobs WHERE id = 'mo-job';
             INSERT INTO pulso.sync_jobs (id, job_type, rfc, auth_type, auth_enc, dl_type, period_from, period_to, created_at, updated_at, status)
               VALUES ('mo-job', 'list', '{RFC}', 'ciec', '', 'ambos', '2026-01-01 00:00:00', '2026-01-31 23:59:59', 'x', 'x', 'completed');
             INSERT INTO pulso.cfdis_raw (uuid, job_id, rfc_emisor, rfc_receptor, nombre_receptor, fecha_emision, year, month,
                                          tipo_comprobante, dl_type, created_at, subtotal, total, total_mxn, tipo_cambio, estado_sat)
               VALUES ('MO-1', 'mo-job', '{RFC}', '{CLIENT_A}', 'CLIENTE A', '2026-01-10T10:00:00', 2026, 1, 'I', 'emitidos', 'x', 1000, 1000, 1000, 1, 'Vigente'),
                      ('MO-2', 'mo-job', '{RFC}', '{CLIENT_B}', 'CLIENTE B', '2026-01-11T10:00:00', 2026, 1, 'I', 'emitidos', 'x', 500, 500, 500, 1, 'Vigente'),
                      ('MO-N', 'mo-job', '{RFC}', '{EMPLOYEE}', 'EMPLEADA', '2026-01-31T10:00:00', 2026, 1, 'N', 'emitidos', 'x', 100, 100, 100, 1, 'Vigente');
             INSERT INTO pulso.cfdi_nomina (uuid, tipo_nomina, fecha_pago, fecha_inicial_pago, fecha_final_pago, total_percepciones, total_deducciones, total_otros_pagos)
               VALUES ('MO-N', 'O', '2026-01-31', '2026-01-01', '2026-01-31', 100, 0, 0);"
        ),
    )
    .await;

    let mut ids = Vec::new();
    for email in ["mo-a@example.com", "mo-b@example.com"] {
        let id: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO public.users (id, email, name) VALUES (gen_random_uuid(), $1, $1)
             ON CONFLICT (id) DO NOTHING RETURNING id",
        )
        .bind(email)
        .fetch_one(pool)
        .await
        .expect("insert account");
        ids.push(id.to_string());
    }
    for user in &ids {
        db::users::create_pulso_user(pool, user, RFC, "", None, None)
            .await
            .expect("register the RFC");
        db::users::mark_verified_now(pool, user, RFC)
            .await
            .expect("verify");
    }
    (ids[0].clone(), ids[1].clone())
}

#[tokio::test]
#[ignore = "writes data: needs PULSO_SCRATCH_DATABASE_URL (a disposable database)"]
async fn each_owner_of_one_rfc_has_its_own_analysis() {
    let main = scratch_pool().await;
    let (a, b) = seed(&main).await;

    // Keys: the first account keeps the RFC itself, the co-owner gets its own.
    let key_a = db::users::analysis_for(&main, &a, RFC, false)
        .await
        .unwrap()
        .unwrap();
    let key_b = db::users::analysis_for(&main, &b, RFC, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(key_a, RFC);
    assert!(
        key_b.starts_with(&format!("{RFC}#")),
        "co-owner key: {key_b}"
    );
    let pool_a = db::analysis::pool_for(&main, &key_a);
    let pool_b = db::analysis::pool_for(&main, &key_b);

    // A excludes CLIENT_A; B scales the employee's payroll to 50 %. Each written through
    // its own analysis, the way the rules routes do.
    sqlx::query(
        "INSERT INTO pulso.normalization_rules (id, owner_rfc, dl_type, source_rfc, action, created_at, updated_at)
         VALUES ('mo-rule-a', pulso.analysis_key($1), 'emitidos', $2, 'exclude', 'x', 'x')",
    )
    .bind(RFC)
    .bind(CLIENT_A)
    .execute(&pool_a)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO pulso.payroll_normalization_rules (id, owner_rfc, rule_family, employee_rfc, action, value_pct, created_at, updated_at)
         VALUES ('mo-rule-b', pulso.analysis_key($1), 'scale_employee_pct', $2, 'normalize', 50, 'x', 'x')",
    )
    .bind(RFC)
    .bind(EMPLOYEE)
    .execute(&pool_b)
    .await
    .unwrap();
    exec(
        &main,
        "REFRESH MATERIALIZED VIEW pulso.nomina_normalizada_mv",
    )
    .await;

    // Each sees only its own rules, shown under the plain RFC.
    let rules_a = normalization::list_rules(&pool_a, RFC).await.unwrap();
    let rules_b = normalization::list_rules(&pool_b, RFC).await.unwrap();
    assert_eq!(rules_a.len(), 1);
    assert_eq!(rules_a[0].owner_rfc, RFC);
    assert!(rules_b.is_empty(), "B must not see A's exclusion");

    // Counterparties: A's exclusion applies to A only.
    let total =
        |r: &counterparties::CounterpartiesResponse| r.top.iter().map(|c| c.total_mxn).sum::<f64>();
    let cp_a = counterparties::get(&pool_a, RFC, "emitidos", "2026-01", "2026-01", 10)
        .await
        .unwrap();
    let cp_b = counterparties::get(&pool_b, RFC, "emitidos", "2026-01", "2026-01", 10)
        .await
        .unwrap();
    assert!(
        (total(&cp_a) - 500.0).abs() < 0.01,
        "A excludes CLIENT_A: {}",
        total(&cp_a)
    );
    assert!(
        (total(&cp_b) - 1500.0).abs() < 0.01,
        "B sees both clients: {}",
        total(&cp_b)
    );

    // Normalized payroll: B's 50 % rule applies to B only.
    let payroll = |p: DbPool| async move {
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT factor, total_percepciones FROM pulso.nomina_normalizada WHERE rfc_emisor = $1",
        )
        .bind(RFC)
        .fetch_all(&p)
        .await
        .unwrap()
    };
    assert_eq!(payroll(pool_a.clone()).await, vec![(1.0, 100.0)]);
    assert_eq!(payroll(pool_b.clone()).await, vec![(0.5, 50.0)]);

    // Without a session analysis (workers, admin) the views are the RFC's own analysis.
    let excluded_main: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pulso.cfdi_exclusion WHERE owner_rfc = $1")
            .bind(RFC)
            .fetch_one(&main)
            .await
            .unwrap();
    assert_eq!(excluded_main, 1);

    // Cached results never cross analyses.
    let cached = |p: DbPool, value: &'static str| async move {
        response_cache::get_or_compute(&p, RFC, "mo-test", "k", || async { Ok(value) })
            .await
            .unwrap()
    };
    assert_eq!(cached(pool_a.clone(), "a").await, "a");
    assert_eq!(cached(pool_b.clone(), "b").await, "b");
    assert_eq!(
        cached(pool_a.clone(), "recomputed").await,
        "a",
        "A reads its own cache row"
    );
    assert_eq!(
        cached(pool_b.clone(), "recomputed").await,
        "b",
        "B reads its own cache row"
    );
}
