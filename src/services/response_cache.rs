//! Cache for expensive per-RFC analytics endpoint responses (migration 074).
//!
//! Keyed by `(rfc, endpoint, params_key)` and invalidated by `data_version`, not a TTL:
//! ingestion is event-driven (jobs finding invoices, an ETL round enriching them, the
//! cancellation-recheck and xml-redownload workers, an admin reprocess, a normalization
//! rule write), not a fixed batch schedule, so a fixed expiry would either serve stale
//! data between arbitrary refreshes or discard perfectly valid cache entries for no
//! reason. `bump_version` (this file) covers every Rust-side ingestion path; rule writes
//! go through a SQL trigger instead (`pulso.bump_rfc_data_version`, migration 076) -- see
//! `bump_version`'s own doc for why those two never overlap. C14-01 through C14-05 is the
//! full accounting of every write path that moves analytics figures and what invalidates
//! each one; C14-06 (this file's `cleanup_worker`) is the sweep that reclaims a row once
//! nothing can serve it as a hit anymore.
//!
//! The cache is purely additive: a read miss or a write failure always falls back to
//! `compute`, never turns into a request error. Losing the cache loses speed, not
//! correctness.
//!
//! `data_version` only tracks *data* changes -- it has no idea the running binary's
//! *logic* for an endpoint changed (a bug fix with no accompanying migration, same RFC,
//! same data). Every cache read also requires `computed_at > process_start()`, so a row
//! written by a previous process (i.e. before the last deploy/restart) never counts as a
//! hit, regardless of `data_version` -- it gets recomputed and overwritten by the current
//! code on its next access, same lazy self-healing path a `data_version` bump already
//! uses. A deploy is a restart, so this makes every deploy invalidate the whole cache for
//! free, with no separate step to remember.

use serde::Serialize;
use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;
use time::OffsetDateTime;

use crate::db::DbPool;

/// C14-06/AUD-149: how often the cleanup sweep runs. Matches the other slow background
/// workers' cadence (recheck_cancelled, xml_redownload) -- this isn't correctness-critical
/// on any tighter schedule, it's just housekeeping.
const CLEANUP_INTERVAL_SECS: u64 = 6 * 3600;
/// Rows deleted per DELETE statement. Batches so a large backlog (a deploy right after a
/// long-running previous process, say) doesn't hold a lock over the whole table at once --
/// see `run_cleanup`'s own note (trap 1: this is about lock/table-bloat hygiene, not reader
/// safety -- a concurrent read of a row mid-sweep just sees it before or after, Postgres's
/// normal MVCC visibility, nothing this code has to coordinate).
const CLEANUP_BATCH_SIZE: i64 = 500;

static PROCESS_START: OnceLock<OffsetDateTime> = OnceLock::new();

/// Wall-clock time this process started serving cache reads (first call wins, which is
/// close enough to actual process start -- within a request or two of it).
fn process_start() -> OffsetDateTime {
    *PROCESS_START.get_or_init(OffsetDateTime::now_utc)
}

/// Invalidates every cached response for `rfc` by advancing its data version. Cheap (one
/// upsert) -- the actual recompute happens lazily, on the next request that misses.
///
/// C14-01/DEC-083: this is the Rust-side half of a two-sided invalidation. The other half
/// is `pulso.bump_rfc_data_version` (SQL, migration 076), called by triggers on
/// `pulso.normalization_rules`/`pulso.payroll_normalization_rules` inside the same
/// transaction as a rule write -- not from here. Every ingestion path (`db::jobs::complete`,
/// `services::etl`, `services::recheck_cancelled`, `services::xml_redownload`) calls this
/// Rust function instead; a rule create/edit/delete must never also call it, or the same
/// bump would happen twice under two different definitions (DEC-084's "one definition per
/// number").
pub async fn bump_version(pool: &DbPool, rfc: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO pulso.rfc_data_version (rfc, version)
           VALUES ($1, 1)
           ON CONFLICT (rfc) DO UPDATE
               SET version = pulso.rfc_data_version.version + 1,
                   updated_at = now()"#,
    )
    .bind(rfc)
    .execute(pool)
    .await?;
    Ok(())
}

async fn current_version(pool: &DbPool, rfc: &str) -> Result<i64, sqlx::Error> {
    let version: Option<i64> =
        sqlx::query_scalar("SELECT version FROM pulso.rfc_data_version WHERE rfc = $1")
            .bind(rfc)
            .fetch_optional(pool)
            .await?;
    Ok(version.unwrap_or(0))
}

/// Returns the cached JSON response for `(rfc, endpoint, params_key)` if it matches the
/// RFC's current data version; otherwise runs `compute`, caches the result, and returns it.
///
/// Returns `serde_json::Value` rather than `T` so a cache hit never needs to deserialize
/// back into the endpoint's response type -- the stored JSONB is served as-is.
///
/// `params_key` must uniquely identify the request's query parameters within `endpoint`
/// (e.g. `"dl_type=emitidos|from=2025-01|to=2026-08"`); two different parameter
/// combinations that share a key would silently serve each other's cached data.
pub async fn get_or_compute<T, F, Fut>(
    pool: &DbPool,
    rfc: &str,
    endpoint: &str,
    params_key: &str,
    compute: F,
) -> anyhow::Result<serde_json::Value>
where
    T: Serialize,
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let version = current_version(pool, rfc).await?;

    let cached: Option<serde_json::Value> = sqlx::query_scalar(
        r#"SELECT payload FROM pulso.endpoint_response_cache
           WHERE rfc = $1 AND endpoint = $2 AND params_key = $3 AND data_version = $4
             AND computed_at > $5"#,
    )
    .bind(rfc)
    .bind(endpoint)
    .bind(params_key)
    .bind(version)
    .bind(process_start())
    .fetch_optional(pool)
    .await?;

    if let Some(payload) = cached {
        return Ok(payload);
    }

    let result = compute().await?;
    let payload = serde_json::to_value(&result)?;

    if let Err(e) = sqlx::query(
        r#"INSERT INTO pulso.endpoint_response_cache
               (rfc, endpoint, params_key, data_version, payload)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (rfc, endpoint, params_key) DO UPDATE
               SET data_version = EXCLUDED.data_version,
                   payload = EXCLUDED.payload,
                   computed_at = now()"#,
    )
    .bind(rfc)
    .bind(endpoint)
    .bind(params_key)
    .bind(version)
    .bind(&payload)
    .execute(pool)
    .await
    {
        tracing::warn!(rfc, endpoint, error = %e, "response_cache: failed to store cached response");
    }

    Ok(payload)
}

/// C14-06/AUD-149: nothing ever deleted a row from `pulso.endpoint_response_cache`. The
/// criterion is exactly what `get_or_compute` already uses to decide a row is unreadable
/// -- computed before this process started, or at a data_version this RFC has since moved
/// past -- so this sweep only ever removes rows nothing could serve as a hit anyway. Run
/// as its own background worker (spawned in `main.rs` alongside the others), outside any
/// request path.
pub async fn cleanup_worker(pool: DbPool) {
    loop {
        tokio::time::sleep(Duration::from_secs(CLEANUP_INTERVAL_SECS)).await;
        if let Err(e) = run_cleanup(&pool).await {
            tracing::error!("response_cache: cleanup cycle error: {e}");
        }
    }
}

async fn run_cleanup(pool: &DbPool) -> Result<(), sqlx::Error> {
    let cutoff = process_start();
    let mut total_deleted = 0u64;
    loop {
        // trap 2: no ORDER BY total-row-count or size-based cutoff -- batched purely to
        // keep each DELETE small, not to cap how much of the real backlog gets cleared.
        let result = sqlx::query(
            r#"DELETE FROM pulso.endpoint_response_cache
               WHERE (rfc, endpoint, params_key) IN (
                   SELECT c.rfc, c.endpoint, c.params_key
                   FROM pulso.endpoint_response_cache c
                   LEFT JOIN pulso.rfc_data_version v ON v.rfc = c.rfc
                   WHERE c.computed_at < $1 OR COALESCE(v.version, 0) <> c.data_version
                   LIMIT $2
               )"#,
        )
        .bind(cutoff)
        .bind(CLEANUP_BATCH_SIZE)
        .execute(pool)
        .await?;

        let deleted = result.rows_affected();
        total_deleted += deleted;
        if deleted < CLEANUP_BATCH_SIZE as u64 {
            break;
        }
        // Yield between batches rather than holding the pool in a tight loop.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    if total_deleted > 0 {
        tracing::info!(
            deleted = total_deleted,
            "response_cache: cleanup sweep done"
        );
    }
    Ok(())
}
