//! Cache for expensive per-RFC analytics endpoint responses (migration 074).
//!
//! Keyed by `(rfc, endpoint, params_key)` and invalidated by `data_version`, not a TTL:
//! ingestion is event-driven (a sync job finding new invoices, the cancellation-recheck
//! worker flipping an `estado_sat`), not a fixed batch schedule, so a fixed expiry would
//! either serve stale data between arbitrary refreshes or discard perfectly valid cache
//! entries for no reason. `bump_version` is called from those two write paths
//! (`db::jobs::complete`, `services::recheck_cancelled`) and is the only way a cached row
//! stops being served.
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
use time::OffsetDateTime;

use crate::db::DbPool;

static PROCESS_START: OnceLock<OffsetDateTime> = OnceLock::new();

/// Wall-clock time this process started serving cache reads (first call wins, which is
/// close enough to actual process start -- within a request or two of it).
fn process_start() -> OffsetDateTime {
    *PROCESS_START.get_or_init(OffsetDateTime::now_utc)
}

/// Invalidates every cached response for `rfc` by advancing its data version. Cheap (one
/// upsert) -- the actual recompute happens lazily, on the next request that misses.
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
