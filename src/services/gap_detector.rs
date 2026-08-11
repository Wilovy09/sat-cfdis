//! Background worker: finds calendar days that are missing from `pulso.cfdis`
//! even though they should have been synced, and requeues them — the
//! equivalent of `recheck_cancelled.rs`, but for coverage gaps instead of
//! stale status.
//!
//! Two distinct failure modes produce the same symptom (a day with real
//! business activity but zero rows in Pulso):
//!
//! 1. A `sync_jobs` row exhausts `retry_transient_or_fail`'s backoff schedule
//!    (5min..24h, 8 steps) and lands on `status='failed'` permanently — nothing
//!    in the rest of the pipeline ever looks at a failed job again, so
//!    whatever date range it hadn't finished stays unsynced forever.
//! 2. A day can end up with zero rows even *inside* a job that's marked
//!    `status='completed'` — a transient per-day miss that didn't abort the
//!    overall run. This one can't be detected from `sync_jobs` metadata at
//!    all; the only signal is the data itself being suspiciously empty.
//!
//! Both were confirmed against a day-by-day reference report for Nubarium
//! (2023-06-28, 2023-12-29, 2024-10-25, 2024-12-02): the 2024-10-25 gap traced
//! to three consecutive `captcha_failed` job failures over Sep-Nov 2024; the
//! others sat inside ranges Pulso already considered fully synced.

use crate::{
    config::Config,
    db::{self, DbPool},
    services::crypto,
};
use aws_sdk_s3::Client as S3Client;
use std::collections::HashMap;
use std::sync::Arc;

/// How often the worker wakes up.
const GAP_POLL_SECS: u64 = 6 * 3600;
/// Fresh restarts a permanently-failed job's leftover range gets before the
/// gap detector stops trying — a persistently broken RFC (revoked
/// credentials, deleted FIEL) shouldn't be restarted forever.
const MAX_GAP_JOB_RETRIES: i32 = 3;
/// Failed jobs re-queued per cycle.
const GAP_JOB_BATCH: i64 = 50;
/// How many days of the zero-activity scan to advance per RFC per cycle —
/// bounds one cycle's SQL cost; a multi-year backlog drains gradually.
const SCAN_WINDOW_DAYS: i64 = 90;
/// Single-day resyncs to attempt before accepting a day as genuinely empty
/// (real holiday, weekend-adjacent lull) instead of re-checking it forever.
const MAX_GAP_RESYNC_ATTEMPTS: i64 = 2;

pub async fn worker(pool: DbPool, cfg: Arc<Config>, s3: Arc<S3Client>) {
    // Let the other startup workers get a head start.
    tokio::time::sleep(std::time::Duration::from_secs(180)).await;

    loop {
        if let Err(e) = requeue_failed_jobs(&pool, &cfg, &s3).await {
            tracing::error!("Gap-detector: requeue_failed_jobs error: {e}");
        }
        if let Err(e) = scan_activity_gaps(&pool, &cfg, &s3).await {
            tracing::error!("Gap-detector: scan_activity_gaps error: {e}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(GAP_POLL_SECS)).await;
    }
}

// ---------------------------------------------------------------------------
// Shared: resolve fresh credentials for a known RFC (FIEL preferred, else
// current stored CIEC password). Mirrors recheck_cancelled.rs's pattern.
// ---------------------------------------------------------------------------

async fn resolve_auth(
    pool: &DbPool,
    s3: &Arc<S3Client>,
    bucket: &str,
    rfc: &str,
    creds: &HashMap<String, String>,
    key: &[u8; 32],
) -> Option<(serde_json::Value, Option<tempfile::TempDir>)> {
    if let Some((fiel_auth, tmp)) = crate::try_fiel_auth(pool, s3, bucket, rfc).await {
        return Some((fiel_auth, Some(tmp)));
    }
    let clave_enc = creds.get(rfc)?;
    match crypto::decrypt(key, clave_enc) {
        Ok(clave) => Some((
            serde_json::json!({ "type": "ciec", "rfc": rfc, "password": clave }),
            None,
        )),
        Err(e) => {
            tracing::error!(rfc = %rfc, "Gap-detector: decrypt failed: {e}");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Part 1 — failed jobs that never got resumed
// ---------------------------------------------------------------------------

async fn requeue_failed_jobs(pool: &DbPool, cfg: &Arc<Config>, s3: &Arc<S3Client>) -> anyhow::Result<()> {
    let failed = db::jobs::find_failed_retryable(pool, MAX_GAP_JOB_RETRIES, GAP_JOB_BATCH).await?;
    if failed.is_empty() {
        return Ok(());
    }
    tracing::info!(count = failed.len(), "Gap-detector: failed jobs eligible for auto-continuation");

    let creds: HashMap<String, String> =
        db::users::get_all_with_credentials(pool).await?.into_iter().collect();
    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    let key = crypto::load_key();

    for job in failed {
        let gap_start = match &job.cursor_date {
            Some(d) => crate::next_day(d),
            None => job.period_from.clone(),
        };
        // Same direct string comparison resume_worker already uses for the
        // equivalent "anything left?" check on cursor vs period_to.
        if gap_start > job.period_to {
            tracing::info!(
                job_id = %job.id, rfc = %job.rfc,
                "Gap-detector: failed job's cursor already reached period_to, nothing to continue"
            );
            db::jobs::mark_superseded(pool, &job.id, "n/a-fully-covered").await?;
            continue;
        }

        let Some((auth_payload, _fiel_tmp)) =
            resolve_auth(pool, s3, &bucket, &job.rfc, &creds, &key).await
        else {
            tracing::warn!(job_id = %job.id, rfc = %job.rfc, "Gap-detector: no credentials available, skipping continuation");
            continue;
        };
        let auth_type_label = auth_payload
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("ciec")
            .to_string();
        let auth_json = serde_json::to_string(&auth_payload)?;
        let auth_enc = crypto::encrypt(&key, &auth_json).map_err(|e| anyhow::anyhow!(e))?;

        let new_id = db::jobs::insert_gap_continuation(
            pool,
            &job.rfc,
            &auth_type_label,
            &auth_enc,
            &job.dl_type,
            &gap_start,
            &job.period_to,
            job.gap_retry_count + 1,
        )
        .await?;
        db::jobs::mark_superseded(pool, &job.id, &new_id).await?;
        tracing::warn!(
            job_id = %job.id, new_job_id = %new_id, rfc = %job.rfc,
            gap_start = %gap_start, period_to = %job.period_to, retry = job.gap_retry_count + 1,
            "Gap-detector: requeued unfinished range from a permanently-failed job"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Part 2 — zero-activity weekdays inside otherwise-"completed" ranges
// ---------------------------------------------------------------------------

async fn scan_activity_gaps(pool: &DbPool, cfg: &Arc<Config>, s3: &Arc<S3Client>) -> anyhow::Result<()> {
    let rfcs = db::jobs::distinct_completed_rfcs(pool).await?;
    if rfcs.is_empty() {
        return Ok(());
    }
    let creds: HashMap<String, String> =
        db::users::get_all_with_credentials(pool).await?.into_iter().collect();
    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    let key = crypto::load_key();
    let yesterday = yesterday_ymd();

    for rfc in rfcs {
        let start = match db::jobs::get_gap_scan_progress(pool, &rfc).await? {
            Some(last) => crate::next_day(&last)[..10].to_string(),
            None => match db::jobs::earliest_completed_period_from(pool, &rfc).await? {
                Some(from) => from[..10.min(from.len())].to_string(),
                None => continue,
            },
        };
        if start > yesterday {
            continue; // fully caught up, nothing new to scan yet
        }
        let end = std::cmp::min(add_days(&start, SCAN_WINDOW_DAYS), yesterday.clone());

        let gap_days = db::jobs::find_activity_gap_days(pool, &rfc, &start, &end).await?;
        if !gap_days.is_empty() {
            tracing::warn!(
                rfc = %rfc, count = gap_days.len(), days = ?gap_days,
                "Gap-detector: zero-activity weekdays found inside a completed sync range"
            );
        }

        for day in &gap_days {
            let attempts = db::jobs::count_gap_resync_attempts(pool, &rfc, day).await?;
            if attempts >= MAX_GAP_RESYNC_ATTEMPTS {
                tracing::warn!(
                    rfc = %rfc, day = %day, attempts,
                    "Gap-detector: giving up on this day — already re-checked and still empty"
                );
                continue;
            }

            let Some((auth_payload, _fiel_tmp)) = resolve_auth(pool, s3, &bucket, &rfc, &creds, &key).await
            else {
                tracing::warn!(rfc = %rfc, day = %day, "Gap-detector: no credentials available, cannot resync this day");
                continue;
            };
            let auth_type_label = auth_payload
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("ciec")
                .to_string();
            let auth_json = serde_json::to_string(&auth_payload)?;
            let auth_enc = crypto::encrypt(&key, &auth_json).map_err(|e| anyhow::anyhow!(e))?;
            let period_from = format!("{day} 00:00:00");
            let period_to = format!("{day} 23:59:59");

            let new_id = db::jobs::insert_queued(
                pool,
                "gap_resync",
                &rfc,
                &auth_type_label,
                &auth_enc,
                "ambos",
                &period_from,
                &period_to,
            )
            .await?;
            tracing::warn!(rfc = %rfc, day = %day, job_id = %new_id, "Gap-detector: requeued zero-activity day for resync");
        }

        db::jobs::set_gap_scan_progress(pool, &rfc, &end).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Minimal date helpers (no chrono dependency in this crate)
// ---------------------------------------------------------------------------

/// Advances `date` (any string starting with "YYYY-MM-DD") forward `n` days,
/// via `n` calls to `crate::next_day` — reuses already-correct calendar math
/// instead of duplicating it. `n` is always small here (<= SCAN_WINDOW_DAYS).
fn add_days(date: &str, n: i64) -> String {
    let mut d = format!("{} 00:00:00", &date[..10.min(date.len())]);
    for _ in 0..n {
        d = crate::next_day(&d);
    }
    d[..10].to_string()
}

fn yesterday_ymd() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
        .saturating_sub(86400);
    ymd_from_epoch_secs(secs)
}

/// Same Gregorian decomposition `daily_sync_worker` uses inline in main.rs —
/// duplicated here as a real function since that one isn't callable from
/// this module. Deliberately not shared: it's ~15 lines of pure calendar
/// math, not worth a cross-cutting refactor of a working, unrelated worker.
fn ymd_from_epoch_secs(secs: u64) -> String {
    const DAYS_IN_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let days = secs / 86400;
    let mut y = 1970u32;
    let mut d = days as u32;
    loop {
        let leap_year = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let days_in_year = if leap_year { 366 } else { 365 };
        if d < days_in_year {
            break;
        }
        d -= days_in_year;
        y += 1;
    }
    let leap_year = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let mut m = 0usize;
    for (i, &dim) in DAYS_IN_MONTH.iter().enumerate() {
        let dim = if i == 1 && leap_year { 29 } else { dim };
        if d < dim {
            m = i;
            break;
        }
        d -= dim;
    }
    format!("{y:04}-{:02}-{:02}", m + 1, d + 1)
}
