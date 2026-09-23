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

async fn requeue_failed_jobs(
    pool: &DbPool,
    cfg: &Arc<Config>,
    s3: &Arc<S3Client>,
) -> anyhow::Result<()> {
    let failed = db::jobs::find_failed_retryable(pool, MAX_GAP_JOB_RETRIES, GAP_JOB_BATCH).await?;
    if failed.is_empty() {
        return Ok(());
    }
    tracing::info!(
        count = failed.len(),
        "Gap-detector: failed jobs eligible for auto-continuation"
    );

    let creds: HashMap<String, String> = db::users::get_all_with_credentials(pool)
        .await?
        .into_iter()
        .collect();
    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    let key = crypto::load_key();

    for job in failed {
        // `cursor_date` is only trustworthy as "completed through this day"
        // when the job actually made progress. A job that fails before ever
        // successfully processing a day (found = 0) can still have
        // cursor_date sitting on its own period_from — the streaming loop
        // sets it to the day it's *attempting*, not the day it *finished*.
        // Trusting cursor_date there computed next_day(period_from) >
        // period_to for every single-day job that failed immediately,
        // which read as "fully covered" and gave up without ever retrying —
        // confirmed against 163 jobs platform-wide (129 on ALA2409253U7
        // alone) that failed with found=0 and were marked superseded despite
        // covering zero real days. When found = 0, ignore cursor_date
        // entirely and restart from period_from.
        let gap_start = if job.found == 0 {
            job.period_from.clone()
        } else {
            match &job.cursor_date {
                Some(d) => crate::next_day(d),
                None => job.period_from.clone(),
            }
        };
        // L17-10: date_prefix, not a raw comparison -- see that fn's doc for why a bare
        // "YYYY-MM-DD" and a "YYYY-MM-DD HH:MM:SS" for the same day can't compare directly.
        if date_prefix(&gap_start) > date_prefix(&job.period_to) {
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

        // L16-12/AUD-171/DEC-084: bump only if this job actually widens the RFC's known
        // coverage -- see `ensancha_rango`'s own doc for why.
        let current_range = db::jobs::rfc_job_range(pool, &job.rfc).await?;
        let ensancha = ensancha_rango(current_range.as_ref(), &gap_start, &job.period_to);

        let new_id = db::jobs::insert_gap_continuation_row(
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
        if ensancha
            && let Err(e) = crate::services::response_cache::bump_version(pool, &job.rfc).await
        {
            tracing::warn!(rfc = %job.rfc, "Gap-detector: failed to bump cache version: {e}");
        }
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

async fn scan_activity_gaps(
    pool: &DbPool,
    cfg: &Arc<Config>,
    s3: &Arc<S3Client>,
) -> anyhow::Result<()> {
    let rfcs = db::jobs::distinct_completed_rfcs(pool).await?;
    if rfcs.is_empty() {
        return Ok(());
    }
    let creds: HashMap<String, String> = db::users::get_all_with_credentials(pool)
        .await?
        .into_iter()
        .collect();
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
        if date_prefix(&start) > date_prefix(&yesterday) {
            continue; // fully caught up, nothing new to scan yet
        }
        let candidate_end = add_days(&start, SCAN_WINDOW_DAYS);
        let end = date_min(&candidate_end, &yesterday).to_string();

        let gap_days = db::jobs::find_activity_gap_days(pool, &rfc, &start, &end).await?;
        if !gap_days.is_empty() {
            tracing::warn!(
                rfc = %rfc, count = gap_days.len(), days = ?gap_days,
                "Gap-detector: zero-activity weekdays found inside a completed sync range"
            );
        }

        for gap in &gap_days {
            let day = &gap.day;
            // A day can be a gap on only one side (Nubarium 2024-08-15/22:
            // 9 and 23 emitidas that day, zero recibidas) — resync just the
            // missing side instead of "ambos" so a healthy side's real
            // invoices aren't redundantly re-fetched every time.
            let dl_type = match (gap.emit_gap, gap.recv_gap) {
                (true, true) => "ambos",
                (true, false) => "emitidos",
                (false, true) => "recibidos",
                (false, false) => continue, // shouldn't happen — query only returns actual gaps
            };

            let attempts = db::jobs::count_gap_resync_attempts(pool, &rfc, day).await?;
            if attempts >= MAX_GAP_RESYNC_ATTEMPTS {
                tracing::warn!(
                    rfc = %rfc, day = %day, attempts,
                    "Gap-detector: giving up on this day — already re-checked and still empty"
                );
                continue;
            }

            let Some((auth_payload, _fiel_tmp)) =
                resolve_auth(pool, s3, &bucket, &rfc, &creds, &key).await
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

            // L16-12/AUD-171/DEC-084: measured live -- 1,612 gap-resync jobs, up to 130/hour
            // for one RFC, and 0 of them widened the RFC's coverage range (every one was a
            // day inside a range Pulso already considered synced -- that's the whole point
            // of a resync). Bumping on every one of those was 130 cache invalidations in an
            // hour for a coverage panel whose four numbers hadn't moved.
            let current_range = db::jobs::rfc_job_range(pool, &rfc).await?;
            let ensancha = ensancha_rango(current_range.as_ref(), &period_from, &period_to);

            let new_id = db::jobs::insert_queued_row(
                pool,
                "gap_resync",
                &rfc,
                &auth_type_label,
                &auth_enc,
                dl_type,
                &period_from,
                &period_to,
            )
            .await?;
            if ensancha
                && let Err(e) = crate::services::response_cache::bump_version(pool, &rfc).await
            {
                tracing::warn!(rfc = %rfc, "Gap-detector: failed to bump cache version: {e}");
            }
            tracing::warn!(rfc = %rfc, day = %day, job_id = %new_id, "Gap-detector: requeued zero-activity day for resync");
        }

        db::jobs::set_gap_scan_progress(pool, &rfc, &end).await?;
    }
    Ok(())
}

/// L17-10: `period_from`/`period_to`/cursor dates mix two formats across job types --
/// "YYYY-MM-DD" (10 chars) from most jobs, "YYYY-MM-DD HH:MM:SS" (19 chars) from this
/// file's own gap-resync jobs (`period_from`/`period_to` built as `"{day} 00:00:00"`/
/// `"{day} 23:59:59"` in `scan_activity_gaps` below). Comparing the two AS-IS is wrong: a
/// bare date always sorts less than the same day's `23:59:59` timestamp, so a job whose
/// coverage ends exactly on the last day of an already-covered range would read as "ends
/// later" (an unnecessary cache bump) purely from the trailing time-of-day, not because it
/// covers anything new. This file's own concept of "coverage" is day-granularity
/// throughout, never sub-day -- truncating both sides to their date-only prefix before
/// comparing makes that format-independent. Measured live: 0 of today's 1,613 resync jobs
/// currently hit the exact combination that would flip a verdict, but 6 jobs already carry
/// mismatched formats on start/end, so the trap exists in the data, just not triggered yet.
pub(crate) fn date_prefix(s: &str) -> &str {
    &s[..10.min(s.len())]
}

/// Same idea as `date_prefix`, for a `min()`-style pick that has to return one of the two
/// ORIGINAL strings (not a truncated copy) -- `scan_activity_gaps` persists whichever one
/// wins as gap-scan progress, so it needs the real value, only *compared* at date
/// granularity.
fn date_min<'a>(a: &'a str, b: &'a str) -> &'a str {
    if date_prefix(a) <= date_prefix(b) {
        a
    } else {
        b
    }
}

/// L16-12/AUD-171/DEC-084: true when a job covering `[new_from, new_to]` would actually
/// widen `current` (this RFC's existing `[min(period_from), max(period_to)]` across every
/// job it's ever had, any status -- `db::jobs::rfc_job_range`). `current: None` (no job at
/// all yet) is deliberately always `true`, not skipped as a no-op: comparing a real date
/// against a null min/max would read as "not wider" and a brand-new RFC's very first job
/// would never bump its own cache.
fn ensancha_rango(current: Option<&(String, String)>, new_from: &str, new_to: &str) -> bool {
    match current {
        None => true,
        Some((min_from, max_to)) => {
            date_prefix(new_from) < date_prefix(min_from)
                || date_prefix(new_to) > date_prefix(max_to)
        }
    }
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

/// "YYYY-MM-DD" for yesterday (UTC) — also used by routes/users.rs's
/// validate_clave_handler to build a single-day validation job.
pub(crate) fn yesterday_ymd() -> String {
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
        let leap_year = (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400);
        let days_in_year = if leap_year { 366 } else { 365 };
        if d < days_in_year {
            break;
        }
        d -= days_in_year;
        y += 1;
    }
    let leap_year = (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400);
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

#[cfg(test)]
mod l16_tests {
    use super::*;

    fn range(from: &str, to: &str) -> (String, String) {
        (from.to_string(), to.to_string())
    }

    #[test]
    fn ensancha_rango_true_when_rfc_has_no_prior_job() {
        // Trap called out by the item itself: comparing against a null min/max reads as
        // "not wider", which would leave a brand-new RFC's very first job unbumped.
        assert!(ensancha_rango(None, "2026-01-01", "2026-01-01"));
    }

    #[test]
    fn ensancha_rango_false_for_a_day_already_inside_the_covered_range() {
        let current = range("2023-01-01", "2026-08-31");
        assert!(!ensancha_rango(
            Some(&current),
            "2025-06-15 00:00:00",
            "2025-06-15 23:59:59"
        ));
    }

    #[test]
    fn ensancha_rango_true_when_the_new_period_starts_earlier() {
        let current = range("2023-01-01", "2026-08-31");
        assert!(ensancha_rango(Some(&current), "2022-12-01", "2023-01-01"));
    }

    #[test]
    fn ensancha_rango_true_when_the_new_period_ends_later() {
        let current = range("2023-01-01", "2026-08-31");
        assert!(ensancha_rango(Some(&current), "2026-09-01", "2026-09-30"));
    }

    // L17-10: the same day, on the `to` boundary, in all four short/long combinations --
    // a job covering exactly the last day of an already-covered range never "ensancha",
    // regardless of which format either side happens to be stored in. Before date_prefix,
    // 2 of these 4 gave the wrong answer (corto-contra-largo and largo-contra-corto): a
    // shorter string always sorts less than a longer one with the same prefix, so
    // "2025-06-15" < "2025-06-15 23:59:59" reads as "ends later" purely from string length,
    // even though both name the identical calendar day.
    #[test]
    fn ensancha_rango_same_day_on_the_to_boundary_agrees_across_formats() {
        let corto = "2025-06-15";
        let largo = "2025-06-15 23:59:59";

        let current_corto = range("2023-01-01", corto);
        let current_largo = range("2023-01-01", largo);

        // new_from stays inside the current range so only the `to` boundary is on trial.
        let new_from = "2023-06-01";

        // corto contra corto -- already correct before this item, included for symmetry.
        assert!(!ensancha_rango(Some(&current_corto), new_from, corto));
        // corto contra largo -- one of the two that used to fail.
        assert!(!ensancha_rango(Some(&current_corto), new_from, largo));
        // largo contra corto -- the other one that used to fail.
        assert!(!ensancha_rango(Some(&current_largo), new_from, corto));
        // largo contra largo -- already correct before this item, included for symmetry.
        assert!(!ensancha_rango(Some(&current_largo), new_from, largo));
    }

    #[test]
    fn date_prefix_truncates_long_format_and_passes_short_format_through() {
        assert_eq!(date_prefix("2025-06-15"), "2025-06-15");
        assert_eq!(date_prefix("2025-06-15 23:59:59"), "2025-06-15");
    }

    #[test]
    fn date_min_compares_at_day_granularity_not_string_length() {
        // Same trap as ensancha_rango: a bare date must not lose to a longer timestamp
        // naming the same or an earlier day.
        assert_eq!(date_min("2025-06-15", "2025-06-15 23:59:59"), "2025-06-15");
        assert_eq!(
            date_min("2025-06-14 23:59:59", "2025-06-15"),
            "2025-06-14 23:59:59"
        );
    }
}
