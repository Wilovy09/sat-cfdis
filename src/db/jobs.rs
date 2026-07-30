//! CRUD operations for `sync_jobs` and `job_invoices`.

use serde::Serialize;
use sqlx::PgPool;

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SyncJob {
    pub id: String,
    pub job_type: String,
    pub rfc: String,
    pub auth_type: String,
    /// AES-GCM encrypted JSON auth payload (never serialised to API clients)
    #[serde(skip)]
    pub auth_enc: String,
    pub dl_type: String,
    pub period_from: String,
    pub period_to: String,
    /// Last date (YYYY-MM-DD) fully processed; None = not started yet
    pub cursor_date: Option<String>,
    pub found: i64,
    /// Total invoices expected (from list-count pre-pass); None until count completes
    pub total_expected: Option<i64>,
    /// Live running tally while list-count is still counting; None once
    /// total_expected is set (final) or before counting has started.
    pub count_progress: Option<i64>,
    /// Date (YYYY-MM-DD) the list-count pre-pass is currently scanning —
    /// mirrors `cursor_date` but for the counting phase, not the download phase.
    pub count_cursor_date: Option<String>,
    pub status: String,
    /// Machine-readable classification of error_msg, from classifySatError() in
    /// cfdi-scraper: "invalid_credentials" | "captcha_failed" | "login_not_registered"
    /// | "fiel_login_failed" | "sat_connection_error" | "rate_limited" | "unknown_error".
    pub error_code: Option<String>,
    pub error_msg: Option<String>,
    /// ISO-8601 UTC — when the worker should resume this job
    pub resume_at: Option<String>,
    /// Automatic retries used for transient (non-SAT) failures — see
    /// `retry_transient_or_fail`. Index into RETRY_BACKOFF_SECS.
    pub retry_count: i32,
    pub created_at: String,
    pub updated_at: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_utc() -> String {
    // Simple UTC timestamp compatible with both SQLite and Postgres TEXT columns.
    // Format: "2026-04-15T14:30:00Z"
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    fmt_unix(secs)
}

fn fmt_unix(secs: u64) -> String {
    // Manual formatting avoids pulling in chrono just for this.
    const DAYS_IN_MONTH: [u64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut remaining = secs;
    let secs_of_day = remaining % 86400;
    remaining /= 86400;
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    // Days since 1970-01-01
    let mut year = 1970u64;
    loop {
        let leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        let days_in_year = if leap { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        year += 1;
    }
    let leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let mut month = 0usize;
    for (i, &dim) in DAYS_IN_MONTH.iter().enumerate() {
        let dim = if i == 1 && leap { 29 } else { dim };
        if remaining < dim {
            month = i;
            break;
        }
        remaining -= dim;
    }
    let day = remaining + 1;
    format!(
        "{year:04}-{:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z",
        month + 1
    )
}

/// Return ISO-8601 UTC timestamp `offset_secs` seconds from now.
pub fn utc_offset(offset_secs: u64) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    fmt_unix(secs + offset_secs)
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

/// Insert a new job record. Returns the job id.
pub async fn insert(
    pool: &PgPool,
    rfc: &str,
    auth_type: &str,
    auth_enc: &str,
    dl_type: &str,
    period_from: &str,
    period_to: &str,
) -> Result<String, sqlx::Error> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = now_utc();
    sqlx::query(
        r#"INSERT INTO pulso.sync_jobs
           (id, job_type, rfc, auth_type, auth_enc, dl_type,
            period_from, period_to, found, status, created_at, updated_at)
           VALUES ($1, 'list', $2, $3, $4, $5, $6, $7, 0, 'running', $8, $9)"#,
    )
    .bind(&id)
    .bind(rfc)
    .bind(auth_type)
    .bind(auth_enc)
    .bind(dl_type)
    .bind(period_from)
    .bind(period_to)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Mark a job as paused due to SAT download limit (or a transient SAT
/// connection error — see classifySatError() in cfdi-scraper).
/// `cursor_date` = last date successfully processed (YYYY-MM-DD).
/// `resume_at`   = when the worker should retry (24.5h for a real rate limit,
///                 much shorter for a transient connection error).
/// `error_code`  = machine-readable classification, e.g. "rate_limited" | "sat_connection_error".
pub async fn pause_limit(
    pool: &PgPool,
    job_id: &str,
    cursor_date: &str,
    found: i64,
    resume_at: &str,
    error_code: Option<&str>,
    error_msg: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.sync_jobs
           SET status='paused_limit', cursor_date=$1, found=$2,
               resume_at=$3, error_code=$4, error_msg=$5, updated_at=$6
           WHERE id=$7"#,
    )
    .bind(cursor_date)
    .bind(found)
    .bind(resume_at)
    .bind(error_code)
    .bind(error_msg)
    .bind(now_utc())
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a job as completed.
pub async fn complete(
    pool: &PgPool,
    job_id: &str,
    cursor_date: &str,
    found: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.sync_jobs
           SET status='completed', cursor_date=$1, found=$2, error_msg=NULL, updated_at=$3
           WHERE id=$4"#,
    )
    .bind(cursor_date)
    .bind(found)
    .bind(now_utc())
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a job as failed.
/// `error_code` = machine-readable classification (see classifySatError() in
/// cfdi-scraper), e.g. "invalid_credentials" | "fiel_login_failed"; `None`
/// for infra-level failures (crash, decrypt error) that aren't SAT-classified.
pub async fn fail(
    pool: &PgPool,
    job_id: &str,
    error_code: Option<&str>,
    error_msg: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.sync_jobs SET status='failed', error_code=$1, error_msg=$2, updated_at=$3 WHERE id=$4"#,
    )
    .bind(error_code)
    .bind(error_msg)
    .bind(now_utc())
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Backoff schedule for transient retries, approved by the partners:
/// 5min → 15min → 1h → 6h → 24h, then 24h again for a few more days before
/// giving up permanently. Index = current `retry_count`. Total span before
/// the final give-up is ~4.3 days — long enough to ride out a SAT outage
/// without hammering its login endpoint every few minutes forever (risk of
/// looking like abuse and getting the whole scraping IP blocked).
const RETRY_BACKOFF_SECS: [u64; 8] = [
    5 * 60,
    15 * 60,
    60 * 60,
    6 * 60 * 60,
    24 * 60 * 60,
    24 * 60 * 60,
    24 * 60 * 60,
    24 * 60 * 60,
];

/// Handle a transient failure — PHP worker idle timeout, crash before sending
/// `__done__`, or a `captcha_failed` auth error (an OCR miss on one attempt,
/// not a persistent credential problem — worth another try, unlike
/// `invalid_credentials`/`login_not_registered`/`fiel_login_failed`, which
/// call `fail()` directly and never reach here). Retries automatically
/// through the same paused_limit/resume_worker path already used for real
/// SAT rate limits — which re-logs in from scratch, giving the captcha a
/// fresh attempt — following RETRY_BACKOFF_SECS; once that schedule is
/// exhausted, fails permanently.
pub async fn retry_transient_or_fail(
    pool: &PgPool,
    job_id: &str,
    cursor_date: &str,
    found: i64,
    error_code: Option<&str>,
    error_msg: &str,
) -> Result<(), sqlx::Error> {
    let (retry_count,): (i32,) =
        sqlx::query_as(r#"SELECT retry_count FROM pulso.sync_jobs WHERE id = $1"#)
            .bind(job_id)
            .fetch_one(pool)
            .await?;

    let Some(&delay_secs) = RETRY_BACKOFF_SECS.get(retry_count as usize) else {
        return fail(
            pool,
            job_id,
            error_code,
            &format!("{error_msg} (tras {retry_count} reintentos automáticos)"),
        )
        .await;
    };

    let resume_at = utc_offset(delay_secs);
    sqlx::query(
        r#"UPDATE pulso.sync_jobs
           SET status='paused_limit', cursor_date=$1, found=$2, resume_at=$3,
               error_code=$4, error_msg=$5, retry_count=retry_count+1, updated_at=$6
           WHERE id=$7"#,
    )
    .bind(cursor_date)
    .bind(found)
    .bind(&resume_at)
    .bind(error_code)
    .bind(error_msg)
    .bind(now_utc())
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a running job as failed (used on server restart to clean up stale state).
pub async fn reset_stale_running(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let r = sqlx::query(
        r#"UPDATE pulso.sync_jobs SET status='queued', updated_at=$1 WHERE status='running'"#,
    )
    .bind(now_utc())
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}

/// Store the pre-count total from the list-count pass.
pub async fn set_total_expected(
    pool: &PgPool,
    job_id: &str,
    total: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.sync_jobs SET total_expected=$1, updated_at=$2 WHERE id=$3"#,
    )
    .bind(total)
    .bind(now_utc())
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Live running tally while the list-count pre-pass is still in progress —
/// lets the frontend show a growing count instead of a static spinner while
/// waiting (a full multi-year range can take hours). Overwritten on every
/// day the pre-pass processes; irrelevant once `set_total_expected` lands.
pub async fn update_count_progress(
    pool: &PgPool,
    job_id: &str,
    count_so_far: i64,
    count_date: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.sync_jobs SET count_progress=$1, count_cursor_date=$2, updated_at=$3 WHERE id=$4"#,
    )
    .bind(count_so_far)
    .bind(count_date)
    .bind(now_utc())
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Update found count in place (called as invoices stream in).
pub async fn update_found(
    pool: &PgPool,
    job_id: &str,
    found: i64,
    cursor_date: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.sync_jobs SET found=$1, cursor_date=$2, updated_at=$3 WHERE id=$4"#,
    )
    .bind(found)
    .bind(cursor_date)
    .bind(now_utc())
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Upsert an invoice row (safe to call multiple times for same uuid).
pub async fn upsert_invoice(
    pool: &PgPool,
    job_id: &str,
    uuid: &str,
    metadata: &str,
) -> Result<(), sqlx::Error> {
    let uuid_upper = uuid.to_uppercase();
    sqlx::query(r#"INSERT INTO pulso.job_invoices (job_id, uuid, metadata) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"#)
        .bind(job_id)
        .bind(&uuid_upper)
        .bind(metadata)
        .execute(pool)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

pub async fn list_all(pool: &PgPool) -> Result<Vec<SyncJob>, sqlx::Error> {
    sqlx::query_as::<_, SyncJob>(
        r#"SELECT * FROM pulso.sync_jobs ORDER BY created_at DESC LIMIT 200"#,
    )
    .fetch_all(pool)
    .await
}

pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<SyncJob>, sqlx::Error> {
    sqlx::query_as::<_, SyncJob>(r#"SELECT * FROM pulso.sync_jobs WHERE id=$1"#)
        .bind(id)
        .fetch_optional(pool)
        .await
}

/// Jobs ready to be resumed by the background worker.
pub async fn find_resumable(pool: &PgPool) -> Result<Vec<SyncJob>, sqlx::Error> {
    let now = now_utc();
    sqlx::query_as::<_, SyncJob>(
        r#"SELECT * FROM pulso.sync_jobs
           WHERE status='paused_limit' AND resume_at <= $1
           ORDER BY resume_at ASC"#,
    )
    .bind(now)
    .fetch_all(pool)
    .await
}

/// Jobs enqueued (status=queued) waiting for a first run.
pub async fn find_queued(pool: &PgPool) -> Result<Vec<SyncJob>, sqlx::Error> {
    sqlx::query_as::<_, SyncJob>(
        r#"SELECT * FROM pulso.sync_jobs WHERE status='queued' ORDER BY created_at ASC"#,
    )
    .fetch_all(pool)
    .await
}

/// True if a non-cancelled/non-failed job already covers this exact period for the RFC.
pub async fn has_job_for_period(
    pool: &PgPool,
    rfc: &str,
    period_from: &str,
    period_to: &str,
) -> Result<bool, sqlx::Error> {
    let (exists,): (bool,) = sqlx::query_as(
        r#"SELECT EXISTS(
               SELECT 1 FROM pulso.sync_jobs
               WHERE rfc = $1 AND period_from = $2 AND period_to = $3
               AND status NOT IN ('cancelled')
           )"#,
    )
    .bind(rfc)
    .bind(period_from)
    .bind(period_to)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Mark a job as running.
pub async fn set_running(pool: &PgPool, job_id: &str) -> Result<(), sqlx::Error> {
    sqlx::query(r#"UPDATE pulso.sync_jobs SET status='running', updated_at=$1 WHERE id=$2"#)
        .bind(now_utc())
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Insert a new job with status 'queued' (will be picked up by the background worker).
/// `job_type`: `"list"` for manual jobs, `"auto_daily"` for automatic daily sync.
pub async fn insert_queued(
    pool: &PgPool,
    job_type: &str,
    rfc: &str,
    auth_type: &str,
    auth_enc: &str,
    dl_type: &str,
    period_from: &str,
    period_to: &str,
) -> Result<String, sqlx::Error> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = now_utc();
    sqlx::query(
        r#"INSERT INTO pulso.sync_jobs
           (id, job_type, rfc, auth_type, auth_enc, dl_type,
            period_from, period_to, found, status, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'queued', $9, $10)"#,
    )
    .bind(&id)
    .bind(job_type)
    .bind(rfc)
    .bind(auth_type)
    .bind(auth_enc)
    .bind(dl_type)
    .bind(period_from)
    .bind(period_to)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Paginated invoice results for a job.
/// Most relevant job for a given RFC: active jobs (running > queued > paused_limit) first,
/// then most recent by created_at. Used by sync_status to surface newer admin-queued jobs
/// that aren't linked to initial_sync_job_id.
pub async fn get_active_for_rfc(pool: &PgPool, rfc: &str) -> Result<Option<SyncJob>, sqlx::Error> {
    sqlx::query_as::<_, SyncJob>(
        r#"SELECT * FROM pulso.sync_jobs
           WHERE rfc = $1
             AND status IN ('running', 'queued', 'paused_limit')
           ORDER BY
             CASE status
               WHEN 'running'      THEN 0
               WHEN 'queued'       THEN 1
               WHEN 'paused_limit' THEN 2
             END,
             created_at DESC
           LIMIT 1"#,
    )
    .bind(rfc.to_uppercase())
    .fetch_optional(pool)
    .await
}

pub async fn get_invoices(
    pool: &PgPool,
    job_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query_scalar::<_, String>(
        r#"SELECT metadata FROM pulso.job_invoices WHERE job_id=$1 ORDER BY uuid LIMIT $2 OFFSET $3"#,
    )
    .bind(job_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
