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
    pub status: String,
    pub error_msg: Option<String>,
    /// ISO-8601 UTC — when the worker should resume this job
    pub resume_at: Option<String>,
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

/// Mark a job as paused due to SAT download limit.
/// `cursor_date` = last date successfully processed (YYYY-MM-DD).
/// `resume_at`   = when the worker should retry (typically +24.5 h).
pub async fn pause_limit(
    pool: &PgPool,
    job_id: &str,
    cursor_date: &str,
    found: i64,
    resume_at: &str,
    error_msg: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.sync_jobs
           SET status='paused_limit', cursor_date=$1, found=$2,
               resume_at=$3, error_msg=$4, updated_at=$5
           WHERE id=$6"#,
    )
    .bind(cursor_date)
    .bind(found)
    .bind(resume_at)
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
           SET status='completed', cursor_date=$1, found=$2, updated_at=$3
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
pub async fn fail(pool: &PgPool, job_id: &str, error_msg: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.sync_jobs SET status='failed', error_msg=$1, updated_at=$2 WHERE id=$3"#,
    )
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
               AND status NOT IN ('cancelled', 'failed')
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
pub async fn insert_queued(
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
           VALUES ($1, 'list', $2, $3, $4, $5, $6, $7, 0, 'queued', $8, $9)"#,
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

/// Paginated invoice results for a job.
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
