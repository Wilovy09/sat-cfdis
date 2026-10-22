mod api_docs;
mod config;
mod db;
mod errors;
mod routes;
mod services;
mod state;

use actix_cors::Cors;
use actix_web::{App, HttpServer, web};
use aws_sdk_s3::Client as S3Client;
use std::sync::Arc;
use tracing::info;
use tracing_actix_web::TracingLogger;
use tracing_subscriber::{EnvFilter, fmt::format};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

use config::Config;
use db::DbPool;
use routes::{
    analytics as analytics_routes, auth as auth_routes, billing as billing_routes,
    fiel as fiel_routes, invoices, logs as logs_routes, queue as queue_routes,
    system as system_routes, users as users_routes,
};
use services::etl;
use state::CaptchaMap;

// ---------------------------------------------------------------------------
// FIEL auth helper — checks DB for FIEL credentials and converts DER → PEM
// ---------------------------------------------------------------------------

/// If the RFC has FIEL configured, downloads .cer/.key from S3, decrypts the
/// password, converts DER to PEM via openssl, and returns a FIEL auth payload
/// together with the TempDir that holds the PEM files.
///
/// The caller must keep the returned `TempDir` alive until the PHP process exits.
/// Returns `None` on any failure (FIEL not configured, S3 error, bad password, …).
pub(crate) async fn try_fiel_auth(
    pool: &DbPool,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    rfc: &str,
) -> Option<(serde_json::Value, tempfile::TempDir)> {
    // L19-03: a stored FIEL only authenticates for a verified owner. Until the current owner
    // proves their own CIEC, a FIEL left behind (or uploaded by them before verification)
    // must not open the SAT on their behalf -- the job's CIEC has to stand on its own.
    match db::users::is_owner_verified(pool, rfc).await {
        Ok(true) => {}
        Ok(false) => return None,
        Err(e) => {
            tracing::error!(rfc = %rfc, "FIEL check: owner verification lookup failed: {e}");
            return None;
        }
    }

    let row = db::fiel::get(pool, rfc)
        .await
        .map_err(|e| tracing::error!(rfc = %rfc, "FIEL check: DB error: {e}"))
        .ok()??;

    let cert_bytes = services::s3::get_fiel(s3, bucket, &row.cert_s3_key)
        .await
        .or_else(|| {
            tracing::error!(rfc = %rfc, "FIEL: S3 cert download failed");
            None
        })?;

    let key_bytes = services::s3::get_fiel(s3, bucket, &row.key_s3_key)
        .await
        .or_else(|| {
            tracing::error!(rfc = %rfc, "FIEL: S3 key download failed");
            None
        })?;

    let enc_key = services::crypto::load_key();
    let password = services::crypto::decrypt(&enc_key, &row.password_enc)
        .map_err(|e| tracing::error!(rfc = %rfc, "FIEL: decrypt password failed: {e}"))
        .ok()?;

    use base64::Engine as _;
    let cert_b64 = base64::engine::general_purpose::STANDARD.encode(&cert_bytes);
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(&key_bytes);

    let tmp = tempfile::TempDir::new()
        .map_err(|e| tracing::error!(rfc = %rfc, "FIEL: TempDir creation failed: {e}"))
        .ok()?;

    let (cert_pem, key_pem) =
        services::fiel::der_to_pem(&cert_b64, &key_b64, &password, tmp.path())
            .await
            .map_err(|e| tracing::error!(rfc = %rfc, "FIEL: der_to_pem failed: {e}"))
            .ok()?;

    let auth = serde_json::json!({
        "type":          "fiel",
        "cert_pem_path": cert_pem.to_string_lossy(),
        "key_pem_path":  key_pem.to_string_lossy(),
        "password":      "",
    });

    Some((auth, tmp))
}

/// The CIEC a job authenticates with, when its auth payload is a CIEC for the job's own
/// RFC. `None` for anything else (a FIEL payload, or a CIEC for a different RFC).
fn job_ciec_password(auth_payload: &serde_json::Value, job_rfc: &str) -> Option<String> {
    let is_ciec = auth_payload["type"].as_str() == Some("ciec");
    let same_rfc = auth_payload["rfc"]
        .as_str()
        .is_some_and(|r| r.eq_ignore_ascii_case(job_rfc));
    if !(is_ciec && same_rfc) {
        return None;
    }
    auth_payload["password"].as_str().map(str::to_string)
}

/// L19-01 point 1 / L19-03 point 1: called once the SAT accepted a job's CIEC login. Marks
/// the RFC's active owner verified only if that CIEC is the owner's current one -- a job
/// queued with a previous owner's CIEC proves nothing about whoever owns the RFC now.
async fn verify_owner_after_sat_login(pool: &DbPool, rfc: &str, job_ciec: &str) {
    let (owner_row_id, clave_enc) = match db::users::get_unverified_owner(pool, rfc).await {
        Ok(Some(owner)) => owner,
        Ok(None) => return,
        Err(e) => {
            tracing::error!(rfc = %rfc, "Owner verification: DB error: {e}");
            return;
        }
    };
    let key = services::crypto::load_key();
    let owner_ciec = match services::crypto::decrypt(&key, &clave_enc) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(rfc = %rfc, "Owner verification: decrypt failed: {e}");
            return;
        }
    };
    if owner_ciec != job_ciec {
        tracing::warn!(rfc = %rfc, "SAT login passed with a CIEC that isn't the current owner's -- owner stays unverified");
        return;
    }
    match db::users::set_owner_verified(pool, &owner_row_id).await {
        Ok(true) => {
            tracing::info!(rfc = %rfc, "RFC owner verified: SAT login passed with their CIEC")
        }
        Ok(false) => {}
        Err(e) => tracing::error!(rfc = %rfc, "Owner verification: update failed: {e}"),
    }
}

/// L19-04 point 2: the scraper's stderr is logged as-is. Reviewed: it never prints the CIEC,
/// the FIEL passphrase or key contents (only library error templates, codes, dates and temp
/// paths) -- this is the backstop in case a future library message echoes the CIEC back.
fn redact_secret(line: &str, secret: Option<&str>) -> String {
    match secret {
        Some(s) if !s.is_empty() => line.replace(s, "[REDACTED]"),
        _ => line.to_string(),
    }
}

/// Whether a scraper output line is the one-time `{"__auth_ok__":true}` login signal.
fn is_sat_login_ok(line: &serde_json::Value) -> bool {
    line.get("__auth_ok__")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Background worker — resumes paused_limit jobs after 24.5 h
// ---------------------------------------------------------------------------

/// How often the worker wakes up to check for resumable jobs (seconds).
const WORKER_POLL_SECS: u64 = 30;

async fn resume_worker(
    pool: DbPool,
    cfg: Arc<Config>,
    s3_client: Arc<S3Client>,
    counting: services::reconciliation::CountingRfcs,
) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(WORKER_POLL_SECS)).await;

        // Collect both queued (new) and paused_limit (SAT limit hit) jobs
        let queued = match db::jobs::find_queued(&pool).await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::error!("Worker: DB error finding queued jobs: {e}");
                vec![]
            }
        };

        let resumable = match db::jobs::find_resumable(&pool).await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::error!("Worker: DB error finding resumable jobs: {e}");
                vec![]
            }
        };

        // list jobs have priority over auto_daily per RFC.
        // If any RFC has a list job pending (queued or paused_limit), skip auto_daily for that RFC.
        let rfcs_with_list: std::collections::HashSet<String> = queued
            .iter()
            .chain(resumable.iter())
            .filter(|j| j.job_type == "list")
            .map(|j| j.rfc.clone())
            .collect();

        let all_jobs: Vec<_> = queued
            .into_iter()
            .chain(resumable)
            .filter(|j| !(j.job_type == "auto_daily" && rfcs_with_list.contains(&j.rfc)))
            .collect();

        for job in all_jobs {
            // Enforce download order: within the same RFC, `list` jobs run
            // strictly in creation order (e.g. emitidos fully before
            // recibidos, per the user's onboarding priority) — skip this one
            // if an earlier-created list job for the same RFC hasn't reached
            // a terminal state yet. It'll be picked up automatically once
            // that earlier job completes or fails.
            if job.job_type == "list" {
                match db::jobs::has_earlier_active_list_job(&pool, &job.rfc, &job.created_at).await
                {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(e) => {
                        tracing::error!(job_id = %job.id, "Worker: earlier-job check failed: {e}");
                        continue;
                    }
                }
            }

            // The reconciliation worker has a SAT counting session open for this RFC; a
            // second concurrent login would make the SAT reset one of them. Picked up on
            // the next poll.
            if counting
                .lock()
                .map(|set| set.contains(&job.rfc))
                .unwrap_or(false)
            {
                continue;
            }

            let label = if job.status == "queued" {
                "Starting queued job"
            } else {
                "Resuming paused job"
            };
            tracing::info!(job_id = %job.id, rfc = %job.rfc, "{label}");

            if let Err(e) = db::jobs::set_running(&pool, &job.id).await {
                tracing::error!(job_id = %job.id, "Worker: could not set running: {e}");
                continue;
            }

            // Decrypt credentials
            let key = services::crypto::load_key();
            let auth_json = match services::crypto::decrypt(&key, &job.auth_enc) {
                Ok(j) => j,
                Err(e) => {
                    tracing::error!(job_id = %job.id, "Worker: decrypt failed: {e}");
                    let _ =
                        db::jobs::fail(&pool, &job.id, None, &format!("Decrypt failed: {e}")).await;
                    continue;
                }
            };

            let auth_payload: serde_json::Value = match serde_json::from_str(&auth_json) {
                Ok(v) => v,
                Err(e) => {
                    let _ =
                        db::jobs::fail(&pool, &job.id, None, &format!("Bad auth JSON: {e}")).await;
                    continue;
                }
            };

            // If RFC has FIEL configured (verified owners only, see try_fiel_auth), override
            // CIEC auth with FIEL. _fiel_tmp must stay alive until run_worker_chunk returns.
            // `own_ciec` is set only when the job logs in with its own CIEC -- the one
            // credential a successful login can verify the owner with.
            let bucket = cfg.s3_bucket.clone().unwrap_or_default();
            let (auth_payload, _fiel_tmp, own_ciec) =
                match try_fiel_auth(&pool, &s3_client, &bucket, &job.rfc).await {
                    Some((fiel_auth, tmp)) => {
                        tracing::info!(rfc = %job.rfc, "Worker: using FIEL auth");
                        (fiel_auth, Some(tmp), None)
                    }
                    None => {
                        let own_ciec = job_ciec_password(&auth_payload, &job.rfc);
                        (auth_payload, None::<tempfile::TempDir>, own_ciec)
                    }
                };

            // Queued jobs start from period_from; paused jobs resume from day after cursor
            let resume_from = match &job.cursor_date {
                Some(d) => next_day(d),
                None => job.period_from.clone(),
            };

            // L17-10/L18-25: extracted to gap_detector::job_is_complete so this comparison
            // has a real test -- see that function's own doc for why date_prefix, not a raw
            // comparison, decides it.
            if services::gap_detector::job_is_complete(&resume_from, &job.period_to) {
                let _ = db::jobs::complete(
                    &pool,
                    &job.id,
                    &job.rfc,
                    job.cursor_date.as_deref().unwrap_or(&job.period_to),
                    job.found,
                )
                .await;
                continue;
            }

            tracing::info!(
                job_id = %job.id,
                from = %resume_from,
                to   = %job.period_to,
                "Worker: running chunk"
            );

            run_worker_chunk(
                pool.clone(),
                cfg.clone(),
                s3_client.clone(),
                job.id.clone(),
                job.job_type.clone(),
                job.rfc.clone(),
                auth_payload,
                own_ciec,
                job.period_from.clone(),
                resume_from,
                job.period_to.clone(),
                job.dl_type.clone(),
                job.found,
                job.total_expected,
            )
            .await;
        }
    }
}

fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// Returns the next day in ISO-8601 format ("YYYY-MM-DD HH:MM:SS").
fn next_day(date_str: &str) -> String {
    // Parse YYYY-MM-DD from first 10 chars
    let ymd = &date_str[..10.min(date_str.len())];
    let parts: Vec<&str> = ymd.split('-').collect();
    if parts.len() < 3 {
        return date_str.to_string();
    }
    let Ok(y) = parts[0].parse::<u32>() else {
        return date_str.to_string();
    };
    let Ok(m) = parts[1].parse::<u32>() else {
        return date_str.to_string();
    };
    let Ok(d) = parts[2].parse::<u32>() else {
        return date_str.to_string();
    };

    let (ny, nm, nd) = if d >= days_in_month(y, m) {
        if m == 12 {
            (y + 1, 1, 1)
        } else {
            (y, m + 1, 1)
        }
    } else {
        (y, m, d + 1)
    };

    format!("{ny:04}-{nm:02}-{nd:02} 00:00:00")
}

// ---------------------------------------------------------------------------
// Daily auto-sync worker
// ---------------------------------------------------------------------------

/// How often the daily worker wakes to check whether yesterday has been synced (1 h).
const DAILY_POLL_SECS: u64 = 3600;
/// Mexico's central time (UTC-6, no daylight saving since 2022).
const MEXICO_UTC_OFFSET_SECS: u64 = 6 * 3600;
/// Extra days each auto_daily job looks back, to pick up CFDIs stamped after their day was
/// already requested.
const DAILY_LOOKBACK_DAYS: u64 = 2;

/// Every day, queue a one-day sync job for yesterday for every registered user
/// whose credentials are stored and whose yesterday period hasn't been synced yet.
async fn daily_sync_worker(pool: DbPool) {
    // Short initial delay so the main worker gets a head start on startup.
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;

    loop {
        // Pulso_Propuestas_Integridad_Descarga_RFC.md 3.5: "yesterday" in Mexico time, not
        // UTC -- in UTC the day rolled over at 18:00 local, so a day was requested while it
        // was still running. And the window reaches DAILY_LOOKBACK_DAYS further back: a CFDI
        // stamped late (the SAT allows up to 72 h) after its day was requested was never
        // looked for again -- 4 emitidos of one client RFC in a single month went missing so.
        let now_secs = {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        };
        let mexico_yesterday_secs = now_secs.saturating_sub(MEXICO_UTC_OFFSET_SECS + 86400);
        let yesterday = services::gap_detector::ymd_from_epoch_secs(mexico_yesterday_secs);
        let window_start = services::gap_detector::ymd_from_epoch_secs(
            mexico_yesterday_secs.saturating_sub(DAILY_LOOKBACK_DAYS * 86400),
        );

        let period_from = format!("{window_start} 00:00:00");
        let period_to = format!("{yesterday} 23:59:59");

        let users = match db::users::get_all_with_credentials(&pool).await {
            Ok(u) => u,
            Err(e) => {
                tracing::error!("Daily worker: DB error fetching users: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(DAILY_POLL_SECS)).await;
                continue;
            }
        };

        let key = services::crypto::load_key();

        for (rfc, clave_enc) in users {
            let already_queued =
                match db::jobs::has_job_for_period(&pool, &rfc, &period_from, &period_to).await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(rfc = %rfc, "Daily worker: period check failed: {e}");
                        continue;
                    }
                };

            if already_queued {
                continue;
            }

            // SYNC-2: first auto_daily run for this RFC — catch up from
            // where the historical load actually ended instead of just
            // asking for yesterday, so the gap between "history finished"
            // and "daily mode got enabled" doesn't go unrequested forever.
            // Every later cycle finds has_any_daily_job = true and falls
            // straight through to the normal single-day period below.
            let effective_period_from = match db::jobs::has_any_daily_job(&pool, &rfc).await {
                Ok(false) => match db::jobs::latest_historical_period_to(&pool, &rfc).await {
                    Ok(Some(hist_to)) => {
                        let gap_start = next_day(&hist_to);
                        // L17-10: date_prefix, not a raw comparison -- see
                        // gap_detector::date_prefix's own doc for why "YYYY-MM-DD" and
                        // "YYYY-MM-DD HH:MM:SS" for the same day can't compare directly.
                        if services::gap_detector::date_prefix(&gap_start)
                            < services::gap_detector::date_prefix(&period_from)
                        {
                            gap_start
                        } else {
                            period_from.clone()
                        }
                    }
                    _ => period_from.clone(),
                },
                _ => period_from.clone(),
            };

            let clave = match services::crypto::decrypt(&key, &clave_enc) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(rfc = %rfc, "Daily worker: decrypt failed: {e}");
                    continue;
                }
            };

            let auth_json = serde_json::json!({
                "type": "ciec",
                "rfc":  rfc,
                "password": clave,
            })
            .to_string();

            let auth_enc = match services::crypto::encrypt(&key, &auth_json) {
                Ok(e) => e,
                Err(e) => {
                    tracing::error!(rfc = %rfc, "Daily worker: encrypt failed: {e}");
                    continue;
                }
            };

            match db::jobs::insert_queued(
                &pool,
                "auto_daily",
                &rfc,
                "ciec",
                &auth_enc,
                "ambos",
                &effective_period_from,
                &period_to,
            )
            .await
            {
                Ok(job_id) => {
                    tracing::info!(
                        rfc = %rfc,
                        job_id = %job_id,
                        date = %yesterday,
                        "Daily auto-sync queued"
                    );
                }
                Err(e) => {
                    tracing::error!(rfc = %rfc, "Daily worker: insert_queued failed: {e}");
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(DAILY_POLL_SECS)).await;
    }
}

/// Returns true if the date string "YYYY-MM-DD …" falls on the last day of its month.
fn is_last_day_of_month(date_str: &str) -> bool {
    // Expect at least "YYYY-MM-DD"
    if date_str.len() < 10 {
        return false;
    }
    let parts: Vec<&str> = date_str[..10].split('-').collect();
    if parts.len() != 3 {
        return false;
    }
    let (Ok(y), Ok(m), Ok(d)) = (
        parts[0].parse::<u32>(),
        parts[1].parse::<u32>(),
        parts[2].parse::<u32>(),
    ) else {
        return false;
    };
    let last = days_in_month(y, m);
    d == last
}

/// Build a Spanish month label like "Julio 2026" from "YYYY-MM-DD …".
fn month_label_es(date_str: &str) -> String {
    const MONTHS: [&str; 12] = [
        "Enero",
        "Febrero",
        "Marzo",
        "Abril",
        "Mayo",
        "Junio",
        "Julio",
        "Agosto",
        "Septiembre",
        "Octubre",
        "Noviembre",
        "Diciembre",
    ];
    if date_str.len() < 7 {
        return date_str.to_string();
    }
    let parts: Vec<&str> = date_str[..7].split('-').collect();
    if parts.len() != 2 {
        return date_str.to_string();
    }
    let year = parts[0];
    let month_idx: usize = parts[1].parse::<usize>().unwrap_or(0);
    if month_idx == 0 || month_idx > 12 {
        return date_str.to_string();
    }
    format!("{} {}", MONTHS[month_idx - 1], year)
}

/// Max time to wait for the next line from the PHP scraper subprocess before
/// treating it as hung (SAT stalled, dead connection) and killing it. Without
/// this, a stuck child blocks resume_worker's sequential job loop forever.
const PHP_IDLE_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Run the list-count PHP pass and return the total invoice count.
/// Only counts complete calendar months within the period.
/// Returns None on any failure (count is best-effort; stream will run regardless).
/// The per-month counts it gathers are kept for the RFC's reconciliation against the SAT
/// (services::reconciliation) -- one SAT pass serves both.
#[allow(clippy::too_many_arguments)]
async fn run_count_pass(
    pool: &DbPool,
    cfg: &Config,
    job_id: &str,
    job_rfc: &str,
    auth_payload: &serde_json::Value,
    own_ciec: Option<&str>,
    period_from: &str,
    period_to: &str,
    dl_type: &str,
) -> Option<i64> {
    let (login_tx, login_rx) = tokio::sync::oneshot::channel::<()>();
    let login_ok = own_ciec.map(|ciec| {
        let pool = pool.clone();
        let rfc = job_rfc.to_string();
        let ciec = ciec.to_string();
        tokio::spawn(async move {
            if login_rx.await.is_ok() {
                verify_owner_after_sat_login(&pool, &rfc, &ciec).await;
            }
        });
        login_tx
    });

    let count = services::reconciliation::run_list_count(
        cfg,
        auth_payload,
        period_from,
        period_to,
        dl_type,
        Some(services::reconciliation::CountProgress { pool, job_id }),
        login_ok,
    )
    .await?;
    if let Some(code) = &count.auth_error {
        tracing::warn!(job_id = %job_id, code = %code, "list-count stopped: SAT login rejected");
        return None;
    }
    if !count.error_days.is_empty() {
        // Still fine as the progress bar's denominator; reconciliation won't trust it.
        tracing::warn!(job_id = %job_id, months = count.error_days.len(), "list-count: some days went unanswered, total may be short");
    }
    count.record(pool, job_rfc).await;
    Some(count.total)
}

/// Emails the client when their sync job becomes permanently inaccessible —
/// exhausted the retry backoff, or hit a non-retryable auth error. Mirrors
/// the completion-email pattern in run_worker_chunk; best-effort, never
/// blocks or fails the worker loop.
async fn notify_sync_failed(
    pool: &DbPool,
    cfg: &Config,
    job_id: &str,
    rfc: &str,
    error_code: Option<&str>,
) {
    let Some(ref api_key) = cfg.sendgrid_api_key else {
        return;
    };
    let Ok(Some((user_id, email))) = crate::db::users::get_owner_contact_by_rfc(pool, rfc).await
    else {
        return;
    };
    match crate::services::email::send_sync_failed(
        api_key,
        &cfg.sendgrid_from,
        &email,
        rfc,
        error_code,
    )
    .await
    {
        Ok(_) => tracing::info!(job_id = %job_id, user_id = %user_id, "Sent sync-failed email"),
        Err(e) => tracing::warn!(job_id = %job_id, "Failed to send sync-failed email: {e}"),
    }
}

/// Run one PHP list-stream chunk for a background worker job.
/// Results go to DB (job_invoices) and S3/local storage.
/// No SSE — silent background processing.
#[allow(clippy::too_many_arguments)]
async fn run_worker_chunk(
    pool: DbPool,
    cfg: Arc<Config>,
    s3: Arc<S3Client>,
    job_id: String,
    job_type: String,
    job_rfc: String,
    auth_payload: serde_json::Value,
    own_ciec: Option<String>, // the job's CIEC, when it logs in with it (not a FIEL)
    full_period_from: String, // original job start (for count pass)
    period_from: String,      // resume point (may differ from full_period_from)
    period_to: String,
    dl_type: String,
    initial_found: i64,
    total_expected: Option<i64>,
) {
    use std::process::Stdio;
    use tokio::io::AsyncBufReadExt as _;
    use tokio::io::AsyncWriteExt as _;

    // Count pass: only for non-auto_daily jobs where total is still unknown.
    if job_type != "auto_daily" && total_expected.is_none() {
        tracing::info!(job_id = %job_id, "Starting list-count pre-pass");
        match run_count_pass(
            &pool,
            &cfg,
            &job_id,
            &job_rfc,
            &auth_payload,
            own_ciec.as_deref(),
            &full_period_from,
            &period_to,
            &dl_type,
        )
        .await
        {
            Some(total) => {
                tracing::info!(job_id = %job_id, total = total, "list-count complete");
                let _ = db::jobs::set_total_expected(&pool, &job_id, total).await;
            }
            None => {
                tracing::warn!(job_id = %job_id, "list-count returned no result, skipping");
            }
        }
    }

    let payload = serde_json::json!({
        "command": "list-stream",
        "auth":    auth_payload,
        "params": {
            "period_from":       period_from,
            "period_to":         period_to,
            "download_type":     dl_type,
            "auto_download_xml": false,
        }
    });

    let mut input_bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            let _ = db::jobs::fail(&pool, &job_id, None, &e.to_string()).await;
            return;
        }
    };
    input_bytes.push(b'\n');

    let mut cmd = tokio::process::Command::new(&cfg.php_bin);
    cmd.arg(&cfg.php_cli_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(ref proxy) = cfg.https_proxy {
        cmd.env("HTTPS_PROXY", proxy).env("https_proxy", proxy);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let _ = db::jobs::fail(&pool, &job_id, None, &e.to_string()).await;
            return;
        }
    };

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(&input_bytes).await;
    }

    // Drain stderr so it never blocks and errors are visible in traces.
    // Classification comes from the structured __auth_error__/__limit_reached__
    // stdout events below (see classifySatError() in cfdi-scraper) — not from
    // sniffing this raw text, which would misclassify a transient SAT
    // connection error (message contains "login data") as a bad password.
    if let Some(stderr) = child.stderr.take() {
        let job_id_err = job_id.clone();
        let ciec = auth_payload["password"]
            .as_str()
            .filter(|p| !p.is_empty())
            .map(str::to_string);
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt as _;
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if !line.is_empty() {
                    let line = redact_secret(&line, ciec.as_deref());
                    tracing::error!(job_id = %job_id_err, php_stderr = %line, "PHP worker stderr");
                }
            }
        });
    }

    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => {
            let _ = db::jobs::fail(&pool, &job_id, None, "no stdout").await;
            return;
        }
    };

    let reader = tokio::io::BufReader::new(stdout);
    let mut lines = reader.lines();
    let mut found = initial_found;
    let mut cursor = period_from.clone();
    let mut limit_hit = false;
    let mut limit_code: Option<String> = None;
    let mut limit_reason: Option<String> = None;
    let mut limit_retry_after: u64 = 24 * 3600 + 1800; // fallback: 24.5h
    let mut auth_error: Option<(String, String)> = None; // (code, message)
    let mut done_received = false;
    let mut idle_timeout = false;
    let mut pending_ciec = own_ciec;

    loop {
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(PHP_IDLE_TIMEOUT_SECS),
            lines.next_line(),
        )
        .await;
        let line = match next {
            Ok(Ok(Some(line))) => line,
            Ok(Ok(None)) => break,
            Ok(Err(_)) => break,
            Err(_) => {
                tracing::error!(job_id = %job_id, "PHP worker idle timeout, killing");
                let _ = child.start_kill();
                idle_timeout = true;
                break;
            }
        };
        if line.is_empty() {
            continue;
        }
        let Ok(data) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };

        if is_sat_login_ok(&data) {
            if let Some(ciec) = pending_ciec.take() {
                verify_owner_after_sat_login(&pool, &job_rfc, &ciec).await;
            }
            continue;
        }

        if data
            .get("__keepalive__")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            continue;
        }

        // Auto-downloaded XML — save to storage and count as found invoice
        if data
            .get("__xml_ready__")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let uuid_str = data["uuid"].as_str().unwrap_or("").to_string();
            let xml_b64 = data["xml_b64"].as_str().unwrap_or("").to_string();
            let s3_ref = s3.clone();
            let bucket = cfg.s3_bucket.clone().unwrap_or_default();
            let uuid_for_upload = uuid_str.clone();
            tokio::spawn(async move {
                let uuid_str = uuid_for_upload;
                use base64::Engine as _;
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(&xml_b64) {
                    let should_upload = cfg!(debug_assertions) || !bucket.is_empty();
                    if should_upload {
                        let (rfc_e, rfc_r, year, month, day) =
                            crate::routes::invoices::extract_cfdi_path_info(&bytes);
                        let _ = crate::services::storage::upload(
                            &s3_ref,
                            &bucket,
                            &rfc_e,
                            &rfc_r,
                            year,
                            month,
                            day,
                            &uuid_str.to_lowercase(),
                            bytes,
                        )
                        .await;
                    }
                }
            });

            // XML already counted when its metadata line arrived — no found increment here.
            continue;
        }

        if data
            .get("__auth_error__")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let code = data["code"].as_str().unwrap_or("unknown_error").to_string();
            let reason = data["reason"].as_str().unwrap_or("").to_string();
            auth_error = Some((code, reason));
            break;
        }

        if data
            .get("__limit_reached__")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            limit_hit = true;
            let reported_date = data["date"].as_str().unwrap_or(&cursor).to_string();
            cursor = reported_date;
            limit_code = data["code"].as_str().map(|s| s.to_string());
            limit_reason = data["reason"].as_str().map(|s| s.to_string());
            if let Some(ra) = data["retry_after"].as_u64() {
                limit_retry_after = ra;
            }
            break;
        }

        if data
            .get("__done__")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            done_received = true;
            break;
        }

        // Invoice row — upsert into DB
        if let Some(uuid) = data["uuid"].as_str().or(data["Uuid"].as_str()) {
            let _ = db::jobs::upsert_invoice(&pool, &job_id, uuid, &line).await;
            found += 1;

            // Extract date from invoice to advance cursor
            if let Some(fecha) = data["fecha"].as_str().or(data["Fecha"].as_str()) {
                let day = &fecha[..10.min(fecha.len())];
                cursor = format!("{day} 00:00:00");
            }

            // Throttle DB updates to every 50 invoices
            if found % 50 == 0 {
                let _ = db::jobs::update_found(&pool, &job_id, found, &cursor).await;
            }
        }
    }

    let php_exit_ok = match child.wait().await {
        Ok(status) => {
            if !status.success() {
                tracing::error!(job_id = %job_id, exit_code = ?status.code(), "PHP worker exited with error");
            }
            status.success()
        }
        Err(e) => {
            tracing::error!(job_id = %job_id, "PHP worker wait failed: {e}");
            false
        }
    };

    // Small yield so the stderr task has a chance to flush its last lines
    tokio::task::yield_now().await;

    if let Some((code, message)) = auth_error {
        if code == "captcha_failed" || code == "login_not_registered" {
            // Both are one-attempt flukes, not persistent credential problems:
            // a captcha OCR miss gets a fresh image next try, and
            // login_not_registered is accessPortalMainPage()'s exact-string
            // check occasionally missing on an otherwise-successful login
            // (evidence: 2026-08-15, ~50 fresh single-day jobs for the same
            // RFC/credentials in one batch, only ~5 hit this — a real bad
            // password fails every attempt, not one in ten). Worth retrying
            // before telling the client anything, unlike invalid_credentials/
            // fiel_login_failed below, which SAT's login step itself rejects
            // immediately and really do need a human to fix.
            match db::jobs::retry_transient_or_fail(
                &pool,
                &job_id,
                &cursor,
                found,
                Some(&code),
                &message,
            )
            .await
            {
                Ok(true) => {
                    tracing::warn!(job_id = %job_id, code = %code, "Job failed permanently: retries exhausted");
                    notify_sync_failed(&pool, &cfg, &job_id, &job_rfc, Some(&code)).await;
                }
                Ok(false) => {
                    tracing::warn!(job_id = %job_id, code = %code, "Job failed transiently, will auto-retry");
                }
                Err(e) => tracing::error!(job_id = %job_id, "retry_transient_or_fail error: {e}"),
            }
        } else {
            let _ = db::jobs::fail(&pool, &job_id, Some(&code), &message).await;
            tracing::warn!(job_id = %job_id, code = %code, "Job failed: auth error");
            // UX-4: a rejection the gap detector will still retry (verified owner, attempts
            // left) is very likely sporadic -- the client hears about it only once it's
            // confirmed, and the team gets an alert at the same moment.
            match db::jobs::credential_rejection_is_final(&pool, &job_id).await {
                Ok(true) => {
                    notify_sync_failed(&pool, &cfg, &job_id, &job_rfc, Some(&code)).await;
                    if db::users::is_owner_verified(&pool, &job_rfc)
                        .await
                        .unwrap_or(false)
                    {
                        services::email::alert_team(
                            &cfg,
                            &format!("Pulso: credencial rechazada en {job_rfc}"),
                            &format!(
                                "El SAT rechazó la credencial guardada del RFC {job_rfc} ({code}) en todos los reintentos automáticos (job {job_id}). La descarga queda detenida hasta que el dueño la actualice."
                            ),
                        )
                        .await;
                    }
                }
                Ok(false) => {
                    tracing::info!(job_id = %job_id, code = %code, "Credential rejection will be retried by the gap detector");
                }
                Err(e) => {
                    tracing::error!(job_id = %job_id, "credential_rejection_is_final error: {e}")
                }
            }
        }
        return;
    }

    // PHP crashed (or was killed after an idle timeout) before sending __done__.
    // This is a transient infra issue, not a SAT auth error — retry automatically
    // (bounded) through the same paused_limit/resume_worker path instead of
    // leaving the job stuck showing "Descarga fallida" for a network hiccup.
    if !done_received && !php_exit_ok {
        let msg = if idle_timeout {
            "PHP worker idle timeout — killed (no output for too long)"
        } else {
            "PHP worker crashed before completion"
        };
        match db::jobs::retry_transient_or_fail(&pool, &job_id, &cursor, found, None, msg).await {
            Ok(true) => {
                tracing::error!(job_id = %job_id, "Job failed permanently: retries exhausted");
                notify_sync_failed(&pool, &cfg, &job_id, &job_rfc, None).await;
            }
            Ok(false) => {
                tracing::error!(job_id = %job_id, idle_timeout, "Job failed transiently: PHP worker did not send __done__, will auto-retry");
            }
            Err(e) => tracing::error!(job_id = %job_id, "retry_transient_or_fail error: {e}"),
        }
        return;
    }

    if limit_hit {
        let resume_at = db::jobs::utc_offset(limit_retry_after);
        let reason = limit_reason.unwrap_or_else(|| {
            "SAT download limit reached — will resume automatically".to_string()
        });
        let _ = db::jobs::pause_limit(
            &pool,
            &job_id,
            &cursor,
            found,
            &resume_at,
            limit_code.as_deref(),
            Some(&reason),
        )
        .await;
        tracing::info!(job_id = %job_id, cursor = %cursor, resume_at = %resume_at, code = ?limit_code, "Job paused (limit)");
    } else {
        let _ = db::jobs::complete(&pool, &job_id, &job_rfc, &period_to, found).await;
        tracing::info!(job_id = %job_id, found = found, "Job completed");

        // Email 1: initial sync complete (job_type == "list" and this is the user's initial_sync_job_id)
        // Email 2: monthly complete (job_type == "auto_daily" and period_to falls on the last day of its month)
        if let Some(ref api_key) = cfg.sendgrid_api_key
            && let Ok(Some((user_id, email))) =
                crate::db::users::get_owner_contact_by_rfc(&pool, &job_rfc).await
        {
            let send_result = if job_type == "list" {
                match crate::db::users::is_initial_sync_job(&pool, &job_rfc, &job_id).await {
                    Ok(true) => Some(
                        crate::services::email::send_sync_complete(
                            api_key,
                            &cfg.sendgrid_from,
                            &email,
                            &job_rfc,
                            found,
                            &period_from,
                            &period_to,
                        )
                        .await,
                    ),
                    _ => None,
                }
            } else if job_type == "auto_daily" && is_last_day_of_month(&period_to) {
                let month_label = month_label_es(&period_to);
                Some(
                    crate::services::email::send_monthly_complete(
                        api_key,
                        &cfg.sendgrid_from,
                        &email,
                        &job_rfc,
                        &month_label,
                    )
                    .await,
                )
            } else {
                None
            };

            match send_result {
                Some(Err(e)) => {
                    tracing::warn!(job_id = %job_id, "Failed to send completion email: {e}")
                }
                Some(Ok(_)) => {
                    tracing::info!(job_id = %job_id, user_id = %user_id, "Sent completion email")
                }
                None => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .event_format(format().json())
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    dotenvy::dotenv().ok();
    let cfg = Config::from_env();

    // L18-03: JWT_SECRET used to fall back to a 9-character literal baked into the
    // binary. Harmless while L18-02's verifiers didn't check signatures at all -- once
    // they do, that default becomes the key to forge any session, admin included. The
    // check lives here (server startup), not inside Config::from_env, because the four
    // test suites call from_env() directly and would break if it required the variable.
    // Also rejects the .env.example placeholders, which are long enough to pass a
    // length-only check.
    if let Err(e) = cfg.validate_secrets() {
        tracing::error!("{e} -- refusing to start. Set real secrets in this environment's .env.");
        std::process::exit(1);
    }

    let bind_addr = format!("{}:{}", cfg.host, cfg.port);

    // ── Database ────────────────────────────────────────────────────────────
    let pool = db::init_pool(&cfg).await.unwrap_or_else(|e| {
        panic!("Failed to connect to PostgreSQL at '{}': {e}", cfg.pg_host);
    });
    // P-05 / AUD-079: background workers get their own pool now, sized separately from the
    // user-facing one above -- a long sync can no longer take a connection away from
    // someone using the dashboard. No migration run here; `init_pool` already did it once.
    let bg_pool = db::init_worker_pool(&cfg).await.unwrap_or_else(|e| {
        panic!(
            "Failed to connect background-worker pool to PostgreSQL at '{}': {e}",
            cfg.pg_host
        );
    });

    // L6-01: catches the environment/repo divergence init_pool's own sqlx::migrate! can't --
    // migration files present that were never applied (or a DB ahead of this checkout).
    db::migration_guard::ensure_current(&pool, std::path::Path::new("./migrations"))
        .await
        .unwrap_or_else(|e| panic!("{e}"));

    // Reset any jobs that were left in 'running' state from a previous crash
    match db::jobs::reset_stale_running(&pool).await {
        Ok(0) => {}
        Ok(n) => tracing::warn!("Reset {n} stale running job(s) to 'queued'"),
        Err(e) => tracing::error!("Could not reset stale jobs: {e}"),
    }

    info!(
        host          = %cfg.host,
        port          = %cfg.port,
        php_bin       = %cfg.php_bin,
        php_cli_path  = %cfg.php_cli_path,
        pg_host       = %cfg.pg_host,
        pg_database   = %cfg.pg_database,
        "Starting pulso-backend"
    );

    // ── S3 ──────────────────────────────────────────────────────────────────
    let aws_cfg = aws_config::load_from_env().await;
    let s3_client = Arc::new(S3Client::new(&aws_cfg));

    // ── Background workers ──────────────────────────────────────────────────
    // P-05: all six now take bg_pool, not pool -- see its own comment above for why.
    let counting_rfcs = services::reconciliation::CountingRfcs::default();
    {
        let worker_pool = bg_pool.clone();
        let worker_cfg = Arc::new(cfg.clone());
        let worker_s3 = s3_client.clone();
        tokio::spawn(resume_worker(
            worker_pool,
            worker_cfg,
            worker_s3,
            counting_rfcs.clone(),
        ));
    }
    {
        tokio::spawn(services::reconciliation::worker(
            bg_pool.clone(),
            Arc::new(cfg.clone()),
            s3_client.clone(),
            counting_rfcs.clone(),
        ));
    }
    let nomina_refresh_state = services::nomina_refresh::NominaRefreshState::new();
    {
        let etl_pool = bg_pool.clone();
        let etl_cfg = Arc::new(cfg.clone());
        let etl_s3 = s3_client.clone();
        let etl_nomina_state = nomina_refresh_state.clone();
        tokio::spawn(etl::etl_worker(etl_pool, etl_cfg, etl_s3, etl_nomina_state));
    }
    {
        tokio::spawn(daily_sync_worker(bg_pool.clone()));
    }
    {
        let recheck_pool = bg_pool.clone();
        let recheck_cfg = Arc::new(cfg.clone());
        let recheck_s3 = s3_client.clone();
        tokio::spawn(services::recheck_cancelled::worker(
            recheck_pool,
            recheck_cfg,
            recheck_s3,
        ));
    }
    {
        let gap_pool = bg_pool.clone();
        let gap_cfg = Arc::new(cfg.clone());
        let gap_s3 = s3_client.clone();
        tokio::spawn(services::gap_detector::worker(gap_pool, gap_cfg, gap_s3));
    }
    {
        let redl_pool = bg_pool.clone();
        let redl_cfg = Arc::new(cfg.clone());
        let redl_s3 = s3_client.clone();
        tokio::spawn(services::xml_redownload::worker(
            redl_pool, redl_cfg, redl_s3,
        ));
    }
    {
        tokio::spawn(services::response_cache::cleanup_worker(bg_pool.clone()));
    }
    {
        tokio::spawn(services::fiel::orphan_sweep_worker(
            bg_pool.clone(),
            Arc::new(cfg.clone()),
            s3_client.clone(),
        ));
    }
    {
        tokio::spawn(services::nomina_refresh::worker(
            bg_pool.clone(),
            nomina_refresh_state.clone(),
        ));
    }
    let nomina_refresh_state_data = web::Data::new(nomina_refresh_state);
    // One long-lived sampler: CPU usage is measured between consecutive requests.
    let system_sampler_data = web::Data::new(system_routes::SystemSampler::new());

    // ── HTTP server ─────────────────────────────────────────────────────────
    let allowed_origins = cfg.allowed_origins.clone();
    let allowed_methods = cfg.allowed_methods.clone();
    let cfg_data = web::Data::new(cfg);
    let pool_data = web::Data::new(pool);
    let captcha_map: web::Data<CaptchaMap> =
        web::Data::new(CaptchaMap::new(std::collections::HashMap::new()));
    let s3_data = web::Data::from(s3_client);

    HttpServer::new(move || {
        let methods: Vec<&str> = allowed_methods.iter().map(String::as_str).collect();
        let mut cors = Cors::default()
            .allowed_methods(methods)
            .allow_any_header()
            .max_age(3600);
        if allowed_origins.is_empty() {
            cors = cors.allow_any_origin();
        } else {
            for origin in &allowed_origins {
                cors = cors.allowed_origin(origin);
            }
        };

        let app = App::new()
            .app_data(cfg_data.clone())
            .app_data(captcha_map.clone())
            .app_data(s3_data.clone())
            .app_data(pool_data.clone())
            .app_data(nomina_refresh_state_data.clone())
            .app_data(system_sampler_data.clone())
            .app_data(web::JsonConfig::default().limit(10 * 1024 * 1024))
            .wrap(cors)
            .wrap(TracingLogger::default());
        // L18-01: /docs describes every route, including the seven invoices ones this
        // same item just closed -- an open API reference is itself part of the exposure.
        // Every deployed environment (test and prod) runs a --release binary, so this
        // only ever registers for a developer's own `cargo run`/`cargo build` -- same
        // debug_assertions convention this codebase already uses for storage.rs/
        // invoices.rs's local-vs-S3 switch, not a new mechanism. A real `#[cfg]` (not a
        // runtime `cfg!()` branch) because `App<T>`'s type changes with every `.service()`
        // call -- an `if`/`else` adding it on only one side wouldn't type-check; `#[cfg]`
        // on the `let` means only one variant of this code ever exists per build.
        #[cfg(debug_assertions)]
        let app = app.service(
            Scalar::with_url("/docs", api_docs::ApiDoc::openapi())
                .custom_html(api_docs::SCALAR_HTML),
        );
        app
            // Health check
            .route("/health", web::get().to(invoices::health))
            // Billing
            .route(
                "/api/v1/billing/status",
                web::get().to(billing_routes::get_status),
            )
            // Auth
            .route(
                "/api/v1/auth/register",
                web::post().to(auth_routes::register),
            )
            .route("/api/v1/auth/login", web::post().to(auth_routes::login))
            .route(
                "/api/v1/auth/google/url",
                web::get().to(auth_routes::google_auth_url),
            )
            .route(
                "/api/v1/auth/google",
                web::post().to(auth_routes::google_login),
            )
            .route(
                "/api/v1/auth/google/status",
                web::get().to(auth_routes::google_status),
            )
            .service(
                web::resource("/api/v1/auth/google/link")
                    .route(web::post().to(auth_routes::google_link))
                    .route(web::delete().to(auth_routes::google_unlink)),
            )
            // Users
            .route(
                "/api/v1/users/profile",
                web::get().to(users_routes::get_profile),
            )
            .route(
                "/api/v1/users/complete-profile",
                web::post().to(users_routes::complete_profile),
            )
            .route(
                "/api/v1/users/trigger-sync",
                web::post().to(users_routes::trigger_sync),
            )
            .route(
                "/api/v1/admin/download",
                web::post().to(users_routes::admin_download),
            )
            .route(
                "/api/v1/admin/reprocess",
                web::post().to(users_routes::admin_reprocess),
            )
            .route(
                "/api/v1/admin/rfcs",
                web::get().to(users_routes::admin_list_rfcs),
            )
            .route(
                "/api/v1/admin/rfcs/full",
                web::get().to(users_routes::admin_list_rfcs_full),
            )
            .route(
                "/api/v1/admin/users",
                web::get().to(users_routes::admin_list_users),
            )
            .route(
                "/api/v1/admin/users/{user_id}/rfcs",
                web::get().to(users_routes::admin_user_rfcs),
            )
            .route(
                "/api/v1/admin/rfcs/{rfc}/xml-years",
                web::get().to(users_routes::admin_rfc_xml_years),
            )
            .route(
                "/api/v1/admin/rfcs/{rfc}/xml-days",
                web::get().to(users_routes::admin_rfc_xml_days),
            )
            .route(
                "/api/v1/admin/rfcs/{rfc}/xml-day",
                web::get().to(users_routes::admin_rfc_xml_day),
            )
            .route(
                "/api/v1/users/sync-status",
                web::get().to(users_routes::sync_status),
            )
            .service(
                web::resource("/api/v1/users/rfcs")
                    .route(web::get().to(users_routes::get_rfcs))
                    .route(web::post().to(users_routes::add_rfc)),
            )
            .service(
                web::resource("/api/v1/users/rfcs/{rfc}/clave")
                    .route(web::put().to(users_routes::update_rfc_clave_handler)),
            )
            .service(
                web::resource("/api/v1/users/rfcs/{rfc}/validate-clave")
                    .route(web::post().to(users_routes::validate_clave_handler)),
            )
            .service(
                web::resource("/api/v1/users/rfcs/{rfc}/priority-analysis")
                    .route(web::put().to(users_routes::update_priority_analysis_handler)),
            )
            .service(
                web::resource("/api/v1/users/rfcs/{rfc}/shares")
                    .route(web::get().to(users_routes::list_rfc_shares_handler))
                    .route(web::post().to(users_routes::share_rfc_handler)),
            )
            .service(
                web::resource("/api/v1/users/rfcs/{rfc}/shares/{share_id}")
                    .route(web::delete().to(users_routes::revoke_rfc_share_handler)),
            )
            .service(
                web::resource("/api/v1/users/rfcs/{rfc}")
                    .route(web::delete().to(users_routes::delete_rfc_handler)),
            )
            // FIEL API
            .service(
                web::resource("/api/v1/users/rfcs/{rfc}/fiel")
                    .route(web::post().to(fiel_routes::upload))
                    .route(web::get().to(fiel_routes::get_status))
                    .route(web::delete().to(fiel_routes::delete)),
            )
            // L18-01: the seven /api/v1/invoices/* routes took no session at all -- any
            // visitor could read a stored CFDI's full XML (payroll receipts included) or
            // use the server as a SAT downloader with credentials they supplied, FIEL
            // included, encrypted into the jobs table. Not called from the frontend (0
            // references in pulso-adquiere). The handlers stay in routes/invoices.rs
            // unchanged -- the sync worker still calls into that module directly; only the
            // HTTP surface is gone.
            // Queue API
            .service(
                web::scope("/api/v1/queue")
                    .route("", web::get().to(queue_routes::list_jobs))
                    .route("/{id}", web::get().to(queue_routes::get_job))
                    .route("/{id}", web::delete().to(queue_routes::cancel_job))
                    .route(
                        "/{id}/results",
                        web::get().to(queue_routes::get_job_results),
                    ),
            )
            // Admin
            .route("/api/v1/admin/logs", web::get().to(logs_routes::get_logs))
            .route(
                "/api/v1/admin/system",
                web::get().to(system_routes::get_system),
            )
            // Analytics API
            .service(
                web::scope("/api/v1/analytics/{rfc}")
                    .route("/summary", web::get().to(analytics_routes::get_summary))
                    .route(
                        "/summary/month-contributor",
                        web::get().to(analytics_routes::get_month_contributor),
                    )
                    .route(
                        "/data-quality",
                        web::get().to(analytics_routes::get_data_quality),
                    )
                    .route(
                        "/counterparties",
                        web::get().to(analytics_routes::get_counterparties),
                    )
                    .route(
                        "/counterparties/evolution",
                        web::get().to(analytics_routes::get_counterparties_evolution),
                    )
                    .route(
                        "/counterparties/selector",
                        web::get().to(analytics_routes::get_counterparties_selector),
                    )
                    .route(
                        "/counterparties/ltm",
                        web::get().to(analytics_routes::get_counterparties_ltm),
                    )
                    .route(
                        "/counterparties/payments-detail",
                        web::get().to(analytics_routes::get_counterparties_payments_detail),
                    )
                    .route(
                        "/counterparties/atypical",
                        web::get().to(analytics_routes::get_counterparties_atypical),
                    )
                    .route(
                        "/counterparties/{cp_rfc}",
                        web::get().to(analytics_routes::get_counterparty_individual),
                    )
                    .route(
                        "/recurrence",
                        web::get().to(analytics_routes::get_recurrence),
                    )
                    .route("/retention", web::get().to(analytics_routes::get_retention))
                    .route("/geography", web::get().to(analytics_routes::get_geography))
                    .route(
                        "/hallazgos-egresos",
                        web::get().to(analytics_routes::get_hallazgos_egresos),
                    )
                    .route("/concepts", web::get().to(analytics_routes::get_concepts))
                    .route("/fiscal", web::get().to(analytics_routes::get_fiscal))
                    .route("/payments", web::get().to(analytics_routes::get_payments))
                    .route("/cashflow", web::get().to(analytics_routes::get_cashflow))
                    .route("/hallazgos", web::get().to(analytics_routes::get_hallazgos))
                    .route(
                        "/payroll/snapshot",
                        web::get().to(analytics_routes::get_payroll_snapshot),
                    )
                    .route("/payroll", web::get().to(analytics_routes::get_payroll))
                    .route("/quarterly", web::get().to(analytics_routes::get_quarterly))
                    .route("/xml-count", web::get().to(analytics_routes::get_xml_count))
                    .route(
                        "/xml-breakdown",
                        web::get().to(analytics_routes::get_xml_breakdown),
                    )
                    .route(
                        "/period-comparison",
                        web::get().to(analytics_routes::get_period_comparison),
                    )
                    // Normalization rules
                    .route(
                        "/normalization",
                        web::get().to(analytics_routes::list_normalization),
                    )
                    .route(
                        "/normalization",
                        web::post().to(analytics_routes::create_normalization),
                    )
                    .route(
                        "/normalization/{rule_id}",
                        web::put().to(analytics_routes::update_normalization),
                    )
                    .route(
                        "/normalization/{rule_id}",
                        web::delete().to(analytics_routes::delete_normalization),
                    )
                    .route(
                        "/normalization/payroll",
                        web::get().to(analytics_routes::list_payroll_normalization),
                    )
                    .route(
                        "/normalization/payroll",
                        web::post().to(analytics_routes::create_payroll_normalization),
                    )
                    .route(
                        "/normalization/payroll/{rule_id}",
                        web::put().to(analytics_routes::update_payroll_normalization),
                    )
                    .route(
                        "/normalization/payroll/{rule_id}",
                        web::delete().to(analytics_routes::delete_payroll_normalization),
                    )
                    .route(
                        "/normalization/payroll/refresh-nomina",
                        web::post().to(analytics_routes::refresh_nomina_normalizada),
                    )
                    .route(
                        "/normalization/excluded",
                        web::get().to(analytics_routes::list_excluded_cfdis),
                    )
                    .service(
                        web::resource("/normalization/counterparties")
                            .route(web::get().to(analytics_routes::list_norm_counterparties)),
                    )
                    .service(
                        web::resource("/normalization/counterparties/{cp_rfc}/cfdis")
                            .route(web::get().to(analytics_routes::list_norm_counterparty_cfdis)),
                    )
                    .service(web::resource("/normalization/individual-rule-ids").route(
                        web::get().to(analytics_routes::list_normalization_individual_rule_ids),
                    ))
                    .service(web::resource("/normalization/payroll/employees").route(
                        web::get().to(analytics_routes::get_normalization_payroll_employees),
                    ))
                    .service(
                        web::resource("/normalization/payroll/employees/{employee_rfc}/receipts")
                            .route(
                                web::get().to(
                                    analytics_routes::get_normalization_payroll_employee_receipts,
                                ),
                            ),
                    )
                    .service(
                        web::resource("/normalization/ebitda-bridge").route(
                            web::get().to(analytics_routes::get_normalization_ebitda_bridge),
                        ),
                    ),
            )
    })
    .bind(&bind_addr)?
    .run()
    .await
}

#[cfg(test)]
mod owner_verification_tests {
    use super::*;

    const RFC: &str = "AAA010101AAA";

    #[test]
    fn job_ciec_password_takes_a_ciec_for_the_jobs_own_rfc() {
        let auth = serde_json::json!({"type": "ciec", "rfc": "aaa010101aaa", "password": "pw"});
        assert_eq!(job_ciec_password(&auth, RFC), Some("pw".to_string()));
    }

    #[test]
    fn job_ciec_password_ignores_fiel_and_other_rfcs() {
        let fiel = serde_json::json!({"type": "fiel", "cert_pem_path": "/tmp/c", "password": ""});
        assert_eq!(job_ciec_password(&fiel, RFC), None);
        let other = serde_json::json!({"type": "ciec", "rfc": "BBB010101BBB", "password": "pw"});
        assert_eq!(job_ciec_password(&other, RFC), None);
    }

    #[test]
    fn is_sat_login_ok_only_matches_the_marker() {
        assert!(is_sat_login_ok(&serde_json::json!({"__auth_ok__": true})));
        assert!(!is_sat_login_ok(
            &serde_json::json!({"__keepalive__": true})
        ));
        assert!(!is_sat_login_ok(&serde_json::json!({"uuid": "x"})));
    }

    #[test]
    fn redact_secret_strips_the_ciec_from_stderr() {
        assert_eq!(
            redact_secret("login failed for s3cr3t!", Some("s3cr3t!")),
            "login failed for [REDACTED]"
        );
        assert_eq!(redact_secret("Day error", None), "Day error");
        assert_eq!(redact_secret("Day error", Some("")), "Day error");
    }
}
