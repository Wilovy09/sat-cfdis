mod api_docs;
mod config;
mod db;
mod errors;
mod models;
mod routes;
mod services;
mod state;

use actix_cors::Cors;
use actix_files::Files;
use actix_web::{App, HttpServer, web};
use aws_sdk_s3::Client as S3Client;
use std::sync::Arc;
use tracing::info;
use tracing_actix_web::TracingLogger;
use tracing_subscriber::EnvFilter;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

use config::Config;
use db::DbPool;
use routes::{
    analytics as analytics_routes, auth as auth_routes, billing as billing_routes, fiel as fiel_routes,
    invoices, queue as queue_routes, users as users_routes,
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
    let row = db::fiel::get(pool, rfc)
        .await
        .map_err(|e| tracing::error!(rfc = %rfc, "FIEL check: DB error: {e}"))
        .ok()??;

    let cert_bytes = services::s3::get_fiel(s3, bucket, &row.cert_s3_key).await.or_else(|| {
        tracing::error!(rfc = %rfc, key = %row.cert_s3_key, "FIEL: S3 cert download failed");
        None
    })?;

    let key_bytes = services::s3::get_fiel(s3, bucket, &row.key_s3_key).await.or_else(|| {
        tracing::error!(rfc = %rfc, key = %row.key_s3_key, "FIEL: S3 key download failed");
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

// ---------------------------------------------------------------------------
// Background worker — resumes paused_limit jobs after 24.5 h
// ---------------------------------------------------------------------------

/// How often the worker wakes up to check for resumable jobs (seconds).
const WORKER_POLL_SECS: u64 = 30;

async fn resume_worker(pool: DbPool, cfg: Arc<Config>, s3_client: Arc<S3Client>) {
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
                    let _ = db::jobs::fail(&pool, &job.id, None, &format!("Decrypt failed: {e}")).await;
                    continue;
                }
            };

            let auth_payload: serde_json::Value = match serde_json::from_str(&auth_json) {
                Ok(v) => v,
                Err(e) => {
                    let _ = db::jobs::fail(&pool, &job.id, None, &format!("Bad auth JSON: {e}")).await;
                    continue;
                }
            };

            // If RFC has FIEL configured, override CIEC auth with FIEL.
            // _fiel_tmp must stay alive until run_worker_chunk returns.
            let bucket = cfg.s3_bucket.clone().unwrap_or_default();
            let (auth_payload, _fiel_tmp) =
                match try_fiel_auth(&pool, &s3_client, &bucket, &job.rfc).await {
                    Some((fiel_auth, tmp)) => {
                        tracing::info!(rfc = %job.rfc, "Worker: using FIEL auth");
                        (fiel_auth, Some(tmp))
                    }
                    None => (auth_payload, None::<tempfile::TempDir>),
                };

            // Queued jobs start from period_from; paused jobs resume from day after cursor
            let resume_from = match &job.cursor_date {
                Some(d) => next_day(d),
                None => job.period_from.clone(),
            };

            if resume_from > job.period_to {
                let _ = db::jobs::complete(
                    &pool,
                    &job.id,
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
                job.auth_type.clone(),
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
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
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

/// Every day, queue a one-day sync job for yesterday for every registered user
/// whose credentials are stored and whose yesterday period hasn't been synced yet.
async fn daily_sync_worker(pool: DbPool) {
    // Short initial delay so the main worker gets a head start on startup.
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;

    loop {
        // Yesterday in UTC (seconds-since-epoch - 86400, then format as date).
        let yesterday_secs = {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .saturating_sub(86400)
        };
        let (yy, ym, yd) = {
            // Gregorian calendar from epoch seconds (no external crate needed).
            let days = yesterday_secs / 86400;
            let mut y = 1970u32;
            let mut d = days as u32;
            loop {
                let dy = if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 366 } else { 365 };
                if d < dy { break; }
                d -= dy;
                y += 1;
            }
            let mut m = 1u32;
            loop {
                let dm = days_in_month(y, m);
                if d < dm { break; }
                d -= dm;
                m += 1;
            }
            (y, m, d + 1)
        };

        let period_from = format!("{yy:04}-{ym:02}-{yd:02} 00:00:00");
        let period_to   = format!("{yy:04}-{ym:02}-{yd:02} 23:59:59");

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
                &period_from,
                &period_to,
            )
            .await
            {
                Ok(job_id) => {
                    tracing::info!(
                        rfc = %rfc,
                        job_id = %job_id,
                        date = %format!("{yy:04}-{ym:02}-{yd:02}"),
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
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
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
async fn run_count_pass(
    pool: &DbPool,
    cfg: &Config,
    job_id: &str,
    auth_payload: &serde_json::Value,
    period_from: &str,
    period_to: &str,
    dl_type: &str,
) -> Option<i64> {
    use std::process::Stdio;
    use tokio::io::AsyncBufReadExt as _;
    use tokio::io::AsyncWriteExt as _;

    let payload = serde_json::json!({
        "command": "list-count",
        "auth":    auth_payload,
        "params": {
            "period_from":   period_from,
            "period_to":     period_to,
            "download_type": dl_type,
        }
    });

    let mut input_bytes = serde_json::to_vec(&payload).ok()?;
    input_bytes.push(b'\n');

    let mut cmd = tokio::process::Command::new(&cfg.php_bin);
    cmd.arg(&cfg.php_cli_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(ref proxy) = cfg.https_proxy {
        cmd.env("HTTPS_PROXY", proxy).env("https_proxy", proxy);
    }

    let mut child = cmd.spawn().ok()?;
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(&input_bytes).await;
    }

    let stdout = child.stdout.take()?;
    let mut lines = tokio::io::BufReader::new(stdout).lines();

    let mut total: Option<i64> = None;
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
                tracing::error!("list-count: PHP worker idle timeout, killing");
                let _ = child.start_kill();
                break;
            }
        };
        if line.is_empty() { continue; }
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
            if val.get("__keepalive__").and_then(|v| v.as_bool()).unwrap_or(false) {
                let total_so_far = val["total_so_far"].as_i64().unwrap_or(0);
                tracing::info!(
                    job_id = %job_id,
                    date = val["date"].as_str().unwrap_or("?"),
                    total_so_far,
                    "list-count progress"
                );
                let _ = db::jobs::update_count_progress(pool, job_id, total_so_far).await;
                continue;
            }
            if val.get("__count__").is_some() {
                total = val["total"].as_i64();
                break;
            }
        }
    }

    let _ = child.wait().await;
    total
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
    _auth_type: String,
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
        match run_count_pass(&pool, &cfg, &job_id, &auth_payload, &full_period_from, &period_to, &dl_type).await {
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
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt as _;
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if !line.is_empty() {
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
        let _ = db::jobs::fail(&pool, &job_id, Some(&code), &message).await;
        tracing::warn!(job_id = %job_id, code = %code, "Job failed: auth error");
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
        let _ = db::jobs::retry_transient_or_fail(&pool, &job_id, &cursor, found, msg).await;
        tracing::error!(job_id = %job_id, idle_timeout, "Job failed transiently: PHP worker did not send __done__, will auto-retry");
        return;
    }

    if limit_hit {
        let resume_at = db::jobs::utc_offset(limit_retry_after);
        let reason = limit_reason
            .unwrap_or_else(|| "SAT download limit reached — will resume automatically".to_string());
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
        let _ = db::jobs::complete(&pool, &job_id, &period_to, found).await;
        tracing::info!(job_id = %job_id, found = found, "Job completed");

        // Email 1: initial sync complete (job_type == "list" and this is the user's initial_sync_job_id)
        // Email 2: monthly complete (job_type == "auto_daily" and period_to falls on the last day of its month)
        if let Some(ref api_key) = cfg.sendgrid_api_key {
            if let Ok(Some(email)) = crate::db::users::get_email_by_rfc(&pool, &job_rfc).await {
                let send_result = if job_type == "list" {
                    match crate::db::users::is_initial_sync_job(&pool, &job_rfc, &job_id).await {
                        Ok(true) => {
                            Some(crate::services::email::send_sync_complete(
                                api_key,
                                &cfg.sendgrid_from,
                                &email,
                                &job_rfc,
                                found,
                                &period_from,
                                &period_to,
                            ).await)
                        }
                        _ => None,
                    }
                } else if job_type == "auto_daily" && is_last_day_of_month(&period_to) {
                    let month_label = month_label_es(&period_to);
                    Some(crate::services::email::send_monthly_complete(
                        api_key,
                        &cfg.sendgrid_from,
                        &email,
                        &job_rfc,
                        &month_label,
                    ).await)
                } else {
                    None
                };

                match send_result {
                    Some(Err(e)) => tracing::warn!(job_id = %job_id, "Failed to send completion email: {e}"),
                    Some(Ok(_))  => tracing::info!(job_id = %job_id, "Sent completion email to {email}"),
                    None         => {}
                }
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
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    dotenvy::dotenv().ok();
    let cfg = Config::from_env();
    let bind_addr = format!("{}:{}", cfg.host, cfg.port);

    // ── Database ────────────────────────────────────────────────────────────
    let pool = db::init_pool(&cfg).await.unwrap_or_else(|e| {
        panic!("Failed to connect to PostgreSQL at '{}': {e}", cfg.pg_host);
    });

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
    {
        let worker_pool = pool.clone();
        let worker_cfg = Arc::new(cfg.clone());
        let worker_s3 = s3_client.clone();
        tokio::spawn(resume_worker(worker_pool, worker_cfg, worker_s3));
    }
    {
        let etl_pool = pool.clone();
        let etl_cfg = Arc::new(cfg.clone());
        let etl_s3 = s3_client.clone();
        tokio::spawn(etl::etl_worker(etl_pool, etl_cfg, etl_s3));
    }
    {
        tokio::spawn(daily_sync_worker(pool.clone()));
    }

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

        App::new()
            .app_data(cfg_data.clone())
            .app_data(captcha_map.clone())
            .app_data(s3_data.clone())
            .app_data(pool_data.clone())
            .app_data(web::JsonConfig::default().limit(10 * 1024 * 1024))
            .wrap(cors)
            .wrap(TracingLogger::default())
            // Docs
            .service(
                Scalar::with_url("/docs", api_docs::ApiDoc::openapi())
                    .custom_html(api_docs::SCALAR_HTML),
            )
            // Static files
            .service(Files::new("/static", "static").prefer_utf8(true))
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
            // Invoice API
            .service(
                web::scope("/api/v1/invoices")
                    .route("/list", web::post().to(invoices::list_invoices))
                    .route("/list/stream", web::post().to(invoices::list_stream))
                    .route("/captcha/solve", web::post().to(invoices::solve_captcha))
                    .route("/download", web::post().to(invoices::download_invoices))
                    .route(
                        "/download/stream",
                        web::post().to(invoices::download_stream),
                    )
                    .route("/xml-content", web::post().to(invoices::xml_content))
                    .route("/bulk/stream", web::post().to(invoices::bulk_stream)),
            )
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
            // Analytics API
            .service(
                web::scope("/api/v1/analytics/{rfc}")
                    .route("/summary", web::get().to(analytics_routes::get_summary))
                    .route(
                        "/counterparties",
                        web::get().to(analytics_routes::get_counterparties),
                    )
                    .route(
                        "/counterparties/evolution",
                        web::get().to(analytics_routes::get_counterparties_evolution),
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
                    .route("/concepts", web::get().to(analytics_routes::get_concepts))
                    .route("/fiscal", web::get().to(analytics_routes::get_fiscal))
                    .route("/payments", web::get().to(analytics_routes::get_payments))
                    .route("/cashflow", web::get().to(analytics_routes::get_cashflow))
                    .route("/hallazgos", web::get().to(analytics_routes::get_hallazgos))
                    .route("/payroll/snapshot", web::get().to(analytics_routes::get_payroll_snapshot))
                    .route("/payroll", web::get().to(analytics_routes::get_payroll))
                    .route("/quarterly", web::get().to(analytics_routes::get_quarterly))
                    .route("/xml-count", web::get().to(analytics_routes::get_xml_count))
                    .route("/xml-breakdown", web::get().to(analytics_routes::get_xml_breakdown))
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
                        web::delete().to(analytics_routes::delete_payroll_normalization),
                    )
                    .route(
                        "/normalization/excluded",
                        web::get().to(analytics_routes::list_excluded_cfdis),
                    )
                    .service(
                        web::resource("/normalization/cfdis")
                            .route(web::get().to(analytics_routes::list_norm_cfdis)),
                    )
                    .service(
                        web::resource("/normalization/counterparties")
                            .route(web::get().to(analytics_routes::list_norm_counterparties)),
                    )
                    .service(
                        web::resource("/normalization/counterparties/{cp_rfc}/cfdis")
                            .route(web::get().to(analytics_routes::list_norm_counterparty_cfdis)),
                    )
                    .service(
                        web::resource("/normalization/payroll/employees")
                            .route(
                                web::get()
                                    .to(analytics_routes::get_normalization_payroll_employees),
                            ),
                    )
                    .service(
                        web::resource("/normalization/ebitda-bridge")
                            .route(
                                web::get().to(analytics_routes::get_normalization_ebitda_bridge),
                            ),
                    ),
            )
    })
    .bind(&bind_addr)?
    .run()
    .await
}
