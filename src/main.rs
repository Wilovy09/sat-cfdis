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
use actix_web::{App, HttpServer, http, middleware, web};
use aws_sdk_s3::Client as S3Client;
use std::sync::Arc;
use tera::Tera;
use tracing::info;
use tracing_subscriber::EnvFilter;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

use config::Config;
use db::DbPool;
use routes::{
    analytics as analytics_routes, auth as auth_routes, invoices, queue as queue_routes,
    web as web_routes,
};
use services::etl;
use state::CaptchaMap;

// ---------------------------------------------------------------------------
// Background worker — resumes paused_limit jobs after 24.5 h
// ---------------------------------------------------------------------------

/// How often the worker wakes up to check for resumable jobs (seconds).
const WORKER_POLL_SECS: u64 = 300; // 5 minutes

async fn resume_worker(pool: DbPool, cfg: Arc<Config>, s3_client: Arc<S3Client>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(WORKER_POLL_SECS)).await;

        let resumable = match db::jobs::find_resumable(&pool).await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::error!("Worker: DB error finding resumable jobs: {e}");
                continue;
            }
        };

        for job in resumable {
            tracing::info!(job_id = %job.id, rfc = %job.rfc, "Resuming paused job");
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
                    let _ = db::jobs::fail(&pool, &job.id, &format!("Decrypt failed: {e}")).await;
                    continue;
                }
            };

            let auth_payload: serde_json::Value = match serde_json::from_str(&auth_json) {
                Ok(v) => v,
                Err(e) => {
                    let _ = db::jobs::fail(&pool, &job.id, &format!("Bad auth JSON: {e}")).await;
                    continue;
                }
            };

            // Determine start date: day after cursor_date, or period_from
            let resume_from = match &job.cursor_date {
                Some(d) => next_day(d),
                None => job.period_from.clone(),
            };

            if resume_from > job.period_to {
                // Already done
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
                job.rfc.clone(),
                auth_payload,
                job.auth_type.clone(),
                resume_from,
                job.period_to.clone(),
                job.dl_type.clone(),
                job.found,
            )
            .await;
        }
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

    let days_in_month = |yr: u32, mo: u32| -> u32 {
        match mo {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if (yr % 4 == 0 && yr % 100 != 0) || yr % 400 == 0 {
                    29
                } else {
                    28
                }
            }
            _ => 30,
        }
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

/// Run one PHP list-stream chunk for a background worker job.
/// Results go to DB (job_invoices) and S3/local storage.
/// No SSE — silent background processing.
#[allow(clippy::too_many_arguments)]
async fn run_worker_chunk(
    pool: DbPool,
    cfg: Arc<Config>,
    s3: Arc<S3Client>,
    job_id: String,
    _rfc: String,
    auth_payload: serde_json::Value,
    _auth_type: String,
    period_from: String,
    period_to: String,
    dl_type: String,
    initial_found: i64,
) {
    use std::process::Stdio;
    use tokio::io::AsyncBufReadExt as _;
    use tokio::io::AsyncWriteExt as _;

    let payload = serde_json::json!({
        "command": "list-stream",
        "auth":    auth_payload,
        "params": {
            "period_from":       period_from,
            "period_to":         period_to,
            "download_type":     dl_type,
            "auto_download_xml": true,
        }
    });

    let mut input_bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            let _ = db::jobs::fail(&pool, &job_id, &e.to_string()).await;
            return;
        }
    };
    input_bytes.push(b'\n');

    let mut child = match tokio::process::Command::new(&cfg.php_bin)
        .arg(&cfg.php_cli_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            let _ = db::jobs::fail(&pool, &job_id, &e.to_string()).await;
            return;
        }
    };

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(&input_bytes).await;
    }

    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => {
            let _ = db::jobs::fail(&pool, &job_id, "no stdout").await;
            return;
        }
    };

    let reader = tokio::io::BufReader::new(stdout);
    let mut lines = reader.lines();
    let mut found = initial_found;
    let mut cursor = period_from.clone();
    let mut limit_hit = false;

    while let Ok(Some(line)) = lines.next_line().await {
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

        // Auto-downloaded XML — save to storage directly from the worker
        if data
            .get("__xml_ready__")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let uuid_str = data["uuid"].as_str().unwrap_or("").to_string();
            let xml_b64 = data["xml_b64"].as_str().unwrap_or("").to_string();
            let s3_ref = s3.clone();
            let bucket = cfg.s3_bucket.clone().unwrap_or_default();
            tokio::spawn(async move {
                use base64::Engine as _;
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(&xml_b64) {
                    let should_upload = cfg!(debug_assertions) || !bucket.is_empty();
                    if should_upload {
                        let (rfc_e, rfc_r, year, month, day) =
                            crate::routes::invoices::extract_cfdi_path_info(&bytes);
                        let _ = crate::services::storage::upload(
                            &s3_ref, &bucket, &rfc_e, &rfc_r, year, month, day, &uuid_str, bytes,
                        )
                        .await;
                    }
                }
            });
            continue;
        }

        if data
            .get("__limit_reached__")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            limit_hit = true;
            let reported_date = data["date"].as_str().unwrap_or(&cursor).to_string();
            cursor = reported_date;
            break;
        }

        if data
            .get("__done__")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
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

    let _ = child.wait().await;

    if limit_hit {
        let resume_at = db::jobs::utc_offset(24 * 3600 + 1800); // +24.5 h
        let _ = db::jobs::pause_limit(
            &pool,
            &job_id,
            &cursor,
            found,
            &resume_at,
            Some("SAT download limit reached — will resume automatically"),
        )
        .await;
        tracing::info!(job_id = %job_id, cursor = %cursor, resume_at = %resume_at, "Job paused (limit)");
    } else {
        let _ = db::jobs::complete(&pool, &job_id, &period_to, found).await;
        tracing::info!(job_id = %job_id, found = found, "Job completed");
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

    let templates_glob =
        std::env::var("TEMPLATES_DIR").unwrap_or_else(|_| "templates/**/*".to_string());
    let tera = Tera::new(&templates_glob).unwrap_or_else(|e| {
        panic!("Failed to load Tera templates from '{templates_glob}': {e}");
    });

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

    // ── HTTP server ─────────────────────────────────────────────────────────
    let cfg_data = web::Data::new(cfg);
    let tera_data = web::Data::new(tera);
    let pool_data = web::Data::new(pool);
    let captcha_map: web::Data<CaptchaMap> =
        web::Data::new(CaptchaMap::new(std::collections::HashMap::new()));
    let s3_data = web::Data::from(s3_client);

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["GET", "POST", "DELETE", "OPTIONS"])
            .allowed_headers(vec![
                http::header::CONTENT_TYPE,
                http::header::AUTHORIZATION,
                http::header::ACCEPT,
            ])
            .max_age(3600);

        App::new()
            .app_data(cfg_data.clone())
            .app_data(tera_data.clone())
            .app_data(captcha_map.clone())
            .app_data(s3_data.clone())
            .app_data(pool_data.clone())
            .app_data(web::JsonConfig::default().limit(10 * 1024 * 1024))
            .wrap(cors)
            .wrap(middleware::Logger::default())
            // Docs
            .service(
                Scalar::with_url("/docs", api_docs::ApiDoc::openapi())
                    .custom_html(api_docs::SCALAR_HTML),
            )
            // Static files
            .service(Files::new("/static", "static").prefer_utf8(true))
            // Health check
            .route("/health", web::get().to(invoices::health))
            // Auth
            .route(
                "/api/v1/auth/register",
                web::post().to(auth_routes::register),
            )
            .route("/api/v1/auth/login", web::post().to(auth_routes::login))
            // Web UI
            .route("/", web::get().to(web_routes::index))
            .route("/analytics", web::get().to(web_routes::analytics_page))
            .route("/web/list", web::post().to(web_routes::list_web))
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
                        "/recurrence",
                        web::get().to(analytics_routes::get_recurrence),
                    )
                    .route("/retention", web::get().to(analytics_routes::get_retention))
                    .route("/geography", web::get().to(analytics_routes::get_geography))
                    .route("/concepts", web::get().to(analytics_routes::get_concepts))
                    .route("/fiscal", web::get().to(analytics_routes::get_fiscal))
                    .route("/payments", web::get().to(analytics_routes::get_payments))
                    .route("/cashflow", web::get().to(analytics_routes::get_cashflow))
                    .route("/payroll", web::get().to(analytics_routes::get_payroll))
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
                    ),
            )
    })
    .bind(&bind_addr)?
    .run()
    .await
}
