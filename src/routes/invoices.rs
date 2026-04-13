use actix_web::{HttpResponse, web};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;
use std::io::Write as _;
use std::process::Stdio;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt as _;
use tokio::io::AsyncWriteExt as _;
use zip::write::SimpleFileOptions;

use crate::{
    config::Config,
    errors::AppError,
    models::{
        auth::Auth,
        invoice::{DownloadRequest, ListRequest},
    },
    services::{fiel, php_cli::PhpCli},
    state::CaptchaMap,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Builds the auth section of the PHP CLI payload.
/// For FIEL: converts DER → PEM inside `work_dir` and returns paths.
/// For CIEC: returns RFC + password directly.
async fn build_auth_payload(auth: Auth, work_dir: &TempDir) -> Result<serde_json::Value, AppError> {
    match auth {
        Auth::Fiel {
            certificate,
            private_key,
            password,
        } => {
            let (cert_pem, key_pem) =
                fiel::der_to_pem(&certificate, &private_key, &password, work_dir.path())
                    .await
                    .map_err(|e| AppError::bad_request(e.to_string()))?;

            Ok(json!({
                "type":          "fiel",
                "cert_pem_path": cert_pem.to_string_lossy(),
                "key_pem_path":  key_pem.to_string_lossy(),
                "password":      ""   // key was already decrypted by openssl pkcs8
            }))
        }
        Auth::Ciec {
            rfc,
            password,
            captcha_api_key,
        } => Ok(json!({
            "type":             "ciec",
            "rfc":              rfc,
            "password":         password,
            "captcha_api_key":  captcha_api_key
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------

pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(json!({ "status": "ok" }))
}

// ---------------------------------------------------------------------------
// POST /api/v1/invoices/list
// ---------------------------------------------------------------------------

pub async fn list_invoices(
    cfg: web::Data<Config>,
    body: web::Json<ListRequest>,
) -> Result<HttpResponse, AppError> {
    let body = body.into_inner();
    let work_dir = TempDir::new().map_err(|e| AppError::internal(e.to_string()))?;

    let auth_payload = build_auth_payload(body.auth, &work_dir).await?;

    let mut params = json!({
        "download_type": body.download_type.as_str(),
    });

    if let Some(uuids) = body.uuids {
        params["uuids"] = json!(uuids);
    } else {
        let from = body.period_from.ok_or_else(|| {
            AppError::bad_request("period_from is required when uuids is not set")
        })?;
        let to = body
            .period_to
            .ok_or_else(|| AppError::bad_request("period_to is required when uuids is not set"))?;
        params["period_from"] = json!(from);
        params["period_to"] = json!(to);
    }

    if let Some(state) = body.state {
        params["state"] = json!(state);
    }

    let payload = json!({
        "command": "list",
        "auth":    auth_payload,
        "params":  params,
    });

    let cli = PhpCli::new(&cfg.php_bin, &cfg.php_cli_path);
    let result = cli.run(&payload).await?;

    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// POST /api/v1/invoices/download
// ---------------------------------------------------------------------------

pub async fn download_invoices(
    cfg: web::Data<Config>,
    body: web::Json<DownloadRequest>,
) -> Result<HttpResponse, AppError> {
    let body = body.into_inner();

    if body.uuids.is_empty() {
        return Err(AppError::bad_request("uuids must not be empty"));
    }

    let work_dir = TempDir::new().map_err(|e| AppError::internal(e.to_string()))?;
    let output_dir = work_dir.path().join("downloads");
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    let auth_payload = build_auth_payload(body.auth, &work_dir).await?;
    let resource_type = body.resource_type;

    let payload = json!({
        "command": "download",
        "auth":    auth_payload,
        "params": {
            "uuids":         body.uuids,
            "download_type": body.download_type.as_str(),
            "resource_type": resource_type.as_str(),
            "output_dir":    output_dir.to_string_lossy(),
        }
    });

    let cli = PhpCli::new(&cfg.php_bin, &cfg.php_cli_path);
    let cli_result = cli.run(&payload).await?;

    // Collect the downloaded files reported by the PHP CLI
    let files = cli_result["files"]
        .as_array()
        .ok_or_else(|| AppError::internal("PHP CLI returned no files array"))?;

    if files.is_empty() {
        return Err(AppError::internal("No files were downloaded"));
    }

    // --- Single file: return it directly ---
    if files.len() == 1 {
        let path = files[0]["path"]
            .as_str()
            .ok_or_else(|| AppError::internal("Missing path in CLI response"))?;
        let filename = files[0]["filename"].as_str().unwrap_or("invoice");

        let content = tokio::fs::read(path)
            .await
            .map_err(|e| AppError::internal(format!("Could not read downloaded file: {e}")))?;

        return Ok(HttpResponse::Ok()
            .content_type(resource_type.mime_type())
            .insert_header((
                "Content-Disposition",
                format!("attachment; filename=\"{filename}\""),
            ))
            .body(content));
    }

    // --- Multiple files: bundle as ZIP ---
    let zip_bytes = tokio::task::spawn_blocking({
        let files = files.clone();
        move || -> Result<Vec<u8>, AppError> {
            let mut buf = Vec::new();
            let cursor = std::io::Cursor::new(&mut buf);
            let mut zip = zip::ZipWriter::new(cursor);
            let options =
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

            for file in &files {
                let path = file["path"]
                    .as_str()
                    .ok_or_else(|| AppError::internal("Missing path in CLI response"))?;
                let filename = file["filename"].as_str().unwrap_or("invoice");

                let content = std::fs::read(path)
                    .map_err(|e| AppError::internal(format!("Could not read file {path}: {e}")))?;

                zip.start_file(filename, options)
                    .map_err(|e| AppError::internal(e.to_string()))?;
                zip.write_all(&content)
                    .map_err(|e| AppError::internal(e.to_string()))?;
            }

            zip.finish()
                .map_err(|e| AppError::internal(e.to_string()))?;
            Ok(buf)
        }
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))??;

    Ok(HttpResponse::Ok()
        .content_type("application/zip")
        .insert_header((
            "Content-Disposition",
            "attachment; filename=\"invoices.zip\"",
        ))
        .body(zip_bytes))
}

// ---------------------------------------------------------------------------
// POST /api/v1/invoices/download/stream  — SSE: handles captcha, ends with
//   {"__download__": true, "filename": "...", "content_type": "...", "data_b64": "..."}
// ---------------------------------------------------------------------------

pub async fn download_stream(
    cfg: web::Data<Config>,
    captcha_map: web::Data<CaptchaMap>,
    body: web::Json<DownloadRequest>,
) -> Result<HttpResponse, AppError> {
    let body = body.into_inner();

    if body.uuids.is_empty() {
        return Err(AppError::bad_request("uuids must not be empty"));
    }

    let work_dir = TempDir::new().map_err(|e| AppError::internal(e.to_string()))?;
    let output_dir = work_dir.path().join("downloads");
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    let auth_payload = build_auth_payload(body.auth, &work_dir).await?;
    let resource_type = body.resource_type;

    let payload = json!({
        "command": "download",
        "auth":    auth_payload,
        "params": {
            "uuids":         body.uuids,
            "download_type": body.download_type.as_str(),
            "resource_type": resource_type.as_str(),
            "output_dir":    output_dir.to_string_lossy(),
        }
    });

    let mut input_bytes =
        serde_json::to_vec(&payload).map_err(|e| AppError::internal(e.to_string()))?;
    input_bytes.push(b'\n');

    let mut child = tokio::process::Command::new(&cfg.php_bin)
        .arg(&cfg.php_cli_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| AppError::internal(format!("Failed to spawn PHP CLI: {e}")))?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| AppError::internal("Could not capture PHP stdin"))?;

    stdin
        .write_all(&input_bytes)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::internal("Could not capture PHP stdout"))?;

    let mime_type = resource_type.mime_type();

    let sse_stream = async_stream::stream! {
        let _keep_alive = work_dir;
        let mut php_stdin = stdin;
        let mut php_result: Option<serde_json::Value> = None;

        let reader = tokio::io::BufReader::new(stdout);
        let mut lines = reader.lines();
        // outputJson() uses JSON_PRETTY_PRINT so the final result spans multiple
        // lines. Accumulate everything that isn't a captcha event here.
        let mut output_buf = String::new();
        let _ = php_result; // will be overwritten after the loop

        while let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() { continue; }

            // Captcha events are always single-line JSON — detect quickly
            if line.contains("\"__captcha__\"") {
                if let Ok(data) = serde_json::from_str::<serde_json::Value>(&line) {
                    if data.get("__captcha__").and_then(|v| v.as_bool()).unwrap_or(false) {
                        let session_id = uuid::Uuid::new_v4().to_string();
                        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
                        if let Ok(mut map) = captcha_map.lock() {
                            map.insert(session_id.clone(), tx);
                        }
                        let evt = json!({
                            "__captcha__":  true,
                            "session_id":   session_id,
                            "image_base64": data["image_base64"],
                            "mime":         data["mime"],
                        });
                        yield Ok::<Bytes, actix_web::Error>(
                            Bytes::from(format!("data: {evt}\n\n"))
                        );
                        match rx.await {
                            Ok(answer) => { let _ = php_stdin.write_all(format!("{answer}\n").as_bytes()).await; }
                            Err(_) => return,
                        }
                        continue;
                    }
                }
            }

            // Accumulate non-captcha lines to parse as the final JSON result
            output_buf.push_str(&line);
            output_buf.push('\n');
        }

        php_result = serde_json::from_str::<serde_json::Value>(&output_buf).ok();

        let _ = child.wait().await;

        // Build download payload from the files PHP saved to disk
        let result = match php_result {
            Some(r) => r,
            None => {
                yield Ok(Bytes::from("data: {\"__error__\":\"PHP returned no output\"}\n\n"));
                return;
            }
        };

        let files = match result["files"].as_array().filter(|f| !f.is_empty()) {
            Some(f) => f.clone(),
            None => {
                yield Ok(Bytes::from("data: {\"__error__\":\"No files were downloaded\"}\n\n"));
                return;
            }
        };

        let packed = tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, String, &'static str), String> {
            use std::io::Write as _;

            if files.len() == 1 {
                let path     = files[0]["path"].as_str().ok_or("Missing path")?;
                let filename = files[0]["filename"].as_str().unwrap_or("invoice").to_string();
                let bytes    = std::fs::read(path).map_err(|e| e.to_string())?;
                Ok((bytes, filename, mime_type))
            } else {
                let mut buf    = Vec::new();
                let cursor     = std::io::Cursor::new(&mut buf);
                let mut zip    = zip::ZipWriter::new(cursor);
                let options    = zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Stored);

                for file in &files {
                    let path     = file["path"].as_str().ok_or("Missing path")?;
                    let filename = file["filename"].as_str().unwrap_or("invoice");
                    let content  = std::fs::read(path).map_err(|e| e.to_string())?;
                    zip.start_file(filename, options).map_err(|e| e.to_string())?;
                    zip.write_all(&content).map_err(|e| e.to_string())?;
                }
                zip.finish().map_err(|e| e.to_string())?;
                Ok((buf, "invoices.zip".to_string(), "application/zip"))
            }
        })
        .await;

        match packed {
            Ok(Ok((bytes, filename, content_type))) => {
                use base64::Engine as _;
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                let evt = json!({
                    "__download__": true,
                    "filename":     filename,
                    "content_type": content_type,
                    "data_b64":     b64,
                });
                yield Ok(Bytes::from(format!("data: {evt}\n\n")));
            }
            Ok(Err(e)) => {
                let msg = format!("{{\"__error__\":\"Failed to read files: {e}\"}}");
                yield Ok(Bytes::from(format!("data: {msg}\n\n")));
            }
            Err(e) => {
                let msg = format!("{{\"__error__\":\"Task error: {e}\"}}");
                yield Ok(Bytes::from(format!("data: {msg}\n\n")));
            }
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("X-Accel-Buffering", "no"))
        .streaming(sse_stream))
}

// ---------------------------------------------------------------------------
// POST /api/v1/invoices/list/stream  — SSE: one invoice per event, live
// ---------------------------------------------------------------------------

pub async fn list_stream(
    cfg: web::Data<Config>,
    captcha_map: web::Data<CaptchaMap>,
    body: web::Json<ListRequest>,
) -> Result<HttpResponse, AppError> {
    let body = body.into_inner();
    let work_dir = TempDir::new().map_err(|e| AppError::internal(e.to_string()))?;

    let auth_payload = build_auth_payload(body.auth, &work_dir).await?;

    let period_from = body
        .period_from
        .ok_or_else(|| AppError::bad_request("period_from is required"))?;
    let period_to = body
        .period_to
        .ok_or_else(|| AppError::bad_request("period_to is required"))?;

    let payload = json!({
        "command": "list-stream",
        "auth":    auth_payload,
        "params": {
            "period_from":   period_from,
            "period_to":     period_to,
            "download_type": body.download_type.as_str(),
        }
    });

    let mut input_bytes =
        serde_json::to_vec(&payload).map_err(|e| AppError::internal(e.to_string()))?;
    input_bytes.push(b'\n'); // PHP uses fgets() which reads until newline

    // Spawn the PHP CLI
    let mut child = tokio::process::Command::new(&cfg.php_bin)
        .arg(&cfg.php_cli_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| AppError::internal(format!("Failed to spawn PHP CLI: {e}")))?;

    // Write JSON to stdin but keep it open — we may need to write captcha answers later
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| AppError::internal("Could not capture PHP stdin"))?;

    stdin
        .write_all(&input_bytes)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::internal("Could not capture PHP stdout"))?;

    // Stream stdout lines as SSE events, handling captcha challenges inline
    let sse_stream = async_stream::stream! {
        // Keep temp dir alive until PHP finishes reading its PEM files
        let _keep_alive = work_dir;
        // Keep stdin open so we can write captcha answers back to PHP
        let mut php_stdin = stdin;

        let reader = tokio::io::BufReader::new(stdout);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() { continue; }

            // Detect captcha challenge from PHP
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&line) {
                if data.get("__captcha__").and_then(|v| v.as_bool()).unwrap_or(false) {
                    // Generate a unique session ID for this challenge
                    let session_id = uuid::Uuid::new_v4().to_string();
                    let (tx, rx) = tokio::sync::oneshot::channel::<String>();

                    // Register the sender so the solve endpoint can deliver the answer
                    if let Ok(mut map) = captcha_map.lock() {
                        map.insert(session_id.clone(), tx);
                    }

                    // Forward captcha event to browser (adds session_id so browser knows where to POST)
                    let evt = json!({
                        "__captcha__":  true,
                        "session_id":   session_id,
                        "image_base64": data["image_base64"],
                        "mime":         data["mime"],
                    });
                    yield Ok::<Bytes, actix_web::Error>(
                        Bytes::from(format!("data: {evt}\n\n"))
                    );

                    // Block the stream until the browser submits an answer
                    match rx.await {
                        Ok(answer) => {
                            // Write answer back to PHP stdin (fgets reads until newline)
                            let _ = php_stdin.write_all(format!("{answer}\n").as_bytes()).await;
                        }
                        Err(_) => break, // client disconnected
                    }
                    continue;
                }
            }

            // Regular invoice JSON line — forward as SSE
            yield Ok::<Bytes, actix_web::Error>(
                Bytes::from(format!("data: {line}\n\n"))
            );
        }

        // Reap the child process (best-effort)
        let _ = child.wait().await;
    };

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("X-Accel-Buffering", "no")) // disable nginx buffering on EC2
        .streaming(sse_stream))
}

// ---------------------------------------------------------------------------
// POST /api/v1/invoices/captcha/solve
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SolveCaptchaBody {
    pub session_id: String,
    pub answer: String,
}

pub async fn solve_captcha(
    captcha_map: web::Data<CaptchaMap>,
    body: web::Json<SolveCaptchaBody>,
) -> Result<HttpResponse, AppError> {
    let body = body.into_inner();

    let sender = captcha_map
        .lock()
        .map_err(|_| AppError::internal("CaptchaMap lock poisoned"))?
        .remove(&body.session_id)
        .ok_or_else(|| AppError::bad_request("Unknown session_id or captcha already solved"))?;

    sender
        .send(body.answer)
        .map_err(|_| AppError::internal("SSE stream has already closed"))?;

    Ok(HttpResponse::Ok().json(json!({ "ok": true })))
}
