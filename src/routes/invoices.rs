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
    db::{DbPool, jobs as db_jobs},
    errors::AppError,
    models::{
        auth::Auth,
        invoice::{DownloadRequest, ListRequest},
    },
    services::{crypto, fiel, php_cli::PhpCli, storage},
    state::CaptchaMap,
};

// ---------------------------------------------------------------------------
// XML date extraction helper
// ---------------------------------------------------------------------------

/// Scan raw CFDI XML bytes and extract the path components for storage.
///
/// Returns `(rfc_emisor, rfc_receptor, year, month, day)`.
/// Falls back to `"UNKNOWN"` for RFCs and current UTC date on parse failure.
pub(crate) fn extract_cfdi_path_info(bytes: &[u8]) -> (String, String, u32, u32, u32) {
    // Find first occurrence of `tag` in bytes, then look for `attr="` within the
    // following `window` bytes and return the value up to the closing `"`.
    fn find_attr(bytes: &[u8], tag: &[u8], attr: &[u8], window: usize) -> Option<String> {
        let pos = bytes.windows(tag.len()).position(|w| w == tag)?;
        let region_end = (pos + window).min(bytes.len());
        let region = &bytes[pos..region_end];
        let a = region.windows(attr.len()).position(|w| w == attr)?;
        let val_start = a + attr.len();
        let end = region[val_start..].iter().position(|&b| b == b'"')?;
        std::str::from_utf8(&region[val_start..val_start + end])
            .ok()
            .map(|s| s.to_uppercase())
    }

    let rfc_emisor =
        find_attr(bytes, b"Emisor", b"Rfc=\"", 300).unwrap_or_else(|| "UNKNOWN".into());
    let rfc_receptor =
        find_attr(bytes, b"Receptor", b"Rfc=\"", 300).unwrap_or_else(|| "UNKNOWN".into());

    // Extract Fecha="YYYY-MM-DD
    let fecha_needle = b"Fecha=\"";
    let (year, month, day) = bytes
        .windows(fecha_needle.len())
        .position(|w| w == fecha_needle)
        .and_then(|pos| {
            let s = pos + fecha_needle.len();
            if bytes.len() < s + 10 {
                return None;
            }
            let y = std::str::from_utf8(&bytes[s..s + 4])
                .ok()?
                .parse::<u32>()
                .ok()?;
            let m = std::str::from_utf8(&bytes[s + 5..s + 7])
                .ok()?
                .parse::<u32>()
                .ok()?;
            let d = std::str::from_utf8(&bytes[s + 8..s + 10])
                .ok()?
                .parse::<u32>()
                .ok()?;
            if m >= 1 && m <= 12 && d >= 1 && d <= 31 {
                Some((y, m, d))
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            let secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let days = secs / 86400;
            let year = 1970u32 + (days / 365) as u32;
            let month = ((days % 365) / 30 + 1).min(12) as u32;
            (year, month, 1)
        });

    (rfc_emisor, rfc_receptor, year, month, day)
}

// ---------------------------------------------------------------------------
// POST /api/v1/invoices/xml-content
// Check S3 first; fall back to SAT download. Returns base64 XML.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct XmlContentRequest {
    pub auth: Auth,
    pub uuid: String,
    /// RFC del emisor (from invoice metadata) — used to build the storage path
    pub rfc_emisor: String,
    /// RFC del receptor (from invoice metadata) — used to build the storage path
    pub rfc_receptor: String,
    /// Emission date "YYYY-MM-DD" — used to build the storage path
    pub fecha: String,
}

#[utoipa::path(
    post,
    path = "/api/v1/invoices/xml-content",
    tag = "Invoices",
    responses(
        (status = 200, description = "Contenido XML del CFDI en base64"),
        (status = 400, description = "Parámetros inválidos"),
    )
)]
pub async fn xml_content(
    cfg: web::Data<Config>,
    s3_client: web::Data<aws_sdk_s3::Client>,
    body: web::Json<XmlContentRequest>,
) -> Result<HttpResponse, AppError> {
    let body = body.into_inner();
    let uuid = body.uuid.to_lowercase();

    // Parse year/month/day from fecha "YYYY-MM-DD"
    let (year, month, day) = parse_fecha_ymd(&body.fecha);

    // 1. Try cache first (local in debug, S3 in release)
    let bucket_ref = cfg.s3_bucket.as_deref().unwrap_or("");
    if let Some(bytes) = storage::get(
        &s3_client,
        bucket_ref,
        &body.rfc_emisor,
        &body.rfc_receptor,
        year,
        month,
        day,
        &uuid,
    )
    .await
    {
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        return Ok(HttpResponse::Ok().json(json!({ "source": "cache", "data_b64": b64 })));
    }

    // 2. Not cached — download from SAT
    let work_dir = TempDir::new().map_err(|e| AppError::internal(e.to_string()))?;
    let output_dir = work_dir.path().join("downloads");
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    let auth_payload = build_auth_payload(body.auth, &work_dir).await?;

    let payload = json!({
        "command": "download",
        "auth":    auth_payload,
        "params": {
            "uuids":         [&uuid],
            "download_type": "emitidos",
            "resource_type": "xml",
            "output_dir":    output_dir.to_string_lossy(),
        }
    });

    let cli = PhpCli::new(&cfg.php_bin, &cfg.php_cli_path);
    let result = cli.run(&payload).await?;

    let files = result["files"]
        .as_array()
        .filter(|f| !f.is_empty())
        .ok_or_else(|| AppError::internal("No files downloaded from SAT"))?;

    let path = files[0]["path"]
        .as_str()
        .ok_or_else(|| AppError::internal("Missing path in CLI response"))?;

    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    // 3. Store for next time — use path info from request (already verified above)
    let _ = storage::upload(
        &s3_client,
        bucket_ref,
        &body.rfc_emisor,
        &body.rfc_receptor,
        year,
        month,
        day,
        &uuid,
        bytes.clone(),
    )
    .await;

    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    Ok(HttpResponse::Ok().json(json!({ "source": "sat", "data_b64": b64 })))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse "YYYY-MM-DD..." into (year, month, day). Falls back to (1970, 1, 1).
fn parse_fecha_ymd(fecha: &str) -> (u32, u32, u32) {
    let parse = || -> Option<(u32, u32, u32)> {
        let y = fecha.get(0..4)?.parse::<u32>().ok()?;
        let m = fecha.get(5..7)?.parse::<u32>().ok()?;
        let d = fecha.get(8..10)?.parse::<u32>().ok()?;
        if m >= 1 && m <= 12 && d >= 1 && d <= 31 {
            Some((y, m, d))
        } else {
            None
        }
    };
    parse().unwrap_or((1970, 1, 1))
}

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
            "rfc":              rfc.to_uppercase(),
            "password":         password,
            "captcha_api_key":  captcha_api_key
        })),
    }
}

// ---------------------------------------------------------------------------
// list-stream helper
// ---------------------------------------------------------------------------

/// Runs the PHP `auth` command for a CIEC session.
///
/// Handles any captcha challenge inline (emitting SSE events to the browser
/// and waiting for the solve endpoint to deliver the answer), then returns
/// the original auth payload enriched with the session cookies so parallel
/// chunk workers can reuse the session without a new login.
/// Spawns a PHP `list-stream` process for one date chunk.
///
/// Forwards all output lines to `line_tx`. Handles `__captcha__` events inline
/// (keeps stdin open, writes answers from captcha_map) so CIEC sessions can
/// authenticate without a separate auth phase.
async fn run_php_chunk(
    php_bin: String,
    php_cli_path: String,
    auth_payload: serde_json::Value,
    chunk_from: String,
    chunk_to: String,
    download_type: String,
    line_tx: tokio::sync::mpsc::Sender<String>,
    captcha_map: web::Data<CaptchaMap>,
    sse_tx: tokio::sync::mpsc::Sender<Bytes>,
) {
    let payload = json!({
        "command": "list-stream",
        "auth":    auth_payload,
        "params": {
            "period_from":       chunk_from,
            "period_to":         chunk_to,
            "download_type":     download_type,
            "auto_download_xml": true,
        }
    });

    let mut input_bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(_) => return,
    };
    input_bytes.push(b'\n');

    let mut child = match tokio::process::Command::new(&php_bin)
        .arg(&php_cli_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    // Keep stdin open — needed to send captcha answers mid-stream.
    let mut php_stdin = child.stdin.take();
    if let Some(ref mut stdin) = php_stdin {
        let _ = stdin.write_all(&input_bytes).await;
    }

    // Drain stderr in a background task so the pipe never blocks and errors are logged.
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if !line.is_empty() {
                    tracing::warn!(php_stderr = %line, "PHP list-stream stderr");
                }
            }
        });
    }

    if let Some(stdout) = child.stdout.take() {
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() {
                continue;
            }

            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&line) {
                // Captcha challenge — forward to browser, wait for answer, write to PHP stdin
                if data
                    .get("__captcha__")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
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
                    let _ = sse_tx.send(Bytes::from(format!("data: {evt}\n\n"))).await;
                    match rx.await {
                        Ok(answer) => {
                            if let Some(ref mut stdin) = php_stdin {
                                let _ = stdin.write_all(format!("{answer}\n").as_bytes()).await;
                            }
                        }
                        Err(_) => break, // client disconnected
                    }
                    continue;
                }
            }

            if line_tx.send(line).await.is_err() {
                break;
            }
        }
    }

    let _ = child.wait().await;
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/health",
    tag = "Health",
    responses((status = 200, description = "Servicio activo"))
)]
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(json!({ "status": "ok" }))
}

// ---------------------------------------------------------------------------
// POST /api/v1/invoices/list
// ---------------------------------------------------------------------------

#[utoipa::path(
    post,
    path = "/api/v1/invoices/list",
    tag = "Invoices",
    responses(
        (status = 200, description = "Lista de CFDIs"),
        (status = 400, description = "Credenciales inválidas"),
    )
)]
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

#[utoipa::path(
    post,
    path = "/api/v1/invoices/download",
    tag = "Invoices",
    responses(
        (status = 200, description = "ZIP con los XMLs/PDFs descargados"),
        (status = 400, description = "Credenciales o UUIDs inválidos"),
    )
)]
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

    // Serialize type counts from PHP CLI for the response header
    let type_counts_header = cli_result["type_counts"]
        .as_object()
        .map(|m| serde_json::to_string(m).unwrap_or_default())
        .unwrap_or_default();

    // --- Single file: return it directly ---
    if files.len() == 1 {
        let path = files[0]["path"]
            .as_str()
            .ok_or_else(|| AppError::internal("Missing path in CLI response"))?;
        let filename = files[0]["filename"].as_str().unwrap_or("invoice");

        let content = tokio::fs::read(path)
            .await
            .map_err(|e| AppError::internal(format!("Could not read downloaded file: {e}")))?;

        let mut response = HttpResponse::Ok();
        response.content_type(resource_type.mime_type());
        response.insert_header((
            "Content-Disposition",
            format!("attachment; filename=\"{filename}\""),
        ));
        if !type_counts_header.is_empty() {
            response.insert_header(("X-Invoice-Type-Counts", type_counts_header));
        }
        return Ok(response.body(content));
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

    let mut response = HttpResponse::Ok();
    response.content_type("application/zip");
    response.insert_header((
        "Content-Disposition",
        "attachment; filename=\"invoices.zip\"",
    ));
    if !type_counts_header.is_empty() {
        response.insert_header(("X-Invoice-Type-Counts", type_counts_header));
    }
    Ok(response.body(zip_bytes))
}

// ---------------------------------------------------------------------------
// POST /api/v1/invoices/download/stream  — SSE: handles captcha, ends with
//   {"__download__": true, "filename": "...", "content_type": "...", "data_b64": "..."}
// ---------------------------------------------------------------------------

pub async fn download_stream(
    cfg: web::Data<Config>,
    captcha_map: web::Data<CaptchaMap>,
    s3_client: web::Data<aws_sdk_s3::Client>,
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

    // Capture RFC and S3 state before auth is consumed
    let s3_bucket = cfg.s3_bucket.clone();
    let s3 = s3_client.into_inner();
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

        // Upload XML files to storage (local in debug, S3 in release)
        let should_upload_dl = cfg!(debug_assertions) || s3_bucket.is_some();
        if should_upload_dl {
            let bucket_dl = s3_bucket.as_deref().unwrap_or("");
            for file in &files {
                if let Some(path) = file["path"].as_str() {
                    if path.ends_with(".xml") {
                        if let Ok(bytes) = tokio::fs::read(path).await {
                            let fname = file["filename"].as_str().unwrap_or("");
                            let uuid_str = fname.trim_end_matches(".xml");
                            let (rfc_e, rfc_r, year, month, day) = extract_cfdi_path_info(&bytes);
                            let _ = storage::upload(
                                &s3, bucket_dl,
                                &rfc_e, &rfc_r, year, month, day,
                                uuid_str, bytes,
                            ).await;
                        }
                    }
                }
            }
        }

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

        let type_counts = result.get("type_counts").cloned().unwrap_or(serde_json::Value::Null);

        match packed {
            Ok(Ok((bytes, filename, content_type))) => {
                use base64::Engine as _;
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                let evt = json!({
                    "__download__": true,
                    "filename":     filename,
                    "content_type": content_type,
                    "data_b64":     b64,
                    "type_counts":  type_counts,
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
// POST /api/v1/invoices/list/stream  — sequential, month-chunked
//
// Splits the date range into monthly chunks and processes them one at a time.
// SAT blocks parallel sessions for the same RFC (CIEC or FIEL), so sequential
// is the only safe approach. Each chunk spawns one PHP list-stream process
// which handles login/captcha inline on first request.
// ---------------------------------------------------------------------------

pub async fn list_stream(
    cfg: web::Data<Config>,
    captcha_map: web::Data<CaptchaMap>,
    pool: web::Data<DbPool>,
    s3_client: web::Data<aws_sdk_s3::Client>,
    body: web::Json<ListRequest>,
) -> Result<HttpResponse, AppError> {
    let body = body.into_inner();
    let work_dir = TempDir::new().map_err(|e| AppError::internal(e.to_string()))?;

    let period_from = body
        .period_from
        .ok_or_else(|| AppError::bad_request("period_from is required"))?;
    let period_to = body
        .period_to
        .ok_or_else(|| AppError::bad_request("period_to is required"))?;

    let download_type = body.download_type.as_str().to_string();
    let user_rfc_ls = body.auth.rfc().unwrap_or_else(|| "UNKNOWN".to_string());
    let auth_type_ls = match &body.auth {
        Auth::Ciec { .. } => "ciec",
        Auth::Fiel { .. } => "fiel",
    }
    .to_string();

    // Encrypt auth for queue storage BEFORE build_auth_payload consumes it.
    let enc_key = crypto::load_key();
    let auth_enc_ls = crypto::encrypt(
        &enc_key,
        &serde_json::to_string(&body.auth).unwrap_or_default(),
    )
    .unwrap_or_default();

    let auth_payload = build_auth_payload(body.auth, &work_dir).await?;

    let php_bin = cfg.php_bin.clone();
    let php_cli_path = cfg.php_cli_path.clone();
    let pool_ls = pool.into_inner();
    let s3_ls = s3_client.into_inner();
    let s3_bucket_ls = cfg.s3_bucket.clone().unwrap_or_default();

    // SSE channel: raw Bytes frames flow from the worker task to the stream.
    // We avoid Result<Bytes, actix_web::Error> because actix_web::Error is !Send.
    let (sse_tx, mut sse_rx) = tokio::sync::mpsc::channel::<Bytes>(500);

    tokio::spawn(async move {
        // Keep temp dir alive until FIEL PEM files have been read by PHP.
        let _keep_alive = work_dir;

        // Single PHP list-stream process for the full date range.
        // The scraper handles day-by-day iteration internally, keeping one
        // session alive throughout — no multiple logins, no SAT blocking.
        let (line_tx, mut line_rx) = tokio::sync::mpsc::channel::<String>(2000);

        let handle = tokio::spawn(run_php_chunk(
            php_bin,
            php_cli_path,
            auth_payload,
            period_from.clone(),
            period_to.clone(),
            download_type.clone(),
            line_tx,
            captcha_map,
            sse_tx.clone(),
        ));

        // ------------------------------------------------------------------
        // Merge: forward invoice lines to SSE; aggregate __done__ events
        // ------------------------------------------------------------------
        let mut total: u64 = 0;
        let mut type_counts = serde_json::Map::new();
        let mut limit_cursor: Option<String> = None; // set when __limit_reached__ received
        let mut job_id_ls: Option<String> = None; // DB job id, created on first invoice

        while let Some(line) = line_rx.recv().await {
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&line) {
                // Keepalive heartbeat
                if data
                    .get("__keepalive__")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    let _ = sse_tx.send(Bytes::from(": keepalive\n\n")).await;
                    continue;
                }

                // Auto-downloaded XML — save to storage, never forward to browser
                if data
                    .get("__xml_ready__")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    let uuid_str = data["uuid"].as_str().unwrap_or("").to_string();
                    let xml_b64 = data["xml_b64"].as_str().unwrap_or("").to_string();
                    let s3_ref = s3_ls.clone();
                    let bucket = s3_bucket_ls.clone();
                    tokio::spawn(async move {
                        use base64::Engine as _;
                        if let Ok(bytes) =
                            base64::engine::general_purpose::STANDARD.decode(&xml_b64)
                        {
                            let should_upload = cfg!(debug_assertions) || !bucket.is_empty();
                            if should_upload {
                                let (rfc_e, rfc_r, year, month, day) =
                                    extract_cfdi_path_info(&bytes);
                                let _ = storage::upload(
                                    &s3_ref, &bucket, &rfc_e, &rfc_r, year, month, day, &uuid_str,
                                    bytes,
                                )
                                .await;
                            }
                        }
                    });
                    continue;
                }

                // SAT download limit reached — save job to DB and forward to browser
                if data
                    .get("__limit_reached__")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    let cursor = data["date"].as_str().unwrap_or(&period_from).to_string();
                    limit_cursor = Some(cursor.clone());

                    // Create or reuse job record
                    let jid = if let Some(ref id) = job_id_ls {
                        id.clone()
                    } else {
                        match db_jobs::insert(
                            &pool_ls,
                            &user_rfc_ls,
                            &auth_type_ls,
                            &auth_enc_ls,
                            &download_type,
                            &period_from,
                            &period_to,
                        )
                        .await
                        {
                            Ok(id) => {
                                job_id_ls = Some(id.clone());
                                id
                            }
                            Err(e) => {
                                tracing::error!("Queue insert failed: {e}");
                                String::new()
                            }
                        }
                    };

                    if !jid.is_empty() {
                        let resume_at = db_jobs::utc_offset(24 * 3600 + 1800); // +24.5 h
                        let _ = db_jobs::pause_limit(
                            &pool_ls,
                            &jid,
                            &cursor,
                            total as i64,
                            &resume_at,
                            data["reason"].as_str(),
                        )
                        .await;

                        // Attach job_id to the event so the browser can poll status
                        let evt = json!({
                            "__limit_reached__": true,
                            "job_id":    jid,
                            "date":      cursor,
                            "resume_at": resume_at,
                            "reason":    data["reason"],
                        });
                        let _ = sse_tx.send(Bytes::from(format!("data: {evt}\n\n"))).await;
                    } else {
                        // No DB — still forward the event
                        let _ = sse_tx.send(Bytes::from(format!("data: {line}\n\n"))).await;
                    }
                    continue;
                }

                if data
                    .get("__done__")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    total += data["total"].as_u64().unwrap_or(0);
                    if let Some(tc) = data["type_counts"].as_object() {
                        for (k, v) in tc {
                            let entry = type_counts.entry(k.clone()).or_insert(json!(0));
                            *entry = json!(entry.as_u64().unwrap_or(0) + v.as_u64().unwrap_or(0));
                        }
                    }
                    continue;
                }

                // Regular invoice — store in DB if job is tracking
                if job_id_ls.is_none() {
                    // Create job lazily on first real invoice (so short queries don't pollute DB)
                    if let Ok(id) = db_jobs::insert(
                        &pool_ls,
                        &user_rfc_ls,
                        &auth_type_ls,
                        &auth_enc_ls,
                        &download_type,
                        &period_from,
                        &period_to,
                    )
                    .await
                    {
                        job_id_ls = Some(id);
                    }
                }
                if let Some(ref jid) = job_id_ls {
                    if let Some(uuid) = data["uuid"].as_str().or(data["Uuid"].as_str()) {
                        let _ = db_jobs::upsert_invoice(&pool_ls, jid, uuid, &line).await;
                        if total % 50 == 0 {
                            let cursor = data["fecha"]
                                .as_str()
                                .or(data["Fecha"].as_str())
                                .map(|f| format!("{} 00:00:00", &f[..10.min(f.len())]))
                                .unwrap_or_else(|| period_from.clone());
                            let _ =
                                db_jobs::update_found(&pool_ls, jid, total as i64, &cursor).await;
                        }
                    }
                }
            }
            // Forward invoice line as SSE
            let _ = sse_tx.send(Bytes::from(format!("data: {line}\n\n"))).await;
        }

        let _ = handle.await;

        // Mark job complete in DB (if it was created and no limit was hit)
        if limit_cursor.is_none() {
            if let Some(ref jid) = job_id_ls {
                let _ = db_jobs::complete(&pool_ls, jid, &period_to, total as i64).await;
            }
        }

        // Send aggregated __done__ event
        let mut done_map = serde_json::Map::new();
        done_map.insert("__done__".into(), json!(true));
        done_map.insert("total".into(), json!(total));
        done_map.insert("type_counts".into(), json!(type_counts));
        if let Some(ref jid) = job_id_ls {
            done_map.insert("job_id".into(), json!(jid));
        }
        let done = serde_json::Value::Object(done_map);
        let _ = sse_tx.send(Bytes::from(format!("data: {done}\n\n"))).await;
    });

    let sse_stream = async_stream::stream! {
        while let Some(item) = sse_rx.recv().await {
            yield Ok::<Bytes, actix_web::Error>(item);
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("X-Accel-Buffering", "no")) // disable nginx buffering on EC2
        .streaming(sse_stream))
}

// ---------------------------------------------------------------------------
// POST /api/v1/invoices/bulk/stream  — SAT Descarga Masiva (FIEL only)
//
// Runs the PHP `descarga-masiva` command which:
//   1. Submits a SolicitaDescarga request → requestId
//   2. Polls VerificaSolicitudDescarga every 5 s until Finished
//   3. Downloads each package zip → extracts XMLs → saves to output_dir
//
// All `__progress__` events are forwarded as SSE so the browser shows a
// live progress indicator.  After the PHP process exits, the downloaded XMLs
// are uploaded to storage (local in debug, S3 in release) and a final
// `__done__` event is emitted.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct BulkStreamRequest {
    pub auth: Auth,
    pub period_from: String,
    pub period_to: String,
    #[serde(default = "default_emitidos")]
    pub download_type: String,
}

fn default_emitidos() -> String {
    "emitidos".to_string()
}

pub async fn bulk_stream(
    cfg: web::Data<Config>,
    s3_client: web::Data<aws_sdk_s3::Client>,
    body: web::Json<BulkStreamRequest>,
) -> Result<HttpResponse, AppError> {
    let body = body.into_inner();

    let work_dir = TempDir::new().map_err(|e| AppError::internal(e.to_string()))?;
    let output_dir = work_dir.path().join("bulk_downloads");
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    let s3_bucket = cfg.s3_bucket.clone();
    let s3 = s3_client.into_inner();
    let auth_payload = build_auth_payload(body.auth, &work_dir).await?;

    let payload = json!({
        "command": "descarga-masiva",
        "auth":    auth_payload,
        "params": {
            "period_from":   body.period_from,
            "period_to":     body.period_to,
            "download_type": body.download_type,
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

    let sse_stream = async_stream::stream! {
        let _keep_alive = work_dir;

        let reader = tokio::io::BufReader::new(stdout);
        let mut lines = reader.lines();
        let mut output_buf = String::new();

        while let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() { continue; }

            // `__progress__` lines are single-line JSON — forward immediately
            if line.contains("\"__progress__\"") {
                yield Ok::<Bytes, actix_web::Error>(
                    Bytes::from(format!("data: {line}\n\n"))
                );
                continue;
            }

            // Accumulate final outputJson (may be multi-line from JSON_PRETTY_PRINT)
            output_buf.push_str(&line);
            output_buf.push('\n');
        }

        let _ = child.wait().await;

        // Parse final result
        let result = match serde_json::from_str::<serde_json::Value>(&output_buf) {
            Ok(v) => v,
            Err(_) => {
                let err = output_buf.trim().replace('"', "\\\"");
                yield Ok(Bytes::from(format!("data: {{\"__error__\":\"PHP returned no valid JSON: {err}\"}}\n\n")));
                return;
            }
        };

        let total = result["total"].as_u64().unwrap_or(0);
        let files = result["files"].as_array().cloned().unwrap_or_default();

        // Upload XMLs to storage.
        // Debug: always writes to local filesystem (bucket param ignored).
        // Release: only when S3_BUCKET is configured.
        let should_upload = cfg!(debug_assertions) || s3_bucket.is_some();
        if should_upload {
            let bucket_str = s3_bucket.as_deref().unwrap_or("");
            for file in &files {
                if let Some(path) = file["path"].as_str() {
                    if let Ok(bytes) = tokio::fs::read(path).await {
                        let uuid_str = file["uuid"].as_str()
                            .unwrap_or_else(|| file["filename"].as_str().unwrap_or(""))
                            .trim_end_matches(".xml");
                        let (rfc_e, rfc_r, year, month, day) = extract_cfdi_path_info(&bytes);
                        let _ = storage::upload(
                            &s3, bucket_str,
                            &rfc_e, &rfc_r, year, month, day,
                            uuid_str, bytes,
                        ).await;
                    }
                }
            }
        }

        let done = json!({
            "__done__": true,
            "total":      total,
            "request_id": result["request_id"],
        });
        yield Ok(Bytes::from(format!("data: {done}\n\n")));
    };

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("X-Accel-Buffering", "no"))
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
