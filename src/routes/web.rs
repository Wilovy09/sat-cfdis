use actix_multipart::Multipart;
use actix_web::{HttpResponse, web};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::TryStreamExt as _;
use serde_json::json;
use std::collections::HashMap;
use tera::Tera;

use crate::{config::Config, errors::AppError, services::php_cli::PhpCli};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse every multipart field into a flat `HashMap<name, bytes>`.
async fn parse_multipart(mut payload: Multipart) -> Result<HashMap<String, Vec<u8>>, AppError> {
    let mut fields: HashMap<String, Vec<u8>> = HashMap::new();
    while let Some(mut field) = payload
        .try_next()
        .await
        .map_err(|e| AppError::bad_request(e.to_string()))?
    {
        let name = match field.name() {
            Some(n) => n.to_string(),
            None => continue, // skip nameless fields
        };
        let mut data: Vec<u8> = Vec::new();
        while let Some(chunk) = field
            .try_next()
            .await
            .map_err(|e| AppError::bad_request(e.to_string()))?
        {
            data.extend_from_slice(&chunk);
        }
        fields.insert(name, data);
    }
    Ok(fields)
}

fn get_str(fields: &HashMap<String, Vec<u8>>, key: &str) -> String {
    fields
        .get(key)
        .map(|v| String::from_utf8_lossy(v).trim().to_string())
        .unwrap_or_default()
}

fn render_error(tmpl: &Tera, message: &str) -> Result<HttpResponse, AppError> {
    let mut ctx = tera::Context::new();
    ctx.insert("message", message);
    let html = tmpl
        .render("error.html", &ctx)
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::UnprocessableEntity()
        .content_type("text/html; charset=utf-8")
        .body(html))
}

// ---------------------------------------------------------------------------
// GET /
// ---------------------------------------------------------------------------

pub async fn analytics_page(tmpl: web::Data<Tera>) -> Result<HttpResponse, AppError> {
    let ctx = tera::Context::new();
    let html = tmpl
        .render("analytics.html", &ctx)
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html))
}

pub async fn index(
    tmpl: web::Data<Tera>,
    cfg: web::Data<Config>,
) -> Result<HttpResponse, AppError> {
    let mut ctx = tera::Context::new();
    ctx.insert("captcha_enabled", &cfg.captcha_enabled);
    let html = tmpl
        .render("index.html", &ctx)
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html))
}

// ---------------------------------------------------------------------------
// POST /web/list
// ---------------------------------------------------------------------------

pub async fn list_web(
    tmpl: web::Data<Tera>,
    cfg: web::Data<Config>,
    payload: Multipart,
) -> Result<HttpResponse, AppError> {
    let fields = parse_multipart(payload).await?;

    let auth_type = get_str(&fields, "auth_type");
    let period_from = get_str(&fields, "period_from");
    let period_to = get_str(&fields, "period_to");
    let download_type = get_str(&fields, "download_type");

    if period_from.is_empty() || period_to.is_empty() {
        return render_error(&tmpl, "El período de fechas es obligatorio.");
    }

    // Build auth payload + keep raw bytes for re-embedding in the template
    // so the download buttons can reuse credentials from JS without a round trip.
    let work_dir = tempfile::TempDir::new().map_err(|e| AppError::internal(e.to_string()))?;

    let (auth_payload, creds_for_js) = match auth_type.as_str() {
        "fiel" => {
            let cert_bytes = fields.get("cert_file").cloned().unwrap_or_default();
            let key_bytes = fields.get("key_file").cloned().unwrap_or_default();
            let password = get_str(&fields, "fiel_password");

            if cert_bytes.is_empty() {
                return render_error(&tmpl, "Sube el certificado (.cer).");
            }
            if key_bytes.is_empty() {
                return render_error(&tmpl, "Sube la clave privada (.key).");
            }

            let cert_b64 = STANDARD.encode(&cert_bytes);
            let key_b64 = STANDARD.encode(&key_bytes);

            let (cert_pem, key_pem) =
                crate::services::fiel::der_to_pem(&cert_b64, &key_b64, &password, work_dir.path())
                    .await
                    .map_err(|e| AppError::bad_request(format!("Error FIEL: {e}")))?;

            let auth = json!({
                "type":          "fiel",
                "cert_pem_path": cert_pem.to_string_lossy(),
                "key_pem_path":  key_pem.to_string_lossy(),
                "password":      ""
            });

            // What JS will use for subsequent download requests
            let creds = json!({
                "type":        "fiel",
                "certificate": cert_b64,
                "private_key": key_b64,
                "password":    password
            });

            (auth, creds)
        }

        "ciec" => {
            let rfc = get_str(&fields, "rfc");
            let password = get_str(&fields, "ciec_password");

            if rfc.is_empty() || password.is_empty() {
                return render_error(&tmpl, "RFC y contraseña CIEC son obligatorios.");
            }

            let auth = json!({"type": "ciec", "rfc": rfc, "password": password});
            let creds = json!({"type": "ciec", "rfc": rfc, "password": password});
            (auth, creds)
        }

        _ => return render_error(&tmpl, "Tipo de autenticación inválido."),
    };

    let cli_payload = json!({
        "command": "list",
        "auth":   auth_payload,
        "params": {
            "period_from":   period_from,
            "period_to":     period_to,
            "download_type": download_type,
        }
    });

    let cli = PhpCli::new(&cfg.php_bin, &cfg.php_cli_path);
    let result = cli.run(&cli_payload).await?;

    let invoices = result["invoices"].as_array().cloned().unwrap_or_default();
    let total = result["total"].as_u64().unwrap_or(invoices.len() as u64);

    let mut ctx = tera::Context::new();
    ctx.insert("invoices", &invoices);
    ctx.insert("total", &total);
    ctx.insert("period_from", &period_from);
    ctx.insert("period_to", &period_to);
    ctx.insert("download_type", &download_type);
    // Embed credentials as JSON string so app.js can read window.__CREDS__
    ctx.insert(
        "creds_json",
        &serde_json::to_string(&creds_for_js).unwrap_or_default(),
    );

    let html = tmpl
        .render("invoices.html", &ctx)
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html))
}
