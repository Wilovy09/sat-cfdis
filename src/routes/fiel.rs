use actix_multipart::Multipart;
use actix_web::{HttpRequest, HttpResponse, web};
use aws_sdk_s3::Client as S3Client;
use futures_util::StreamExt as _;
use serde::Serialize;

use crate::config::Config;
use crate::db::DbPool;
use crate::errors::GENERIC_INTERNAL_ERROR;
use crate::services::sat_login::{self, LoginOutcome, SatCredential};
use crate::services::session;

#[derive(Serialize)]
struct FielStatus {
    configured: bool,
    uploaded_at: Option<String>,
}

/// POST /api/v1/users/rfcs/{rfc}/fiel
/// Multipart body: fields "cert" (.cer bytes), "key" (.key bytes), "password" (text).
pub async fn upload(
    req: HttpRequest,
    path: web::Path<String>,
    mut payload: Multipart,
    pool: web::Data<DbPool>,
    s3: web::Data<S3Client>,
    cfg: web::Data<Config>,
) -> HttpResponse {
    let rfc = path.into_inner().to_uppercase();

    let user_id = match session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };

    match crate::db::users::get_credentials_for_rfc(&pool, &user_id, &rfc).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return HttpResponse::Forbidden()
                .json(serde_json::json!({"error": "RFC not found or not yours"}));
        }
        Err(e) => {
            tracing::error!(rfc = %rfc, "FIEL upload: ownership check failed: {e}");
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "DB error"}));
        }
    }

    let mut cert_bytes: Option<Vec<u8>> = None;
    let mut key_bytes: Option<Vec<u8>> = None;
    let mut password = String::new();

    while let Some(item) = payload.next().await {
        let mut field = match item {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(user_id = %user_id, "FIEL upload: multipart read failed: {e}");
                return HttpResponse::BadRequest()
                    .json(serde_json::json!({"error": GENERIC_INTERNAL_ERROR}));
            }
        };

        let name = field.name().unwrap_or("").to_string();
        let mut data: Vec<u8> = Vec::new();
        while let Some(chunk) = field.next().await {
            match chunk {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(e) => {
                    tracing::warn!(user_id = %user_id, "FIEL upload: multipart chunk failed: {e}");
                    return HttpResponse::BadRequest()
                        .json(serde_json::json!({"error": GENERIC_INTERNAL_ERROR}));
                }
            }
        }

        match name.as_str() {
            "cert" => cert_bytes = Some(data),
            "key" => key_bytes = Some(data),
            "password" => password = String::from_utf8_lossy(&data).into_owned(),
            _ => {}
        }
    }

    let cert = match cert_bytes {
        Some(b) if !b.is_empty() => b,
        _ => {
            return HttpResponse::BadRequest()
                .json(serde_json::json!({"error": "Missing or empty cert field"}));
        }
    };
    let key = match key_bytes {
        Some(b) if !b.is_empty() => b,
        _ => {
            return HttpResponse::BadRequest()
                .json(serde_json::json!({"error": "Missing or empty key field"}));
        }
    };

    // A FIEL is only stored once the SAT opens a session with it for this RFC; that same
    // login verifies the owner (L19-01), which a FIEL-only RFC has no other way to do.
    let fiel = SatCredential::Fiel {
        cert_der: &cert,
        key_der: &key,
        password: &password,
    };
    match sat_login::check(&cfg, &rfc, &fiel).await {
        LoginOutcome::Accepted => {}
        LoginOutcome::Rejected => {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": "El SAT rechazó la e.firma. Revisa que los archivos sean de este RFC y que la contraseña sea la correcta."
            }));
        }
        LoginOutcome::Unavailable => {
            return HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "error": "No pudimos comunicarnos con el SAT. Inténtalo de nuevo en unos minutos."
            }));
        }
    }

    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    if let Err(e) =
        crate::services::fiel::store(&pool, &s3, &bucket, &rfc, &cert, &key, &password).await
    {
        tracing::error!(rfc = %rfc, "FIEL upload: {e}");
        return HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": GENERIC_INTERNAL_ERROR}));
    }
    if let Err(e) = crate::db::users::mark_verified_now(&pool, &user_id, &rfc).await {
        tracing::error!(rfc = %rfc, "FIEL upload: marking the owner verified failed: {e}");
    }

    tracing::info!(rfc = %rfc, "FIEL credentials validated and stored");
    HttpResponse::Ok().json(serde_json::json!({"ok": true}))
}

/// GET /api/v1/users/rfcs/{rfc}/fiel
pub async fn get_status(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
) -> HttpResponse {
    let rfc = path.into_inner().to_uppercase();

    let user_id = match session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };

    match crate::db::users::get_credentials_for_rfc(&pool, &user_id, &rfc).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return HttpResponse::Forbidden()
                .json(serde_json::json!({"error": "RFC not found or not yours"}));
        }
        Err(e) => {
            tracing::error!(rfc = %rfc, "FIEL status: DB error: {e}");
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "DB error"}));
        }
    }

    match crate::db::fiel::get(&pool, &rfc).await {
        Ok(Some(row)) => HttpResponse::Ok().json(FielStatus {
            configured: true,
            uploaded_at: Some(row.uploaded_at),
        }),
        Ok(None) => HttpResponse::Ok().json(FielStatus {
            configured: false,
            uploaded_at: None,
        }),
        Err(e) => {
            tracing::error!(rfc = %rfc, "FIEL status: DB error: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({"error": "DB error"}))
        }
    }
}

/// DELETE /api/v1/users/rfcs/{rfc}/fiel
pub async fn delete(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
    s3: web::Data<S3Client>,
    cfg: web::Data<Config>,
) -> HttpResponse {
    let rfc = path.into_inner().to_uppercase();

    let user_id = match session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };

    match crate::db::users::get_credentials_for_rfc(&pool, &user_id, &rfc).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return HttpResponse::Forbidden()
                .json(serde_json::json!({"error": "RFC not found or not yours"}));
        }
        Err(e) => {
            tracing::error!(rfc = %rfc, "FIEL delete: ownership check failed: {e}");
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "DB error"}));
        }
    }

    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    if !bucket.is_empty() {
        for s3_key in [
            format!("fiel/{rfc}/cert.cer"),
            format!("fiel/{rfc}/key.key"),
        ] {
            if let Err(e) = crate::services::s3::delete_fiel(&s3, &bucket, &s3_key).await {
                tracing::warn!(rfc = %rfc, "FIEL delete: S3 delete failed (continuing): {e}");
            }
        }
    }

    match crate::db::fiel::delete(&pool, &rfc).await {
        Ok(true) => {
            tracing::info!(rfc = %rfc, "FIEL credentials removed");
            HttpResponse::Ok().json(serde_json::json!({"ok": true}))
        }
        Ok(false) => HttpResponse::NotFound()
            .json(serde_json::json!({"error": "FIEL not configured for this RFC"})),
        Err(e) => {
            tracing::error!(rfc = %rfc, "FIEL delete: DB error: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({"error": "DB error"}))
        }
    }
}
