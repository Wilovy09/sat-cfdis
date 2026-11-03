use actix_multipart::Multipart;
use actix_web::{HttpRequest, HttpResponse, web};
use aws_sdk_s3::Client as S3Client;
use futures_util::StreamExt as _;
use serde::Serialize;

use crate::config::Config;
use crate::db::DbPool;
use crate::routes::credential_links::is_valid_rfc;
use crate::routes::users::{
    LinkedCredential, TRY_AGAIN_LATER, check_can_register, register_validated_rfc,
};
use crate::services::sat_login::{self, LoginOutcome, SatCredential};
use crate::services::session;

#[derive(Serialize)]
struct FielStatus {
    configured: bool,
    uploaded_at: Option<String>,
}

/// FIEL files are a few KB; anything far bigger isn't one.
const MAX_FIELD_BYTES: usize = 64 * 1024;

const NOT_YOUR_RFC: &str = "Este RFC no está en tu cuenta.";
const FIEL_REJECTED: &str = "El SAT no aceptó la e.firma. Revisa que el archivo .cer y el .key sean de este RFC, que la contraseña sea la de la clave privada y que la e.firma esté vigente.";
const SAT_UNAVAILABLE: &str =
    "No pudimos comunicarnos con el SAT. Inténtalo de nuevo en unos minutos.";
const FIEL_NOT_SAVED: &str = "El SAT aceptó tu e.firma, pero no pudimos guardarla. Inténtalo de nuevo en unos minutos; si el problema sigue, escríbenos a soporte.";

/// The multipart form both FIEL uploads send: `cert` (.cer), `key` (.key), `password`, and
/// when registering a new RFC, `rfc` and the optional `priority_analysis`.
#[derive(Default)]
struct FielForm {
    rfc: String,
    priority_analysis: Option<String>,
    cert: Vec<u8>,
    key: Vec<u8>,
    password: String,
}

impl FielForm {
    async fn read(mut payload: Multipart) -> Result<Self, &'static str> {
        const UNREADABLE: &str =
            "No pudimos leer los archivos. Vuelve a adjuntarlos e inténtalo de nuevo.";
        let mut form = Self::default();
        while let Some(item) = payload.next().await {
            let mut field = item.map_err(|_| UNREADABLE)?;
            let name = field.name().unwrap_or_default().to_string();
            let mut data = Vec::new();
            while let Some(chunk) = field.next().await {
                let chunk = chunk.map_err(|_| UNREADABLE)?;
                if data.len() + chunk.len() > MAX_FIELD_BYTES {
                    return Err("Uno de los archivos es demasiado grande para ser una e.firma.");
                }
                data.extend_from_slice(&chunk);
            }
            let text = || String::from_utf8_lossy(&data).trim().to_string();
            match name.as_str() {
                "rfc" => form.rfc = text().to_uppercase(),
                "priority_analysis" => {
                    form.priority_analysis = Some(text()).filter(|p| !p.is_empty());
                }
                "password" => form.password = String::from_utf8_lossy(&data).into_owned(),
                "cert" => form.cert = data,
                "key" => form.key = data,
                _ => {}
            }
        }
        if form.cert.is_empty() || form.key.is_empty() || form.password.is_empty() {
            return Err(
                "Adjunta el archivo .cer, el archivo .key y escribe la contraseña de la clave privada.",
            );
        }
        Ok(form)
    }

    fn credential(&self) -> SatCredential<'_> {
        SatCredential::Fiel {
            cert_der: &self.cert,
            key_der: &self.key,
            password: &self.password,
        }
    }

    /// Opens a SAT session with this FIEL for `rfc`: nothing is stored unless it works.
    async fn validate_with_sat(&self, cfg: &Config, rfc: &str) -> Result<(), HttpResponse> {
        match sat_login::check(cfg, rfc, &self.credential()).await {
            LoginOutcome::Accepted => Ok(()),
            LoginOutcome::Rejected => {
                Err(error(HttpResponse::UnprocessableEntity(), FIEL_REJECTED))
            }
            LoginOutcome::Unavailable => {
                Err(error(HttpResponse::ServiceUnavailable(), SAT_UNAVAILABLE))
            }
        }
    }

    async fn store(
        &self,
        pool: &DbPool,
        s3: &S3Client,
        cfg: &Config,
        rfc: &str,
    ) -> Result<(), HttpResponse> {
        let bucket = cfg.s3_bucket.clone().unwrap_or_default();
        crate::services::fiel::store(
            pool,
            s3,
            &bucket,
            rfc,
            &self.cert,
            &self.key,
            &self.password,
        )
        .await
        .map_err(|e| {
            tracing::error!(rfc = %rfc, "FIEL store failed: {e}");
            error(HttpResponse::InternalServerError(), FIEL_NOT_SAVED)
        })
    }
}

fn error(mut builder: actix_web::HttpResponseBuilder, message: &str) -> HttpResponse {
    builder.json(serde_json::json!({ "error": message }))
}

/// Whether `user_id` has `rfc`; the response to send when not (or when the check failed).
async fn require_own_rfc(pool: &DbPool, user_id: &str, rfc: &str) -> Result<(), HttpResponse> {
    match crate::db::users::get_credentials_for_rfc(pool, user_id, rfc).await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(error(HttpResponse::Forbidden(), NOT_YOUR_RFC)),
        Err(e) => {
            tracing::error!(rfc = %rfc, "FIEL: ownership check failed: {e}");
            Err(error(HttpResponse::InternalServerError(), TRY_AGAIN_LATER))
        }
    }
}

/// POST /api/v1/users/rfcs/fiel
/// Registers a new RFC with its FIEL in one step. Multipart: `rfc`, `cert`, `key`,
/// `password`, optional `priority_analysis`. The SAT validates the FIEL and it is stored
/// before the RFC is created, so a FIEL that fails leaves nothing behind -- an RFC with no
/// working credential could only produce failing syncs.
pub async fn register_with_fiel(
    req: HttpRequest,
    payload: Multipart,
    pool: web::Data<DbPool>,
    s3: web::Data<S3Client>,
    cfg: web::Data<Config>,
) -> HttpResponse {
    let user_id = match session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };
    let form = match FielForm::read(payload).await {
        Ok(form) => form,
        Err(message) => return error(HttpResponse::BadRequest(), message),
    };
    let rfc = form.rfc.as_str();
    if !is_valid_rfc(rfc) {
        return error(
            HttpResponse::UnprocessableEntity(),
            "Revisa el RFC: debe tener 12 caracteres (empresa) o 13 (persona física).",
        );
    }
    let is_admin = crate::db::users::is_user_admin(&pool, &user_id)
        .await
        .unwrap_or(false);
    if let Err(resp) = check_can_register(&pool, &user_id, rfc, is_admin).await {
        return resp;
    }
    if let Err(resp) = form.validate_with_sat(&cfg, rfc).await {
        return resp;
    }
    if let Err(resp) = form.store(&pool, &s3, &cfg, rfc).await {
        return resp;
    }
    if let Err(e) = register_validated_rfc(
        &pool,
        &user_id,
        rfc,
        LinkedCredential::Fiel,
        form.priority_analysis.as_deref(),
    )
    .await
    {
        tracing::error!(rfc = %rfc, "FIEL register: creating the RFC failed: {e}");
        return error(HttpResponse::InternalServerError(), TRY_AGAIN_LATER);
    }

    tracing::info!(rfc = %rfc, "RFC registered with a validated FIEL");
    HttpResponse::Ok().json(serde_json::json!({ "ok": true }))
}

/// POST /api/v1/users/rfcs/{rfc}/fiel
/// Adds or replaces the FIEL of an RFC the user already has. Multipart: `cert`, `key`,
/// `password`.
pub async fn upload(
    req: HttpRequest,
    path: web::Path<String>,
    payload: Multipart,
    pool: web::Data<DbPool>,
    s3: web::Data<S3Client>,
    cfg: web::Data<Config>,
) -> HttpResponse {
    let rfc = path.into_inner().to_uppercase();
    let user_id = match session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };
    if let Err(resp) = require_own_rfc(&pool, &user_id, &rfc).await {
        return resp;
    }
    let form = match FielForm::read(payload).await {
        Ok(form) => form,
        Err(message) => return error(HttpResponse::BadRequest(), message),
    };

    // A FIEL is only stored once the SAT opens a session with it for this RFC; that same
    // login verifies the owner (L19-01), which a FIEL-only RFC has no other way to do.
    if let Err(resp) = form.validate_with_sat(&cfg, &rfc).await {
        return resp;
    }
    if let Err(resp) = form.store(&pool, &s3, &cfg, &rfc).await {
        return resp;
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

    if let Err(resp) = require_own_rfc(&pool, &user_id, &rfc).await {
        return resp;
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
            error(HttpResponse::InternalServerError(), TRY_AGAIN_LATER)
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

    if let Err(resp) = require_own_rfc(&pool, &user_id, &rfc).await {
        return resp;
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
        Ok(false) => error(
            HttpResponse::NotFound(),
            "Este RFC no tiene una e.firma guardada.",
        ),
        Err(e) => {
            tracing::error!(rfc = %rfc, "FIEL delete: DB error: {e}");
            error(HttpResponse::InternalServerError(), TRY_AGAIN_LATER)
        }
    }
}
