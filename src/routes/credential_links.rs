//! Credential links (docs/Pulso_Feature_Liga_Credenciales.md): a Pulso user generates a
//! link tied to one RFC and hands it to whoever holds the SAT credentials (accountant,
//! admin staff, a shareholder). That person opens a public page, captures a CIEC or a FIEL,
//! and the SAT validates it live; the RFC lands in the requester's account.
//!
//! Privacy between accounts (rule 6): if another account already has the RFC, nothing in
//! these responses differs -- same link, same messages, same validation. Pulso doesn't yet
//! let two accounts own one RFC, so the validated credential is kept encrypted on the link
//! (`pending_clave_enc`) until it does, and the requester sees the RFC as still being
//! prepared.

use actix_multipart::Multipart;
use actix_web::{HttpRequest, HttpResponse, http::StatusCode, web};
use aws_sdk_s3::Client as S3Client;
use futures_util::StreamExt as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::Config;
use crate::db::DbPool;
use crate::db::credential_links as links;
use crate::db::users::CreateUserError;
use crate::errors::GENERIC_INTERNAL_ERROR;
use crate::routes::users::{LinkedCredential, register_validated_rfc};
use crate::services::crypto;
use crate::services::sat_login::{self, LoginOutcome, SatCredential};
use crate::services::session;

const MSG_UNAVAILABLE_LINK: &str =
    "Esta liga ya no está disponible. Pide a la persona que te la envió una nueva.";
const MSG_REJECTED: &str =
    "El SAT rechazó las credenciales. Revisa los datos e inténtalo de nuevo.";
const MSG_SAT_DOWN: &str =
    "No pudimos comunicarnos con el SAT. Inténtalo de nuevo en unos minutos.";
const MSG_LOCKED: &str = "Demasiados intentos con esta liga. Espera una hora e inténtalo de nuevo.";
const MSG_RFC_LIMIT: &str =
    "Tu cuenta solo puede tener un RFC. Contacta a soporte si necesitas acceso a más.";

/// The contact's position at the company, as offered on the public page.
const CONTACT_ROLES: &[&str] = &[
    "Contador o despacho externo",
    "Área Administrativa",
    "Accionista",
    "CEO",
    "CFO",
    "Otro",
];

/// FIEL files are a few KB; anything far bigger isn't one.
const MAX_FIELD_BYTES: usize = 64 * 1024;

fn json_error(status: StatusCode, message: &str) -> HttpResponse {
    HttpResponse::build(status).json(serde_json::json!({ "error": message }))
}

fn internal_error() -> HttpResponse {
    json_error(StatusCode::INTERNAL_SERVER_ERROR, GENERIC_INTERNAL_ERROR)
}

/// SAT RFC shape: 3 (company) or 4 (person) letters, a YYMMDD date, a 3-character homoclave.
fn is_valid_rfc(rfc: &str) -> bool {
    let chars: Vec<char> = rfc.chars().collect();
    let letters = match chars.len() {
        12 => 3,
        13 => 4,
        _ => return false,
    };
    let (head, rest) = chars.split_at(letters);
    let (date, homoclave) = rest.split_at(6);
    head.iter()
        .all(|c| c.is_ascii_uppercase() || *c == 'Ñ' || *c == '&')
        && date.iter().all(char::is_ascii_digit)
        && homoclave.iter().all(char::is_ascii_alphanumeric)
}

fn is_generic_rfc(rfc: &str) -> bool {
    rfc.starts_with("XAXX") || rfc.starts_with("XEXX")
}

#[derive(Debug, Deserialize)]
pub struct CreateLinkDto {
    pub rfc: String,
}

#[derive(Debug, Serialize)]
struct LinkView {
    rfc: String,
    /// The public page lives at `/credenciales/{token}` on the frontend's own origin.
    token: String,
    expires_at: String,
    /// `esperando`: nobody has validated credentials yet. `procesando`: validated, the
    /// download is being prepared.
    status: &'static str,
}

impl LinkView {
    fn from_row(row: links::RequesterLink, key: &[u8; 32]) -> Result<Self, String> {
        Ok(Self {
            token: crypto::decrypt(key, &row.token_enc)?,
            status: if row.validated {
                "procesando"
            } else {
                "esperando"
            },
            rfc: row.rfc,
            expires_at: row.expires_at,
        })
    }
}

/// POST /api/v1/users/credential-links -- `{ "rfc": "..." }`. Returns the requester's open
/// link for that RFC, or a new one. Allowed before paying, like capturing the credentials.
pub async fn create_link(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
    body: web::Json<CreateLinkDto>,
) -> HttpResponse {
    let user_id = match session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };
    let Ok(uid) = Uuid::parse_str(&user_id) else {
        return json_error(StatusCode::UNAUTHORIZED, "Token inválido o expirado");
    };
    let rfc = body.rfc.trim().to_uppercase();
    if !is_valid_rfc(&rfc) {
        return json_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "El RFC no tiene un formato válido.",
        );
    }
    if is_generic_rfc(&rfc) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "Ese RFC es genérico del SAT y no puede registrarse.",
        );
    }

    // An RFC this account already has is a credentials update; another account's RFC is
    // treated exactly like a new one (rule 6). Only a brand-new RFC for this account counts
    // against the per-account limit.
    let owned_by_self = match crate::db::users::check_rfc_ownership(&pool, &user_id, &rfc).await {
        Err(CreateUserError::AlreadyOwnedBySelf) => true,
        Ok(()) | Err(CreateUserError::AlreadyOwnedByOther) => false,
        Err(CreateUserError::Db(e)) => {
            tracing::error!(user_id = %user_id, "credential link: ownership check failed: {e}");
            return internal_error();
        }
    };
    if !owned_by_self
        && !crate::db::users::is_user_admin(&pool, &user_id)
            .await
            .unwrap_or(false)
    {
        let owned = crate::db::users::count_user_rfcs(&pool, &user_id).await;
        let pending = links::count_open_for_other_rfcs(&pool, uid, &rfc).await;
        match (owned, pending) {
            (Ok(owned), Ok(pending)) if owned + pending >= 1 => {
                return json_error(StatusCode::CONFLICT, MSG_RFC_LIMIT);
            }
            (Ok(_), Ok(_)) => {}
            (Err(e), _) | (_, Err(e)) => {
                tracing::error!(user_id = %user_id, "credential link: RFC count failed: {e}");
                return internal_error();
            }
        }
    }

    let key = crypto::load_key();
    let row = match links::open_for(&pool, uid, &rfc).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            let token = crypto::random_token();
            let Ok(token_enc) = crypto::encrypt(&key, &token) else {
                return internal_error();
            };
            match links::create(&pool, uid, &rfc, &crypto::token_hash(&token), &token_enc).await {
                Ok(row) => {
                    tracing::info!(user_id = %user_id, rfc = %rfc, "Credential link created");
                    row
                }
                Err(e) => {
                    tracing::error!(user_id = %user_id, "credential link: insert failed: {e}");
                    return internal_error();
                }
            }
        }
        Err(e) => {
            tracing::error!(user_id = %user_id, "credential link: lookup failed: {e}");
            return internal_error();
        }
    };
    match LinkView::from_row(row, &key) {
        Ok(view) => HttpResponse::Ok().json(view),
        Err(e) => {
            tracing::error!(user_id = %user_id, "credential link: token decrypt failed: {e}");
            internal_error()
        }
    }
}

/// GET /api/v1/users/credential-links -- the requester's RFCs still waiting on a link.
pub async fn list_links(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
) -> HttpResponse {
    let user_id = match session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };
    let Ok(uid) = Uuid::parse_str(&user_id) else {
        return json_error(StatusCode::UNAUTHORIZED, "Token inválido o expirado");
    };
    let rows = match links::list_for_requester(&pool, uid).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(user_id = %user_id, "credential links: list failed: {e}");
            return internal_error();
        }
    };
    let key = crypto::load_key();
    let views: Result<Vec<LinkView>, String> = rows
        .into_iter()
        .map(|row| LinkView::from_row(row, &key))
        .collect();
    match views {
        Ok(links) => HttpResponse::Ok().json(serde_json::json!({ "links": links })),
        Err(e) => {
            tracing::error!(user_id = %user_id, "credential links: token decrypt failed: {e}");
            internal_error()
        }
    }
}

#[derive(Debug, Serialize)]
struct PublicLinkView {
    rfc: String,
    requester_name: String,
    requester_company: Option<String>,
    expires_at: String,
    roles: &'static [&'static str],
}

/// GET /api/v1/public/credential-links/{token} -- what the contact's page shows. Nothing of
/// the requester's account beyond their name and, if any, company.
pub async fn public_get(pool: web::Data<DbPool>, path: web::Path<String>) -> HttpResponse {
    let link = match links::find_by_token_hash(&pool, &crypto::token_hash(&path)).await {
        Ok(Some(link)) if link.open => link,
        Ok(_) => return json_error(StatusCode::GONE, MSG_UNAVAILABLE_LINK),
        Err(e) => {
            tracing::error!("credential link (public): lookup failed: {e}");
            return internal_error();
        }
    };
    let requester = match links::requester(&pool, link.requester_user_id).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("credential link (public): requester lookup failed: {e}");
            return internal_error();
        }
    };
    HttpResponse::Ok().json(PublicLinkView {
        rfc: link.rfc,
        requester_name: requester
            .name
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| "Un usuario de Pulso".to_string()),
        requester_company: requester.company.filter(|c| !c.trim().is_empty()),
        expires_at: link.expires_at,
        roles: CONTACT_ROLES,
    })
}

/// What the contact sent, as parsed from the multipart form.
#[derive(Default)]
struct Submission {
    role: String,
    kind: String,
    password: String,
    cert: Vec<u8>,
    key: Vec<u8>,
}

impl Submission {
    async fn read(mut payload: Multipart) -> Result<Self, &'static str> {
        let mut s = Self::default();
        while let Some(item) = payload.next().await {
            let mut field = item.map_err(|_| "No pudimos leer el formulario.")?;
            let name = field.name().unwrap_or_default().to_string();
            let mut data = Vec::new();
            while let Some(chunk) = field.next().await {
                let chunk = chunk.map_err(|_| "No pudimos leer el formulario.")?;
                if data.len() + chunk.len() > MAX_FIELD_BYTES {
                    return Err("Uno de los archivos es demasiado grande para ser una FIEL.");
                }
                data.extend_from_slice(&chunk);
            }
            let text = || String::from_utf8_lossy(&data).trim().to_string();
            match name.as_str() {
                "role" => s.role = text(),
                "type" => s.kind = text(),
                "password" => s.password = String::from_utf8_lossy(&data).into_owned(),
                "cert" => s.cert = data,
                "key" => s.key = data,
                _ => {}
            }
        }
        if !CONTACT_ROLES.contains(&s.role.as_str()) {
            return Err("Elige tu cargo en la empresa.");
        }
        match s.kind.as_str() {
            "ciec" if !s.password.is_empty() => Ok(s),
            "ciec" => Err("Escribe la contraseña (CIEC)."),
            "fiel" if !s.cert.is_empty() && !s.key.is_empty() && !s.password.is_empty() => Ok(s),
            "fiel" => {
                Err("Adjunta el archivo .cer, el archivo .key y la contraseña de la clave privada.")
            }
            _ => Err("Elige CIEC o FIEL."),
        }
    }

    fn credential(&self) -> SatCredential<'_> {
        if self.kind == "fiel" {
            SatCredential::Fiel {
                cert_der: &self.cert,
                key_der: &self.key,
                password: &self.password,
            }
        } else {
            SatCredential::Ciec {
                password: &self.password,
            }
        }
    }
}

/// POST /api/v1/public/credential-links/{token} -- multipart: `role`, `type` (`ciec` |
/// `fiel`), `password`, and for a FIEL `cert` + `key`. Validates live with the SAT.
pub async fn public_submit(
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
    s3: web::Data<S3Client>,
    path: web::Path<String>,
    payload: Multipart,
) -> HttpResponse {
    let link = match links::find_by_token_hash(&pool, &crypto::token_hash(&path)).await {
        Ok(Some(link)) if link.open => link,
        Ok(_) => return json_error(StatusCode::GONE, MSG_UNAVAILABLE_LINK),
        Err(e) => {
            tracing::error!("credential link (submit): lookup failed: {e}");
            return internal_error();
        }
    };
    let submission = match Submission::read(payload).await {
        Ok(s) => s,
        Err(message) => return json_error(StatusCode::UNPROCESSABLE_ENTITY, message),
    };
    match links::claim(&pool, link.id).await {
        Ok(true) => {}
        Ok(false) => return json_error(StatusCode::TOO_MANY_REQUESTS, MSG_LOCKED),
        Err(e) => {
            tracing::error!("credential link (submit): claim failed: {e}");
            return internal_error();
        }
    }

    match sat_login::check(&cfg, &link.rfc, &submission.credential()).await {
        LoginOutcome::Accepted => {}
        LoginOutcome::Rejected => {
            if let Err(e) = links::record_rejection(&pool, link.id).await {
                tracing::error!("credential link (submit): recording rejection failed: {e}");
            }
            tracing::info!(rfc = %link.rfc, "Credential link: SAT rejected the credentials");
            return HttpResponse::UnprocessableEntity()
                .json(serde_json::json!({ "status": "rejected", "error": MSG_REJECTED }));
        }
        LoginOutcome::Unavailable => {
            if let Err(e) = links::release(&pool, link.id).await {
                tracing::error!("credential link (submit): release failed: {e}");
            }
            return HttpResponse::ServiceUnavailable()
                .json(serde_json::json!({ "status": "unavailable", "error": MSG_SAT_DOWN }));
        }
    }

    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    match attach(&pool, &s3, &bucket, &link, &submission).await {
        Ok(pending) => {
            if let Err(e) = links::mark_validated(
                &pool,
                link.id,
                &submission.kind,
                &submission.role,
                pending.as_deref(),
            )
            .await
            {
                tracing::error!(rfc = %link.rfc, "credential link: closing the link failed: {e}");
            }
            tracing::info!(rfc = %link.rfc, kind = %submission.kind, "Credential link: credentials validated");
            HttpResponse::Ok().json(serde_json::json!({ "status": "validated" }))
        }
        Err(e) => {
            tracing::error!(rfc = %link.rfc, "credential link: storing the credentials failed: {e}");
            if let Err(e) = links::release(&pool, link.id).await {
                tracing::error!("credential link (submit): release failed: {e}");
            }
            internal_error()
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum AttachError {
    #[error("encrypting failed: {0}")]
    Encrypt(String),
    #[error(transparent)]
    Fiel(#[from] crate::services::fiel::StoreFielError),
    #[error(transparent)]
    Register(#[from] crate::routes::users::RegisterValidatedError),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error("ownership check failed: {0}")]
    Ownership(String),
}

/// Puts SAT-validated credentials where they belong. Returns the encrypted credential only
/// when it has to wait on the link (another account owns the RFC).
async fn attach(
    pool: &DbPool,
    s3: &S3Client,
    bucket: &str,
    link: &links::PublicLink,
    submission: &Submission,
) -> Result<Option<String>, AttachError> {
    let requester = link.requester_user_id.to_string();
    let rfc = link.rfc.as_str();
    let key = crypto::load_key();
    let is_fiel = submission.kind == "fiel";

    match crate::db::users::check_rfc_ownership(pool, &requester, rfc).await {
        // Rules 4 and 5: a new CIEC replaces the old one; a FIEL is used as soon as it's
        // stored (jobs prefer it over the CIEC).
        Err(CreateUserError::AlreadyOwnedBySelf) => {
            if is_fiel {
                crate::services::fiel::store(
                    pool,
                    s3,
                    bucket,
                    rfc,
                    &submission.cert,
                    &submission.key,
                    &submission.password,
                )
                .await?;
            } else {
                let clave_enc =
                    crypto::encrypt(&key, &submission.password).map_err(AttachError::Encrypt)?;
                crate::db::users::update_rfc_clave(pool, &requester, rfc, &clave_enc).await?;
            }
            crate::db::users::mark_verified_now(pool, &requester, rfc).await?;
            Ok(None)
        }
        Err(CreateUserError::AlreadyOwnedByOther) => {
            use base64::Engine as _;
            let b64 = base64::engine::general_purpose::STANDARD;
            let held = if is_fiel {
                serde_json::json!({
                    "type": "fiel",
                    "cert": b64.encode(&submission.cert),
                    "key": b64.encode(&submission.key),
                    "password": submission.password,
                })
            } else {
                serde_json::json!({ "type": "ciec", "password": submission.password })
            };
            let held_enc =
                crypto::encrypt(&key, &held.to_string()).map_err(AttachError::Encrypt)?;
            Ok(Some(held_enc))
        }
        Ok(()) => {
            if is_fiel {
                crate::services::fiel::store(
                    pool,
                    s3,
                    bucket,
                    rfc,
                    &submission.cert,
                    &submission.key,
                    &submission.password,
                )
                .await?;
                register_validated_rfc(pool, &requester, rfc, LinkedCredential::Fiel).await?;
            } else {
                register_validated_rfc(
                    pool,
                    &requester,
                    rfc,
                    LinkedCredential::Ciec(&submission.password),
                )
                .await?;
            }
            Ok(None)
        }
        Err(CreateUserError::Db(e)) => Err(AttachError::Ownership(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc_format_accepts_companies_and_people_only() {
        assert!(is_valid_rfc("AAA010101AAA"));
        assert!(is_valid_rfc("AAAA800101XX1"));
        assert!(is_valid_rfc("Ñ&A010101A1B"));
        assert!(!is_valid_rfc("AAA01010AAA"));
        assert!(!is_valid_rfc("AAA0101O1AAA"));
        assert!(!is_valid_rfc("aaa010101aaa"));
        assert!(!is_valid_rfc(""));
    }

    #[test]
    fn generic_rfcs_are_refused() {
        assert!(is_generic_rfc("XAXX010101000"));
        assert!(is_generic_rfc("XEXX010101000"));
        assert!(!is_generic_rfc("AAA010101AAA"));
    }
}
