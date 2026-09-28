use actix_web::{HttpRequest, HttpResponse, web};
use jsonwebtoken::{EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::config::Config;
use crate::db::DbPool;
use crate::errors::GENERIC_INTERNAL_ERROR;

#[derive(Debug, Deserialize, ToSchema)]
pub struct RegisterDto {
    pub email: String,
    pub name: String,
    pub password: String,
    pub phone: String,
    pub dial_code: Option<String>,
    pub website: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct LoginDto {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// Decode JWT payload and return the `sub` claim without signature verification.
fn jwt_sub(token: &str) -> Option<String> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    json.get("id")
        .or_else(|| json.get("sub"))?
        .as_str()
        .map(|s| s.to_string())
}

/// After a successful Adquiere auth response, enrich the JSON body with
/// `pulso_complete_profile` and `is_admin` queried from our local DB. Also returns the
/// user id extracted from the token -- L18-05: callers use it to record the span's
/// `user_id` field instead of logging the email that used to identify the request.
async fn enrich_with_profile(
    pool: &DbPool,
    mut body: serde_json::Value,
) -> (serde_json::Value, Option<String>) {
    let user_id = body
        .get("access_token")
        .and_then(|t| t.as_str())
        .and_then(jwt_sub);
    if let Some(user_id) = &user_id {
        let complete = crate::db::users::get_profile_complete(pool, user_id)
            .await
            .unwrap_or(false);
        let is_admin = crate::db::users::is_user_admin(pool, user_id)
            .await
            .unwrap_or(false);
        body["pulso_complete_profile"] = serde_json::Value::Bool(complete);
        body["is_admin"] = serde_json::Value::Bool(is_admin);
    }
    (body, user_id)
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/register",
    tag = "Auth",
    request_body = RegisterDto,
    responses(
        (status = 201, description = "Usuario creado exitosamente"),
        (status = 400, description = "Datos inválidos"),
        (status = 502, description = "Error al conectar con Adquiere API"),
    )
)]
#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty))]
pub async fn register(
    cfg: web::Data<Config>,
    pool: web::Data<DbPool>,
    body: web::Json<RegisterDto>,
) -> HttpResponse {
    tracing::info!("Register attempt");
    let client = match reqwest::Client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("register: reqwest client build failed: {e}");
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: GENERIC_INTERNAL_ERROR.to_string(),
            });
        }
    };

    let url = format!("{}/pulso/register", cfg.adquiere_api);

    let mut payload = serde_json::json!({
        "email":    body.email,
        "name":     body.name,
        "password": body.password,
        "phone":    body.phone,
    });

    if let Some(ref dc) = body.dial_code {
        payload["dial_code"] = serde_json::Value::String(dc.clone());
    }
    if let Some(ref ws) = body.website {
        payload["website"] = serde_json::Value::String(ws.clone());
    }

    let resp = match client.post(&url).json(&payload).send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("register: request to Adquiere API failed: {e}");
            return HttpResponse::BadGateway().json(ErrorBody {
                error: GENERIC_INTERNAL_ERROR.to_string(),
            });
        }
    };

    let status = actix_web::http::StatusCode::from_u16(resp.status().as_u16())
        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);

    match resp.json::<serde_json::Value>().await {
        Ok(json) => {
            if status.is_success() {
                let (enriched, user_id) = enrich_with_profile(&pool, json).await;
                if let Some(id) = &user_id {
                    tracing::Span::current().record("user_id", id.as_str());
                }
                tracing::info!(status = %status.as_u16(), "Register successful");
                HttpResponse::build(status).json(enriched)
            } else {
                tracing::warn!(status = %status.as_u16(), "Register rejected by upstream");
                HttpResponse::build(status).json(json)
            }
        }
        Err(_) => HttpResponse::build(status).finish(),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/login",
    tag = "Auth",
    request_body = LoginDto,
    responses(
        (status = 200, description = "Sesión iniciada, retorna token"),
        (status = 401, description = "Credenciales inválidas"),
        (status = 502, description = "Error al conectar con Adquiere API"),
    )
)]
#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty))]
pub async fn login(
    cfg: web::Data<Config>,
    pool: web::Data<DbPool>,
    body: web::Json<LoginDto>,
) -> HttpResponse {
    tracing::info!("Login attempt");
    let client = match reqwest::Client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("login: reqwest client build failed: {e}");
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: GENERIC_INTERNAL_ERROR.to_string(),
            });
        }
    };

    let url = format!("{}/pulso/sessions", cfg.adquiere_api);

    let payload = serde_json::json!({
        "email":    body.email,
        "password": body.password,
    });

    let resp = match client.post(&url).json(&payload).send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("login: request to Adquiere API failed: {e}");
            return HttpResponse::BadGateway().json(ErrorBody {
                error: GENERIC_INTERNAL_ERROR.to_string(),
            });
        }
    };

    let status = actix_web::http::StatusCode::from_u16(resp.status().as_u16())
        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);

    match resp.json::<serde_json::Value>().await {
        Ok(json) => {
            if status.is_success() {
                let (enriched, user_id) = enrich_with_profile(&pool, json).await;
                if let Some(id) = &user_id {
                    tracing::Span::current().record("user_id", id.as_str());
                }
                tracing::info!(status = %status.as_u16(), "Login successful");
                HttpResponse::build(status).json(enriched)
            } else {
                tracing::warn!(status = %status.as_u16(), "Login rejected by upstream");
                HttpResponse::build(status).json(json)
            }
        }
        Err(_) => HttpResponse::build(status).finish(),
    }
}

// ---------------------------------------------------------------------------
// Google OAuth
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct GoogleUrlBody {
    url: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct GoogleCodeDto {
    pub code: String,
}

#[derive(Debug, Deserialize)]
struct GoogleTokenResponse {
    id_token: String,
}

#[derive(Debug, Deserialize)]
struct GoogleIdTokenClaims {
    sub: String,
    email: String,
    #[allow(dead_code)]
    name: Option<String>,
}

#[derive(Debug, Serialize)]
struct JwtClaims {
    id: String,
    email: String,
    name: String,
    is_admin: bool,
    iat: i64,
    exp: i64,
}

fn make_jwt(secret: &str, id: &str, email: &str, name: &str, is_admin: bool) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let claims = JwtClaims {
        id: id.to_string(),
        email: email.to_string(),
        name: name.to_string(),
        is_admin,
        iat: now,
        exp: now + 3600,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap_or_default()
}

/// Exchange a Google authorization code for the user's profile.
async fn exchange_google_code(cfg: &Config, code: &str) -> Result<GoogleIdTokenClaims, String> {
    let client = reqwest::Client::new();

    let token_resp = client
        .post("https://oauth2.googleapis.com/token")
        .json(&serde_json::json!({
            "code": code,
            "client_id": cfg.google_client_id,
            "client_secret": cfg.google_client_secret,
            "redirect_uri": cfg.google_redirect_uri,
            "grant_type": "authorization_code"
        }))
        .send()
        .await
        .map_err(|e| format!("Google token request failed: {e}"))?;

    if !token_resp.status().is_success() {
        let body = token_resp.text().await.unwrap_or_default();
        return Err(format!("Google token error: {body}"));
    }

    let tokens: GoogleTokenResponse = token_resp
        .json()
        .await
        .map_err(|e| format!("Google token parse: {e}"))?;

    // Decode id_token payload without verifying signature
    use base64::Engine as _;
    let payload_b64 = tokens
        .id_token
        .split('.')
        .nth(1)
        .ok_or("Invalid id_token")?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload_b64))
        .map_err(|e| format!("id_token decode: {e}"))?;

    serde_json::from_slice::<GoogleIdTokenClaims>(&bytes)
        .map_err(|e| format!("id_token parse: {e}"))
}

fn urlencoding_simple(s: &str) -> String {
    s.chars()
        .flat_map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~') {
                vec![c]
            } else {
                format!("%{:02X}", c as u32).chars().collect()
            }
        })
        .collect()
}

/// Returns the Google OAuth consent URL. Frontend redirects the user to it.
pub async fn google_auth_url(cfg: web::Data<Config>) -> HttpResponse {
    let url = format!(
        "https://accounts.google.com/o/oauth2/v2/auth\
         ?client_id={}&redirect_uri={}&response_type=code\
         &scope=email%20profile&access_type=offline&prompt=consent",
        urlencoding_simple(&cfg.google_client_id),
        urlencoding_simple(&cfg.google_redirect_uri),
    );
    HttpResponse::Ok().json(GoogleUrlBody { url })
}

/// Exchange a Google code for a Pulso session.
/// Auto-links Google to an existing account if the email matches.
pub async fn google_login(
    cfg: web::Data<Config>,
    pool: web::Data<DbPool>,
    body: web::Json<GoogleCodeDto>,
) -> HttpResponse {
    let google_user = match exchange_google_code(&cfg, &body.code).await {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!("Google code exchange failed: {e}");
            return HttpResponse::BadGateway().json(ErrorBody { error: e });
        }
    };

    // Find by google_id first; fall back to email (auto-link on first Google login)
    let user = match crate::db::users::find_by_google_id(&pool, &google_user.sub).await {
        Ok(Some(u)) => Some(u),
        _ => match crate::db::users::find_by_email(&pool, &google_user.email).await {
            Ok(u) => u,
            Err(e) => {
                tracing::error!("DB error finding user by email: {e}");
                return HttpResponse::InternalServerError().json(ErrorBody {
                    error: GENERIC_INTERNAL_ERROR.to_string(),
                });
            }
        },
    };

    let (user_id, email, name) = match user {
        Some(u) => u,
        None => {
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "No tienes una cuenta en Pulso. Regístrate primero.".to_string(),
            });
        }
    };

    // Link google_id if not yet set
    if let Err(e) = crate::db::users::set_google_id(&pool, &user_id, &google_user.sub).await {
        tracing::warn!("Could not set google_id for {user_id}: {e}");
    }

    let profile_complete = crate::db::users::get_profile_complete(&pool, &user_id)
        .await
        .unwrap_or(false);
    let is_admin = crate::db::users::is_user_admin(&pool, &user_id)
        .await
        .unwrap_or(false);
    let jwt = make_jwt(&cfg.jwt_secret, &user_id, &email, &name, is_admin);

    tracing::info!(user_id = %user_id, "Google login successful");
    HttpResponse::Ok().json(serde_json::json!({
        "access_token": jwt,
        "pulso_complete_profile": profile_complete,
        "is_admin": is_admin,
    }))
}

/// Link (or re-link) a Google account to the currently authenticated user.
///
/// L18-02 point 2 (the closed permanent backdoor): this is the route a forged/unverified
/// token could reach before, letting an attacker link *their own* Google account to any
/// victim user id and get a real, properly-signed Pulso session on demand from then on --
/// signature verification everywhere else wouldn't have caught it retroactively. Now goes
/// through the same verified session as every other route.
pub async fn google_link(
    req: HttpRequest,
    cfg: web::Data<Config>,
    pool: web::Data<DbPool>,
    body: web::Json<GoogleCodeDto>,
) -> HttpResponse {
    let user_id = match crate::services::session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };

    let google_user = match exchange_google_code(&cfg, &body.code).await {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!("Google code exchange failed: {e}");
            return HttpResponse::BadGateway().json(ErrorBody { error: e });
        }
    };

    // Check if this google_id is already linked to a different account
    match crate::db::users::find_user_id_by_google_id(&pool, &google_user.sub).await {
        Ok(Some(existing_id)) if existing_id != user_id => {
            return HttpResponse::BadRequest().json(ErrorBody {
                error: "Esta cuenta de Google ya está vinculada a otro usuario.".to_string(),
            });
        }
        Err(e) => {
            tracing::error!("google_link: DB error checking existing google_id link: {e}");
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: GENERIC_INTERNAL_ERROR.to_string(),
            });
        }
        _ => {}
    }

    if let Err(e) = crate::db::users::set_google_id(&pool, &user_id, &google_user.sub).await {
        tracing::error!(user_id = %user_id, "google_link: set_google_id failed: {e}");
        return HttpResponse::InternalServerError().json(ErrorBody {
            error: GENERIC_INTERNAL_ERROR.to_string(),
        });
    }

    tracing::info!(user_id = %user_id, "Google account linked");
    HttpResponse::Ok().json(serde_json::json!({ "ok": true }))
}

/// Returns whether the current user has a Google account linked.
pub async fn google_status(
    req: HttpRequest,
    cfg: web::Data<Config>,
    pool: web::Data<DbPool>,
) -> HttpResponse {
    let user_id = match crate::services::session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };

    let linked = match crate::db::users::find_by_google_id_linked(&pool, &user_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(user_id = %user_id, "google_status: DB error: {e}");
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: GENERIC_INTERNAL_ERROR.to_string(),
            });
        }
    };

    HttpResponse::Ok().json(serde_json::json!({ "linked": linked }))
}

/// Unlink Google from the current user's account.
pub async fn google_unlink(
    req: HttpRequest,
    cfg: web::Data<Config>,
    pool: web::Data<DbPool>,
) -> HttpResponse {
    let user_id = match crate::services::session::require_session(&req, &cfg) {
        Ok(id) => id,
        Err(e) => return actix_web::ResponseError::error_response(&e),
    };

    if let Err(e) = crate::db::users::clear_google_id(&pool, &user_id).await {
        tracing::error!(user_id = %user_id, "google_unlink: clear_google_id failed: {e}");
        return HttpResponse::InternalServerError().json(ErrorBody {
            error: GENERIC_INTERNAL_ERROR.to_string(),
        });
    }

    tracing::info!(user_id = %user_id, "Google account unlinked");
    HttpResponse::Ok().json(serde_json::json!({ "ok": true }))
}
