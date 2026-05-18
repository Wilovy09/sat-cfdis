use actix_web::{HttpRequest, HttpResponse, web};
use jsonwebtoken::{EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::config::Config;
use crate::db::DbPool;

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
/// `pulso_complete_profile` queried from our local DB.
async fn enrich_with_profile(pool: &DbPool, mut body: serde_json::Value) -> serde_json::Value {
    if let Some(token) = body.get("access_token").and_then(|t| t.as_str()) {
        if let Some(user_id) = jwt_sub(token) {
            let complete = crate::db::users::get_profile_complete(pool, &user_id)
                .await
                .unwrap_or(false);
            body["pulso_complete_profile"] = serde_json::Value::Bool(complete);
        }
    }
    body
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
#[tracing::instrument(skip_all, fields(email = %body.email))]
pub async fn register(
    cfg: web::Data<Config>,
    pool: web::Data<DbPool>,
    body: web::Json<RegisterDto>,
) -> HttpResponse {
    tracing::info!("Register attempt");
    let client = match reqwest::Client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: e.to_string(),
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
            return HttpResponse::BadGateway().json(ErrorBody {
                error: e.to_string(),
            });
        }
    };

    let status = actix_web::http::StatusCode::from_u16(resp.status().as_u16())
        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);

    match resp.json::<serde_json::Value>().await {
        Ok(json) => {
            if status.is_success() {
                tracing::info!(status = %status.as_u16(), "Register successful");
                let enriched = enrich_with_profile(&pool, json).await;
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
#[tracing::instrument(skip_all, fields(email = %body.email))]
pub async fn login(
    cfg: web::Data<Config>,
    pool: web::Data<DbPool>,
    body: web::Json<LoginDto>,
) -> HttpResponse {
    tracing::info!("Login attempt");
    let client = match reqwest::Client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: e.to_string(),
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
            return HttpResponse::BadGateway().json(ErrorBody {
                error: e.to_string(),
            });
        }
    };

    let status = actix_web::http::StatusCode::from_u16(resp.status().as_u16())
        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);

    match resp.json::<serde_json::Value>().await {
        Ok(json) => {
            if status.is_success() {
                tracing::info!(status = %status.as_u16(), "Login successful");
                let enriched = enrich_with_profile(&pool, json).await;
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
    name: Option<String>,
}

#[derive(Debug, Serialize)]
struct JwtClaims {
    id: String,
    email: String,
    name: String,
    iat: i64,
    exp: i64,
}

fn make_jwt(secret: &str, id: &str, email: &str, name: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let claims = JwtClaims {
        id: id.to_string(),
        email: email.to_string(),
        name: name.to_string(),
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

fn bearer_token_auth(req: &HttpRequest) -> Option<String> {
    let header = req
        .headers()
        .get(actix_web::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let lower = header.to_lowercase();
    let token = header[lower.find("bearer ")? + 7..].trim();
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

/// Exchange a Google authorization code for the user's profile.
async fn exchange_google_code(
    cfg: &Config,
    code: &str,
) -> Result<GoogleIdTokenClaims, String> {
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
                return HttpResponse::InternalServerError()
                    .json(ErrorBody { error: e.to_string() });
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
    if let Err(e) =
        crate::db::users::set_google_id(&pool, &user_id, &google_user.sub).await
    {
        tracing::warn!("Could not set google_id for {user_id}: {e}");
    }

    let jwt = make_jwt(&cfg.jwt_secret, &user_id, &email, &name);
    let profile_complete = crate::db::users::get_profile_complete(&pool, &user_id)
        .await
        .unwrap_or(false);

    tracing::info!(user_id = %user_id, "Google login successful");
    HttpResponse::Ok().json(serde_json::json!({
        "access_token": jwt,
        "pulso_complete_profile": profile_complete,
    }))
}

/// Link (or re-link) a Google account to the currently authenticated user.
pub async fn google_link(
    req: HttpRequest,
    cfg: web::Data<Config>,
    pool: web::Data<DbPool>,
    body: web::Json<GoogleCodeDto>,
) -> HttpResponse {
    let token = match bearer_token_auth(&req) {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized()
                .json(ErrorBody { error: "Missing token".to_string() });
        }
    };

    let user_id = match jwt_sub(&token) {
        Some(id) => id,
        None => {
            return HttpResponse::Unauthorized()
                .json(ErrorBody { error: "Invalid token".to_string() });
        }
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
            return HttpResponse::InternalServerError()
                .json(ErrorBody { error: e.to_string() });
        }
        _ => {}
    }

    if let Err(e) =
        crate::db::users::set_google_id(&pool, &user_id, &google_user.sub).await
    {
        return HttpResponse::InternalServerError()
            .json(ErrorBody { error: e.to_string() });
    }

    tracing::info!(user_id = %user_id, "Google account linked");
    HttpResponse::Ok().json(serde_json::json!({ "ok": true }))
}

/// Returns whether the current user has a Google account linked.
pub async fn google_status(
    req: HttpRequest,
    pool: web::Data<DbPool>,
) -> HttpResponse {
    let token = match bearer_token_auth(&req) {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized()
                .json(ErrorBody { error: "Missing token".to_string() });
        }
    };
    let user_id = match jwt_sub(&token) {
        Some(id) => id,
        None => {
            return HttpResponse::Unauthorized()
                .json(ErrorBody { error: "Invalid token".to_string() });
        }
    };

    let linked = match crate::db::users::find_by_google_id_linked(&pool, &user_id).await {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .json(ErrorBody { error: e.to_string() });
        }
    };

    HttpResponse::Ok().json(serde_json::json!({ "linked": linked }))
}

/// Unlink Google from the current user's account.
pub async fn google_unlink(
    req: HttpRequest,
    pool: web::Data<DbPool>,
) -> HttpResponse {
    let token = match bearer_token_auth(&req) {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized()
                .json(ErrorBody { error: "Missing token".to_string() });
        }
    };
    let user_id = match jwt_sub(&token) {
        Some(id) => id,
        None => {
            return HttpResponse::Unauthorized()
                .json(ErrorBody { error: "Invalid token".to_string() });
        }
    };

    if let Err(e) = crate::db::users::clear_google_id(&pool, &user_id).await {
        return HttpResponse::InternalServerError()
            .json(ErrorBody { error: e.to_string() });
    }

    tracing::info!(user_id = %user_id, "Google account unlinked");
    HttpResponse::Ok().json(serde_json::json!({ "ok": true }))
}
