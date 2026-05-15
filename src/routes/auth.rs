use actix_web::{HttpResponse, web};
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
