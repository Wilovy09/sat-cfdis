use actix_web::{HttpResponse, web};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::config::Config;

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
pub async fn register(cfg: web::Data<Config>, body: web::Json<RegisterDto>) -> HttpResponse {
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
        Ok(json) => HttpResponse::build(status).json(json),
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
pub async fn login(cfg: web::Data<Config>, body: web::Json<LoginDto>) -> HttpResponse {
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
        Ok(json) => HttpResponse::build(status).json(json),
        Err(_) => HttpResponse::build(status).finish(),
    }
}
