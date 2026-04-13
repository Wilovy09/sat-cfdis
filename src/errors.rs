use actix_web::{HttpResponse, ResponseError};
use serde_json::json;
use std::fmt;

/// Unified API error that implements Actix's `ResponseError` so handlers can
/// return `Result<_, AppError>` and get a proper JSON error response.
#[derive(Debug)]
pub struct AppError {
    pub message: String,
    pub status: u16,
}

impl AppError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), status: 400 }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), status: 500 }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        let body = json!({ "error": self.message });
        match self.status {
            400 => HttpResponse::BadRequest().json(body),
            _ => HttpResponse::InternalServerError().json(body),
        }
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::internal(e.to_string())
    }
}
