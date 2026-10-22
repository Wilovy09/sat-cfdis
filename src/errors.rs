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
        Self {
            message: msg.into(),
            status: 400,
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            status: 500,
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            status: 404,
        }
    }

    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            status: 401,
        }
    }

    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            status: 403,
        }
    }

    pub fn payment_required(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            status: 402,
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// L18-10: the text every 500 sends to the browser instead of the real error message.
/// `AppError::internal`'s whole point is "something broke that the caller can't act
/// on" -- DB errors, encryption failures, downstream-service errors -- so its message
/// can carry table/column names, storage paths, or raw driver text. Every OTHER
/// constructor here (bad_request/not_found/unauthorized/forbidden/payment_required)
/// keeps its real message: those are meant to be read and acted on by whoever's calling.
pub(crate) const GENERIC_INTERNAL_ERROR: &str =
    "Ocurrió un error inesperado. Intenta de nuevo; si continúa, contacta a soporte.";

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        if self.status >= 500 {
            tracing::error!("Internal error: {}", self.message);
            let body = json!({ "error": GENERIC_INTERNAL_ERROR });
            return HttpResponse::InternalServerError().json(body);
        }
        let body = json!({ "error": self.message });
        match self.status {
            400 => HttpResponse::BadRequest().json(body),
            401 => HttpResponse::Unauthorized().json(body),
            402 => HttpResponse::PaymentRequired().json(body),
            403 => HttpResponse::Forbidden().json(body),
            404 => HttpResponse::NotFound().json(body),
            _ => HttpResponse::InternalServerError().json(body),
        }
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::internal(e.to_string())
    }
}

#[cfg(test)]
mod l18_10_tests {
    use super::*;
    use actix_web::body::MessageBody as _;

    async fn body_json(resp: HttpResponse) -> serde_json::Value {
        let bytes = resp.into_body().try_into_bytes().unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn internal_error_hides_its_real_message_from_the_response() {
        let e = AppError::internal("relation \"pulso.secret_table\" does not exist");
        let body = body_json(e.error_response()).await;
        assert_eq!(body["error"], GENERIC_INTERNAL_ERROR);
        assert_ne!(
            body["error"],
            "relation \"pulso.secret_table\" does not exist"
        );
    }

    #[tokio::test]
    async fn non_internal_errors_keep_their_real_message() {
        for e in [
            AppError::bad_request("RFC es requerido"),
            AppError::not_found("Job no encontrado"),
            AppError::unauthorized("Token inválido o expirado"),
            AppError::forbidden("Acceso denegado"),
            AppError::payment_required("Suscripción requerida"),
        ] {
            let msg = e.message.clone();
            let body = body_json(e.error_response()).await;
            assert_eq!(body["error"], msg);
        }
    }
}
