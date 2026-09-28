use actix_web::{HttpRequest, HttpResponse, web};
use uuid::Uuid;

use crate::config::Config;
use crate::db::DbPool;
use crate::errors::AppError;
use crate::services::session;

// ── GET /api/v1/billing/status ────────────────────────────────────────────────

pub async fn get_status(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
) -> Result<HttpResponse, AppError> {
    let user_id = session::require_session(&req, &cfg)?;
    let uid = Uuid::parse_str(&user_id)
        .map_err(|_| AppError::unauthorized("Token inválido o expirado"))?;

    let status = crate::db::subscriptions::get_pulso_status(pool.get_ref(), uid)
        .await
        .map_err(|e| {
            tracing::error!(user_id = %uid, "Error fetching subscription status: {e}");
            AppError::internal("Error al obtener estado de suscripción")
        })?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": status.as_ref().map(|s| s.status.as_str()).unwrap_or("inactive"),
        "current_period_end": status.and_then(|s| s.current_period_end),
    })))
}

// ── Subscription access guard ─────────────────────────────────────────────────

/// Returns `true` if user has an active pulso subscription or is an admin.
// P-03 / AUD-077: `is_admin` is resolved once by the caller (check_rfc_access) and passed
// in, instead of this function running the same is_user_admin query a third time per
// request.
pub async fn has_access(pool: &DbPool, user_id: &str, is_admin: bool) -> bool {
    if is_admin {
        return true;
    }
    let Ok(uid) = Uuid::parse_str(user_id) else {
        return false;
    };
    crate::db::subscriptions::is_pulso_active(pool, uid)
        .await
        .unwrap_or(false)
}
