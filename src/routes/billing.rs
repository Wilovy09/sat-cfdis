use actix_web::{HttpRequest, HttpResponse, web};
use uuid::Uuid;

use crate::db::DbPool;
use crate::errors::AppError;

// ── Auth helpers (same pattern as users.rs / analytics.rs) ───────────────────

fn bearer_token(req: &HttpRequest) -> Option<String> {
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

fn jwt_user_id(token: &str) -> Option<String> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(payload))
        .ok()?;
    let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    json.get("id")
        .or_else(|| json.get("sub"))?
        .as_str()
        .map(|s| s.to_string())
}

fn parse_user(req: &HttpRequest) -> Option<(String, Uuid)> {
    let token = bearer_token(req)?;
    let id_str = jwt_user_id(&token)?;
    let uid = Uuid::parse_str(&id_str).ok()?;
    Some((id_str, uid))
}

// ── GET /api/v1/billing/status ────────────────────────────────────────────────

pub async fn get_status(
    req: HttpRequest,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let (_, uid) = parse_user(&req)
        .ok_or_else(|| AppError::unauthorized("Token requerido"))?;

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
pub async fn has_access(pool: &DbPool, user_id: &str) -> bool {
    if crate::db::users::is_user_admin(pool, user_id)
        .await
        .unwrap_or(false)
    {
        return true;
    }
    let Ok(uid) = Uuid::parse_str(user_id) else {
        return false;
    };
    crate::db::subscriptions::is_pulso_active(pool, uid)
        .await
        .unwrap_or(false)
}
