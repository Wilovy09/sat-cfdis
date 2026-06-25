use actix_web::{HttpRequest, HttpResponse, web};
use serde::Deserialize;

use crate::{
    db::DbPool,
    errors::AppError,
    services::analytics::{
        cashflow, concepts, counterparties, fiscal, geography, hallazgos, normalization, payments,
        payroll, period_comparison, quarterly, recurrence, retention, summary, xml_breakdown, xml_count,
    },
};

// ---------------------------------------------------------------------------
// Auth helpers (inlined — do not refactor the other files)
// ---------------------------------------------------------------------------

fn bearer_token_analytics(req: &HttpRequest) -> Option<String> {
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

fn jwt_user_id_analytics(token: &str, secret: &str) -> Option<String> {
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    let key = DecodingKey::from_secret(secret.as_bytes());
    let data = decode::<serde_json::Value>(token, &key, &validation).ok()?;
    data.claims
        .get("id")
        .or_else(|| data.claims.get("sub"))?
        .as_str()
        .map(|s| s.to_string())
}

async fn check_rfc_access(
    pool: &crate::db::DbPool,
    req: &HttpRequest,
    rfc: &str,
) -> Result<(), AppError> {
    let token =
        bearer_token_analytics(req).ok_or_else(|| AppError::unauthorized("Token requerido"))?;
    let cfg = req
        .app_data::<web::Data<crate::config::Config>>()
        .ok_or_else(|| AppError::internal("Config no disponible"))?;
    let user_id = jwt_user_id_analytics(&token, &cfg.jwt_secret)
        .ok_or_else(|| AppError::unauthorized("Token inválido o expirado"))?;

    let rfc_access = crate::db::users::user_has_rfc_or_admin(pool, &user_id, rfc)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    if !rfc_access {
        return Err(AppError::forbidden("Acceso denegado"));
    }

    // Non-admins must have an active pulso subscription.
    let is_admin = crate::db::users::is_user_admin(pool, &user_id)
        .await
        .unwrap_or(false);
    if !is_admin {
        let subscribed = crate::routes::billing::has_access(pool, &user_id).await;
        if !subscribed {
            return Err(AppError::payment_required("Suscripción requerida"));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Common query params
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct AnalyticsParams {
    pub dl_type: Option<String>, // emitidos|recibidos|ambos  (default: emitidos)
    pub from: Option<String>,    // YYYY-MM  (default: 12 months ago)
    pub to: Option<String>,      // YYYY-MM  (default: current month)
    pub limit: Option<i64>,      // for counterparties, default 50
}

impl AnalyticsParams {
    fn dl_type(&self) -> String {
        self.dl_type.clone().unwrap_or_else(|| "emitidos".into())
    }
    fn from(&self) -> String {
        self.from.clone().unwrap_or_else(default_from)
    }
    fn to(&self) -> String {
        self.to.clone().unwrap_or_else(current_month)
    }
    fn limit(&self) -> i64 {
        self.limit.unwrap_or(50).clamp(1, 500)
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/summary
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/summary",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos|ambos"),
        ("from" = Option<String>, Query, description = "YYYY-MM"),
        ("to" = Option<String>, Query, description = "YYYY-MM"),
    ),
    responses((status = 200, description = "Resumen financiero"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_summary(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let p = summary::SummaryParams {
        dl_type: query.dl_type(),
        from: query.from(),
        to: query.to(),
    };
    let result = summary::get(&pool, &rfc, &p)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/counterparties
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/counterparties",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos|ambos"),
        ("from" = Option<String>, Query, description = "YYYY-MM"),
        ("to" = Option<String>, Query, description = "YYYY-MM"),
        ("limit" = Option<i64>, Query, description = "Top N contrapartes (default 50)"),
    ),
    responses((status = 200, description = "Top contrapartes"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_counterparties(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = counterparties::get(
        &pool,
        &rfc,
        &query.dl_type(),
        &query.from(),
        &query.to(),
        query.limit(),
    )
    .await
    .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/recurrence
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/recurrence",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del propietario"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos"),
        ("window_months" = Option<i32>, Query, description = "Meses de ventana (default 24)"),
    ),
    responses((status = 200, description = "Recurrence analysis"))
)]
pub async fn get_recurrence(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let dl_type = query
        .get("dl_type")
        .map(|s| s.as_str())
        .unwrap_or("emitidos");
    let window_months: i32 = query
        .get("window_months")
        .and_then(|s| s.parse().ok())
        .unwrap_or(24)
        .clamp(6, 60);
    let from = query.get("from").map(|s| s.as_str());
    let to   = query.get("to").map(|s| s.as_str());
    let result = recurrence::get(&pool, &rfc, dl_type, window_months, from, to)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/retention
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/retention",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del propietario"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos"),
    ),
    responses((status = 200, description = "Retention analysis"))
)]
pub async fn get_retention(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let dl_type = query
        .get("dl_type")
        .map(|s| s.as_str())
        .unwrap_or("emitidos");
    let result = retention::get(&pool, &rfc, dl_type)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/geography
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/geography",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos|ambos"),
        ("from" = Option<String>, Query, description = "YYYY-MM"),
        ("to" = Option<String>, Query, description = "YYYY-MM"),
    ),
    responses((status = 200, description = "Distribución geográfica"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_geography(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = geography::get(&pool, &rfc, &query.dl_type(), &query.from(), &query.to())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/concepts
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/concepts",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos|ambos"),
        ("from" = Option<String>, Query, description = "YYYY-MM"),
        ("to" = Option<String>, Query, description = "YYYY-MM"),
    ),
    responses((status = 200, description = "Conceptos más frecuentes"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_concepts(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = concepts::get(&pool, &rfc, &query.dl_type(), &query.from(), &query.to())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/fiscal
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/fiscal",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos|ambos"),
        ("from" = Option<String>, Query, description = "YYYY-MM"),
        ("to" = Option<String>, Query, description = "YYYY-MM"),
    ),
    responses((status = 200, description = "Análisis fiscal"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_fiscal(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = fiscal::get(&pool, &rfc, &query.dl_type(), &query.from(), &query.to())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/payments
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/payments",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos|ambos"),
        ("from" = Option<String>, Query, description = "YYYY-MM"),
        ("to" = Option<String>, Query, description = "YYYY-MM"),
    ),
    responses((status = 200, description = "Análisis de pagos"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_payments(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = payments::get(&pool, &rfc, &query.dl_type(), &query.from(), &query.to())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/cashflow
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/cashflow",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos|ambos"),
        ("from" = Option<String>, Query, description = "YYYY-MM"),
        ("to" = Option<String>, Query, description = "YYYY-MM"),
    ),
    responses((status = 200, description = "Flujo de caja"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_cashflow(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = cashflow::get(&pool, &rfc, &query.dl_type(), &query.from(), &query.to())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/payroll
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/payroll",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("from" = Option<String>, Query, description = "YYYY-MM"),
        ("to" = Option<String>, Query, description = "YYYY-MM"),
    ),
    responses((status = 200, description = "Análisis de nómina"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_payroll(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = payroll::get(&pool, &rfc, &query.from(), &query.to())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/payroll/snapshot
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/payroll/snapshot",
    tag = "Analytics",
    params(("rfc" = String, Path, description = "RFC del contribuyente")),
    responses((status = 200, description = "Snapshot de nómina: headcount, run-rate, YoY, pasivo laboral"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_payroll_snapshot(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = payroll::get_snapshot(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/hallazgos
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/hallazgos",
    tag = "Analytics",
    params(("rfc" = String, Path, description = "RFC del contribuyente")),
    responses((status = 200, description = "Hallazgos clave automáticos"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_hallazgos(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = hallazgos::get(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// Normalization rules
// GET  /api/v1/analytics/{rfc}/normalization
// POST /api/v1/analytics/{rfc}/normalization
// DELETE /api/v1/analytics/{rfc}/normalization/{id}
// GET  /api/v1/analytics/{rfc}/normalization/payroll
// POST /api/v1/analytics/{rfc}/normalization/payroll
// DELETE /api/v1/analytics/{rfc}/normalization/payroll/{id}
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/normalization",
    tag = "Normalization",
    params(("rfc" = String, Path, description = "RFC del contribuyente")),
    responses((status = 200, description = "Reglas de normalización"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn list_normalization(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let rules = normalization::list_rules(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(rules))
}

#[utoipa::path(
    post,
    path = "/api/v1/analytics/{rfc}/normalization",
    tag = "Normalization",
    params(("rfc" = String, Path, description = "RFC del contribuyente")),
    request_body = normalization::CreateRuleRequest,
    responses(
        (status = 201, description = "Regla creada"),
        (status = 400, description = "Datos inválidos"),
    )
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn create_normalization(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
    body: web::Json<normalization::CreateRuleRequest>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let rule = normalization::create_rule(&pool, &rfc, &body)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Created().json(rule))
}

#[utoipa::path(
    delete,
    path = "/api/v1/analytics/{rfc}/normalization/{rule_id}",
    tag = "Normalization",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("rule_id" = String, Path, description = "ID de la regla"),
    ),
    responses(
        (status = 204, description = "Regla eliminada"),
        (status = 404, description = "Regla no encontrada"),
    )
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty, rule_id = tracing::field::Empty))]
pub async fn delete_normalization(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let (rfc, id) = path.into_inner();
    tracing::Span::current().record("rfc", &rfc.to_uppercase().as_str());
    tracing::Span::current().record("rule_id", &id.as_str());
    check_rfc_access(&pool, &req, &rfc.to_uppercase()).await?;
    let deleted = normalization::delete_rule(&pool, &id, &rfc.to_uppercase())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    if deleted {
        Ok(HttpResponse::NoContent().finish())
    } else {
        Err(AppError::not_found("Rule not found"))
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/normalization/payroll",
    tag = "Normalization",
    params(("rfc" = String, Path, description = "RFC del contribuyente")),
    responses((status = 200, description = "Reglas de nómina"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn list_payroll_normalization(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let rules = normalization::list_payroll_rules(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(rules))
}

#[utoipa::path(
    post,
    path = "/api/v1/analytics/{rfc}/normalization/payroll",
    tag = "Normalization",
    params(("rfc" = String, Path, description = "RFC del contribuyente")),
    request_body = normalization::CreatePayrollRuleRequest,
    responses(
        (status = 201, description = "Regla de nómina creada"),
        (status = 400, description = "Datos inválidos"),
    )
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn create_payroll_normalization(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
    body: web::Json<normalization::CreatePayrollRuleRequest>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let rule = normalization::create_payroll_rule(&pool, &rfc, &body)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Created().json(rule))
}

#[utoipa::path(
    delete,
    path = "/api/v1/analytics/{rfc}/normalization/payroll/{rule_id}",
    tag = "Normalization",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("rule_id" = String, Path, description = "ID de la regla"),
    ),
    responses(
        (status = 204, description = "Regla eliminada"),
        (status = 404, description = "Regla no encontrada"),
    )
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty, rule_id = tracing::field::Empty))]
pub async fn delete_payroll_normalization(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let (rfc, id) = path.into_inner();
    tracing::Span::current().record("rfc", &rfc.to_uppercase().as_str());
    tracing::Span::current().record("rule_id", &id.as_str());
    check_rfc_access(&pool, &req, &rfc.to_uppercase()).await?;
    let deleted = normalization::delete_payroll_rule(&pool, &id, &rfc.to_uppercase())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    if deleted {
        Ok(HttpResponse::NoContent().finish())
    } else {
        Err(AppError::not_found("Payroll rule not found"))
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/normalization/excluded
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/normalization/excluded",
    tag = "Normalization",
    params(("rfc" = String, Path, description = "RFC del contribuyente")),
    responses((status = 200, description = "CFDIs excluidos por reglas de normalización"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn list_excluded_cfdis(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let cfdis = normalization::list_excluded_cfdis(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(cfdis))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/normalization/cfdis
// ---------------------------------------------------------------------------

pub async fn list_norm_cfdis(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let dl_type = query.dl_type();
    let from = query.from();
    let to = query.to();
    let limit = query.limit();
    let (from_y, from_m) = crate::services::analytics::summary::parse_ym(&from);
    let (to_y, to_m) = crate::services::analytics::summary::parse_ym(&to);
    let rows = normalization::list_cfdis_for_normalization(
        &pool, &rfc, &dl_type, from_y, from_m, to_y, to_m, limit,
    )
    .await
    .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(rows))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/normalization/counterparties
// ---------------------------------------------------------------------------

pub async fn list_norm_counterparties(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let dl_type = query.dl_type();
    let from = query.from();
    let to = query.to();
    let (from_y, from_m) = crate::services::analytics::summary::parse_ym(&from);
    let (to_y, to_m) = crate::services::analytics::summary::parse_ym(&to);
    let rows = normalization::list_counterparties_for_normalization(
        &pool, &rfc, &dl_type, from_y, from_m, to_y, to_m,
    )
    .await
    .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(rows))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/normalization/counterparties/{cp_rfc}/cfdis
// ---------------------------------------------------------------------------

pub async fn list_norm_counterparty_cfdis(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let (rfc, cp_rfc) = path.into_inner();
    let rfc = rfc.to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let dl_type = query.dl_type();
    let from = query.from();
    let to = query.to();
    let limit = query.limit();
    let (from_y, from_m) = crate::services::analytics::summary::parse_ym(&from);
    let (to_y, to_m) = crate::services::analytics::summary::parse_ym(&to);
    let rows = normalization::list_cfdis_for_counterparty(
        &pool, &rfc, &cp_rfc.to_uppercase(), &dl_type, from_y, from_m, to_y, to_m, limit,
    )
    .await
    .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(rows))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/normalization/payroll/employees
// ---------------------------------------------------------------------------

pub async fn get_normalization_payroll_employees(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let from = query.from();
    let to = query.to();
    let (from_y, from_m) = crate::services::analytics::summary::parse_ym(&from);
    let (to_y, to_m) = crate::services::analytics::summary::parse_ym(&to);
    let rows = normalization::list_payroll_employees(&pool, &rfc, from_y, from_m, to_y, to_m)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(rows))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/normalization/ebitda-bridge
// ---------------------------------------------------------------------------

pub async fn get_normalization_ebitda_bridge(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let from = query.from();
    let to = query.to();
    let (from_y, from_m) = crate::services::analytics::summary::parse_ym(&from);
    let (to_y, to_m) = crate::services::analytics::summary::parse_ym(&to);
    let rows =
        normalization::list_ebitda_bridge_adjustments(&pool, &rfc, from_y, from_m, to_y, to_m)
            .await
            .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(rows))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/counterparties/evolution
// ---------------------------------------------------------------------------

pub async fn get_counterparties_evolution(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let result =
        counterparties::get_evolution(&pool, &rfc, &query.dl_type(), &query.from(), &query.to())
            .await
            .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/counterparties/ltm
// ---------------------------------------------------------------------------

pub async fn get_counterparties_ltm(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = counterparties::get_ltm_comparison(&pool, &rfc, &query.dl_type(), &query.to())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/counterparties/payments-detail
// ---------------------------------------------------------------------------

pub async fn get_counterparties_payments_detail(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = counterparties::get_payments_detail(
        &pool,
        &rfc,
        &query.dl_type(),
        &query.from(),
        &query.to(),
    )
    .await
    .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/counterparties/atypical
// ---------------------------------------------------------------------------

pub async fn get_counterparties_atypical(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let result =
        counterparties::get_atypical(&pool, &rfc, &query.dl_type(), &query.from(), &query.to())
            .await
            .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/counterparties/{cp_rfc}
// ---------------------------------------------------------------------------

pub async fn get_counterparty_individual(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let (rfc, cp_rfc) = path.into_inner();
    let rfc = rfc.to_uppercase();
    let cp_rfc = cp_rfc.to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = counterparties::get_individual(
        &pool,
        &rfc,
        &cp_rfc,
        &query.dl_type(),
        &query.from(),
        &query.to(),
    )
    .await
    .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the last fully-closed month (i.e. never the current in-progress month).
fn current_month() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86400;
    let (y, m, _) = days_to_ymd(days);
    // Step back one month to get the last complete month
    let total = y as i64 * 12 + m as i64 - 1 - 1;
    let ly = total / 12;
    let lm = total % 12 + 1;
    format!("{ly:04}-{lm:02}")
}

fn default_from() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86400;
    let (y, m, _) = days_to_ymd(days);
    // 12 months back from the last complete month (current month - 13)
    let total = y as i64 * 12 + m as i64 - 1 - 12;
    let fy = total / 12;
    let fm = total % 12 + 1;
    format!("{fy:04}-{fm:02}")
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/quarterly
// ---------------------------------------------------------------------------

#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_quarterly(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = quarterly::get(&pool, &rfc, &query.dl_type(), &query.from(), &query.to())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let mut y = 1970u64;
    let mut rem = days;
    loop {
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let dy = if leap { 366 } else { 365 };
        if rem < dy {
            break;
        }
        rem -= dy;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let months = [
        31u64,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut mo = 1u64;
    for &dm in &months {
        if rem < dm {
            break;
        }
        rem -= dm;
        mo += 1;
    }
    (y, mo, rem + 1)
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/period-comparison
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct PeriodComparisonParams {
    pub dl_type: Option<String>,
    pub from_month: Option<i32>,
    pub to_month: Option<i32>,
    pub years: Option<String>, // comma-separated e.g. "2023,2024,2025,2026"
    pub limit: Option<i64>,
}

#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_period_comparison(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<PeriodComparisonParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let dl_type = query.dl_type.clone().unwrap_or_else(|| "emitidos".into());
    let from_month = query.from_month.unwrap_or(1).clamp(1, 12);
    let to_month = query.to_month.unwrap_or(12).clamp(1, 12);
    let years: Vec<i32> = query
        .years
        .as_deref()
        .unwrap_or("2023,2024,2025,2026")
        .split(',')
        .filter_map(|s| s.trim().parse::<i32>().ok())
        .collect();
    let limit = query.limit.unwrap_or(10).clamp(1, 50);

    let result = period_comparison::get(&pool, &rfc, &dl_type, from_month, to_month, &years, limit)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_xml_count(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let dl_type = query.get("dl_type").map(|s| s.as_str()).unwrap_or("emitidos");
    let result = xml_count::get(&pool, &rfc, dl_type)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

pub async fn get_xml_breakdown(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let result = xml_breakdown::get(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}
