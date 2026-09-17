use actix_web::{HttpRequest, HttpResponse, web};
use serde::Deserialize;

use crate::{
    db::DbPool,
    errors::AppError,
    services::{
        analytics::{
            cashflow, concepts, counterparties, data_quality, fiscal, geography, hallazgos,
            hallazgos_egresos, normalization, payments, payroll, period_comparison, quarterly,
            recurrence, retention, summary, xml_breakdown, xml_count,
        },
        response_cache,
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

fn jwt_user_id_analytics(token: &str) -> Option<String> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    // Reject expired tokens
    if let Some(exp) = json.get("exp").and_then(|v| v.as_i64()) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        if now > exp {
            return None;
        }
    }
    json.get("id")
        .or_else(|| json.get("sub"))?
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
    let user_id = jwt_user_id_analytics(&token)
        .ok_or_else(|| AppError::unauthorized("Token inválido o expirado"))?;

    // P-03 / AUD-077: role resolved once per request, not three times (once inside
    // user_has_rfc_or_admin, once here, once inside has_access -- all the same query, same
    // argument). Six permission queries become four; the three identical ones become one.
    // Order unchanged: RFC access is still checked before subscription, so a user with no
    // access gets "denegado", not "paga tu suscripción".
    let is_admin = crate::db::users::is_user_admin(pool, &user_id)
        .await
        .unwrap_or(false);

    let rfc_access = crate::db::users::user_has_rfc_or_admin(pool, &user_id, rfc, is_admin)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    if !rfc_access {
        return Err(AppError::forbidden("Acceso denegado"));
    }

    // Non-admins must have an active pulso subscription.
    if !is_admin {
        let subscribed = crate::routes::billing::has_access(pool, &user_id, is_admin).await;
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

/// Builds a response-cache `params_key` from the query fields an endpoint actually reads
/// -- deliberately not every field on whatever query struct the handler happens to use, so
/// an unrelated param the endpoint ignores can't fragment the cache.
fn cache_key(parts: &[(&str, &str)]) -> String {
    parts
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("|")
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let p = summary::SummaryParams {
        dl_type: query.dl_type(),
        from: query.from(),
        to: query.to(),
    };
    let key = cache_key(&[("dl_type", &p.dl_type), ("from", &p.from), ("to", &p.to)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "summary", &key, || async {
        summary::get(&pool, &rfc, &p).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/summary/month-contributor
// L11-02: on-demand lookup for RES04's quick-read -- only called for a month the frontend
// has already flagged as atypical, never for every month up front.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct MonthContributorParams {
    pub dl_type: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub year: i64,
    pub month: i64,
}

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/summary/month-contributor",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("dl_type" = Option<String>, Query, description = "emitidos|recibidos|ambos"),
        ("from" = Option<String>, Query, description = "YYYY-MM"),
        ("to" = Option<String>, Query, description = "YYYY-MM"),
        ("year" = i64, Query, description = "Año del mes atípico"),
        ("month" = i64, Query, description = "Mes atípico (1-12)"),
    ),
    responses((status = 200, description = "Contraparte que explica el exceso del mes"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_month_contributor(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<MonthContributorParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let p = summary::SummaryParams {
        dl_type: query
            .dl_type
            .clone()
            .unwrap_or_else(|| "emitidos".to_string()),
        from: query.from.clone().unwrap_or_else(default_from),
        to: query.to.clone().unwrap_or_else(current_month),
    };
    let result = summary::month_top_contributor(&pool, &rfc, &p, query.year, query.month)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(result))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/data-quality
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/data-quality",
    tag = "Analytics",
    params(("rfc" = String, Path, description = "RFC del contribuyente")),
    responses((status = 200, description = "Cobertura de XML por sección (emitidas/recibidas/nómina)"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_data_quality(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let value = response_cache::get_or_compute(&pool, &rfc, "data-quality", "", || async {
        data_quality::get(&pool, &rfc).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let (dl_type, from, to, limit) = (query.dl_type(), query.from(), query.to(), query.limit());
    let key = cache_key(&[
        ("dl_type", &dl_type),
        ("from", &from),
        ("to", &to),
        ("limit", &limit.to_string()),
    ]);
    let value = response_cache::get_or_compute(&pool, &rfc, "counterparties", &key, || async {
        counterparties::get(&pool, &rfc, &dl_type, &from, &to, limit).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    let to = query.get("to").map(|s| s.as_str());
    // C14-03/AUD-146: `to` omitted means the service falls back to the current cutoff
    // internally -- the key must carry that resolved value, not an empty placeholder, or
    // it freezes at whatever cutoff was in effect the first time this combination was
    // requested with no `to` at all.
    let to_key = to.map_or_else(|| current_month_yyyymm().to_string(), str::to_string);
    let key = cache_key(&[
        ("dl_type", dl_type),
        ("window_months", &window_months.to_string()),
        ("from", from.unwrap_or("")),
        ("to", &to_key),
    ]);
    let value = response_cache::get_or_compute(&pool, &rfc, "recurrence", &key, || async {
        recurrence::get(&pool, &rfc, dl_type, window_months, from, to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
        ("to" = Option<String>, Query, description = "YYYY-MM, corte superior (por defecto el último mes cerrado)"),
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
    let to = query.get("to").map(|s| s.as_str());
    // C14-03/AUD-146: same fix as recurrence -- an omitted `to` must key on the resolved
    // cutoff, not an empty placeholder that never changes as the real cutoff moves.
    let to_key = to.map_or_else(|| current_month_yyyymm().to_string(), str::to_string);
    let key = cache_key(&[("dl_type", dl_type), ("to", &to_key)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "retention", &key, || async {
        retention::get(&pool, &rfc, dl_type, to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "geography", &key, || async {
        geography::get(&pool, &rfc, &dl_type, &from, &to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/hallazgos-egresos
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/analytics/{rfc}/hallazgos-egresos",
    tag = "Analytics",
    params(
        ("rfc" = String, Path, description = "RFC del propietario"),
        ("to" = Option<String>, Query, description = "YYYY-MM, corte superior (por defecto el último mes cerrado)"),
    ),
    responses((status = 200, description = "Hallazgos de Egresos (Lote 12): H-E1/H-E2/H-E3"))
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_hallazgos_egresos(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    // C14-03/AUD-146: same fix as recurrence/retention -- an omitted `to` must key on the
    // resolved cutoff, not an empty placeholder.
    let to_key = query
        .to
        .as_deref()
        .map_or_else(|| current_month_yyyymm().to_string(), str::to_string);
    let key = cache_key(&[("to", &to_key)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "hallazgos-egresos", &key, || async {
        hallazgos_egresos::get(&pool, &rfc, query.to.as_deref()).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "concepts", &key, || async {
        concepts::get(&pool, &rfc, &dl_type, &from, &to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "fiscal", &key, || async {
        fiscal::get(&pool, &rfc, &dl_type, &from, &to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "payments", &key, || async {
        payments::get(&pool, &rfc, &dl_type, &from, &to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "cashflow", &key, || async {
        cashflow::get(&pool, &rfc, &dl_type, &from, &to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let (from, to) = (query.from(), query.to());
    let key = cache_key(&[("from", &from), ("to", &to)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "payroll", &key, || async {
        payroll::get(&pool, &rfc, &from, &to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    // C14-03/AUD-145: get_snapshot reads CURRENT_DATE five times internally -- an empty
    // key never invalidated across a month boundary. Month granularity (not daily) is
    // enough: everything it computes is anchored to month-end by design (DEC's tenure/
    // pasivo laboral reproducibility within the month); the one daily-sensitive bit (a
    // future-dated alta sanity check) only ever changes on anomalous data.
    let key = cache_key(&[("month", &today_yyyymm().to_string())]);
    let value = response_cache::get_or_compute(&pool, &rfc, "payroll-snapshot", &key, || async {
        payroll::get_snapshot(&pool, &rfc).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    // C14-03/AUD-145: same fix as payroll-snapshot -- 3 uses of today's date and 3 of the
    // last closed month, an empty key never invalidated across a month boundary.
    let key = cache_key(&[("month", &today_yyyymm().to_string())]);
    let value = response_cache::get_or_compute(&pool, &rfc, "hallazgos", &key, || async {
        hallazgos::get(&pool, &rfc).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let rules = normalization::list_rules(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    validate_comprobante_rule_fields(&body)?;
    let rule = normalization::create_rule(&pool, &rfc, &body)
        .await
        .map_err(map_normalization_rule_error)?;
    Ok(HttpResponse::Created().json(rule))
}

#[utoipa::path(
    put,
    path = "/api/v1/analytics/{rfc}/normalization/{rule_id}",
    tag = "Normalization",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("rule_id" = String, Path, description = "ID de la regla"),
    ),
    request_body = normalization::CreateRuleRequest,
    responses(
        (status = 200, description = "Regla actualizada"),
        (status = 400, description = "Datos inválidos"),
        (status = 404, description = "Regla no encontrada"),
    )
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty, rule_id = tracing::field::Empty))]
pub async fn update_normalization(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    pool: web::Data<DbPool>,
    body: web::Json<normalization::CreateRuleRequest>,
) -> Result<HttpResponse, AppError> {
    let (rfc, id) = path.into_inner();
    let rfc = rfc.to_uppercase();
    tracing::Span::current().record("rfc", rfc.as_str());
    tracing::Span::current().record("rule_id", id.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    validate_comprobante_rule_fields(&body)?;
    let rule = normalization::update_rule(&pool, &id, &rfc, &body)
        .await
        .map_err(map_normalization_rule_error)?;
    match rule {
        Some(rule) => Ok(HttpResponse::Ok().json(rule)),
        None => Err(AppError::not_found("Rule not found")),
    }
}

/// L3-13 + L5-12: accounting_line (línea del P&L) and motivo are mandatory on every
/// comprobante-level rule, for both create and update -- a rule saved without either used
/// to fall out of the EBITDA bridge silently, or be impossible to defend months later.
fn validate_comprobante_rule_fields(
    body: &normalization::CreateRuleRequest,
) -> Result<(), AppError> {
    if body
        .accounting_line
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .is_empty()
    {
        return Err(AppError::bad_request(
            "accounting_line es obligatorio: selecciona la línea del P&L de la que sale este ajuste",
        ));
    }
    if body
        .motivo
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .is_empty()
    {
        return Err(AppError::bad_request(
            "motivo es obligatorio: documenta por qué se hace este ajuste",
        ));
    }
    Ok(())
}

/// C4 (L6-10): migration 067 adds a partial unique index on
/// `(owner_rfc, cfdi_uuid) WHERE cfdi_uuid IS NOT NULL AND action = 'exclude'` -- the DB
/// now rejects what the UI already avoided by hiding "Ajustar CFDI individual" once a
/// receipt is excluded (still reachable by a stale tab or a race between two requests).
/// Translates the resulting 23505 into the same conflict, instead of a raw 500.
fn map_normalization_rule_error(e: anyhow::Error) -> AppError {
    let is_duplicate_cfdi_rule = e
        .downcast_ref::<sqlx::Error>()
        .and_then(|se| se.as_database_error())
        .is_some_and(|de| de.code().as_deref() == Some("23505"));
    if is_duplicate_cfdi_rule {
        AppError::bad_request(
            "Ya existe una regla que excluye este mismo comprobante. Dos reglas no pueden \
             apuntar al mismo CFDI.",
        )
    } else {
        AppError::internal(e.to_string())
    }
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
    tracing::Span::current().record("rfc", rfc.to_uppercase().as_str());
    tracing::Span::current().record("rule_id", id.as_str());
    check_rfc_access(&pool, &req, &rfc.to_uppercase()).await?;
    let deleted = normalization::delete_rule(&pool, &id, &rfc.to_uppercase())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let rules = normalization::list_payroll_rules(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;

    match normalization::check_payroll_rule(&pool, &rfc, &body, None)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
    {
        normalization::PayrollRuleCheck::Rejected(msg) => return Err(AppError::bad_request(msg)),
        normalization::PayrollRuleCheck::NeedsConfirmation(warnings) => {
            return Ok(HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "needs_confirmation": true,
                "warnings": warnings,
            })));
        }
        normalization::PayrollRuleCheck::Ok => {}
    }

    let rule = normalization::create_payroll_rule(&pool, &rfc, &body)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Created().json(rule))
}

#[utoipa::path(
    put,
    path = "/api/v1/analytics/{rfc}/normalization/payroll/{rule_id}",
    tag = "Normalization",
    params(
        ("rfc" = String, Path, description = "RFC del contribuyente"),
        ("rule_id" = String, Path, description = "ID de la regla"),
    ),
    request_body = normalization::CreatePayrollRuleRequest,
    responses(
        (status = 200, description = "Regla de nómina actualizada"),
        (status = 400, description = "Datos inválidos"),
        (status = 404, description = "Regla no encontrada"),
    )
)]
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty, rule_id = tracing::field::Empty))]
pub async fn update_payroll_normalization(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    pool: web::Data<DbPool>,
    body: web::Json<normalization::CreatePayrollRuleRequest>,
) -> Result<HttpResponse, AppError> {
    let (rfc, id) = path.into_inner();
    let rfc = rfc.to_uppercase();
    tracing::Span::current().record("rfc", rfc.as_str());
    tracing::Span::current().record("rule_id", id.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;

    // L5-10: same locks/validations as creation, run against this rule's own id so it
    // doesn't get rejected for overlapping itself (L5-08 C1/C2, L5-12, L4-04/L4-12).
    match normalization::check_payroll_rule(&pool, &rfc, &body, Some(&id))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
    {
        normalization::PayrollRuleCheck::Rejected(msg) => return Err(AppError::bad_request(msg)),
        normalization::PayrollRuleCheck::NeedsConfirmation(warnings) => {
            return Ok(HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "needs_confirmation": true,
                "warnings": warnings,
            })));
        }
        normalization::PayrollRuleCheck::Ok => {}
    }

    let rule = normalization::update_payroll_rule(&pool, &id, &rfc, &body)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    match rule {
        Some(rule) => Ok(HttpResponse::Ok().json(rule)),
        None => Err(AppError::not_found("Payroll rule not found")),
    }
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
    tracing::Span::current().record("rfc", rfc.to_uppercase().as_str());
    tracing::Span::current().record("rule_id", id.as_str());
    check_rfc_access(&pool, &req, &rfc.to_uppercase()).await?;
    let deleted = normalization::delete_payroll_rule(&pool, &id, &rfc.to_uppercase())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let cfdis = normalization::list_excluded_cfdis(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(cfdis))
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
    .map_err(|e| AppError::internal(e.to_string()))?;
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
        &pool,
        &rfc,
        &cp_rfc.to_uppercase(),
        &dl_type,
        from_y,
        from_m,
        to_y,
        to_m,
        limit,
    )
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(rows))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/normalization/individual-rule-ids
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct IndividualRuleIdsParams {
    pub source_rfc: Option<String>,
    pub dl_type: Option<String>,
    pub source_name_key: Option<String>,
    pub period_start: Option<String>,
    pub period_end: Option<String>,
}

/// L6-11: server-side replacement for the client-side `findCpIndividualRuleIds`, which
/// paginated up to 500 comprobantes per request (the server's own `limit()` cap) to spot
/// individual rules a counterparty-wide rule would make redundant -- silently missing
/// anything past that cap on a relationship as large as 4,599 comprobantes (9.2x over).
/// Same match logic as `pulso.cfdi_exclusion`'s counterparty branches (migration 062),
/// resolved entirely in SQL: no comprobante rows returned, no page limit, no date floor.
#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn list_normalization_individual_rule_ids(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<IndividualRuleIdsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;

    let source_rfc = query
        .source_rfc
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::bad_request("source_rfc es obligatorio"))?
        .to_uppercase();
    let dl_type = query
        .dl_type
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::bad_request("dl_type es obligatorio"))?;

    let rule_ids = normalization::list_individual_rule_ids_for_counterparty(
        &pool,
        &rfc,
        &source_rfc,
        dl_type,
        query.source_name_key.as_deref(),
        query.period_start.as_deref(),
        query.period_end.as_deref(),
    )
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;

    Ok(HttpResponse::Ok().json(rule_ids))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/normalization/payroll/employees
// ---------------------------------------------------------------------------

pub async fn get_normalization_payroll_employees(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    // L5-02: this catalog is no longer windowed by a date range -- it always returns each
    // employee's real full history, which is the whole point (see normalization.rs).
    let rows = normalization::list_payroll_employees(&pool, &rfc)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(rows))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/normalization/payroll/employees/{employee_rfc}/receipts
// ---------------------------------------------------------------------------

pub async fn get_normalization_payroll_employee_receipts(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let (rfc, employee_rfc) = path.into_inner();
    let rfc = rfc.to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let rows =
        normalization::list_nomina_receipts_for_employee(&pool, &rfc, &employee_rfc.to_uppercase())
            .await
            .map_err(|e| AppError::internal(e.to_string()))?;
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
            .map_err(|e| AppError::internal(e.to_string()))?;
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
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value =
        response_cache::get_or_compute(&pool, &rfc, "counterparties-evolution", &key, || async {
            counterparties::get_evolution(&pool, &rfc, &dl_type, &from, &to).await
        })
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/{rfc}/counterparties/selector
// L11-08: full-universe counterparty list (no exclusion filter, no Top-N cap) for the
// CNT07 "buscar y seleccionar" dropdown.
// ---------------------------------------------------------------------------

pub async fn get_counterparties_selector(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AnalyticsParams>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    check_rfc_access(&pool, &req, &rfc).await?;
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value =
        response_cache::get_or_compute(&pool, &rfc, "counterparties-selector", &key, || async {
            counterparties::list_selector(&pool, &rfc, &dl_type, &from, &to).await
        })
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    let (dl_type, to) = (query.dl_type(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("to", &to)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "counterparties-ltm", &key, || async {
        counterparties::get_ltm_comparison(&pool, &rfc, &dl_type, &to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value = response_cache::get_or_compute(
        &pool,
        &rfc,
        "counterparties-payments-detail",
        &key,
        || async { counterparties::get_payments_detail(&pool, &rfc, &dl_type, &from, &to).await },
    )
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value =
        response_cache::get_or_compute(&pool, &rfc, "counterparties-atypical", &key, || async {
            counterparties::get_atypical(&pool, &rfc, &dl_type, &from, &to).await
        })
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[
        ("cp_rfc", &cp_rfc),
        ("dl_type", &dl_type),
        ("from", &from),
        ("to", &to),
    ]);
    let value =
        response_cache::get_or_compute(&pool, &rfc, "counterparty-individual", &key, || async {
            counterparties::get_individual(&pool, &rfc, &cp_rfc, &dl_type, &from, &to).await
        })
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// L7-03: the definitions live in services::analytics::summary now (it's mirrored into the
// lib crate root; routes isn't). `current_month_yyyymm` is re-exported as `pub(crate)` so
// external callers (`recurrence.rs`, `period_comparison.rs`) keep calling
// `crate::routes::analytics::current_month_yyyymm()` unchanged; `current_month`/
// `days_to_ymd` are only used internally below.
pub(crate) use crate::services::analytics::summary::current_month_yyyymm;
use crate::services::analytics::summary::{current_month, days_to_ymd};

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

/// C14-03/AUD-145: today's REAL calendar month (unlike `current_month_yyyymm()`, which is
/// deliberately one month behind) -- only for keying a cache entry whose query reads
/// `CURRENT_DATE` internally, so it invalidates at the same month boundary the query
/// itself is sensitive to. Never read by the query -- that still calls `CURRENT_DATE`
/// itself, in SQL.
fn today_yyyymm() -> i64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86400;
    let (y, m, _) = days_to_ymd(days);
    (y * 100 + m) as i64
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
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let (dl_type, from, to) = (query.dl_type(), query.from(), query.to());
    let key = cache_key(&[("dl_type", &dl_type), ("from", &from), ("to", &to)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "quarterly", &key, || async {
        quarterly::get(&pool, &rfc, &dl_type, &from, &to).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
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
    tracing::Span::current().record("rfc", rfc.as_str());
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

    // C14-03/AUD-147: keyed on the EFFECTIVE (clamped) to_month, computed the same way
    // `period_comparison::get` clamps it internally -- not the raw query param, or the
    // key stays frozen at a cutoff the calendar has already moved past.
    let effective_to_month = period_comparison::effective_to_month(to_month, &years);
    let years_key = years
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let key = cache_key(&[
        ("dl_type", &dl_type),
        ("from_month", &from_month.to_string()),
        ("to_month", &effective_to_month.to_string()),
        ("years", &years_key),
        ("limit", &limit.to_string()),
    ]);
    let value = response_cache::get_or_compute(&pool, &rfc, "period-comparison", &key, || async {
        period_comparison::get(&pool, &rfc, &dl_type, from_month, to_month, &years, limit).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
}

#[tracing::instrument(skip_all, fields(rfc = tracing::field::Empty))]
pub async fn get_xml_count(
    req: HttpRequest,
    path: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let dl_type = query
        .get("dl_type")
        .map(|s| s.as_str())
        .unwrap_or("emitidos");
    let key = cache_key(&[("dl_type", dl_type)]);
    let value = response_cache::get_or_compute(&pool, &rfc, "xml-count", &key, || async {
        xml_count::get(&pool, &rfc, dl_type).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
}

pub async fn get_xml_breakdown(
    req: HttpRequest,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, AppError> {
    let rfc = path.into_inner().to_uppercase();
    tracing::Span::current().record("rfc", rfc.as_str());
    check_rfc_access(&pool, &req, &rfc).await?;
    let value = response_cache::get_or_compute(&pool, &rfc, "xml-breakdown", "", || async {
        xml_breakdown::get(&pool, &rfc).await
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(value))
}

#[cfg(test)]
mod cache_key_tests {
    use super::cache_key;

    #[test]
    fn joins_parts_in_order() {
        assert_eq!(
            cache_key(&[
                ("dl_type", "emitidos"),
                ("from", "2025-01"),
                ("to", "2026-08")
            ]),
            "dl_type=emitidos|from=2025-01|to=2026-08"
        );
    }

    #[test]
    fn different_values_never_collide() {
        let a = cache_key(&[("from", "2025-01"), ("to", "2026-08")]);
        let b = cache_key(&[("from", "2025-02"), ("to", "2026-08")]);
        assert_ne!(a, b);
    }

    #[test]
    fn empty_parts_is_empty_string() {
        assert_eq!(cache_key(&[]), "");
    }
}
