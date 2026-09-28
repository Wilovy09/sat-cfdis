//! Queue management API.
//!
//! GET  /api/v1/queue              — list all jobs (newest first)
//! GET  /api/v1/queue/{id}         — job detail
//! GET  /api/v1/queue/{id}/results — paginated invoice metadata for a job
//! DELETE /api/v1/queue/{id}       — cancel a pending/paused job

use actix_web::{HttpRequest, HttpResponse, web};
use serde::Deserialize;
use serde_json::json;

use crate::{config::Config, db::jobs, errors::AppError, services::session};

pub type DbPool = crate::db::DbPool;

// ---------------------------------------------------------------------------
// Admin auth — this whole module is admin-only (raw job/RFC data, cancel power)
// ---------------------------------------------------------------------------

async fn require_admin(req: &HttpRequest, pool: &DbPool, cfg: &Config) -> Result<(), AppError> {
    let user_id = session::require_session(req, cfg)?;
    let is_admin = crate::db::users::is_user_admin(pool, &user_id)
        .await
        .unwrap_or(false);
    if !is_admin {
        return Err(AppError::forbidden("Acceso denegado"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// GET /api/v1/queue
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ListJobsQuery {
    #[serde(default)]
    search: String,
    #[serde(default = "default_page")]
    page: i64,
    #[serde(default = "default_page_size")]
    page_size: i64,
}

fn default_page() -> i64 {
    1
}

fn default_page_size() -> i64 {
    15
}

#[utoipa::path(
    get,
    path = "/api/v1/queue",
    tag = "Queue",
    params(
        ("search" = Option<String>, Query, description = "Filtra por RFC o error_code"),
        ("page" = Option<i64>, Query, description = "Página (default 1)"),
        ("page_size" = Option<i64>, Query, description = "Tamaño de página (default 15)"),
    ),
    responses(
        (status = 200, description = "Lista de jobs paginada"),
    )
)]
#[tracing::instrument(skip(req, pool, cfg, query))]
pub async fn list_jobs(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
    query: web::Query<ListJobsQuery>,
) -> Result<HttpResponse, AppError> {
    require_admin(&req, pool.get_ref(), &cfg).await?;

    let page = query.page.max(1);
    let page_size = query.page_size.clamp(1, 100);
    let search = query.search.trim();

    let total = jobs::count_all(pool.get_ref(), search)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    let items = jobs::list_paginated(pool.get_ref(), search, page_size, (page - 1) * page_size)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    tracing::debug!(count = items.len(), total, "list_jobs");
    Ok(HttpResponse::Ok().json(json!({
        "jobs": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/queue/{id}
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/queue/{id}",
    tag = "Queue",
    params(("id" = String, Path, description = "Job ID")),
    responses(
        (status = 200, description = "Detalle del job"),
        (status = 404, description = "Job no encontrado"),
    )
)]
#[tracing::instrument(skip(req, pool, cfg), fields(id = %path))]
pub async fn get_job(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    require_admin(&req, pool.get_ref(), &cfg).await?;
    let id = path.into_inner();
    match jobs::get_by_id(pool.get_ref(), &id)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
    {
        Some(job) => Ok(HttpResponse::Ok().json(job)),
        None => Err(AppError::not_found(format!("job {id} not found"))),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/queue/{id}/results?limit=50&offset=0
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ResultsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_limit() -> i64 {
    50
}

#[utoipa::path(
    get,
    path = "/api/v1/queue/{id}/results",
    tag = "Queue",
    params(
        ("id" = String, Path, description = "Job ID"),
        ("limit" = Option<i64>, Query, description = "Máximo de resultados (default 50)"),
        ("offset" = Option<i64>, Query, description = "Offset de paginación"),
    ),
    responses(
        (status = 200, description = "Facturas del job"),
        (status = 404, description = "Job no encontrado"),
    )
)]
#[tracing::instrument(skip(req, pool, cfg, query), fields(id = %path))]
pub async fn get_job_results(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
    path: web::Path<String>,
    query: web::Query<ResultsQuery>,
) -> Result<HttpResponse, AppError> {
    require_admin(&req, pool.get_ref(), &cfg).await?;
    let id = path.into_inner();
    let limit = query.limit.clamp(1, 500);
    let offset = query.offset.max(0);

    // Verify job exists
    if jobs::get_by_id(pool.get_ref(), &id)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .is_none()
    {
        return Err(AppError::not_found(format!("job {id} not found")));
    }

    let raw_rows = jobs::get_invoices(pool.get_ref(), &id, limit, offset)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    // Parse each metadata JSON string back into a value
    let invoices: Vec<serde_json::Value> = raw_rows
        .iter()
        .filter_map(|s| serde_json::from_str(s).ok())
        .collect();

    Ok(HttpResponse::Ok().json(json!({
        "job_id": id,
        "limit":  limit,
        "offset": offset,
        "count":  invoices.len(),
        "invoices": invoices,
    })))
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/queue/{id}
// ---------------------------------------------------------------------------

#[utoipa::path(
    delete,
    path = "/api/v1/queue/{id}",
    tag = "Queue",
    params(("id" = String, Path, description = "Job ID")),
    responses(
        (status = 200, description = "Job cancelado"),
        (status = 400, description = "No se puede cancelar en el estado actual"),
        (status = 404, description = "Job no encontrado"),
    )
)]
#[tracing::instrument(skip(req, pool, cfg), fields(id = %path))]
pub async fn cancel_job(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    require_admin(&req, pool.get_ref(), &cfg).await?;
    let id = path.into_inner();
    let job = jobs::get_by_id(pool.get_ref(), &id)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .ok_or_else(|| AppError::not_found(format!("job {id} not found")))?;

    match job.status.as_str() {
        "running" => {
            return Err(AppError::bad_request(
                "Cannot cancel a running job — wait for it to pause or complete".to_string(),
            ));
        }
        "completed" | "failed" | "cancelled" => {
            return Err(AppError::bad_request(format!(
                "Job is already {}",
                job.status
            )));
        }
        _ => {}
    }

    sqlx::query("UPDATE pulso.sync_jobs SET status='cancelled', updated_at=$1 WHERE id=$2")
        .bind(jobs::utc_offset(0))
        .bind(&id)
        .execute(pool.get_ref())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    tracing::info!(id = %id, "Job cancelled");
    Ok(HttpResponse::Ok().json(json!({ "cancelled": id })))
}
