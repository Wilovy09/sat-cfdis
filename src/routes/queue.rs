//! Queue management API.
//!
//! GET  /api/v1/queue              — list all jobs (newest first)
//! GET  /api/v1/queue/{id}         — job detail
//! GET  /api/v1/queue/{id}/results — paginated invoice metadata for a job
//! DELETE /api/v1/queue/{id}       — cancel a pending/paused job

use actix_web::{HttpResponse, web};
use serde::Deserialize;
use serde_json::json;

use crate::{db::jobs, errors::AppError};

pub type DbPool = crate::db::DbPool;

// ---------------------------------------------------------------------------
// GET /api/v1/queue
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/queue",
    tag = "Queue",
    responses(
        (status = 200, description = "Lista de jobs"),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_jobs(pool: web::Data<DbPool>) -> Result<HttpResponse, AppError> {
    let jobs = jobs::list_all(pool.get_ref())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    tracing::debug!(count = jobs.len(), "list_jobs");
    Ok(HttpResponse::Ok().json(json!({ "jobs": jobs })))
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
#[tracing::instrument(skip(pool), fields(id = %path))]
pub async fn get_job(
    pool: web::Data<DbPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
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
#[tracing::instrument(skip(pool, query), fields(id = %path))]
pub async fn get_job_results(
    pool: web::Data<DbPool>,
    path: web::Path<String>,
    query: web::Query<ResultsQuery>,
) -> Result<HttpResponse, AppError> {
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
#[tracing::instrument(skip(pool), fields(id = %path))]
pub async fn cancel_job(
    pool: web::Data<DbPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
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
