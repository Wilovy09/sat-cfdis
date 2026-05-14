use actix_web::{HttpRequest, HttpResponse, web};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::DbPool;
use crate::errors::AppError;
use crate::services::crypto;

#[derive(Debug, Deserialize, ToSchema)]
pub struct CompleteProfileDto {
    pub rfc: String,
    pub clave: String,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

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
    // Adquiere uses "id"; fall back to "sub" for spec compliance
    json.get("id")
        .or_else(|| json.get("sub"))?
        .as_str()
        .map(|s| s.to_string())
}

/// Compute period_from / period_to for the initial 3-year + current-year sync.
/// Example (today = 2026-04-27): from = 2023-01-01, to = 2026-03-31.
fn initial_sync_period() -> (String, String) {
    let now = crate::db::jobs::utc_offset(0); // "2026-04-27T..."
    let year: u32 = now[0..4].parse().unwrap_or(2026);
    let month: u32 = now[5..7].parse().unwrap_or(4);

    let period_from = format!("{:04}-01-01 00:00:00", year - 3);

    let (to_year, to_month) = if month <= 1 {
        (year - 1, 12u32)
    } else {
        (year, month - 1)
    };
    let last_day = days_in_month(to_year, to_month);
    let period_to = format!("{to_year:04}-{to_month:02}-{last_day:02} 23:59:59");

    (period_from, period_to)
}

fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/users/profile",
    tag = "Users",
    responses(
        (status = 200, description = "Perfil del usuario"),
        (status = 401, description = "No autenticado"),
        (status = 404, description = "Perfil incompleto"),
    )
)]
#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty))]
pub async fn get_profile(req: HttpRequest, pool: web::Data<DbPool>) -> HttpResponse {
    let token = match bearer_token(&req) {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token requerido".to_string(),
            });
        }
    };
    let user_id = match jwt_user_id(&token) {
        Some(id) => id,
        None => {
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token inválido".to_string(),
            });
        }
    };
    tracing::Span::current().record("user_id", &user_id.as_str());

    match crate::db::users::get_user_rfcs(&pool, &user_id).await {
        Ok(rfcs) if !rfcs.is_empty() => HttpResponse::Ok().json(serde_json::json!({ "rfcs": rfcs })),
        Ok(_) => HttpResponse::NotFound().json(ErrorBody {
            error: "Perfil no encontrado".to_string(),
        }),
        Err(e) => {
            tracing::error!(user_id = %user_id, "get_profile: DB error: {e}");
            HttpResponse::InternalServerError().json(ErrorBody {
                error: "Error de base de datos".to_string(),
            })
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/users/complete-profile",
    tag = "Users",
    request_body = CompleteProfileDto,
    responses(
        (status = 200, description = "Perfil completado exitosamente"),
        (status = 401, description = "No autenticado"),
        (status = 422, description = "RFC o CIEC inválidos"),
    )
)]
#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty, rfc = tracing::field::Empty))]
pub async fn complete_profile(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    body: web::Json<CompleteProfileDto>,
) -> HttpResponse {
    let token = match bearer_token(&req) {
        Some(t) => t,
        None => {
            tracing::warn!("complete_profile: missing token");
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token requerido".to_string(),
            });
        }
    };

    let user_id = match jwt_user_id(&token) {
        Some(id) => id,
        None => {
            tracing::warn!("complete_profile: invalid token");
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token inválido".to_string(),
            });
        }
    };
    tracing::Span::current().record("user_id", &user_id.as_str());

    let rfc = body.rfc.trim().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    if rfc.is_empty() || body.clave.is_empty() {
        return HttpResponse::UnprocessableEntity().json(ErrorBody {
            error: "RFC y CIEC son requeridos".to_string(),
        });
    }

    let key = crypto::load_key();

    // Encrypt the CIEC password for storage in pulso.users
    let clave_enc = match crypto::encrypt(&key, &body.clave) {
        Ok(enc) => enc,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: format!("Error al cifrar credenciales: {e}"),
            });
        }
    };

    // Build and encrypt auth payload for the background sync job
    let auth_json = serde_json::json!({
        "type": "ciec",
        "rfc": rfc,
        "password": body.clave,
    })
    .to_string();

    let auth_enc = match crypto::encrypt(&key, &auth_json) {
        Ok(enc) => enc,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: format!("Error al cifrar auth: {e}"),
            });
        }
    };

    // Create the background sync job covering 3 full years + complete months of current year
    let (period_from, period_to) = initial_sync_period();
    let sync_job_id =
        match crate::db::jobs::insert_queued(&pool, &rfc, "ciec", &auth_enc, "ambos", &period_from, &period_to)
            .await
        {
            Ok(id) => {
                tracing::info!(user_id = %user_id, job_id = %id, "Initial sync job queued");
                Some(id)
            }
            Err(e) => {
                tracing::error!(user_id = %user_id, "Failed to queue initial sync: {e}");
                None
            }
        };

    // Save RFC + encrypted CIEC to pulso.users
    if let Err(e) = crate::db::users::create_pulso_user(
        &pool,
        &user_id,
        &rfc,
        &clave_enc,
        sync_job_id.as_deref(),
    )
    .await
    {
        tracing::error!(user_id = %user_id, "Error creating pulso user: {e}");
        return HttpResponse::InternalServerError().json(ErrorBody {
            error: "Error al guardar el perfil".to_string(),
        });
    }

    if let Err(e) = crate::db::users::set_profile_complete(&pool, &user_id).await {
        tracing::error!(user_id = %user_id, "Error setting profile complete: {e}");
        return HttpResponse::InternalServerError().json(ErrorBody {
            error: "Error al actualizar el perfil".to_string(),
        });
    }

    tracing::info!("Profile completed successfully");
    HttpResponse::Ok().json(serde_json::json!({
        "ok": true,
        "sync_job_id": sync_job_id,
    }))
}

#[derive(Debug, Deserialize)]
pub struct TriggerSyncDto {
    pub rfc: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/v1/users/trigger-sync",
    tag = "Users",
    responses(
        (status = 200, description = "Job encolado"),
        (status = 401, description = "No autenticado"),
        (status = 404, description = "Perfil incompleto"),
        (status = 409, description = "Ya existe un job activo"),
    )
)]
#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty, rfc = tracing::field::Empty))]
pub async fn trigger_sync(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    body: Option<web::Json<TriggerSyncDto>>,
) -> HttpResponse {
    let token = match bearer_token(&req) {
        Some(t) => t,
        None => {
            tracing::warn!("trigger_sync: missing token");
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token requerido".to_string(),
            });
        }
    };
    let user_id = match jwt_user_id(&token) {
        Some(id) => id,
        None => {
            tracing::warn!("trigger_sync: invalid token");
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token inválido".to_string(),
            });
        }
    };
    tracing::Span::current().record("user_id", &user_id.as_str());

    let requested_rfc = body.as_ref().and_then(|b| b.rfc.as_deref().map(|s| s.trim().to_uppercase())).filter(|s| !s.is_empty());

    let (rfc, clave_enc, existing_job_id) = if let Some(ref specific_rfc) = requested_rfc {
        // Specific RFC requested — look up its credentials
        match crate::db::users::get_credentials_for_rfc(&pool, &user_id, specific_rfc).await {
            Ok(Some((clave_enc, job_id))) => (specific_rfc.clone(), clave_enc, job_id),
            Ok(None) => {
                tracing::warn!(user_id = %user_id, rfc = %specific_rfc, "trigger_sync: RFC not found");
                return HttpResponse::NotFound().json(ErrorBody {
                    error: "RFC no encontrado".to_string(),
                });
            }
            Err(e) => {
                tracing::error!(user_id = %user_id, "trigger_sync: DB error: {e}");
                return HttpResponse::InternalServerError().json(ErrorBody {
                    error: "Error de base de datos".to_string(),
                });
            }
        }
    } else {
        // Fallback: first RFC (backward compat)
        match crate::db::users::get_user_credentials(&pool, &user_id).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                tracing::warn!(user_id = %user_id, "trigger_sync: profile not found");
                return HttpResponse::NotFound().json(ErrorBody {
                    error: "Perfil no encontrado".to_string(),
                });
            }
            Err(e) => {
                tracing::error!(user_id = %user_id, "trigger_sync: DB error: {e}");
                return HttpResponse::InternalServerError().json(ErrorBody {
                    error: "Error de base de datos".to_string(),
                });
            }
        }
    };
    tracing::Span::current().record("rfc", &rfc.as_str());

    // If there's already an active job, return its status instead of creating a duplicate
    if let Some(ref job_id) = existing_job_id {
        if let Ok(Some(job)) = crate::db::jobs::get_by_id(&pool, job_id).await {
            if matches!(job.status.as_str(), "queued" | "running" | "paused_limit") {
                return HttpResponse::Conflict().json(serde_json::json!({
                    "error": "Ya existe un job activo",
                    "job_id": job_id,
                    "status": job.status,
                }));
            }
        }
    }

    // Decrypt the stored CIEC password and rebuild auth payload
    let key = crate::services::crypto::load_key();
    let clave = match crate::services::crypto::decrypt(&key, &clave_enc) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(user_id = %user_id, "trigger_sync: decrypt failed: {e}");
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: "Error al descifrar credenciales".to_string(),
            });
        }
    };

    let auth_json = serde_json::json!({
        "type": "ciec",
        "rfc":  rfc,
        "password": clave,
    })
    .to_string();

    let auth_enc = match crate::services::crypto::encrypt(&key, &auth_json) {
        Ok(e) => e,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: format!("Error al cifrar auth: {e}"),
            });
        }
    };

    let (period_from, period_to) = initial_sync_period();
    let job_id = match crate::db::jobs::insert_queued(
        &pool, &rfc, "ciec", &auth_enc, "ambos", &period_from, &period_to,
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(user_id = %user_id, "trigger_sync: insert_queued failed: {e}");
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: "Error al crear el job".to_string(),
            });
        }
    };

    // Always update only the specific RFC's row — never overwrite other RFCs' job ids
    let _ = crate::db::users::set_initial_sync_job_for_rfc(&pool, &user_id, &rfc, &job_id).await;
    tracing::info!(user_id = %user_id, job_id = %job_id, "Sync job triggered manually");

    HttpResponse::Ok().json(serde_json::json!({
        "ok": true,
        "job_id": job_id,
        "status": "queued",
    }))
}

#[derive(Debug, Deserialize)]
pub struct SyncStatusQuery {
    pub rfc: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/v1/users/sync-status",
    tag = "Users",
    responses(
        (status = 200, description = "Estado del sync inicial"),
        (status = 401, description = "No autenticado"),
    )
)]
#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty))]
pub async fn sync_status(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    query: web::Query<SyncStatusQuery>,
) -> HttpResponse {
    let token = match bearer_token(&req) {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token requerido".to_string(),
            });
        }
    };

    let user_id = match jwt_user_id(&token) {
        Some(id) => id,
        None => {
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token inválido".to_string(),
            });
        }
    };
    tracing::Span::current().record("user_id", &user_id.as_str());

    let specific_rfc = query.rfc.as_deref().map(|s| s.trim().to_uppercase()).filter(|s| !s.is_empty());

    let job_id_opt = if let Some(ref rfc) = specific_rfc {
        // Look up sync info for a specific RFC
        match crate::db::users::get_credentials_for_rfc(&pool, &user_id, rfc).await {
            Ok(Some((_clave, job_id))) => job_id,
            Ok(None) => {
                return HttpResponse::Ok().json(serde_json::json!({ "status": "none" }));
            }
            Err(e) => {
                tracing::error!(user_id = %user_id, "Error fetching sync info for RFC: {e}");
                return HttpResponse::InternalServerError().json(ErrorBody {
                    error: "Error al consultar estado".to_string(),
                });
            }
        }
    } else {
        // Fallback: first RFC (backward compat)
        let sync_info = match crate::db::users::get_user_sync_info(&pool, &user_id).await {
            Ok(Some(info)) => info,
            Ok(None) => {
                return HttpResponse::Ok().json(serde_json::json!({ "status": "none" }));
            }
            Err(e) => {
                tracing::error!(user_id = %user_id, "Error fetching sync info: {e}");
                return HttpResponse::InternalServerError().json(ErrorBody {
                    error: "Error al consultar estado".to_string(),
                });
            }
        };
        sync_info.1
    };

    let Some(job_id) = job_id_opt else {
        return HttpResponse::Ok().json(serde_json::json!({ "status": "none" }));
    };

    match crate::db::jobs::get_by_id(&pool, &job_id).await {
        Ok(Some(job)) => HttpResponse::Ok().json(serde_json::json!({
            "status":      job.status,
            "found":       job.found,
            "job_id":      job.id,
            "period_from": job.period_from,
            "period_to":   job.period_to,
        })),
        Ok(None) => HttpResponse::Ok().json(serde_json::json!({ "status": "none" })),
        Err(e) => {
            tracing::error!(job_id = %job_id, "Error fetching job: {e}");
            HttpResponse::InternalServerError().json(ErrorBody {
                error: "Error al consultar el job".to_string(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/users/rfcs
// ---------------------------------------------------------------------------

#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty))]
pub async fn get_rfcs(req: HttpRequest, pool: web::Data<DbPool>) -> Result<HttpResponse, AppError> {
    let token = bearer_token(&req).ok_or_else(|| AppError::unauthorized("Token requerido"))?;
    let user_id = jwt_user_id(&token).ok_or_else(|| AppError::unauthorized("Token inválido"))?;
    tracing::Span::current().record("user_id", &user_id.as_str());

    let is_admin = crate::db::users::is_user_admin(&pool, &user_id)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;

    if is_admin {
        // Return all (user_id, rfc) pairs
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT user_id::text, rfc FROM pulso.users ORDER BY ctid",
        )
        .fetch_all(pool.as_ref())
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
        return Ok(HttpResponse::Ok().json(serde_json::json!({ "rfcs": rows.into_iter().map(|(uid, rfc)| serde_json::json!({ "user_id": uid, "rfc": rfc })).collect::<Vec<_>>() })));
    }

    let rfcs = crate::db::users::get_user_rfcs(&pool, &user_id)
        .await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(HttpResponse::Ok().json(serde_json::json!({ "rfcs": rfcs })))
}

// ---------------------------------------------------------------------------
// POST /api/v1/users/rfcs
// ---------------------------------------------------------------------------

#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty, rfc = tracing::field::Empty))]
pub async fn add_rfc(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    body: web::Json<CompleteProfileDto>,
) -> HttpResponse {
    let token = match bearer_token(&req) {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token requerido".to_string(),
            });
        }
    };
    let user_id = match jwt_user_id(&token) {
        Some(id) => id,
        None => {
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token inválido".to_string(),
            });
        }
    };
    tracing::Span::current().record("user_id", &user_id.as_str());

    let rfc = body.rfc.trim().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());
    if rfc.is_empty() || body.clave.is_empty() {
        return HttpResponse::UnprocessableEntity().json(ErrorBody {
            error: "RFC y CIEC son requeridos".to_string(),
        });
    }

    let key = crypto::load_key();

    let clave_enc = match crypto::encrypt(&key, &body.clave) {
        Ok(enc) => enc,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: format!("Error al cifrar credenciales: {e}"),
            });
        }
    };

    let auth_json = serde_json::json!({
        "type": "ciec",
        "rfc": rfc,
        "password": body.clave,
    })
    .to_string();

    let auth_enc = match crypto::encrypt(&key, &auth_json) {
        Ok(enc) => enc,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: format!("Error al cifrar auth: {e}"),
            });
        }
    };

    let (period_from, period_to) = initial_sync_period();
    let sync_job_id =
        match crate::db::jobs::insert_queued(&pool, &rfc, "ciec", &auth_enc, "ambos", &period_from, &period_to)
            .await
        {
            Ok(id) => {
                tracing::info!(user_id = %user_id, job_id = %id, "Initial sync job queued for new RFC");
                Some(id)
            }
            Err(e) => {
                tracing::error!(user_id = %user_id, "Failed to queue initial sync: {e}");
                None
            }
        };

    if let Err(e) = crate::db::users::create_pulso_user(
        &pool,
        &user_id,
        &rfc,
        &clave_enc,
        sync_job_id.as_deref(),
    )
    .await
    {
        // Detect unique constraint violation (RFC already exists for this user)
        let err_str = e.to_string();
        if err_str.contains("users_user_id_rfc_unique") || err_str.contains("unique") || err_str.contains("duplicate") {
            return HttpResponse::Conflict().json(ErrorBody {
                error: "Este RFC ya está registrado para este usuario".to_string(),
            });
        }
        tracing::error!(user_id = %user_id, "Error creating pulso user (add_rfc): {e}");
        return HttpResponse::InternalServerError().json(ErrorBody {
            error: "Error al guardar el RFC".to_string(),
        });
    }

    if let Err(e) = crate::db::users::set_profile_complete(&pool, &user_id).await {
        tracing::error!(user_id = %user_id, "Error setting profile complete: {e}");
    }

    tracing::info!("RFC added successfully");
    HttpResponse::Ok().json(serde_json::json!({
        "ok": true,
        "sync_job_id": sync_job_id,
    }))
}

// ---------------------------------------------------------------------------
// PUT /api/v1/users/rfcs/{rfc}/clave
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct UpdateClaveDto {
    pub clave: String,
}

#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty, rfc = tracing::field::Empty))]
pub async fn update_rfc_clave_handler(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    path: web::Path<String>,
    body: web::Json<UpdateClaveDto>,
) -> HttpResponse {
    let token = match bearer_token(&req) {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token requerido".to_string(),
            });
        }
    };
    let user_id = match jwt_user_id(&token) {
        Some(id) => id,
        None => {
            return HttpResponse::Unauthorized().json(ErrorBody {
                error: "Token inválido".to_string(),
            });
        }
    };
    tracing::Span::current().record("user_id", &user_id.as_str());

    let rfc = path.into_inner().trim().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());

    if body.clave.is_empty() {
        return HttpResponse::UnprocessableEntity().json(ErrorBody {
            error: "CIEC es requerida".to_string(),
        });
    }

    let key = crypto::load_key();
    let clave_enc = match crypto::encrypt(&key, &body.clave) {
        Ok(enc) => enc,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ErrorBody {
                error: format!("Error al cifrar credenciales: {e}"),
            });
        }
    };

    match crate::db::users::update_rfc_clave(&pool, &user_id, &rfc, &clave_enc).await {
        Ok(true) => HttpResponse::Ok().json(serde_json::json!({ "ok": true })),
        Ok(false) => HttpResponse::NotFound().json(ErrorBody {
            error: "RFC no encontrado".to_string(),
        }),
        Err(e) => {
            tracing::error!(user_id = %user_id, rfc = %rfc, "update_rfc_clave: DB error: {e}");
            HttpResponse::InternalServerError().json(ErrorBody {
                error: "Error de base de datos".to_string(),
            })
        }
    }
}

#[tracing::instrument(skip_all, fields(user_id = tracing::field::Empty, rfc = tracing::field::Empty))]
pub async fn delete_rfc_handler(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    path: web::Path<String>,
) -> HttpResponse {
    let token = match bearer_token(&req) {
        Some(t) => t,
        None => return HttpResponse::Unauthorized().json(ErrorBody { error: "Token requerido".to_string() }),
    };
    let user_id = match jwt_user_id(&token) {
        Some(id) => id,
        None => return HttpResponse::Unauthorized().json(ErrorBody { error: "Token inválido".to_string() }),
    };
    tracing::Span::current().record("user_id", &user_id.as_str());

    let rfc = path.into_inner().trim().to_uppercase();
    tracing::Span::current().record("rfc", &rfc.as_str());

    // Prevent deleting the last RFC
    match crate::db::users::get_user_rfcs(&pool, &user_id).await {
        Ok(rfcs) if rfcs.len() <= 1 => {
            return HttpResponse::UnprocessableEntity().json(ErrorBody {
                error: "No puedes eliminar tu único RFC".to_string(),
            });
        }
        Err(e) => {
            tracing::error!(user_id = %user_id, "delete_rfc: DB error: {e}");
            return HttpResponse::InternalServerError().json(ErrorBody { error: "Error de base de datos".to_string() });
        }
        _ => {}
    }

    match crate::db::users::delete_user_rfc(&pool, &user_id, &rfc).await {
        Ok(true) => HttpResponse::Ok().json(serde_json::json!({ "ok": true })),
        Ok(false) => HttpResponse::NotFound().json(ErrorBody { error: "RFC no encontrado".to_string() }),
        Err(e) => {
            tracing::error!(user_id = %user_id, rfc = %rfc, "delete_rfc: DB error: {e}");
            HttpResponse::InternalServerError().json(ErrorBody { error: "Error de base de datos".to_string() })
        }
    }
}
