use sqlx::PgPool;
use uuid::Uuid;

fn parse_uuid(user_id: &str) -> Result<Uuid, sqlx::Error> {
    Uuid::parse_str(user_id).map_err(|e| sqlx::Error::Decode(Box::new(e)))
}

pub async fn get_profile_complete(pool: &PgPool, user_id: &str) -> Result<bool, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(Option<bool>,)> =
        sqlx::query_as("SELECT pulso_complete_profile FROM public.users WHERE id = $1")
            .bind(uid)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(v,)| v).unwrap_or(false))
}

pub async fn create_pulso_user(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
    clave_enc: &str,
    initial_sync_job_id: Option<&str>,
) -> Result<(), sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO pulso.users (id, user_id, rfc, clave, initial_sync_job_id)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(uid)
    .bind(rfc.to_uppercase())
    .bind(clave_enc)
    .bind(initial_sync_job_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_profile_complete(pool: &PgPool, user_id: &str) -> Result<(), sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    sqlx::query("UPDATE public.users SET pulso_complete_profile = true WHERE id = $1")
        .bind(uid)
        .execute(pool)
        .await?;
    Ok(())
}

/// Returns (rfc, encrypted_clave, initial_sync_job_id) for trigger-sync.
/// Returns the first RFC (by ctid) for backward compat with multi-RFC users.
pub async fn get_user_credentials(
    pool: &PgPool,
    user_id: &str,
) -> Result<Option<(String, String, Option<String>)>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT rfc, clave, initial_sync_job_id FROM pulso.users WHERE user_id = $1 ORDER BY ctid LIMIT 1",
    )
    .bind(uid)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Set initial_sync_job_id on an existing pulso.users row.
pub async fn set_initial_sync_job(
    pool: &PgPool,
    user_id: &str,
    job_id: &str,
) -> Result<(), sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    sqlx::query(
        "UPDATE pulso.users SET initial_sync_job_id = $1 WHERE user_id = $2",
    )
    .bind(job_id)
    .bind(uid)
    .execute(pool)
    .await?;
    Ok(())
}

/// Returns (rfc, clave_enc) for every user — used by the monthly sync worker.
pub async fn get_all_with_credentials(pool: &PgPool) -> Result<Vec<(String, String)>, sqlx::Error> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT rfc, clave FROM pulso.users")
            .fetch_all(pool)
            .await?;
    Ok(rows)
}

/// Returns (rfc, initial_sync_job_id) for the given user.
/// Returns the first RFC (by ctid) for backward compat with multi-RFC users.
pub async fn get_user_sync_info(
    pool: &PgPool,
    user_id: &str,
) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT rfc, initial_sync_job_id FROM pulso.users WHERE user_id = $1 ORDER BY ctid LIMIT 1",
    )
    .bind(uid)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// All RFCs belonging to a user (ordered by creation time).
pub async fn get_user_rfcs(pool: &PgPool, user_id: &str) -> Result<Vec<String>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT rfc FROM pulso.users WHERE user_id = $1 ORDER BY ctid",
    )
    .bind(uid)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(rfc,)| rfc).collect())
}

/// True if user owns this RFC OR is admin.
pub async fn user_has_rfc_or_admin(pool: &PgPool, user_id: &str, rfc: &str) -> Result<bool, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    // Check admin first
    let admin_row: Option<(Option<bool>,)> =
        sqlx::query_as("SELECT is_admin FROM public.users WHERE id = $1")
            .bind(uid)
            .fetch_optional(pool)
            .await?;
    if admin_row.and_then(|(v,)| v).unwrap_or(false) {
        return Ok(true);
    }
    // Check RFC ownership
    let rfc_row: Option<(String,)> = sqlx::query_as(
        "SELECT rfc FROM pulso.users WHERE user_id = $1 AND rfc = $2",
    )
    .bind(uid)
    .bind(rfc.to_uppercase())
    .fetch_optional(pool)
    .await?;
    Ok(rfc_row.is_some())
}

/// True if user has is_admin = true in public.users.
pub async fn is_user_admin(pool: &PgPool, user_id: &str) -> Result<bool, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(Option<bool>,)> =
        sqlx::query_as("SELECT is_admin FROM public.users WHERE id = $1")
            .bind(uid)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(v,)| v).unwrap_or(false))
}

/// Credentials for a specific RFC (clave_enc, initial_sync_job_id).
pub async fn get_credentials_for_rfc(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT clave, initial_sync_job_id FROM pulso.users WHERE user_id = $1 AND rfc = $2",
    )
    .bind(uid)
    .bind(rfc.to_uppercase())
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Update CIEC password for a specific RFC. Returns true if row was found and updated.
pub async fn update_rfc_clave(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
    clave_enc: &str,
) -> Result<bool, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let result = sqlx::query(
        "UPDATE pulso.users SET clave = $1 WHERE user_id = $2 AND rfc = $3",
    )
    .bind(clave_enc)
    .bind(uid)
    .bind(rfc.to_uppercase())
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Set initial_sync_job_id for a specific RFC.
pub async fn set_initial_sync_job_for_rfc(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
    job_id: &str,
) -> Result<(), sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    sqlx::query(
        "UPDATE pulso.users SET initial_sync_job_id = $1 WHERE user_id = $2 AND rfc = $3",
    )
    .bind(job_id)
    .bind(uid)
    .bind(rfc.to_uppercase())
    .execute(pool)
    .await?;
    Ok(())
}
