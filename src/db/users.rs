use sqlx::PgPool;
use uuid::Uuid;

fn parse_uuid(user_id: &str) -> Result<Uuid, sqlx::Error> {
    Uuid::parse_str(user_id).map_err(|e| sqlx::Error::Decode(Box::new(e)))
}

// ── RFC sharing ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum CreateUserError {
    AlreadyOwnedBySelf,
    AlreadyOwnedByOther,
    Db(sqlx::Error),
}

impl From<sqlx::Error> for CreateUserError {
    fn from(e: sqlx::Error) -> Self {
        CreateUserError::Db(e)
    }
}

impl std::fmt::Display for CreateUserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyOwnedBySelf => write!(f, "RFC already registered for this user"),
            Self::AlreadyOwnedByOther => write!(f, "RFC already registered by another user"),
            Self::Db(e) => write!(f, "DB error: {e}"),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct RfcShare {
    pub id: String,
    pub rfc: String,
    pub shared_with: String,
    pub invited_email: Option<String>,
    pub granted_at: String,
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

/// Register an RFC for a user.
/// Returns `Ok(true)` if newly added (or restored from soft-delete).
/// Returns `Err(CreateUserError::AlreadyOwnedBySelf)` if this user already owns it.
/// Returns `Err(CreateUserError::AlreadyOwnedByOther)` if another user owns it globally.
pub async fn create_pulso_user(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
    clave_enc: &str,
    initial_sync_job_id: Option<&str>,
) -> Result<bool, CreateUserError> {
    let uid = parse_uuid(user_id)?;
    let rfc_upper = rfc.to_uppercase();

    // Already active for THIS user?
    let owned_by_self: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pulso.users WHERE user_id = $1 AND rfc = $2 AND deleted_at IS NULL",
    )
    .bind(uid)
    .bind(&rfc_upper)
    .fetch_one(pool)
    .await?;
    if owned_by_self > 0 {
        return Err(CreateUserError::AlreadyOwnedBySelf);
    }

    // Owned by a DIFFERENT user?
    let owned_by_other: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pulso.users WHERE rfc = $1 AND user_id <> $2 AND deleted_at IS NULL",
    )
    .bind(&rfc_upper)
    .bind(uid)
    .fetch_one(pool)
    .await?;
    if owned_by_other > 0 {
        return Err(CreateUserError::AlreadyOwnedByOther);
    }

    // Restore a previously soft-deleted row if one exists.
    let restored = sqlx::query(
        r#"UPDATE pulso.users
           SET deleted_at = NULL, clave = $1, initial_sync_job_id = $2
           WHERE user_id = $3 AND rfc = $4 AND deleted_at IS NOT NULL"#,
    )
    .bind(clave_enc)
    .bind(initial_sync_job_id)
    .bind(uid)
    .bind(&rfc_upper)
    .execute(pool)
    .await?
    .rows_affected();
    if restored > 0 {
        return Ok(true);
    }

    // Fresh insert.
    let id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"INSERT INTO pulso.users (id, user_id, rfc, clave, initial_sync_job_id, deleted_at)
           VALUES ($1, $2, $3, $4, $5, NULL)"#,
    )
    .bind(id)
    .bind(uid)
    .bind(&rfc_upper)
    .bind(clave_enc)
    .bind(initial_sync_job_id)
    .execute(pool)
    .await?;
    Ok(true)
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
/// Returns the first active RFC (by ctid) for backward compat with multi-RFC users.
pub async fn get_user_credentials(
    pool: &PgPool,
    user_id: &str,
) -> Result<Option<(String, String, Option<String>)>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT rfc, clave, initial_sync_job_id FROM pulso.users WHERE user_id = $1 AND deleted_at IS NULL ORDER BY ctid LIMIT 1",
    )
    .bind(uid)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Returns (rfc, clave_enc) for every active user — used by the monthly sync worker.
pub async fn get_all_with_credentials(pool: &PgPool) -> Result<Vec<(String, String)>, sqlx::Error> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT rfc, clave FROM pulso.users WHERE deleted_at IS NULL")
            .fetch_all(pool)
            .await?;
    Ok(rows)
}

/// Returns encrypted clave for a given RFC regardless of owner — admin use only.
pub async fn get_clave_for_rfc(pool: &PgPool, rfc: &str) -> Result<Option<String>, sqlx::Error> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT clave FROM pulso.users WHERE rfc = $1 AND deleted_at IS NULL LIMIT 1",
    )
    .bind(rfc)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(c,)| c))
}

/// Returns (rfc, initial_sync_job_id) for the given user.
/// Returns the first active RFC (by ctid) for backward compat with multi-RFC users.
pub async fn get_user_sync_info(
    pool: &PgPool,
    user_id: &str,
) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT rfc, initial_sync_job_id FROM pulso.users WHERE user_id = $1 AND deleted_at IS NULL ORDER BY ctid LIMIT 1",
    )
    .bind(uid)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// All active RFCs belonging to a user (ordered by creation time).
pub async fn get_user_rfcs(pool: &PgPool, user_id: &str) -> Result<Vec<String>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT rfc FROM pulso.users WHERE user_id = $1 AND deleted_at IS NULL ORDER BY ctid",
    )
    .bind(uid)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(rfc,)| rfc).collect())
}

pub async fn get_user_rfcs_with_nombre(
    pool: &PgPool,
    user_id: &str,
) -> Result<Vec<(String, Option<String>)>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        r#"SELECT u.rfc,
                  (SELECT c.nombre_emisor
                   FROM pulso.cfdis c
                   WHERE c.rfc_emisor = u.rfc AND c.nombre_emisor IS NOT NULL
                   ORDER BY c.created_at DESC LIMIT 1) AS nombre
           FROM pulso.users u
           WHERE u.user_id = $1 AND u.deleted_at IS NULL
           ORDER BY u.ctid"#,
    )
    .bind(uid)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Returns (rfc, nombre, role) for all RFCs a user can access.
/// role = "owner" | "viewer"
/// Includes both owned RFCs and those shared with the user.
pub async fn get_user_rfcs_with_role(
    pool: &PgPool,
    user_id: &str,
) -> Result<Vec<(String, Option<String>, String)>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let rows: Vec<(String, Option<String>, String)> = sqlx::query_as(
        r#"SELECT rfc, nombre, role FROM (
            SELECT u.rfc,
                   (SELECT c.nombre_emisor FROM pulso.cfdis c
                    WHERE c.rfc_emisor = u.rfc AND c.nombre_emisor IS NOT NULL
                    ORDER BY c.created_at DESC LIMIT 1) AS nombre,
                   'owner'::text AS role,
                   u.ctid AS ord
            FROM pulso.users u
            WHERE u.user_id = $1 AND u.deleted_at IS NULL
            UNION ALL
            SELECT s.rfc,
                   (SELECT c.nombre_emisor FROM pulso.cfdis c
                    WHERE c.rfc_emisor = s.rfc AND c.nombre_emisor IS NOT NULL
                    ORDER BY c.created_at DESC LIMIT 1) AS nombre,
                   'viewer'::text AS role,
                   s.ctid AS ord
            FROM pulso.rfc_shares s
            WHERE s.shared_with = $1 AND s.revoked_at IS NULL
        ) combined
        ORDER BY role DESC, ord"#,
    )
    .bind(uid)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// True if user has the 'admin' role via public.user_roles → catalogs.roles.
pub async fn count_user_rfcs(pool: &PgPool, user_id: &str) -> Result<i64, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM pulso.users WHERE user_id = $1 AND deleted_at IS NULL",
    )
    .bind(uid)
    .fetch_one(pool)
    .await
}

pub async fn is_user_admin(pool: &PgPool, user_id: &str) -> Result<bool, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(i32,)> = sqlx::query_as(
        r#"
        SELECT 1
        FROM public.user_roles ur
        JOIN catalogs.roles r ON r.id = ur.role_id
        WHERE ur.user_id = $1 AND r.name = 'admin'
        LIMIT 1
        "#,
    )
    .bind(uid)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// True if user owns this active RFC, has a valid share grant, OR has the 'admin' role.
pub async fn user_has_rfc_or_admin(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
) -> Result<bool, sqlx::Error> {
    if is_user_admin(pool, user_id).await? {
        return Ok(true);
    }
    let uid = parse_uuid(user_id)?;
    let rfc_upper = rfc.to_uppercase();
    // Owner check
    let owns: Option<(String,)> = sqlx::query_as(
        "SELECT rfc FROM pulso.users WHERE user_id = $1 AND rfc = $2 AND deleted_at IS NULL",
    )
    .bind(uid)
    .bind(&rfc_upper)
    .fetch_optional(pool)
    .await?;
    if owns.is_some() {
        return Ok(true);
    }
    // Share check
    let shared: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM pulso.rfc_shares WHERE rfc = $1 AND shared_with = $2 AND revoked_at IS NULL LIMIT 1",
    )
    .bind(&rfc_upper)
    .bind(uid)
    .fetch_optional(pool)
    .await?;
    Ok(shared.is_some())
}

/// True if user is the owner (not just a shared viewer) of this RFC.
pub async fn user_owns_rfc(pool: &PgPool, user_id: &str, rfc: &str) -> Result<bool, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT rfc FROM pulso.users WHERE user_id = $1 AND rfc = $2 AND deleted_at IS NULL",
    )
    .bind(uid)
    .bind(rfc.to_uppercase())
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// True if user owns this RFC OR has the admin role (for owner-only mutating ops).
pub async fn user_owns_rfc_or_admin(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
) -> Result<bool, sqlx::Error> {
    if is_user_admin(pool, user_id).await? {
        return Ok(true);
    }
    user_owns_rfc(pool, user_id, rfc).await
}

/// Grant a viewer their own `pulso.users` row for the shared RFC (no clave, no sync job).
/// Idempotent: restores soft-deleted row or skips if already active.
pub async fn add_viewer_rfc(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
) -> Result<(), sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let rfc_upper = rfc.to_uppercase();

    // Restore previously soft-deleted viewer row if exists.
    let restored = sqlx::query(
        "UPDATE pulso.users SET deleted_at = NULL WHERE user_id = $1 AND rfc = $2 AND deleted_at IS NOT NULL",
    )
    .bind(uid)
    .bind(&rfc_upper)
    .execute(pool)
    .await?
    .rows_affected();
    if restored > 0 {
        return Ok(());
    }

    // Already active row (owner or prior viewer) — nothing to do.
    let exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pulso.users WHERE user_id = $1 AND rfc = $2 AND deleted_at IS NULL",
    )
    .bind(uid)
    .bind(&rfc_upper)
    .fetch_one(pool)
    .await?;
    if exists > 0 {
        return Ok(());
    }

    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO pulso.users (id, user_id, rfc, clave, initial_sync_job_id, deleted_at) VALUES ($1, $2, $3, '', NULL, NULL)",
    )
    .bind(id)
    .bind(uid)
    .bind(&rfc_upper)
    .execute(pool)
    .await?;
    Ok(())
}

// ── Sharing CRUD ─────────────────────────────────────────────────────────────

pub async fn create_rfc_share(
    pool: &PgPool,
    rfc: &str,
    owner_id: &str,
    shared_with_id: &str,
    invited_email: Option<&str>,
) -> Result<String, sqlx::Error> {
    let owner_uid = parse_uuid(owner_id)?;
    let shared_uid = parse_uuid(shared_with_id)?;
    let rfc_upper = rfc.to_uppercase();
    let share_id: (Uuid,) = sqlx::query_as(
        r#"INSERT INTO pulso.rfc_shares (rfc, owner_id, shared_with, invited_email, granted_at)
           VALUES ($1, $2, $3, $4, NOW())
           ON CONFLICT (rfc, shared_with) WHERE revoked_at IS NULL DO NOTHING
           RETURNING id"#,
    )
    .bind(&rfc_upper)
    .bind(owner_uid)
    .bind(shared_uid)
    .bind(invited_email)
    .fetch_one(pool)
    .await?;
    Ok(share_id.0.to_string())
}

pub async fn list_rfc_shares(
    pool: &PgPool,
    rfc: &str,
) -> Result<Vec<RfcShare>, sqlx::Error> {
    let rfc_upper = rfc.to_uppercase();
    // Fetch granted_at as ISO-8601 string directly from Postgres
    let rows: Vec<(Uuid, String, Uuid, Option<String>, String)> = sqlx::query_as(
        r#"SELECT s.id, s.rfc, s.shared_with, s.invited_email,
                  to_char(s.granted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS granted_at
           FROM pulso.rfc_shares s
           WHERE s.rfc = $1 AND s.revoked_at IS NULL
           ORDER BY s.granted_at"#,
    )
    .bind(&rfc_upper)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, rfc, shared_with, invited_email, granted_at)| RfcShare {
            id: id.to_string(),
            rfc,
            shared_with: shared_with.to_string(),
            invited_email,
            granted_at,
        })
        .collect())
}

/// Revoke a share. Returns true if found and revoked.
pub async fn revoke_rfc_share(
    pool: &PgPool,
    share_id: &str,
    owner_id: &str,
) -> Result<bool, sqlx::Error> {
    let sid = parse_uuid(share_id)?;
    let owner_uid = parse_uuid(owner_id)?;
    let result = sqlx::query(
        "UPDATE pulso.rfc_shares SET revoked_at = NOW() WHERE id = $1 AND owner_id = $2 AND revoked_at IS NULL",
    )
    .bind(sid)
    .bind(owner_uid)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Revoke all active shares for an RFC (called when owner deletes the RFC).
pub async fn revoke_all_shares_for_rfc(pool: &PgPool, rfc: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE pulso.rfc_shares SET revoked_at = NOW() WHERE rfc = $1 AND revoked_at IS NULL",
    )
    .bind(rfc.to_uppercase())
    .execute(pool)
    .await?;
    Ok(())
}

/// Find a public.users row by email. Returns (id, email).
pub async fn find_user_by_email_for_share(
    pool: &PgPool,
    email: &str,
) -> Result<Option<(String, String)>, sqlx::Error> {
    let row: Option<(Uuid, String)> = sqlx::query_as(
        "SELECT id, email FROM public.users WHERE email = $1 AND deleted_at IS NULL LIMIT 1",
    )
    .bind(email)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id, email)| (id.to_string(), email)))
}

pub async fn get_email_by_user_id(pool: &PgPool, user_id: &str) -> Result<Option<String>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT email FROM public.users WHERE id = $1 AND deleted_at IS NULL LIMIT 1",
    )
    .bind(uid)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(e,)| e))
}

/// Credentials for a specific active RFC (clave_enc, initial_sync_job_id).
pub async fn get_credentials_for_rfc(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT clave, initial_sync_job_id FROM pulso.users WHERE user_id = $1 AND rfc = $2 AND deleted_at IS NULL",
    )
    .bind(uid)
    .bind(rfc.to_uppercase())
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Update CIEC password for a specific active RFC. Returns true if row was found and updated.
pub async fn update_rfc_clave(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
    clave_enc: &str,
) -> Result<bool, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let result = sqlx::query(
        "UPDATE pulso.users SET clave = $1 WHERE user_id = $2 AND rfc = $3 AND deleted_at IS NULL",
    )
    .bind(clave_enc)
    .bind(uid)
    .bind(rfc.to_uppercase())
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Get the email address of the user who owns the given RFC (active rows only).
pub async fn get_email_by_rfc(pool: &PgPool, rfc: &str) -> Result<Option<String>, sqlx::Error> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT u.email
         FROM pulso.users pu
         JOIN public.users u ON u.id = pu.user_id
         WHERE pu.rfc = $1 AND pu.deleted_at IS NULL
         LIMIT 1",
    )
    .bind(rfc.to_uppercase())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(email,)| email))
}

/// Find a public user by google_id. Returns (id, email, name).
pub async fn find_by_google_id(
    pool: &PgPool,
    google_id: &str,
) -> Result<Option<(String, String, String)>, sqlx::Error> {
    let row: Option<(uuid::Uuid, String, Option<String>)> = sqlx::query_as(
        "SELECT id, email, name FROM public.users WHERE google_id = $1 LIMIT 1",
    )
    .bind(google_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id, email, name)| (id.to_string(), email, name.unwrap_or_default())))
}

/// Find a public user by email. Returns (id, email, name).
pub async fn find_by_email(
    pool: &PgPool,
    email: &str,
) -> Result<Option<(String, String, String)>, sqlx::Error> {
    let row: Option<(uuid::Uuid, String, Option<String>)> = sqlx::query_as(
        "SELECT id, email, name FROM public.users WHERE email = $1 AND deleted_at IS NULL LIMIT 1",
    )
    .bind(email)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id, email, name)| (id.to_string(), email, name.unwrap_or_default())))
}

/// Set google_id on a user (used to link Google on first OAuth login).
pub async fn set_google_id(
    pool: &PgPool,
    user_id: &str,
    google_id: &str,
) -> Result<(), sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    sqlx::query("UPDATE public.users SET google_id = $1 WHERE id = $2")
        .bind(google_id)
        .bind(uid)
        .execute(pool)
        .await?;
    Ok(())
}

/// Returns true if the given user_id has a non-null google_id.
pub async fn find_by_google_id_linked(
    pool: &PgPool,
    user_id: &str,
) -> Result<bool, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT google_id FROM public.users WHERE id = $1 LIMIT 1")
            .bind(uid)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(g,)| g).is_some())
}

/// Clear google_id from a user (unlink).
pub async fn clear_google_id(pool: &PgPool, user_id: &str) -> Result<(), sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    sqlx::query("UPDATE public.users SET google_id = NULL WHERE id = $1")
        .bind(uid)
        .execute(pool)
        .await?;
    Ok(())
}

/// Find google_id owner (for conflict checks). Returns user_id string if found.
pub async fn find_user_id_by_google_id(
    pool: &PgPool,
    google_id: &str,
) -> Result<Option<String>, sqlx::Error> {
    let row: Option<(uuid::Uuid,)> =
        sqlx::query_as("SELECT id FROM public.users WHERE google_id = $1 LIMIT 1")
            .bind(google_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(id,)| id.to_string()))
}

/// Set initial_sync_job_id for a specific active RFC.
pub async fn set_initial_sync_job_for_rfc(
    pool: &PgPool,
    user_id: &str,
    rfc: &str,
    job_id: &str,
) -> Result<(), sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    sqlx::query(
        "UPDATE pulso.users SET initial_sync_job_id = $1 WHERE user_id = $2 AND rfc = $3 AND deleted_at IS NULL",
    )
    .bind(job_id)
    .bind(uid)
    .bind(rfc.to_uppercase())
    .execute(pool)
    .await?;
    Ok(())
}

/// Soft-delete an RFC. Returns true if a row was marked deleted.
pub async fn delete_user_rfc(pool: &PgPool, user_id: &str, rfc: &str) -> Result<bool, sqlx::Error> {
    let uid = parse_uuid(user_id)?;
    let result = sqlx::query(
        "UPDATE pulso.users SET deleted_at = NOW() WHERE user_id = $1 AND rfc = $2 AND deleted_at IS NULL",
    )
    .bind(uid)
    .bind(rfc.to_uppercase())
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}
