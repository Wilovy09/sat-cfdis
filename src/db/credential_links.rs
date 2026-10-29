//! `pulso.credential_requests` (migration 095): links a Pulso user hands to a third party
//! (accountant, admin staff) so they capture the SAT credentials of one RFC. See
//! `routes::credential_links`.

use sqlx::PgPool;
use uuid::Uuid;

/// How long a link stays usable when nobody validates credentials through it.
const LINK_LIFETIME_DAYS: i32 = 7;
/// Rejected attempts in a row before the link pauses, so it can't be used to try passwords.
pub const MAX_FAILED_ATTEMPTS: i32 = 5;
const LOCKOUT_MINUTES: i32 = 60;
/// How long one submission holds the link while the SAT answers; also keeps two submissions
/// from running at once.
const IN_FLIGHT_MINUTES: i32 = 3;

const ISO: &str = r#"'YYYY-MM-DD"T"HH24:MI:SS"Z"'"#;

/// An open link as its requester sees it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RequesterLink {
    pub rfc: String,
    pub token_enc: String,
    /// ISO-8601 UTC.
    pub expires_at: String,
    /// Credentials were validated but the RFC can't join this account yet (another account
    /// already has it); see `routes::credential_links` for why that stays invisible.
    pub validated: bool,
}

/// What the public page needs, looked up by the token's hash.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PublicLink {
    pub id: Uuid,
    pub rfc: String,
    pub requester_user_id: Uuid,
    pub expires_at: String,
    /// Not validated and not expired.
    pub open: bool,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Requester {
    pub name: Option<String>,
    pub company: Option<String>,
}

/// The requester's open link for this RFC, if any: while it lasts the same link is shown
/// again instead of a new one.
pub async fn open_for(
    pool: &PgPool,
    requester: Uuid,
    rfc: &str,
) -> Result<Option<RequesterLink>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT rfc, token_enc, to_char(expires_at AT TIME ZONE 'UTC', {ISO}) AS expires_at,
                false AS validated
         FROM pulso.credential_requests
         WHERE requester_user_id = $1 AND rfc = $2
           AND validated_at IS NULL AND expires_at > now()
         ORDER BY expires_at DESC
         LIMIT 1"
    ))
    .bind(requester)
    .bind(rfc)
    .fetch_optional(pool)
    .await
}

pub async fn create(
    pool: &PgPool,
    requester: Uuid,
    rfc: &str,
    token_hash: &str,
    token_enc: &str,
) -> Result<RequesterLink, sqlx::Error> {
    sqlx::query_as(&format!(
        "INSERT INTO pulso.credential_requests
             (id, token_hash, token_enc, rfc, requester_user_id, expires_at)
         VALUES ($1, $2, $3, $4, $5, now() + make_interval(days => $6))
         RETURNING rfc, token_enc, to_char(expires_at AT TIME ZONE 'UTC', {ISO}) AS expires_at,
                   false AS validated"
    ))
    .bind(Uuid::new_v4())
    .bind(token_hash)
    .bind(token_enc)
    .bind(rfc)
    .bind(requester)
    .bind(LINK_LIFETIME_DAYS)
    .fetch_one(pool)
    .await
}

/// The requester's links still worth showing: open ones ("Esperando credenciales") and
/// validated ones whose RFC hasn't joined the account.
pub async fn list_for_requester(
    pool: &PgPool,
    requester: Uuid,
) -> Result<Vec<RequesterLink>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT DISTINCT ON (rfc) rfc, token_enc,
                to_char(expires_at AT TIME ZONE 'UTC', {ISO}) AS expires_at,
                validated_at IS NOT NULL AS validated
         FROM pulso.credential_requests
         WHERE requester_user_id = $1
           AND ((validated_at IS NULL AND expires_at > now())
                OR pending_clave_enc IS NOT NULL)
         ORDER BY rfc, created_at DESC"
    ))
    .bind(requester)
    .fetch_all(pool)
    .await
}

/// Open links for RFCs this user doesn't own yet: each counts against the per-account RFC
/// limit, as the RFC it will become.
pub async fn count_open_for_other_rfcs(
    pool: &PgPool,
    requester: Uuid,
    rfc: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COUNT(DISTINCT rfc) FROM pulso.credential_requests
         WHERE requester_user_id = $1 AND rfc <> $2
           AND ((validated_at IS NULL AND expires_at > now()) OR pending_clave_enc IS NOT NULL)",
    )
    .bind(requester)
    .bind(rfc)
    .fetch_one(pool)
    .await
}

pub async fn find_by_token_hash(
    pool: &PgPool,
    token_hash: &str,
) -> Result<Option<PublicLink>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT id, rfc, requester_user_id,
                to_char(expires_at AT TIME ZONE 'UTC', {ISO}) AS expires_at,
                validated_at IS NULL AND expires_at > now() AS open
         FROM pulso.credential_requests
         WHERE token_hash = $1"
    ))
    .bind(token_hash)
    .fetch_optional(pool)
    .await
}

/// Takes the link for one submission. `false` when it's closed, paused, or another
/// submission holds it.
pub async fn claim(pool: &PgPool, id: Uuid) -> Result<bool, sqlx::Error> {
    let claimed = sqlx::query(
        "UPDATE pulso.credential_requests
         SET locked_until = now() + make_interval(mins => $2)
         WHERE id = $1 AND validated_at IS NULL AND expires_at > now()
           AND (locked_until IS NULL OR locked_until <= now())",
    )
    .bind(id)
    .bind(IN_FLIGHT_MINUTES)
    .execute(pool)
    .await?;
    Ok(claimed.rows_affected() == 1)
}

/// The SAT gave no verdict: the link is free again, nothing counted.
pub async fn release(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE pulso.credential_requests SET locked_until = NULL WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// The SAT rejected them. Every `MAX_FAILED_ATTEMPTS`-th rejection pauses the link; a
/// rejection never closes it.
pub async fn record_rejection(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE pulso.credential_requests
         SET failed_attempts = failed_attempts + 1,
             locked_until = CASE WHEN (failed_attempts + 1) % $2 = 0
                                 THEN now() + make_interval(mins => $3) END
         WHERE id = $1",
    )
    .bind(id)
    .bind(MAX_FAILED_ATTEMPTS)
    .bind(LOCKOUT_MINUTES)
    .execute(pool)
    .await?;
    Ok(())
}

/// Closes the link. `pending_credential_enc` holds the credential only when it couldn't
/// be attached to the requester's account yet.
pub async fn mark_validated(
    pool: &PgPool,
    id: Uuid,
    credential_type: &str,
    contact_role: &str,
    pending_credential_enc: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE pulso.credential_requests
         SET validated_at = now(), locked_until = NULL, credential_type = $2,
             contact_role = $3, pending_clave_enc = $4
         WHERE id = $1",
    )
    .bind(id)
    .bind(credential_type)
    .bind(contact_role)
    .bind(pending_credential_enc)
    .execute(pool)
    .await?;
    Ok(())
}

/// Name and, when the profile has one, company of whoever asked for the credentials --
/// the only account details the public page shows.
pub async fn requester(pool: &PgPool, user_id: Uuid) -> Result<Requester, sqlx::Error> {
    sqlx::query_as(
        "SELECT u.name,
                COALESCE(
                    (SELECT c.name FROM public.companies c WHERE c.id = u.last_active_company_id),
                    (SELECT c.name FROM public.companies c WHERE c.user_id = u.id LIMIT 1)
                ) AS company
         FROM public.users u
         WHERE u.id = $1",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await
    .map(|row| {
        row.unwrap_or(Requester {
            name: None,
            company: None,
        })
    })
}
