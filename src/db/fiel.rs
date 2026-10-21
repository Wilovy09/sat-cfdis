use sqlx::PgPool;

pub struct FielRow {
    pub rfc: String,
    pub cert_s3_key: String,
    pub key_s3_key: String,
    pub password_enc: String,
    pub uploaded_at: String,
}

pub async fn get(pool: &PgPool, rfc: &str) -> Result<Option<FielRow>, sqlx::Error> {
    let row: Option<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT rfc, cert_s3_key, key_s3_key, password_enc, uploaded_at::text \
         FROM pulso.rfc_fiel WHERE rfc = $1",
    )
    .bind(rfc.to_uppercase())
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(rfc, cert_s3_key, key_s3_key, password_enc, uploaded_at)| FielRow {
            rfc,
            cert_s3_key,
            key_s3_key,
            password_enc,
            uploaded_at,
        },
    ))
}

pub async fn upsert(
    pool: &PgPool,
    rfc: &str,
    cert_s3_key: &str,
    key_s3_key: &str,
    password_enc: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO pulso.rfc_fiel (rfc, cert_s3_key, key_s3_key, password_enc) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (rfc) DO UPDATE SET \
             cert_s3_key  = EXCLUDED.cert_s3_key, \
             key_s3_key   = EXCLUDED.key_s3_key, \
             password_enc = EXCLUDED.password_enc, \
             uploaded_at  = NOW()",
    )
    .bind(rfc.to_uppercase())
    .bind(cert_s3_key)
    .bind(key_s3_key)
    .bind(password_enc)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete(pool: &PgPool, rfc: &str) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM pulso.rfc_fiel WHERE rfc = $1")
        .bind(rfc.to_uppercase())
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// L19-03: every stored FIEL whose RFC has no active owner -- no live `pulso.users` row
/// whose Adquiere account (`public.users`) is itself still live.
pub async fn list_orphaned(pool: &PgPool) -> Result<Vec<FielRow>, sqlx::Error> {
    let rows: Vec<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT f.rfc, f.cert_s3_key, f.key_s3_key, f.password_enc, f.uploaded_at::text \
         FROM pulso.rfc_fiel f \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM pulso.users pu JOIN public.users u ON u.id = pu.user_id \
             WHERE pu.rfc = f.rfc AND pu.deleted_at IS NULL AND u.deleted_at IS NULL)",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(rfc, cert_s3_key, key_s3_key, password_enc, uploaded_at)| FielRow {
                rfc,
                cert_s3_key,
                key_s3_key,
                password_enc,
                uploaded_at,
            },
        )
        .collect())
}
