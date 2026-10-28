use sqlx::PgPool;
use uuid::Uuid;

/// Stripe price lookup keys that grant Pulso access. Adquiere's webhook writes the key to
/// `public.user_receipts.lookup_key`; plans that bundle Pulso get added here.
const PULSO_LOOKUP_KEYS: &[&str] = &["pulso_monthly"];

/// Receipts written before Adquiere's `lookup_key` column only carry the Stripe price's
/// display name, which is this text only when the price was named after the key.
const LEGACY_PULSO_NICKNAME: &str = "pulso_monthly";

#[derive(Debug, serde::Serialize)]
pub struct SubscriptionStatus {
    pub status: String,
    pub current_period_end: Option<i64>,
}

/// Returns the most recent active pulso subscription from adquiere's shared table,
/// or None if the user has no active subscription.
pub async fn get_pulso_status(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Option<SubscriptionStatus>, sqlx::Error> {
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let row: Option<(String, Option<i64>)> = sqlx::query_as(
        r#"
        SELECT status, period_end
        FROM public.user_receipts
        WHERE user_id = $1
          AND (lookup_key = ANY($3) OR nickname = $4)
          AND status NOT IN ('incomplete_expired', 'canceled', 'unpaid')
          AND (period_end IS NULL OR period_end > $2)
        ORDER BY period_end DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .bind(user_id)
    .bind(now_unix)
    .bind(PULSO_LOOKUP_KEYS)
    .bind(LEGACY_PULSO_NICKNAME)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(status, period_end)| SubscriptionStatus {
        status,
        current_period_end: period_end,
    }))
}

/// Returns true if the user has a current active pulso subscription.
pub async fn is_pulso_active(pool: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
    Ok(get_pulso_status(pool, user_id).await?.is_some())
}
