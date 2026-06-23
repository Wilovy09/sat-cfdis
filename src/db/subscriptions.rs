use sqlx::PgPool;
use uuid::Uuid;

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
          AND nickname = 'pulso_monthly'
          AND status NOT IN ('incomplete_expired', 'canceled', 'unpaid')
          AND (period_end IS NULL OR period_end > $2)
        ORDER BY period_end DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .bind(user_id)
    .bind(now_unix)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(status, period_end)| SubscriptionStatus {
        status,
        current_period_end: period_end,
    }))
}

/// Returns true if the user has a current active pulso subscription.
pub async fn is_pulso_active(pool: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM public.user_receipts
        WHERE user_id = $1
          AND nickname = 'pulso_monthly'
          AND status NOT IN ('incomplete_expired', 'canceled', 'unpaid')
          AND (period_end IS NULL OR period_end > $2)
        "#,
    )
    .bind(user_id)
    .bind(now_unix)
    .fetch_one(pool)
    .await?;

    Ok(count > 0)
}
