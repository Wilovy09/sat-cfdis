//! `pulso.rfc_month_reconciliation` (migration 092): per RFC x closed month x direction,
//! the SAT's own count against what Pulso holds. See `services::reconciliation` for the
//! worker that fills and acts on it.

use sqlx::PgPool;

/// One row of `pulso.rfc_month_reconciliation`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MonthRow {
    pub ym: String,
    pub direction: String,
    pub sat_count: Option<i32>,
    pub pulso_count: Option<i32>,
    pub pulso_xml_missing: Option<i32>,
    pub verified: bool,
    pub resync_attempts: i32,
    pub recount_attempts: i32,
    pub needs_review: bool,
    /// The SAT count is missing, predates the month's close (late stamping could still
    /// move it), or is due for the periodic re-check of an already verified month.
    pub count_due: bool,
}

/// An RFC the reconciliation worker looks after: active, verified owner (its CIEC has
/// passed the SAT login), with the floor year of its analysis window.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WatchedRfc {
    pub rfc: String,
    pub clave_enc: String,
    pub anio_piso: i32,
}

pub async fn watched_rfcs(pool: &PgPool) -> Result<Vec<WatchedRfc>, sqlx::Error> {
    sqlx::query_as(
        r#"SELECT pu.rfc, pu.clave AS clave_enc, pu.anio_piso
           FROM pulso.users pu
           JOIN public.users u ON u.id = pu.user_id
           WHERE pu.deleted_at IS NULL AND u.deleted_at IS NULL
             AND pu.verified_at IS NOT NULL AND pu.clave <> ''
           ORDER BY pu.rfc"#,
    )
    .fetch_all(pool)
    .await
}

/// Creates the missing rows for every month in `[from_ym, to_ym]` ("YYYY-MM"), both
/// directions, then recomputes what Pulso holds for each of this RFC's rows. A verified
/// month whose numbers no longer agree (a CFDI arrived or vanished, an XML went missing)
/// drops back to unverified.
pub async fn sync_rows(
    pool: &PgPool,
    rfc: &str,
    from_ym: &str,
    to_ym: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"INSERT INTO pulso.rfc_month_reconciliation (rfc, ym, direction)
           SELECT $1, to_char(m, 'YYYY-MM'), d.direction
           FROM generate_series(($2 || '-01')::date, ($3 || '-01')::date, interval '1 month') m
           CROSS JOIN (VALUES ('emitidos'), ('recibidos')) d(direction)
           ON CONFLICT DO NOTHING"#,
    )
    .bind(rfc)
    .bind(from_ym)
    .bind(to_ym)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"WITH held AS (
               SELECT left(c.fecha_emision, 7) AS ym, 'emitidos' AS direction,
                      COUNT(*)::int AS n,
                      COUNT(*) FILTER (WHERE c.xml_available IS DISTINCT FROM 1)::int AS missing
               FROM pulso.cfdis_raw c WHERE c.rfc_emisor = $1
               GROUP BY 1
               UNION ALL
               SELECT left(c.fecha_emision, 7), 'recibidos',
                      COUNT(*)::int,
                      COUNT(*) FILTER (WHERE c.xml_available IS DISTINCT FROM 1)::int
               FROM pulso.cfdis_raw c WHERE c.rfc_receptor = $1
               GROUP BY 1
           )
           UPDATE pulso.rfc_month_reconciliation r
           SET pulso_count = COALESCE(h.n, 0),
               pulso_xml_missing = COALESCE(h.missing, 0),
               verified_at = CASE
                   WHEN r.verified_at IS NOT NULL
                        AND (r.sat_count IS DISTINCT FROM COALESCE(h.n, 0)
                             OR COALESCE(h.missing, 0) > 0)
                   THEN NULL ELSE r.verified_at END,
               updated_at = now()
           FROM pulso.rfc_month_reconciliation r2
           LEFT JOIN held h ON h.ym = r2.ym AND h.direction = r2.direction
           WHERE r.rfc = $1 AND r2.rfc = r.rfc AND r2.ym = r.ym AND r2.direction = r.direction"#,
    )
    .bind(rfc)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

/// This RFC's rows within `[from_ym, to_ym]`, oldest first. `close_days` is the grace
/// after a month ends before its SAT count is final; `recheck_days` the age of a verified
/// month's count before it is checked again.
pub async fn rows(
    pool: &PgPool,
    rfc: &str,
    from_ym: &str,
    to_ym: &str,
    close_days: i32,
    recheck_days: i32,
) -> Result<Vec<MonthRow>, sqlx::Error> {
    sqlx::query_as(
        r#"SELECT ym, direction, sat_count, pulso_count, pulso_xml_missing,
                  verified_at IS NOT NULL AS verified, resync_attempts, recount_attempts,
                  needs_review,
                  (sat_count IS NULL
                   OR sat_counted_at < (ym || '-01')::date + interval '1 month' + make_interval(days => $4)
                   OR (verified_at IS NOT NULL
                       AND sat_counted_at < now() - make_interval(days => $5))) AS count_due
           FROM pulso.rfc_month_reconciliation
           WHERE rfc = $1 AND ym BETWEEN $2 AND $3
           ORDER BY ym, direction"#,
    )
    .bind(rfc)
    .bind(from_ym)
    .bind(to_ym)
    .bind(close_days)
    .bind(recheck_days)
    .fetch_all(pool)
    .await
}

/// Stores a SAT count for one month x direction. `error_days > 0` means some days went
/// unanswered: the number is not stored -- only the attempt -- and any earlier good count
/// stays, so a flaky session can't undo a verified month.
pub async fn record_count(
    pool: &PgPool,
    rfc: &str,
    ym: &str,
    direction: &str,
    sat_count: i64,
    error_days: i64,
) -> Result<(), sqlx::Error> {
    let trusted = error_days == 0;
    sqlx::query(
        r#"INSERT INTO pulso.rfc_month_reconciliation
               (rfc, ym, direction, sat_count, count_error_days, sat_counted_at, recount_attempts)
           VALUES ($1, $2, $3, CASE WHEN $6 THEN $4::int END, $5::int,
                   CASE WHEN $6 THEN now() END, CASE WHEN $6 THEN 0 ELSE 1 END)
           ON CONFLICT (rfc, ym, direction) DO UPDATE SET
               sat_count = CASE WHEN $6 THEN $4::int
                                ELSE pulso.rfc_month_reconciliation.sat_count END,
               count_error_days = $5::int,
               sat_counted_at = CASE WHEN $6 THEN now()
                                     ELSE pulso.rfc_month_reconciliation.sat_counted_at END,
               recount_attempts = pulso.rfc_month_reconciliation.recount_attempts
                                  + CASE WHEN $6 THEN 0 ELSE 1 END,
               updated_at = now()"#,
    )
    .bind(rfc)
    .bind(ym)
    .bind(direction)
    .bind(sat_count)
    .bind(error_days)
    .bind(trusted)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_verified(
    pool: &PgPool,
    rfc: &str,
    ym: &str,
    direction: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.rfc_month_reconciliation
           SET verified_at = now(), resync_attempts = 0, recount_attempts = 0,
               needs_review = false, updated_at = now()
           WHERE rfc = $1 AND ym = $2 AND direction = $3"#,
    )
    .bind(rfc)
    .bind(ym)
    .bind(direction)
    .execute(pool)
    .await?;
    Ok(())
}

/// Pulso holds fewer CFDIs than the SAT lists: a targeted job was queued for the month,
/// and the count is cleared so the month is counted again once that job's data is in.
pub async fn mark_resync_queued(
    pool: &PgPool,
    rfc: &str,
    ym: &str,
    direction: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.rfc_month_reconciliation
           SET resync_attempts = resync_attempts + 1, sat_count = NULL, updated_at = now()
           WHERE rfc = $1 AND ym = $2 AND direction = $3"#,
    )
    .bind(rfc)
    .bind(ym)
    .bind(direction)
    .execute(pool)
    .await?;
    Ok(())
}

/// Pulso holds more than the SAT counted: count again before believing either side.
pub async fn mark_recount(
    pool: &PgPool,
    rfc: &str,
    ym: &str,
    direction: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.rfc_month_reconciliation
           SET recount_attempts = recount_attempts + 1, sat_count = NULL, updated_at = now()
           WHERE rfc = $1 AND ym = $2 AND direction = $3"#,
    )
    .bind(rfc)
    .bind(ym)
    .bind(direction)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_needs_review(
    pool: &PgPool,
    rfc: &str,
    ym: &str,
    direction: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE pulso.rfc_month_reconciliation
           SET needs_review = true, updated_at = now()
           WHERE rfc = $1 AND ym = $2 AND direction = $3"#,
    )
    .bind(rfc)
    .bind(ym)
    .bind(direction)
    .execute(pool)
    .await?;
    Ok(())
}

/// When this RFC's most recent job last changed -- the reconciliation waits a while after
/// it so the ETL has loaded that job's CFDIs before anything is compared.
pub async fn last_job_activity(pool: &PgPool, rfc: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(r#"SELECT MAX(updated_at) FROM pulso.sync_jobs WHERE rfc = $1"#)
        .bind(rfc)
        .fetch_one(pool)
        .await
}
