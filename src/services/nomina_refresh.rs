//! PULSO_Plan_Mejoras_SQL.md, punto #6: `pulso.nomina_normalizada` (migration 086) went
//! from a plain VIEW -- recomputed in full on every one of its 31 call sites across
//! hallazgos.rs/payroll.rs, 372.9ms per call even warm -- to a MATERIALIZED VIEW with the
//! exact same name and SELECT body. Every call site keeps working unchanged; what changes
//! is that reads are now served from a snapshot instead of recomputed live.
//!
//! That snapshot needs refreshing, which is what this worker does. Tradeoff accepted
//! explicitly over a per-RFC targeted-refresh table: staleness up to REFRESH_INTERVAL_SECS
//! for anything that touches the view's inputs -- an ETL sync landing new nomina CFDIs, a
//! CFDI getting cancelled, or a payroll_normalization_rules/normalization_rules edit from
//! the UI. A user who edits an exclusion rule won't see it reflected in Nomina/Dashboard
//! until the next refresh tick, not instantly -- at 23h between ticks (chosen explicitly
//! over a faster default), that can be most of a business day.
//!
//! CEO decision (PULSO_Plan_Mejoras_SQL.md): keep the 23h background tick, but add a
//! manual "refrescar ahora" escape hatch (routes::analytics::refresh_nomina_normalizada)
//! that the frontend surfaces right after a payroll normalization rule is created,
//! edited or deleted -- exactly the moment someone needs the corrected number now, not
//! in up to 23h.

use crate::db::DbPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const REFRESH_INTERVAL_SECS: u64 = 23 * 3600;

/// Shared between the periodic worker and the manual-refresh route so only one
/// REFRESH MATERIALIZED VIEW CONCURRENTLY runs at a time -- Postgres itself would just
/// serialize a second one behind the first (same relation, can't refresh twice at once),
/// but that means a manual click landing mid-cycle would silently block for however long
/// the in-flight refresh takes instead of getting an honest "already running" response.
#[derive(Default)]
pub struct NominaRefreshState {
    in_progress: AtomicBool,
}

impl NominaRefreshState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

pub enum RefreshOutcome {
    Refreshed(Duration),
    AlreadyInProgress,
}

pub async fn worker(pool: DbPool, state: Arc<NominaRefreshState>) {
    // Refresh once at startup, not only after REFRESH_INTERVAL_SECS -- a restart is
    // exactly when the snapshot is most likely to already be stale (whatever changed
    // since the last refresh before the process stopped), same reasoning as
    // response_cache::cleanup_worker's own startup pass.
    loop {
        match refresh_guarded(&pool, &state).await {
            Ok(RefreshOutcome::Refreshed(elapsed)) => {
                tracing::info!(
                    elapsed_ms = elapsed.as_millis() as u64,
                    "nomina_refresh: REFRESH MATERIALIZED VIEW CONCURRENTLY completed"
                );
            }
            // Only a manual trigger could be holding the lock right as this tick fires --
            // next tick tries again, nothing lost.
            Ok(RefreshOutcome::AlreadyInProgress) => {
                tracing::info!(
                    "nomina_refresh: skipped scheduled tick, a refresh was already running"
                );
            }
            Err(e) => {
                tracing::error!("nomina_refresh: refresh cycle error: {e}");
            }
        }
        tokio::time::sleep(Duration::from_secs(REFRESH_INTERVAL_SECS)).await;
    }
}

/// Used by both the background worker and the manual-refresh route.
pub async fn refresh_guarded(
    pool: &DbPool,
    state: &NominaRefreshState,
) -> Result<RefreshOutcome, sqlx::Error> {
    if state
        .in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Ok(RefreshOutcome::AlreadyInProgress);
    }
    let result = refresh(pool).await;
    state.in_progress.store(false, Ordering::SeqCst);
    result.map(RefreshOutcome::Refreshed)
}

async fn refresh(pool: &DbPool) -> Result<Duration, sqlx::Error> {
    let start = std::time::Instant::now();
    // CONCURRENTLY needs nomina_normalizada_uuid_idx (migration 086) -- without it this
    // statement fails outright rather than silently locking readers.
    sqlx::query("REFRESH MATERIALIZED VIEW CONCURRENTLY pulso.nomina_normalizada")
        .execute(pool)
        .await?;
    Ok(start.elapsed())
}
