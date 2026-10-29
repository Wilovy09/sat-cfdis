//! One RFC, several accounts, one analysis each (migration 096). The CFDIs are the RFC's;
//! normalization rules, the normalized payroll and cached results belong to an analysis,
//! keyed by `pulso.users.analysis_key`: the RFC itself for the account that had it first,
//! `RFC#xxxxxxxx` for each other one.
//!
//! The database picks the analysis from the session variable `pulso.analysis`
//! (`pulso.analysis_key(rfc)`), so every analytics query keeps comparing against the data
//! RFC. A request working on a non-first analysis runs on a pool whose connections set that
//! variable when they open; everything else -- workers, ETL, the first account -- uses the
//! main pool, where the variable is unset and the views behave exactly as before.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

use super::DbPool;

/// Few analyses are ever open at once, each by one person.
const ANALYSIS_POOL_SIZE: u32 = 3;
const ANALYSIS_POOL_IDLE: Duration = Duration::from_secs(300);

/// Whether an analysis key belongs to a co-owner (`RFC#...`) rather than the RFC's own.
#[must_use]
pub fn is_co_owner_key(analysis_key: &str) -> bool {
    analysis_key.contains('#')
}

/// The pool to run a request's analytics on. The RFC's own analysis is the main pool
/// itself; a co-owner's gets (and reuses) one that sets `pulso.analysis` on every
/// connection, built lazily from the main pool's own connection settings.
pub fn pool_for(main: &DbPool, analysis_key: &str) -> DbPool {
    if !is_co_owner_key(analysis_key) {
        return main.clone();
    }
    static POOLS: OnceLock<Mutex<HashMap<String, DbPool>>> = OnceLock::new();
    // A poisoned lock only means another request panicked mid-insert; the map is still a
    // valid map of pools.
    let mut pools = POOLS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    pools
        .entry(analysis_key.to_string())
        .or_insert_with(|| build(main, analysis_key))
        .clone()
}

fn build(main: &DbPool, analysis_key: &str) -> DbPool {
    let key = analysis_key.to_string();
    PgPoolOptions::new()
        .max_connections(ANALYSIS_POOL_SIZE)
        .min_connections(0)
        .idle_timeout(ANALYSIS_POOL_IDLE)
        .acquire_timeout(main.options().get_acquire_timeout())
        .after_connect(move |conn, _meta| {
            let key = key.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('pulso.analysis', $1, false)")
                    .bind(key)
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect_lazy_with((*main.connect_options()).clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_hash_keys_are_co_owner_analyses() {
        assert!(!is_co_owner_key("AAA010101AAA"));
        assert!(is_co_owner_key("AAA010101AAA#1a2b3c4d"));
    }
}
