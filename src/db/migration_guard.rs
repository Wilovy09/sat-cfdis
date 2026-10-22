//! L6-01: migration guard.
//!
//! `db::init_pool` already runs `sqlx::migrate!`, which applies whatever pending migrations
//! it can reach -- but it never notices the opposite failure: a fresh/frozen environment
//! whose `_sqlx_migrations` table stopped advancing while `migrations/` kept growing (or,
//! more rarely, a DB somehow ahead of the checked-out repo). Both leave the environment
//! silently out of sync with the code running against it. This module compares the two
//! high-water marks and refuses to start when they disagree.

use std::path::Path;

use sqlx::Row;

use crate::db::DbPool;

/// Pure comparison: does the highest applied migration version match the highest version
/// present in the migrations directory? Kept free of DB/filesystem I/O so it has fast unit
/// tests -- see `ensure_current` for the real-world wrapper.
pub fn check_migrations_current(applied_max: i64, present_max: i64) -> Result<(), String> {
    if applied_max == present_max {
        return Ok(());
    }

    if applied_max < present_max {
        Err(format!(
            "Migration guard: {} migration file(s) present but never applied \
             (highest applied = {applied_max}, highest present = {present_max}). \
             Missing version(s): {}..={present_max}.",
            present_max - applied_max,
            applied_max + 1
        ))
    } else {
        Err(format!(
            "Migration guard: _sqlx_migrations is ahead of the checked-out repo \
             (highest applied = {applied_max}, highest present = {present_max}). \
             Extra version(s) applied: {}..={applied_max}.",
            present_max + 1
        ))
    }
}

/// Highest migration version recorded as applied. `NULL` (empty table) reads as 0.
pub async fn applied_max_version(pool: &DbPool) -> Result<i64, sqlx::Error> {
    let row = sqlx::query("SELECT MAX(version) AS max_version FROM _sqlx_migrations")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<Option<i64>, _>("max_version")?.unwrap_or(0))
}

/// Highest version present in `migrations_dir`, parsed from each `NNN_description.sql`
/// filename's leading integer. Non-`.sql` entries (and any file whose leading segment
/// isn't a number) are ignored rather than treated as an error.
pub fn present_max_version(migrations_dir: &Path) -> Result<i64, std::io::Error> {
    let mut max_version = 0i64;
    for entry in std::fs::read_dir(migrations_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if !Path::new(name)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
        {
            continue;
        }
        let Some(version) = name.split('_').next().and_then(|s| s.parse::<i64>().ok()) else {
            continue;
        };
        max_version = max_version.max(version);
    }
    Ok(max_version)
}

/// Thin wrapper: reads the real applied/present high-water marks and runs the pure check.
/// Called once at startup, right after `init_pool` succeeds.
pub async fn ensure_current(pool: &DbPool, migrations_dir: &Path) -> Result<(), String> {
    let applied_max = applied_max_version(pool)
        .await
        .map_err(|e| format!("Migration guard: could not read _sqlx_migrations: {e}"))?;
    let present_max = present_max_version(migrations_dir).map_err(|e| {
        format!(
            "Migration guard: could not read '{}': {e}",
            migrations_dir.display()
        )
    })?;
    check_migrations_current(applied_max, present_max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_when_applied_matches_present() {
        assert!(check_migrations_current(66, 66).is_ok());
    }

    #[test]
    fn fails_when_present_is_ahead_of_applied() {
        let err = check_migrations_current(65, 66).expect_err("must fail: DB is one behind");
        assert!(err.contains("65"), "message should name applied_max: {err}");
        assert!(
            err.contains("66"),
            "message should name the missing version: {err}"
        );
    }

    #[test]
    fn fails_when_applied_is_ahead_of_present() {
        let err = check_migrations_current(67, 66).expect_err("must fail: DB is ahead of repo");
        assert!(
            err.contains("67"),
            "message should name the extra version: {err}"
        );
        assert!(err.contains("66"), "message should name present_max: {err}");
    }
}
