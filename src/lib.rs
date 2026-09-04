//! Library crate exposing just enough of `main.rs`'s module tree for integration tests
//! under `tests/` (which run as separate crates and can't see a binary's private items) to
//! reach `Config`, `db::init_pool`/`db::migration_guard`, and the payroll analytics
//! functions directly against a real pool.
//!
//! `main.rs` keeps its own `mod` declarations unchanged — both targets compile the same
//! files under `src/`, Cargo's standard way to pair a binary with a library without
//! restructuring the binary's internals.
//!
//! Deliberately NOT mirrored here: `api_docs`, `errors`, `models`, `routes`, `state`, and
//! most of `services` (`crypto`, `email`, `etl`, `fiel`, `gap_detector`, `php_cli`,
//! `recheck_cancelled`, `s3`, `storage`, `xml_redownload`, and
//! `services::analytics::recurrence`). Several of those call back into helpers
//! (`try_fiel_auth`, `next_day`, `routes::analytics::current_month_yyyymm`, …) that only
//! exist in the binary crate — pulling them in here would fail to compile a crate whose
//! root is `lib.rs`, not `main.rs`.

pub mod config;
pub mod db;

pub mod services {
    pub mod analytics {
        pub mod normalization;
        pub mod payroll;
        pub mod summary;
        // L6-02/L6-03 addition: needed to compare "ingresos netos normalizados" across the
        // three screens that are supposed to report the same figure (Ingresos, Resumen
        // trimestral, Contrapartes). Same no-binary-only-dependency shape as the three
        // modules above (only `super::summary` and `crate::db::DbPool`).
        pub mod counterparties;
        pub mod quarterly;
    }
    // `db::cfdis` (pulled in by `pub mod db` above) depends on this for XML parsing.
    pub mod xml_parser;
}
