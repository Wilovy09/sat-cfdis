pub mod cfdis;
pub mod fiel;
pub mod jobs;
pub mod migration_guard;
pub mod subscriptions;
pub mod users;

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

use crate::config::Config;

pub type DbPool = PgPool;

fn connect_options(cfg: &Config) -> PgConnectOptions {
    let mut opts = PgConnectOptions::new()
        .host(&cfg.pg_host)
        .port(cfg.pg_port)
        .username(&cfg.pg_user)
        .password(&cfg.pg_password)
        .database(&cfg.pg_database);

    if !cfg!(debug_assertions) {
        opts = opts
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert(cfg.pg_cert_path.as_str());
    } else {
        opts = opts.ssl_mode(PgSslMode::Prefer);
    }
    opts
}

// P-05 / AUD-079: max_connections, acquire_timeout and idle_timeout used to be
// max_connections(5) and nothing else -- no env var (recompile to change it), no acquire
// timeout (a request queuing for a connection waited forever instead of failing fast), no
// idle timeout (a connection could sit open indefinitely). All three now come from Config.
fn pool_options(cfg: &Config, max_connections: u32) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(std::time::Duration::from_secs(cfg.pg_acquire_timeout_secs))
        .idle_timeout(std::time::Duration::from_secs(600))
}

/// User-facing pool -- what the HTTP server hands to request handlers. Runs migrations
/// (the one place they run; `init_worker_pool` below doesn't, so they don't race or apply
/// twice when both pools are created at startup).
pub async fn init_pool(cfg: &Config) -> Result<DbPool, sqlx::Error> {
    let pool = pool_options(cfg, cfg.pg_pool_size)
        .connect_with(connect_options(cfg))
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    Ok(pool)
}

/// Background-worker pool -- resume, ETL, daily sync, recheck-cancelled, gap-detector,
/// xml-redownload all share this one instead of the user-facing pool above, so a long
/// sync can't take a connection away from someone using the dashboard. No migration run
/// here -- `init_pool` already does it once, and running it twice at startup would mean
/// two pools racing to apply the same migration.
pub async fn init_worker_pool(cfg: &Config) -> Result<DbPool, sqlx::Error> {
    pool_options(cfg, cfg.pg_worker_pool_size)
        .connect_with(connect_options(cfg))
        .await
}
