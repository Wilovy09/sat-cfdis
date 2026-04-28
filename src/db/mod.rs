pub mod cfdis;
pub mod jobs;
pub mod users;

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

use crate::config::Config;

pub type DbPool = PgPool;

pub async fn init_pool(cfg: &Config) -> Result<DbPool, sqlx::Error> {
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

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(opts)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    Ok(pool)
}
