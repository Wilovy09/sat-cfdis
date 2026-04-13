mod config;
mod errors;
mod models;
mod routes;
mod services;
mod state;

use actix_files::Files;
use actix_web::{App, HttpServer, middleware, web};
use tera::Tera;
use tracing::info;
use tracing_subscriber::EnvFilter;

use config::Config;
use routes::{invoices, web as web_routes};
use state::CaptchaMap;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    dotenvy::dotenv().ok();
    let cfg = Config::from_env();
    let bind_addr = format!("{}:{}", cfg.host, cfg.port);

    // Tera templates — path relative to CWD (run from repo root)
    let templates_glob =
        std::env::var("TEMPLATES_DIR").unwrap_or_else(|_| "templates/**/*".to_string());
    let tera = Tera::new(&templates_glob).unwrap_or_else(|e| {
        panic!("Failed to load Tera templates from '{templates_glob}': {e}");
    });

    info!(
        host = %cfg.host,
        port = %cfg.port,
        php_bin = %cfg.php_bin,
        php_cli_path = %cfg.php_cli_path,
        "Starting pulso-backend"
    );

    let cfg_data = web::Data::new(cfg);
    let tera_data = web::Data::new(tera);
    let captcha_map: web::Data<CaptchaMap> =
        web::Data::new(CaptchaMap::new(std::collections::HashMap::new()));

    HttpServer::new(move || {
        App::new()
            .app_data(cfg_data.clone())
            .app_data(tera_data.clone())
            .app_data(captcha_map.clone())
            .app_data(web::JsonConfig::default().limit(10 * 1024 * 1024))
            .wrap(middleware::Logger::default())
            // Static files (CSS, JS)
            .service(Files::new("/static", "static").prefer_utf8(true))
            // Health check
            .route("/health", web::get().to(invoices::health))
            // Web UI
            .route("/", web::get().to(web_routes::index))
            .route("/web/list", web::post().to(web_routes::list_web))
            // JSON API
            .service(
                web::scope("/api/v1/invoices")
                    .route("/list", web::post().to(invoices::list_invoices))
                    .route("/list/stream", web::post().to(invoices::list_stream))
                    .route("/captcha/solve", web::post().to(invoices::solve_captcha))
                    .route("/download", web::post().to(invoices::download_invoices))
                    .route(
                        "/download/stream",
                        web::post().to(invoices::download_stream),
                    ),
            )
    })
    .bind(&bind_addr)?
    .run()
    .await
}
