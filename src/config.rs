use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub php_bin: String,
    pub php_cli_path: String,
    #[allow(dead_code)]
    pub captcha_enabled: bool,
    pub s3_bucket: Option<String>,
    pub adquiere_api: String,
    // PostgreSQL connection
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_database: String,
    pub pg_cert_path: String,
    // P-05 / AUD-079: pool size and acquire timeout used to be hardcoded (max_connections(5),
    // no timeout at all) -- configurable now, and split from the background-worker pool
    // below so a long sync doesn't compete with user traffic for the same five slots.
    /// User-facing (HTTP server) pool size. Default sized for the Dashboard's own load: 9
    /// simultaneous requests, plus headroom for more than one browser tab/user at once.
    pub pg_pool_size: u32,
    /// Background-worker pool size (resume, ETL, daily sync, recheck-cancelled,
    /// gap-detector, xml-redownload) -- same size the single shared pool used to be, since
    /// the point of splitting it off is to stop it from taking slots away from users, not
    /// to give the workers more capacity than they had.
    pub pg_worker_pool_size: u32,
    /// How long a request waits for a free connection before giving up, instead of
    /// queuing forever.
    pub pg_acquire_timeout_secs: u64,
    /// Optional residential proxy for PHP CLI SAT requests (e.g. http://user:pass@host:port)
    pub https_proxy: Option<String>,
    /// SendGrid API key for transactional email notifications
    pub sendgrid_api_key: Option<String>,
    /// Sender address for SendGrid emails (defaults to team@adquiere.co)
    pub sendgrid_from: String,
    /// Comma-separated list of allowed CORS origins (e.g. https://pulso.adquiere.co)
    pub allowed_origins: Vec<String>,
    /// Comma-separated list of allowed CORS methods (e.g. GET,POST,PUT,DELETE,OPTIONS)
    pub allowed_methods: Vec<String>,
    pub google_client_id: String,
    pub google_client_secret: String,
    pub google_redirect_uri: String,
    pub jwt_secret: String,
    #[allow(dead_code)]
    pub app_base_url: String,
    /// pm2 process name for this app, e.g. what `pm2 start ... --name <this>` used --
    /// locates its log files on disk (/root/.pm2/logs/<name>-out.log and -error.log) for
    /// the admin-only GET /api/v1/admin/logs endpoint. Defaults to the name this app is
    /// actually registered under on the current server.
    pub pm2_app_name: String,
    /// Shared secret that lets a trusted log-aggregator (adquiere-logs) call GET
    /// /api/v1/admin/logs without a per-environment admin JWT -- see
    /// `routes::logs::admin_logs_key_matches` for why a personal JWT can't do this job
    /// across environments. `None` (unset) disables the bypass entirely; only the
    /// existing per-user JWT + DB admin check applies.
    pub admin_logs_key: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: env::var("PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8080),
            php_bin: env::var("PHP_BIN").unwrap_or_else(|_| "php".to_string()),
            php_cli_path: env::var("PHP_CLI_PATH")
                .unwrap_or_else(|_| "./php-cli/bin/cfdi-scraper".to_string()),
            captcha_enabled: env::var("BOXFACTURA_CONFIG_PATH").is_ok(),
            s3_bucket: env::var("S3_BUCKET").ok(),
            adquiere_api: env::var("ADQUIERE_API")
                .unwrap_or_else(|_| "https://api-test.adquiere.co".to_string()),
            pg_host: env::var("POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            pg_port: env::var("POSTGRES_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5432),
            pg_user: env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string()),
            pg_password: env::var("POSTGRES_PASSWORD").unwrap_or_default(),
            pg_database: env::var("POSTGRES_DATABASE").unwrap_or_else(|_| "adquiere".to_string()),
            pg_cert_path: env::var("POSTGRES_CERT_PATH")
                .unwrap_or_else(|_| "/arena/certs/rds-ca-bundle.pem".to_string()),
            pg_pool_size: env::var("POSTGRES_POOL_SIZE")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(15),
            pg_worker_pool_size: env::var("POSTGRES_WORKER_POOL_SIZE")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5),
            pg_acquire_timeout_secs: env::var("POSTGRES_ACQUIRE_TIMEOUT_SECS")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(10),
            https_proxy: env::var("HTTPS_PROXY").ok(),
            sendgrid_api_key: env::var("SENDGRID_API_KEY").ok(),
            sendgrid_from: env::var("SENDGRID_FROM")
                .unwrap_or_else(|_| "team@adquiere.co".to_string()),
            allowed_origins: env::var("ALLOWED_ORIGINS")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            allowed_methods: env::var("ALLOWED_METHODS")
                .unwrap_or_else(|_| "GET,POST,PUT,DELETE,OPTIONS".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            google_client_id: env::var("GOOGLE_CLIENT_ID").unwrap_or_default(),
            google_client_secret: env::var("GOOGLE_CLIENT_SECRET").unwrap_or_default(),
            google_redirect_uri: env::var("GOOGLE_REDIRECT_URI")
                .unwrap_or_default()
                .trim()
                .to_string(),
            jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| "jwtsecret".to_string()),
            app_base_url: env::var("APP_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:5173".to_string()),
            pm2_app_name: env::var("PM2_APP_NAME").unwrap_or_else(|_| "pulso-backend".to_string()),
            admin_logs_key: env::var("ADMIN_LOGS_KEY").ok(),
        }
    }
}
