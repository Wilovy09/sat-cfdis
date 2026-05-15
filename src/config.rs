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
        }
    }
}
