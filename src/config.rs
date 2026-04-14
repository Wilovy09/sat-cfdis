use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    /// Path to the `php` binary (default: `php`)
    pub php_bin: String,
    /// Absolute path to `php-cli/bin/cfdi-scraper`
    pub php_cli_path: String,
    /// Whether BOXFACTURA_CONFIG_PATH is set (enables CIEC captcha support)
    pub captcha_enabled: bool,
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
            php_cli_path: env::var("PHP_CLI_PATH").unwrap_or_else(|_| {
                // Relative to the working directory where the binary runs
                "./php-cli/bin/cfdi-scraper".to_string()
            }),
            captcha_enabled: env::var("BOXFACTURA_CONFIG_PATH").is_ok(),
        }
    }
}
