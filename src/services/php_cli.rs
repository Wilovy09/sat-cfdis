use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// Thin wrapper around the PHP CLI (`php-cli/bin/cfdi-scraper`).
///
/// Sends a JSON payload to the process's stdin and parses the JSON from stdout.
/// Any output on stderr is captured and returned as an error on non-zero exit.
pub struct PhpCli {
    php_bin: String,
    php_cli_path: String,
}

impl PhpCli {
    pub fn new(php_bin: impl Into<String>, php_cli_path: impl Into<String>) -> Self {
        Self {
            php_bin: php_bin.into(),
            php_cli_path: php_cli_path.into(),
        }
    }

    /// Runs the PHP CLI with `payload` as JSON on stdin.
    /// Returns the parsed JSON output on success, or an error with the stderr message.
    pub async fn run(&self, payload: &Value) -> Result<Value> {
        let input_json = serde_json::to_vec(payload).context("Could not serialize CLI payload")?;

        let mut child = Command::new(&self.php_bin)
            .arg(&self.php_cli_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn PHP CLI. Is 'php' in PATH?")?;

        // Write JSON to stdin then close it so PHP's stream_get_contents() can finish
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(&input_json)
                .await
                .context("Failed to write to PHP CLI stdin")?;
            // Drop stdin so the child receives EOF
        }

        let output = child
            .wait_with_output()
            .await
            .context("Failed to wait for PHP CLI process")?;

        if !output.status.success() {
            // Try to parse a structured error from stderr first
            let stderr = String::from_utf8_lossy(&output.stderr);
            if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&output.stderr) {
                if let Some(msg) = map.get("error").and_then(|v| v.as_str()) {
                    bail!("PHP CLI error: {msg}");
                }
            }
            bail!("PHP CLI exited with status {}: {stderr}", output.status);
        }

        let result: Value =
            serde_json::from_slice(&output.stdout).context("PHP CLI returned invalid JSON")?;

        Ok(result)
    }
}
