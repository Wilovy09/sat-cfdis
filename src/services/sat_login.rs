//! Live check of SAT credentials: opens a SAT session through the php-cli `login` command,
//! without queueing a job, so whoever typed them gets an answer while still on the page.

use std::time::Duration;

use base64::Engine as _;
use tokio::process::Command;

use crate::config::Config;
use crate::services::php_cli::PhpCli;

/// Past this the caller tells the user to try again later; the SAT login (captcha
/// included) normally answers in well under a minute.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(90);

/// Credentials as typed, never stored in this form.
pub enum SatCredential<'a> {
    Ciec {
        password: &'a str,
    },
    Fiel {
        cert_der: &'a [u8],
        key_der: &'a [u8],
        password: &'a str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoginOutcome {
    /// The SAT opened a session with them, for this RFC.
    Accepted,
    /// The SAT (or the FIEL files themselves) rejected them: wrong password, a FIEL that
    /// doesn't open, or a FIEL issued to another RFC.
    Rejected,
    /// No verdict: the SAT didn't answer, the captcha failed, or it timed out.
    Unavailable,
}

/// Codes from php-cli's `ErrorClassifier` that mean the credentials themselves are wrong.
fn outcome_for_code(code: &str) -> LoginOutcome {
    match code {
        "invalid_credentials" | "fiel_login_failed" | "login_not_registered" => {
            LoginOutcome::Rejected
        }
        _ => LoginOutcome::Unavailable,
    }
}

pub async fn check(cfg: &Config, rfc: &str, credential: &SatCredential<'_>) -> LoginOutcome {
    let rfc = rfc.to_uppercase();
    // Backs the FIEL's PEM files until the login finishes.
    let mut _fiel_dir: Option<tempfile::TempDir> = None;

    let auth = match credential {
        SatCredential::Ciec { password } => {
            serde_json::json!({ "type": "ciec", "rfc": rfc, "password": password })
        }
        SatCredential::Fiel {
            cert_der,
            key_der,
            password,
        } => {
            let Ok(dir) = tempfile::TempDir::new() else {
                tracing::error!("SAT login check: TempDir creation failed");
                return LoginOutcome::Unavailable;
            };
            let b64 = base64::engine::general_purpose::STANDARD;
            // A wrong password or files that aren't a FIEL fail right here.
            let Ok((cert_pem, key_pem)) = crate::services::fiel::der_to_pem(
                &b64.encode(cert_der),
                &b64.encode(key_der),
                password,
                dir.path(),
            )
            .await
            else {
                return LoginOutcome::Rejected;
            };
            if !certificate_is_for_rfc(&cert_pem, &rfc).await {
                return LoginOutcome::Rejected;
            }
            let auth = serde_json::json!({
                "type": "fiel",
                "cert_pem_path": cert_pem.to_string_lossy(),
                "key_pem_path": key_pem.to_string_lossy(),
                "password": "",
            });
            _fiel_dir = Some(dir);
            auth
        }
    };

    let cli = PhpCli::new(&cfg.php_bin, &cfg.php_cli_path).with_proxy(cfg.https_proxy.clone());
    let payload = serde_json::json!({ "command": "login", "auth": auth, "params": {} });
    match tokio::time::timeout(LOGIN_TIMEOUT, cli.run(&payload)).await {
        Ok(Ok(result)) if result["ok"].as_bool() == Some(true) => LoginOutcome::Accepted,
        Ok(Ok(result)) => outcome_for_code(result["code"].as_str().unwrap_or_default()),
        Ok(Err(e)) => {
            tracing::warn!(rfc = %rfc, "SAT login check: php-cli failed: {e}");
            LoginOutcome::Unavailable
        }
        Err(_) => {
            tracing::warn!(rfc = %rfc, "SAT login check: timed out");
            LoginOutcome::Unavailable
        }
    }
}

/// A SAT certificate carries its holder's RFC in the subject (`x500UniqueIdentifier`);
/// without this check a FIEL of another company would open that company's session.
async fn certificate_is_for_rfc(cert_pem: &std::path::Path, rfc: &str) -> bool {
    let output = Command::new("openssl")
        .args(["x509", "-noout", "-subject", "-nameopt", "RFC2253", "-in"])
        .arg(cert_pem)
        .output()
        .await;
    match output {
        Ok(out) if out.status.success() => {
            subject_names_rfc(&String::from_utf8_lossy(&out.stdout), rfc)
        }
        Ok(out) => {
            tracing::warn!(
                "SAT login check: openssl subject failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            false
        }
        Err(e) => {
            tracing::error!("SAT login check: could not run openssl: {e}");
            false
        }
    }
}

/// The subject lists the RFC as its own token (for a company, "RFC / RFC of its legal
/// representative"), so a plain substring check would also accept a longer RFC that merely
/// contains this one.
fn subject_names_rfc(subject: &str, rfc: &str) -> bool {
    subject
        .to_uppercase()
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '&' && c != 'Ñ')
        .any(|token| token == rfc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_credential_errors_count_as_rejected() {
        assert_eq!(
            outcome_for_code("invalid_credentials"),
            LoginOutcome::Rejected
        );
        assert_eq!(
            outcome_for_code("fiel_login_failed"),
            LoginOutcome::Rejected
        );
        assert_eq!(
            outcome_for_code("captcha_failed"),
            LoginOutcome::Unavailable
        );
        assert_eq!(outcome_for_code("rate_limited"), LoginOutcome::Unavailable);
        assert_eq!(outcome_for_code(""), LoginOutcome::Unavailable);
    }

    #[test]
    fn subject_must_name_the_rfc_as_a_whole_token() {
        let moral = "x500UniqueIdentifier=AAA010101AAA / BBBB800101XX1,CN=EMPRESA SA DE CV";
        assert!(subject_names_rfc(moral, "AAA010101AAA"));
        assert!(subject_names_rfc(moral, "BBBB800101XX1"));
        assert!(!subject_names_rfc(moral, "AA010101AAA"));
        assert!(!subject_names_rfc(
            "x500UniqueIdentifier=CCC010101CCC",
            "AAA010101AAA"
        ));
    }
}
