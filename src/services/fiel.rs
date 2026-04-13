use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use std::path::{Path, PathBuf};
use tokio::process::Command;

/// Converts DER-format FIEL certificate and private key to PEM files
/// inside `work_dir`, using `openssl` as the user specified.
///
/// Returns `(cert_pem_path, key_pem_path)`.  The key PEM is decrypted
/// (no passphrase required when reading it later in PHP).
pub async fn der_to_pem(
    cert_b64: &str,
    key_b64: &str,
    password: &str,
    work_dir: &Path,
) -> Result<(PathBuf, PathBuf)> {
    // Decode base64 → raw DER bytes
    let cert_der = STANDARD.decode(cert_b64).context("Invalid base64 in certificate")?;
    let key_der  = STANDARD.decode(key_b64).context("Invalid base64 in private_key")?;

    // Write DER files
    let cert_der_path = work_dir.join("cert.cer");
    let key_der_path  = work_dir.join("key.key");
    let cert_pem_path = work_dir.join("cert.pem");
    let key_pem_path  = work_dir.join("key.pem");

    tokio::fs::write(&cert_der_path, &cert_der)
        .await
        .context("Could not write cert.cer to temp dir")?;
    tokio::fs::write(&key_der_path, &key_der)
        .await
        .context("Could not write key.key to temp dir")?;

    // openssl x509 -inform DER -in cert.cer -out cert.pem
    let cert_status = Command::new("openssl")
        .args(["x509", "-inform", "DER", "-in"])
        .arg(&cert_der_path)
        .arg("-out")
        .arg(&cert_pem_path)
        .output()
        .await
        .context("Failed to spawn openssl for certificate conversion")?;

    if !cert_status.status.success() {
        let stderr = String::from_utf8_lossy(&cert_status.stderr);
        bail!("openssl certificate conversion failed: {stderr}");
    }

    // openssl pkcs8 -inform DER -in key.key -passin pass:PASSWORD -out key.pem
    // This decrypts the DER private key into an unencrypted PEM file.
    let key_status = Command::new("openssl")
        .args(["pkcs8", "-inform", "DER", "-in"])
        .arg(&key_der_path)
        .arg("-passin")
        .arg(format!("pass:{password}"))
        .arg("-out")
        .arg(&key_pem_path)
        .output()
        .await
        .context("Failed to spawn openssl for private key conversion")?;

    if !key_status.status.success() {
        let stderr = String::from_utf8_lossy(&key_status.stderr);
        bail!("openssl private key conversion failed (wrong password?): {stderr}");
    }

    Ok((cert_pem_path, key_pem_path))
}
