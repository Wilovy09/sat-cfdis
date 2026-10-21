use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
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
    let cert_der = STANDARD
        .decode(cert_b64)
        .context("Invalid base64 in certificate")?;
    let key_der = STANDARD
        .decode(key_b64)
        .context("Invalid base64 in private_key")?;

    // Write DER files
    let cert_der_path = work_dir.join("cert.cer");
    let key_der_path = work_dir.join("key.key");
    let cert_pem_path = work_dir.join("cert.pem");
    let key_pem_path = work_dir.join("key.pem");

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

/// How often the orphaned-FIEL sweep runs after its first pass at startup.
const ORPHAN_SWEEP_INTERVAL_SECS: u64 = 3600;

/// L18-06 / L19-03: deletes one RFC's stored FIEL -- its S3 files, then the row holding the
/// encrypted password. Best-effort on S3 (logged, never blocks): the row goes regardless,
/// and without it the files can't be used, since the password lives only there.
pub async fn delete_stored(
    pool: &crate::db::DbPool,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    row: &crate::db::fiel::FielRow,
) {
    if !bucket.is_empty() {
        for s3_key in [&row.cert_s3_key, &row.key_s3_key] {
            if let Err(e) = crate::services::s3::delete_fiel(s3, bucket, s3_key).await {
                tracing::warn!(rfc = %row.rfc, "FIEL delete: S3 delete failed (continuing): {e}");
            }
        }
    }
    match crate::db::fiel::delete(pool, &row.rfc).await {
        Ok(true) => {
            tracing::info!(rfc = %row.rfc, "Orphaned FIEL removed (RFC has no active owner)")
        }
        Ok(false) => {}
        Err(e) => tracing::error!(rfc = %row.rfc, "FIEL delete: DB error: {e}"),
    }
}

/// L19-03 point 2: removes every FIEL whose RFC is left without an active owner, however
/// that happened -- the owner removing the RFC, an admin, a migration, or the owner's
/// Adquiere account being deleted outside Pulso. Runs at startup, then hourly.
pub async fn orphan_sweep_worker(
    pool: crate::db::DbPool,
    cfg: std::sync::Arc<crate::config::Config>,
    s3: std::sync::Arc<aws_sdk_s3::Client>,
) {
    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    loop {
        match crate::db::fiel::list_orphaned(&pool).await {
            Ok(rows) => {
                for row in &rows {
                    delete_stored(&pool, &s3, &bucket, row).await;
                }
            }
            Err(e) => tracing::error!("FIEL orphan sweep: DB error: {e}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(ORPHAN_SWEEP_INTERVAL_SECS)).await;
    }
}
