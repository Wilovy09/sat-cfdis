//! Background worker: periodically re-verifies `estado_sat` for invoices
//! currently flagged cancelled.
//!
//! SAT lets the receptor reject a cancellation request within ~72h, reverting
//! the CFDI back to "Vigente". Nothing else in the pipeline ever re-polls SAT
//! after the initial scrape (`estado_sat` is set once at ingestion from a
//! frozen `job_invoices.metadata` snapshot and never refreshed), so a rejected
//! cancellation stays wrongly marked cancelled forever — silently
//! underreporting revenue every month it recurs. This worker closes that gap
//! by re-running the same PHP `list` (by UUID) lookup already used for
//! interactive invoice lookups, and correcting `estado_sat` when it disagrees
//! with what SAT reports now.

use crate::{
    config::Config,
    db::{self, DbPool},
    services::{crypto, php_cli::PhpCli},
};
use aws_sdk_s3::Client as S3Client;
use std::collections::HashMap;
use std::sync::Arc;

/// How often the worker wakes up.
const RECHECK_POLL_SECS: u64 = 6 * 3600;
/// Don't re-verify the same invoice more than once within this window.
const MIN_RECHECK_HOURS: i32 = 12;
/// Once an invoice has been checked at least once, only keep re-checking it
/// while it's this recent — SAT's rejection window is 72h, so this is a
/// generous margin, not a real deadline. Invoices never checked at all
/// (the historical backlog) are always included regardless of age.
const RECENT_DAYS: i32 = 14;
/// Candidates pulled per cycle, across all RFCs — bounds one cycle's SAT
/// load; a large historical backlog drains gradually over multiple cycles.
const BATCH_LIMIT: i64 = 300;
/// Max UUIDs sent in a single PHP `list` call.
const CHUNK_SIZE: usize = 100;

pub async fn worker(pool: DbPool, cfg: Arc<Config>, s3: Arc<S3Client>) {
    // Let the other startup workers get a head start.
    tokio::time::sleep(std::time::Duration::from_secs(120)).await;

    loop {
        if let Err(e) = run_cycle(&pool, &cfg, &s3).await {
            tracing::error!("Recheck-cancelled: cycle error: {e}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(RECHECK_POLL_SECS)).await;
    }
}

async fn run_cycle(pool: &DbPool, cfg: &Arc<Config>, s3: &Arc<S3Client>) -> anyhow::Result<()> {
    let candidates = db::cfdis::find_cancelled_recheck_candidates(
        pool,
        MIN_RECHECK_HOURS,
        RECENT_DAYS,
        BATCH_LIMIT,
    )
    .await?;
    if candidates.is_empty() {
        return Ok(());
    }
    tracing::info!(count = candidates.len(), "Recheck-cancelled: candidates this cycle");

    let creds: HashMap<String, String> =
        db::users::get_all_with_credentials(pool).await?.into_iter().collect();

    // Group by (owner_rfc, download_type): the owner is whichever side of the
    // invoice is a tracked Pulso RFC — that's the identity we can log into SAT
    // with to look the UUID up. Rows where neither side is tracked anymore
    // (RFC removed after ingestion) can't be checked; mark them seen so they
    // stop resurfacing every cycle.
    let mut groups: HashMap<(String, &'static str), Vec<String>> = HashMap::new();
    let mut orphaned: Vec<String> = Vec::new();
    for (uuid, rfc_emisor, rfc_receptor) in candidates {
        if creds.contains_key(&rfc_emisor) {
            groups.entry((rfc_emisor, "emitidos")).or_default().push(uuid);
        } else if creds.contains_key(&rfc_receptor) {
            groups.entry((rfc_receptor, "recibidos")).or_default().push(uuid);
        } else {
            orphaned.push(uuid);
        }
    }
    if !orphaned.is_empty() {
        tracing::info!(count = orphaned.len(), "Recheck-cancelled: no tracked RFC owns these, skipped");
        db::cfdis::touch_estado_sat_checked(pool, &orphaned).await?;
    }

    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    let key = crypto::load_key();

    for ((owner_rfc, download_type), uuids) in groups {
        // _fiel_tmp must stay alive for the whole group — it backs the
        // cert_pem_path/key_pem_path referenced by auth_payload, and the PHP
        // subprocess reads those paths later, not during this match. Binding
        // it only inside the match arm (as `_tmp`) would drop the TempDir —
        // and delete the cert files — before recheck_chunk ever runs.
        let (auth_payload, _fiel_tmp) = match crate::try_fiel_auth(pool, s3, &bucket, &owner_rfc).await {
            Some((fiel_auth, tmp)) => (fiel_auth, Some(tmp)),
            None => {
                let Some(clave_enc) = creds.get(&owner_rfc) else { continue };
                match crypto::decrypt(&key, clave_enc) {
                    Ok(clave) => (
                        serde_json::json!({
                            "type": "ciec",
                            "rfc": owner_rfc,
                            "password": clave,
                        }),
                        None::<tempfile::TempDir>,
                    ),
                    Err(e) => {
                        tracing::error!(rfc = %owner_rfc, "Recheck-cancelled: decrypt failed: {e}");
                        continue;
                    }
                }
            }
        };

        for chunk in uuids.chunks(CHUNK_SIZE) {
            if let Err(e) =
                recheck_chunk(pool, cfg, &owner_rfc, download_type, &auth_payload, chunk).await
            {
                tracing::error!(rfc = %owner_rfc, "Recheck-cancelled: chunk failed: {e}");
            }
        }
    }

    Ok(())
}

async fn recheck_chunk(
    pool: &DbPool,
    cfg: &Config,
    owner_rfc: &str,
    download_type: &str,
    auth_payload: &serde_json::Value,
    uuids: &[String],
) -> anyhow::Result<()> {
    let payload = serde_json::json!({
        "command": "list",
        "auth": auth_payload,
        "params": {
            "download_type": download_type,
            "uuids": uuids,
        }
    });

    let cli = PhpCli::new(&cfg.php_bin, &cfg.php_cli_path).with_proxy(cfg.https_proxy.clone());
    let result = cli.run(&payload).await?;

    // Metadata::getData() always keys the UUID lowercase (see
    // libs/cfdi-sat-scraper/src/Metadata.php) — normalize back to uppercase
    // to match how uuids are stored in pulso.cfdis.
    let mut found: HashMap<String, String> = HashMap::new();
    if let Some(invoices) = result.get("invoices").and_then(|v| v.as_array()) {
        for inv in invoices {
            let Some(uuid) = inv.get("uuid").and_then(|v| v.as_str()) else { continue };
            let estado = inv
                .get("estadoComprobante")
                .or_else(|| inv.get("EstadoComprobante"))
                .and_then(|v| v.as_str())
                .unwrap_or("vigente")
                .to_lowercase();
            found.insert(uuid.to_uppercase(), estado);
        }
    }

    // TEMP diagnostic: confirms whether SAT is actually returning metadata for
    // these UUIDs at all, vs genuinely reporting them all still Cancelado.
    // Remove once the mismatch with the reference export is understood.
    if found.len() < uuids.len() {
        tracing::warn!(
            rfc = %owner_rfc, requested = uuids.len(), returned = found.len(),
            raw = %result, "Recheck-cancelled: SAT returned fewer results than requested"
        );
    } else {
        tracing::info!(
            rfc = %owner_rfc, requested = uuids.len(), returned = found.len(),
            estados = ?found.values().collect::<std::collections::HashSet<_>>(),
            "Recheck-cancelled: SAT response summary"
        );
    }

    let mut reverted = 0usize;
    for uuid in uuids {
        match found.get(uuid.as_str()) {
            Some(new_estado) => {
                db::cfdis::update_estado_sat(pool, uuid, new_estado).await?;
                if !new_estado.contains("cancel") {
                    reverted += 1;
                    tracing::info!(
                        uuid = %uuid, rfc = %owner_rfc, estado = %new_estado,
                        "Recheck-cancelled: cancellation reverted, invoice restored to Vigente"
                    );
                }
            }
            None => {
                // SAT no longer lists this UUID under this RFC/download_type
                // (rare). Leave estado_sat untouched, just bump checked_at so
                // it doesn't retry every cycle.
                db::cfdis::touch_estado_sat_checked(pool, std::slice::from_ref(uuid)).await?;
            }
        }
    }
    if reverted > 0 {
        tracing::warn!(
            rfc = %owner_rfc, count = reverted,
            "Recheck-cancelled: {reverted} invoice(s) reverted from Cancelado to Vigente this cycle"
        );
    }
    Ok(())
}
