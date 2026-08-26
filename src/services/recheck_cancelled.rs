//! Background worker: periodically re-verifies `estado_sat` against SAT in
//! both directions.
//!
//! `estado_sat` is set once at ingestion from a frozen `job_invoices.metadata`
//! snapshot and nothing else in the pipeline ever refreshes it, so either
//! direction can go stale silently:
//!
//! 1. SAT lets the receptor reject a cancellation request within ~72h,
//!    reverting the CFDI back to "Vigente" — a rejected cancellation stays
//!    wrongly marked cancelled forever, underreporting revenue.
//! 2. A CFDI marked Vigente at ingestion can be cancelled later (SAT allows it
//!    well past the 72h reversal window) — nothing ever looks at it again
//!    once it's not flagged cancelled, so the cancellation is invisible and
//!    it keeps inflating revenue for as long as it's counted. Confirmed
//!    against 5 Nubarium invoices SAT already showed cancelled days before
//!    this worker's own cancelled-only scan ran — they were never in its
//!    candidate set to begin with, not a check that failed.
//!
//! This worker closes both gaps by re-running the same PHP `list` (by UUID)
//! lookup already used for interactive invoice lookups, and correcting
//! `estado_sat` when it disagrees with what SAT reports now — the update path
//! doesn't care which direction the correction goes.

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
/// Consecutive inconclusive checks (SAT's response didn't include the UUID)
/// before giving up and letting the invoice fall under the normal recency
/// gate — otherwise a UUID SAT genuinely never returns would retry forever.
const MAX_MISS_ATTEMPTS: i32 = 8;

/// Vigente-side recheck runs far less often per invoice than the
/// cancelled-side one: a cancellation reversal has a hard 72h window, so
/// checking every 12h makes sense there. A Vigente invoice flipping to
/// cancelled has no such window — checking weekly is enough to catch it
/// without burning SAT/captcha budget on rows that essentially never change.
const VIGENTE_MIN_RECHECK_HOURS: i32 = 24 * 7;
/// Wider than `RECENT_DAYS` on purpose: SAT cancellations for I/E invoices
/// happen well beyond two weeks after issuance in practice (the reference
/// audit's example was reissued invoices cancelled months later), so ruling
/// a Vigente row out of scope needs a longer horizon than the cancelled-side
/// check does.
const VIGENTE_RECENT_DAYS: i32 = 180;
/// Separate budget from `BATCH_LIMIT` — the Vigente universe (tens of
/// thousands of never-checked I/E rows platform-wide) is much larger than the
/// cancelled one, but each cycle still only spends a bounded number of SAT
/// lookups on it; the backlog drains gradually the same way the cancelled
/// backlog does.
const VIGENTE_BATCH_LIMIT: i64 = 300;

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
    let mut candidates = db::cfdis::find_cancelled_recheck_candidates(
        pool,
        MIN_RECHECK_HOURS,
        RECENT_DAYS,
        BATCH_LIMIT,
    )
    .await?;
    let vigente_candidates = db::cfdis::find_vigente_recheck_candidates(
        pool,
        VIGENTE_MIN_RECHECK_HOURS,
        VIGENTE_RECENT_DAYS,
        VIGENTE_BATCH_LIMIT,
    )
    .await?;
    tracing::info!(
        cancelled = candidates.len(),
        vigente = vigente_candidates.len(),
        "Recheck-cancelled: candidates this cycle"
    );
    candidates.extend(vigente_candidates);
    if candidates.is_empty() {
        return Ok(());
    }

    let creds: HashMap<String, String> = db::users::get_all_with_credentials(pool)
        .await?
        .into_iter()
        .collect();

    // Group by (owner_rfc, download_type): the owner is whichever side of the
    // invoice is a tracked Pulso RFC — that's the identity we can log into SAT
    // with to look the UUID up. Rows where neither side is tracked anymore
    // (RFC removed after ingestion) can't be checked; mark them seen so they
    // stop resurfacing every cycle.
    let mut groups: HashMap<(String, &'static str), Vec<String>> = HashMap::new();
    let mut orphaned: Vec<String> = Vec::new();
    for (uuid, rfc_emisor, rfc_receptor) in candidates {
        if creds.contains_key(&rfc_emisor) {
            groups
                .entry((rfc_emisor, "emitidos"))
                .or_default()
                .push(uuid);
        } else if creds.contains_key(&rfc_receptor) {
            groups
                .entry((rfc_receptor, "recibidos"))
                .or_default()
                .push(uuid);
        } else {
            orphaned.push(uuid);
        }
    }
    if !orphaned.is_empty() {
        tracing::info!(
            count = orphaned.len(),
            "Recheck-cancelled: no tracked RFC owns these, skipped"
        );
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
        let (auth_payload, _fiel_tmp) = match crate::try_fiel_auth(pool, s3, &bucket, &owner_rfc)
            .await
        {
            Some((fiel_auth, tmp)) => (fiel_auth, Some(tmp)),
            None => {
                let Some(clave_enc) = creds.get(&owner_rfc) else {
                    continue;
                };
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
            let Some(uuid) = inv.get("uuid").and_then(|v| v.as_str()) else {
                continue;
            };
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
                // SAT's response didn't include this UUID — inconclusive, not
                // a confirmation of anything. Keep retrying every cycle
                // (checked_at stays untouched) until MAX_MISS_ATTEMPTS, so one
                // empty response can't strand an invoice past the recency gate.
                let attempts =
                    db::cfdis::record_estado_sat_miss(pool, uuid, MAX_MISS_ATTEMPTS).await?;
                if attempts >= MAX_MISS_ATTEMPTS {
                    tracing::warn!(
                        uuid = %uuid, rfc = %owner_rfc, attempts,
                        "Recheck-cancelled: giving up, SAT never returned this UUID after {attempts} attempts"
                    );
                }
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
