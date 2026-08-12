//! Background worker: re-downloads the real XML for CFDIs permanently marked
//! `xml_available = -1` — the corruption behind the Axented audit (ADC101206334):
//! subtotal guessed as total/1.16 (wrong for anything not at the standard 16%
//! rate), currency/tipo_cambio defaulted to MXN 1:1 regardless of the truth,
//! and payroll detail (`cfdi_nomina`) simply absent, because none of that is
//! in SAT's listing metadata — only the real XML has it.
//!
//! The existing ETL enrichment path (`etl::jobs_needing_enrichment`) only
//! ever looks at `xml_available = 0` — rows that reach `-1` were deliberately
//! excluded from any future retry once `mark_xml_unavailable_for_job` gives
//! up. This worker is the intentional exception: a slower, batched sweep
//! that specifically targets the `-1` backlog, reusing the exact same
//! "parse real XML, write everything" step (`etl::apply_xml_bytes`) so a
//! recovered row is indistinguishable from one that had its XML from the
//! start.
//!
//! Batching matters here: each PHP CLI invocation is a fresh process with its
//! own SAT login, and `listByUuids` still fetches one UUID at a time once
//! logged in — chunking many UUIDs into one `download` call amortizes the
//! login cost across the chunk instead of paying it per UUID.

use crate::{
    config::Config,
    db::{self, DbPool},
    services::{crypto, etl, php_cli::PhpCli, storage},
};
use aws_sdk_s3::Client as S3Client;
use std::collections::HashMap;
use std::sync::Arc;

/// How often the worker wakes up.
const REDOWNLOAD_POLL_SECS: u64 = 6 * 3600;
/// Candidates pulled per cycle, across all RFCs — a large historical backlog
/// (Axented: ~4,176) drains over several cycles rather than all at once.
const BATCH_LIMIT: i64 = 2000;
/// UUIDs per single PHP `download` call. Bigger chunks amortize the SAT
/// login better; kept well under SAT's daily download ceiling per attempt.
const CHUNK_SIZE: usize = 100;
/// Re-download attempts before accepting a UUID as unrecoverable (genuinely
/// purged from SAT's portal, or some other permanent block) instead of
/// retrying forever.
const MAX_ATTEMPTS: i32 = 5;

pub async fn worker(pool: DbPool, cfg: Arc<Config>, s3: Arc<S3Client>) {
    // Let the other startup workers get a head start.
    tokio::time::sleep(std::time::Duration::from_secs(240)).await;

    loop {
        if let Err(e) = run_cycle(&pool, &cfg, &s3).await {
            tracing::error!("Xml-redownload: cycle error: {e}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(REDOWNLOAD_POLL_SECS)).await;
    }
}

async fn run_cycle(pool: &DbPool, cfg: &Arc<Config>, s3: &Arc<S3Client>) -> anyhow::Result<()> {
    let candidates = db::cfdis::find_needing_redownload(pool, MAX_ATTEMPTS, BATCH_LIMIT).await?;
    if candidates.is_empty() {
        return Ok(());
    }
    tracing::info!(count = candidates.len(), "Xml-redownload: candidates this cycle");

    let creds: HashMap<String, String> =
        db::users::get_all_with_credentials(pool).await?.into_iter().collect();

    // Group by (owner_rfc, download_type) — same ownership rule as
    // recheck_cancelled.rs: whichever side of the CFDI is a tracked Pulso RFC
    // is the identity we can log into SAT with.
    let mut groups: HashMap<(String, &'static str), Vec<(String, String)>> = HashMap::new();
    let mut orphaned: Vec<String> = Vec::new();
    for (uuid, rfc_emisor, rfc_receptor, metadata) in candidates {
        if creds.contains_key(&rfc_emisor) {
            groups.entry((rfc_emisor, "emitidos")).or_default().push((uuid, metadata));
        } else if creds.contains_key(&rfc_receptor) {
            groups.entry((rfc_receptor, "recibidos")).or_default().push((uuid, metadata));
        } else {
            orphaned.push(uuid);
        }
    }
    if !orphaned.is_empty() {
        tracing::info!(count = orphaned.len(), "Xml-redownload: no tracked RFC owns these, counting as a miss");
        for uuid in &orphaned {
            let _ = db::cfdis::record_redownload_miss(pool, uuid).await;
        }
    }

    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    let key = crypto::load_key();

    for ((owner_rfc, download_type), items) in groups {
        // _fiel_tmp must stay alive for the whole group, across every chunk —
        // it backs the cert_pem_path/key_pem_path the PHP subprocess reads
        // later. See recheck_cancelled.rs's identical note; got this wrong
        // once already (dropped inside a match arm), fixed the pattern here
        // from the start.
        let (auth_payload, _fiel_tmp) = match crate::try_fiel_auth(pool, s3, &bucket, &owner_rfc).await {
            Some((fiel_auth, tmp)) => (fiel_auth, Some(tmp)),
            None => {
                let Some(clave_enc) = creds.get(&owner_rfc) else { continue };
                match crypto::decrypt(&key, clave_enc) {
                    Ok(clave) => (
                        serde_json::json!({ "type": "ciec", "rfc": owner_rfc, "password": clave }),
                        None::<tempfile::TempDir>,
                    ),
                    Err(e) => {
                        tracing::error!(rfc = %owner_rfc, "Xml-redownload: decrypt failed: {e}");
                        continue;
                    }
                }
            }
        };

        for chunk in items.chunks(CHUNK_SIZE) {
            if let Err(e) =
                redownload_chunk(pool, cfg, s3, &bucket, &owner_rfc, download_type, &auth_payload, chunk).await
            {
                tracing::error!(rfc = %owner_rfc, "Xml-redownload: chunk failed: {e}");
            }
        }
    }

    Ok(())
}

async fn redownload_chunk(
    pool: &DbPool,
    cfg: &Config,
    s3: &Arc<S3Client>,
    bucket: &str,
    owner_rfc: &str,
    download_type: &str,
    auth_payload: &serde_json::Value,
    items: &[(String, String)],
) -> anyhow::Result<()> {
    let work_dir = tempfile::TempDir::new()?;
    let output_dir = work_dir.path().join("xml");
    tokio::fs::create_dir_all(&output_dir).await?;

    let uuids_lower: Vec<String> = items.iter().map(|(uuid, _)| uuid.to_lowercase()).collect();

    let payload = serde_json::json!({
        "command": "download",
        "auth": auth_payload,
        "params": {
            "uuids":         uuids_lower,
            "download_type": download_type,
            "resource_type": "xml",
            "output_dir":    output_dir.to_string_lossy(),
        }
    });

    let cli = PhpCli::new(&cfg.php_bin, &cfg.php_cli_path).with_proxy(cfg.https_proxy.clone());
    // A whole-chunk failure (auth/captcha/connection) is left alone entirely —
    // no misses recorded — so the same UUIDs are simply retried next cycle
    // rather than burning down their attempt budget for a problem that wasn't
    // about any specific UUID.
    cli.run(&payload).await?;

    let mut recovered = 0usize;
    let mut missed = 0usize;
    for (uuid, metadata) in items {
        let path = output_dir.join(format!("{}.xml", uuid.to_lowercase()));
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(_) => {
                let attempts = db::cfdis::record_redownload_miss(pool, uuid).await?;
                missed += 1;
                if attempts >= MAX_ATTEMPTS {
                    tracing::warn!(
                        uuid = %uuid, rfc = %owner_rfc, attempts,
                        "Xml-redownload: giving up, SAT never returned this UUID's XML after {attempts} attempts"
                    );
                }
                continue;
            }
        };

        let (rfc_e, rfc_r, year, month, day) = etl::extract_path_from_meta(metadata);
        let _ = storage::upload(s3, bucket, &rfc_e, &rfc_r, year, month, day, &uuid.to_lowercase(), bytes.clone()).await;

        if etl::apply_xml_bytes(pool, uuid, metadata, &bytes).await {
            recovered += 1;
        } else {
            let _ = db::cfdis::record_redownload_miss(pool, uuid).await;
            missed += 1;
        }
    }

    tracing::info!(
        rfc = %owner_rfc, requested = items.len(), recovered, missed,
        "Xml-redownload: chunk done"
    );
    Ok(())
}
