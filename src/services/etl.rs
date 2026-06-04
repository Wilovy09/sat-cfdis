/// Background ETL worker: reads job_invoices, fetches XMLs from storage,
/// parses CFDI data, and populates normalized cfdis tables.
use crate::{
    config::Config,
    db::{self, DbPool},
    services::{php_cli::PhpCli, storage, xml_parser},
};
use aws_sdk_s3::Client as S3Client;
use tempfile::TempDir;
use std::collections::HashMap;
use std::sync::Arc;

const ETL_POLL_SECS: u64 = 120;
const BATCH_SIZE: usize = 100;
const ENRICH_BATCH: i64 = 50;
/// Max skip cycles before a job is considered permanently unresolvable this session (~24h).
const ENRICH_MAX_SKIP: u32 = 720;
/// Rounds of failed SAT downloads before marking CFDIs as permanently unavailable.
const ENRICH_MAX_SAT_FAIL: u32 = 5;

pub async fn etl_worker(pool: DbPool, cfg: Arc<Config>, s3: Arc<S3Client>) {
    // Tracks remaining skip cycles per job_id. Doubles on each failed round, capped at ENRICH_MAX_SKIP.
    let mut enrich_skip: HashMap<String, u32> = HashMap::new();
    let mut enrich_fail_rounds: HashMap<String, u32> = HashMap::new();
    // Counts consecutive rounds where SAT download was attempted but nothing enriched.
    let mut enrich_sat_fail_rounds: HashMap<String, u32> = HashMap::new();

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(ETL_POLL_SECS)).await;

        // Normal ETL: insert new invoices not yet in cfdis
        let job_ids = match db::cfdis::jobs_needing_etl(&pool).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::error!("ETL: DB error finding jobs: {e}");
                continue;
            }
        };
        for job_id in job_ids {
            // Reset backoff when a job gets new invoices
            enrich_skip.remove(&job_id);
            enrich_fail_rounds.remove(&job_id);
            enrich_sat_fail_rounds.remove(&job_id);
            if let Err(e) = process_job(&pool, &cfg, &s3, &job_id).await {
                tracing::error!(job_id = %job_id, "ETL: error processing job: {e}");
            }
        }

        // Enrichment: re-try invoices parsed from metadata that may now have XML in storage
        let enrich_ids = match db::cfdis::jobs_needing_enrichment(&pool).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::error!("ETL: DB error finding enrichment jobs: {e}");
                continue;
            }
        };
        for job_id in enrich_ids {
            // Decrement and skip if in backoff
            if let Some(remaining) = enrich_skip.get_mut(&job_id) {
                if *remaining > 0 {
                    *remaining -= 1;
                    continue;
                }
            }

            let (enriched, sat_failed) = match enrich_job(&pool, &cfg, &s3, &job_id).await {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!(job_id = %job_id, "ETL: error enriching job: {e}");
                    (0, 0)
                }
            };

            if enriched > 0 {
                // Progress made — reset all counters
                enrich_skip.remove(&job_id);
                enrich_fail_rounds.remove(&job_id);
                enrich_sat_fail_rounds.remove(&job_id);
            } else {
                // No enrichment this round — apply backoff
                let rounds = enrich_fail_rounds.entry(job_id.clone()).or_insert(0);
                *rounds += 1;
                let skip = (2u32.saturating_pow(*rounds)).min(ENRICH_MAX_SKIP);
                tracing::debug!(job_id = %job_id, rounds = *rounds, skip_cycles = skip, "ETL: enrichment backoff");
                enrich_skip.insert(job_id.clone(), skip);

                // Track SAT-download failures separately
                if sat_failed > 0 {
                    let sat_rounds = enrich_sat_fail_rounds.entry(job_id.clone()).or_insert(0);
                    *sat_rounds += 1;
                    tracing::warn!(
                        job_id = %job_id,
                        sat_fail_rounds = *sat_rounds,
                        max = ENRICH_MAX_SAT_FAIL,
                        "ETL: SAT download round failed"
                    );

                    if *sat_rounds >= ENRICH_MAX_SAT_FAIL {
                        // Give up — mark remaining as permanently unavailable
                        match db::cfdis::mark_xml_unavailable_for_job(&pool, &job_id).await {
                            Ok(n) if n > 0 => {
                                tracing::warn!(
                                    job_id = %job_id,
                                    marked = n,
                                    "ETL: marked CFDIs as xml_available=-1 (SAT unreachable after retries)"
                                );
                            }
                            Ok(_) => {}
                            Err(e) => tracing::error!(job_id = %job_id, "ETL: mark_xml_unavailable failed: {e}"),
                        }
                        enrich_skip.remove(&job_id);
                        enrich_fail_rounds.remove(&job_id);
                        enrich_sat_fail_rounds.remove(&job_id);
                    }
                }
            }
        }
    }
}

async fn process_job(
    pool: &DbPool,
    cfg: &Config,
    s3: &S3Client,
    job_id: &str,
) -> anyhow::Result<()> {
    // Get job metadata (dl_type, rfc)
    let job = match db::jobs::get_by_id(pool, job_id).await? {
        Some(j) => j,
        None => return Ok(()),
    };

    let pending = db::cfdis::find_pending_etl(pool, job_id).await?;
    if pending.is_empty() {
        return Ok(());
    }

    tracing::info!(
        job_id = %job_id,
        rfc    = %job.rfc,
        count  = pending.len(),
        "ETL: processing invoices"
    );

    for chunk in pending.chunks(BATCH_SIZE) {
        for (uuid, metadata) in chunk {
            process_invoice(pool, cfg, s3, job_id, uuid, metadata, &job.dl_type).await;
        }
    }

    Ok(())
}

async fn process_invoice(
    pool: &DbPool,
    cfg: &Config,
    s3: &S3Client,
    job_id: &str,
    uuid: &str,
    metadata: &str,
    dl_type: &str,
) {
    // Try to find the XML in storage first
    let xml_bytes = try_load_xml(cfg, s3, uuid, metadata).await;

    let cfdi = if let Some(bytes) = &xml_bytes {
        // Parse from full XML
        let estado = extract_estado_from_meta(metadata);
        xml_parser::parse(bytes, job_id, dl_type, &estado)
    } else {
        // Fallback: build from metadata JSON (no XML)
        let preview_end = metadata.char_indices().nth(120).map(|(i,_)| i).unwrap_or(metadata.len());
        tracing::warn!(uuid = %uuid, meta_preview = %&metadata[..preview_end], "ETL: no XML in storage, trying from_metadata");
        xml_parser::from_metadata(metadata, job_id, dl_type)
    };

    let Some(mut cfdi) = cfdi else {
        tracing::warn!(uuid = %uuid, "ETL: could not parse invoice, skipping");
        return;
    };

    // Always normalize UUID to uppercase to match job_invoices
    if cfdi.uuid.is_empty() {
        cfdi.uuid = uuid.to_uppercase();
    } else {
        cfdi.uuid = cfdi.uuid.to_uppercase();
    }

    // Insert header
    if let Err(e) = db::cfdis::upsert_cfdi(pool, &cfdi).await {
        tracing::error!(uuid = %uuid, "ETL: upsert_cfdi failed: {e}");
        return;
    }

    // Insert taxes
    if !cfdi.taxes.is_empty() {
        if let Err(e) = db::cfdis::insert_taxes(pool, &cfdi.uuid, &cfdi.taxes).await {
            tracing::warn!(uuid = %uuid, "ETL: insert_taxes: {e}");
        }
    }

    // Insert concepts (only if XML was available to avoid duplicates)
    if xml_bytes.is_some() && !cfdi.concepts.is_empty() {
        if let Err(e) = db::cfdis::insert_concepts(pool, &cfdi.uuid, &cfdi.concepts).await {
            tracing::warn!(uuid = %uuid, "ETL: insert_concepts: {e}");
        }
    }

    // Insert payment complement data
    if !cfdi.payments.is_empty() {
        if let Err(e) = db::cfdis::insert_payments(pool, &cfdi.uuid, &cfdi.payments).await {
            tracing::warn!(uuid = %uuid, "ETL: insert_payments: {e}");
        }
    }

    // Insert cfdi_relacionados (credit notes, etc.)
    if !cfdi.relacionados.is_empty() {
        if let Err(e) = db::cfdis::insert_relacionados(pool, &cfdi.uuid, &cfdi.relacionados).await {
            tracing::warn!(uuid = %uuid, "ETL: insert_relacionados: {e}");
        }
    }

    // Insert nomina data
    if let Some(nomina) = &cfdi.nomina {
        if let Err(e) = db::cfdis::insert_nomina(pool, &cfdi.uuid, nomina).await {
            tracing::warn!(uuid = %uuid, "ETL: insert_nomina: {e}");
        }
    }
}

/// Re-processes invoices that were parsed from metadata only.
/// First tries storage, then downloads from SAT.
/// Returns (enriched, sat_failed).
async fn enrich_job(
    pool: &DbPool,
    cfg: &Config,
    s3: &S3Client,
    job_id: &str,
) -> anyhow::Result<(usize, usize)> {
    let pending = db::cfdis::find_needs_enrichment(pool, job_id, ENRICH_BATCH).await?;
    if pending.is_empty() {
        return Ok((0, 0));
    }

    // Load job for auth credentials (needed for SAT download fallback)
    let job = db::jobs::get_by_id(pool, job_id).await?;

    tracing::info!(job_id = %job_id, count = pending.len(), "ETL: enriching invoices");

    let mut enriched = 0usize;
    let mut sat_failed = 0usize;
    for (uuid, metadata) in &pending {
        let (ok, tried_sat) = enrich_invoice(pool, cfg, s3, job.as_ref(), uuid, metadata).await;
        if ok {
            enriched += 1;
        } else if tried_sat {
            sat_failed += 1;
        }
    }

    if enriched > 0 {
        tracing::info!(job_id = %job_id, enriched, "ETL: enrichment batch done");
    }

    Ok((enriched, sat_failed))
}

/// Returns (enriched, tried_sat). Tries storage first, then SAT download.
async fn enrich_invoice(
    pool: &DbPool,
    cfg: &Config,
    s3: &S3Client,
    job: Option<&db::jobs::SyncJob>,
    uuid: &str,
    metadata: &str,
) -> (bool, bool) {
    // 1. Try storage
    let xml_bytes = try_load_xml(cfg, s3, uuid, metadata).await;

    // 2. If not in storage, try to download from SAT using the job's credentials
    let (xml_bytes, tried_sat) = if xml_bytes.is_none() {
        if let Some(j) = job {
            let sat_bytes = try_download_from_sat(cfg, s3, j, uuid, metadata).await;
            let tried = sat_bytes.is_none(); // only "tried and failed" if still None
            (sat_bytes, tried)
        } else {
            tracing::warn!(uuid = %uuid, "ETL enrich: XML not found in storage, no job credentials");
            (None, false)
        }
    } else {
        (xml_bytes, false)
    };

    let Some(bytes) = xml_bytes else {
        return (false, tried_sat);
    };

    let estado = extract_estado_from_meta(metadata);

    // job_id and dl_type don't affect enrichment (upsert ON CONFLICT preserves originals)
    let Some(mut cfdi) = xml_parser::parse(&bytes, "", "ambos", &estado) else {
        tracing::warn!(uuid = %uuid, "ETL enrich: could not parse XML");
        return (false, tried_sat);
    };

    cfdi.uuid = uuid.to_uppercase();

    // Update header row so xml_available flips to true
    if let Err(e) = db::cfdis::upsert_cfdi(pool, &cfdi).await {
        tracing::warn!(uuid = %uuid, "ETL enrich: upsert_cfdi failed: {e}");
        return (false, tried_sat);
    }

    if !cfdi.taxes.is_empty() {
        if let Err(e) = db::cfdis::insert_taxes(pool, &cfdi.uuid, &cfdi.taxes).await {
            tracing::warn!(uuid = %uuid, "ETL enrich: insert_taxes: {e}");
        }
    }

    if !cfdi.payments.is_empty() {
        if let Err(e) = db::cfdis::insert_payments(pool, &cfdi.uuid, &cfdi.payments).await {
            tracing::warn!(uuid = %uuid, "ETL enrich: insert_payments: {e}");
        }
    }

    if !cfdi.relacionados.is_empty() {
        if let Err(e) = db::cfdis::insert_relacionados(pool, &cfdi.uuid, &cfdi.relacionados).await {
            tracing::warn!(uuid = %uuid, "ETL enrich: insert_relacionados: {e}");
        }
    }

    if !cfdi.concepts.is_empty() && !db::cfdis::concepts_exist(pool, &cfdi.uuid).await {
        if let Err(e) = db::cfdis::insert_concepts(pool, &cfdi.uuid, &cfdi.concepts).await {
            tracing::warn!(uuid = %uuid, "ETL enrich: insert_concepts: {e}");
        }
    }

    if let Some(nomina) = &cfdi.nomina {
        if let Err(e) = db::cfdis::insert_nomina(pool, &cfdi.uuid, nomina).await {
            tracing::warn!(uuid = %uuid, "ETL enrich: insert_nomina: {e}");
        }
    }

    tracing::debug!(uuid = %uuid, "ETL enrich: enriched from XML");
    (true, false)
}

/// Attempt to load XML bytes from local storage or S3.
/// Returns None if not found or config not set.
async fn try_load_xml(cfg: &Config, s3: &S3Client, uuid: &str, metadata: &str) -> Option<Vec<u8>> {
    let (rfc_e, rfc_r, year, month, day) = extract_path_from_meta(metadata);
    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    let uuid_lower = uuid.to_lowercase();
    tracing::debug!(
        uuid = %uuid,
        path = %format!("cfdis/{rfc_e}/{rfc_r}/{year}/{month:02}/{day:02}/{uuid_lower}.xml"),
        "ETL: looking up XML in storage"
    );
    let result = storage::get(s3, &bucket, &rfc_e, &rfc_r, year, month, day, &uuid_lower).await;
    if result.is_none() {
        tracing::warn!(
            uuid    = %uuid,
            rfc_e   = %rfc_e,
            rfc_r   = %rfc_r,
            date    = %format!("{year}-{month:02}-{day:02}"),
            "ETL: XML not in storage — path: cfdis/{rfc_e}/{rfc_r}/{year}/{month:02}/{day:02}/{uuid_lower}.xml"
        );
    }
    result
}

/// Try to download a single XML from SAT using the job's stored credentials.
/// On success, uploads the XML to storage and returns the bytes.
async fn try_download_from_sat(
    cfg: &Config,
    s3: &S3Client,
    job: &db::jobs::SyncJob,
    uuid: &str,
    metadata: &str,
) -> Option<Vec<u8>> {
    // Decrypt auth payload
    let key = crate::services::crypto::load_key();
    let auth_json = crate::services::crypto::decrypt(&key, &job.auth_enc).ok()?;
    let auth: serde_json::Value = serde_json::from_str(&auth_json).ok()?;

    // Determine download_type from RFC ownership
    let (rfc_e, rfc_r, year, month, day) = extract_path_from_meta(metadata);
    let download_type = if job.rfc.eq_ignore_ascii_case(&rfc_r) {
        "recibidos"
    } else {
        "emitidos"
    };

    // Temp dir for the PHP CLI output
    let work_dir = TempDir::new().ok()?;
    let output_dir = work_dir.path().join("xml");
    tokio::fs::create_dir_all(&output_dir).await.ok()?;

    let payload = serde_json::json!({
        "command": "download",
        "auth": auth,
        "params": {
            "uuids":         [uuid.to_lowercase()],
            "download_type": download_type,
            "resource_type": "xml",
            "output_dir":    output_dir.to_string_lossy(),
        }
    });

    let cli = PhpCli::new(&cfg.php_bin, &cfg.php_cli_path);
    let result = match cli.run(&payload).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(uuid = %uuid, "ETL: SAT download failed: {e}");
            return None;
        }
    };

    let path = result["files"]
        .as_array()
        .and_then(|f| f.first())
        .and_then(|f| f["path"].as_str())
        .map(|s| s.to_string())?;

    let bytes = tokio::fs::read(&path).await.ok()?;

    // Upload to S3 so future enrichment rounds find it in storage
    let uuid_lower = uuid.to_lowercase();
    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    let _ = storage::upload(s3, &bucket, &rfc_e, &rfc_r, year, month, day, &uuid_lower, bytes.clone()).await;

    tracing::info!(uuid = %uuid, download_type, "ETL: downloaded XML from SAT");
    Some(bytes)
}

fn extract_path_from_meta(metadata: &str) -> (String, String, u32, u32, u32) {
    let v: serde_json::Value = serde_json::from_str(metadata).unwrap_or_default();

    let rfc_e = v["rfcEmisor"]
        .as_str()
        .or_else(|| v["RfcEmisor"].as_str())
        .or_else(|| v["rfc_emisor"].as_str())
        .unwrap_or("UNKNOWN")
        .to_uppercase();

    let rfc_r = v["rfcReceptor"]
        .as_str()
        .or_else(|| v["RfcReceptor"].as_str())
        .or_else(|| v["rfc_receptor"].as_str())
        .unwrap_or("UNKNOWN")
        .to_uppercase();

    let fecha = v["fecha"]
        .as_str()
        .or_else(|| v["Fecha"].as_str())
        .or_else(|| v["fechaEmision"].as_str())
        .unwrap_or("2000-01-01");

    let parts: Vec<&str> = fecha.splitn(4, |c| c == '-' || c == 'T').collect();
    let year = parts.first().and_then(|s| s.parse().ok()).unwrap_or(2000);
    let month = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
    let day = parts
        .get(2)
        .and_then(|s| s[..2.min(s.len())].parse().ok())
        .unwrap_or(1);

    (rfc_e, rfc_r, year, month, day)
}

fn extract_estado_from_meta(metadata: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(metadata).unwrap_or_default();
    v["estado"]
        .as_str()
        .or_else(|| v["Estado"].as_str())
        .or_else(|| v["estadoComprobante"].as_str())
        .or_else(|| v["EstadoComprobante"].as_str())
        .unwrap_or("vigente")
        .to_lowercase()
}
