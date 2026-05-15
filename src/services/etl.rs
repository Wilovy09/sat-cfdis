/// Background ETL worker: reads job_invoices, fetches XMLs from storage,
/// parses CFDI data, and populates normalized cfdis tables.
use crate::{
    config::Config,
    db::{self, DbPool},
    services::{storage, xml_parser},
};
use aws_sdk_s3::Client as S3Client;
use std::sync::Arc;

const ETL_POLL_SECS: u64 = 120;
const BATCH_SIZE: usize = 100;
const ENRICH_BATCH: i64 = 50;

pub async fn etl_worker(pool: DbPool, cfg: Arc<Config>, s3: Arc<S3Client>) {
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
            if let Err(e) = enrich_job(&pool, &cfg, &s3, &job_id).await {
                tracing::error!(job_id = %job_id, "ETL: error enriching job: {e}");
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

    // Insert nomina data
    if let Some(nomina) = &cfdi.nomina {
        if let Err(e) = db::cfdis::insert_nomina(pool, &cfdi.uuid, nomina).await {
            tracing::warn!(uuid = %uuid, "ETL: insert_nomina: {e}");
        }
    }
}

/// Re-processes invoices that were parsed from metadata only.
/// Fetches XML from storage; if found, enriches taxes/payments/concepts in place.
async fn enrich_job(
    pool: &DbPool,
    cfg: &Config,
    s3: &S3Client,
    job_id: &str,
) -> anyhow::Result<()> {
    let pending = db::cfdis::find_needs_enrichment(pool, job_id, ENRICH_BATCH).await?;
    if pending.is_empty() {
        return Ok(());
    }

    tracing::info!(job_id = %job_id, count = pending.len(), "ETL: enriching invoices");

    for (uuid, metadata) in &pending {
        enrich_invoice(pool, cfg, s3, uuid, metadata).await;
    }

    Ok(())
}

async fn enrich_invoice(
    pool: &DbPool,
    cfg: &Config,
    s3: &S3Client,
    uuid: &str,
    metadata: &str,
) {
    let xml_bytes = try_load_xml(cfg, s3, uuid, metadata).await;
    let Some(bytes) = xml_bytes else {
        return; // XML not in storage yet — will retry next cycle
    };

    let estado = extract_estado_from_meta(metadata);

    // job_id and dl_type don't affect enrichment (upsert ON CONFLICT preserves originals)
    let Some(mut cfdi) = xml_parser::parse(&bytes, "", "ambos", &estado) else {
        tracing::warn!(uuid = %uuid, "ETL enrich: could not parse XML");
        return;
    };

    cfdi.uuid = uuid.to_uppercase();

    // Update header row so xml_available flips to true
    if let Err(e) = db::cfdis::upsert_cfdi(pool, &cfdi).await {
        tracing::warn!(uuid = %uuid, "ETL enrich: upsert_cfdi failed: {e}");
        return;
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
}

/// Attempt to load XML bytes from local storage or S3.
/// Returns None if not found or config not set.
async fn try_load_xml(cfg: &Config, s3: &S3Client, uuid: &str, metadata: &str) -> Option<Vec<u8>> {
    // Extract path components from metadata
    let (rfc_e, rfc_r, year, month, day) = extract_path_from_meta(metadata);

    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    storage::get(s3, &bucket, &rfc_e, &rfc_r, year, month, day, uuid).await
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
        .unwrap_or("vigente")
        .to_lowercase()
}
