//! Surfaces `xml_available = -1` corruption to the frontend so a dashboard
//! can warn (or refuse to render) instead of silently showing numbers built
//! from SAT's listing snippet alone — no subtotal, no currency, no payroll
//! detail. See PULSO-021/Anexo B of the Axented audit: today nothing marks
//! these rows, and Axented's dashboards reproduced the corrupted totals to
//! the peso with zero indication anything was wrong.

use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct DataQualitySection {
    pub total: i64,
    pub missing_xml: i64,
    pub missing_ratio: f64,
    /// True once `missing_ratio` exceeds MATERIAL_THRESHOLD — the frontend's
    /// cue to block the view rather than just show a subtle warning.
    pub material: bool,
}

#[derive(Debug, Serialize)]
pub struct DataQualityResponse {
    pub emitidas: DataQualitySection,
    pub recibidas: DataQualitySection,
    pub nomina: DataQualitySection,
}

/// Above this fraction of a section's rows missing their XML, the numbers
/// aren't just "a little off" — subtotal is a guessed /1.16 of total (wrong
/// for anything not at the standard 16% rate), currency/tipo_cambio defaults
/// to MXN 1:1 regardless of what it really was, and payroll detail is simply
/// absent. Every clean RFC seen so far sits under 1%; Axented sits at
/// 48-74% per year. 5% leaves a wide, deliberately conservative margin.
const MATERIAL_THRESHOLD: f64 = 0.05;

fn section(total: i64, missing: i64) -> DataQualitySection {
    let missing_ratio = if total > 0 { missing as f64 / total as f64 } else { 0.0 };
    DataQualitySection {
        total,
        missing_xml: missing,
        missing_ratio,
        material: missing_ratio > MATERIAL_THRESHOLD,
    }
}

pub async fn get(pool: &DbPool, rfc: &str) -> anyhow::Result<DataQualityResponse> {
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE rfc_emisor = $1 AND dl_type IN ('emitidos','ambos') AND tipo_comprobante IN ('I','E'))                                   AS emitidas_total,
            COUNT(*) FILTER (WHERE rfc_emisor = $1 AND dl_type IN ('emitidos','ambos') AND tipo_comprobante IN ('I','E') AND xml_available = -1)             AS emitidas_missing,
            COUNT(*) FILTER (WHERE rfc_receptor = $1 AND dl_type IN ('recibidos','ambos') AND tipo_comprobante IN ('I','E'))                                  AS recibidas_total,
            COUNT(*) FILTER (WHERE rfc_receptor = $1 AND dl_type IN ('recibidos','ambos') AND tipo_comprobante IN ('I','E') AND xml_available = -1)           AS recibidas_missing,
            COUNT(*) FILTER (WHERE (rfc_emisor = $1 OR rfc_receptor = $1) AND tipo_comprobante = 'N')                                                        AS nomina_total,
            COUNT(*) FILTER (WHERE (rfc_emisor = $1 OR rfc_receptor = $1) AND tipo_comprobante = 'N' AND xml_available = -1)                                 AS nomina_missing
        FROM pulso.cfdis
        WHERE rfc_emisor = $1 OR rfc_receptor = $1
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;

    Ok(DataQualityResponse {
        emitidas: section(row.try_get("emitidas_total")?, row.try_get("emitidas_missing")?),
        recibidas: section(row.try_get("recibidas_total")?, row.try_get("recibidas_missing")?),
        nomina: section(row.try_get("nomina_total")?, row.try_get("nomina_missing")?),
    })
}
