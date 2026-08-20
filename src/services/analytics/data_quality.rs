//! Surfaces two distinct ways an RFC's numbers can look fine but not be:
//! rows that exist but were built without their XML (`xml_available = -1` —
//! no subtotal, no currency, no payroll detail), and calendar months inside
//! the RFC's own synced range that have *no rows at all*. See PULSO-021/
//! Anexo B of the Axented audit for the first; the second is the same blind
//! spot Anexo B.4 flags separately ("que la interfaz no pueda pintar un
//! periodo... cuando existe una ventana sin cobertura de jobs") — confirmed
//! live on 2026-08-18 when Axented's own missing-XML ratio read a clean 0.3%
//! (after the historical rebuild) while ~15 months of it still had zero
//! CFDIs, because the rebuild hadn't reached them yet. A ratio-of-bad-rows
//! check can't see a month with no rows to be bad — it needs the coverage
//! check below.

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
    pub coverage: CoverageInfo,
}

#[derive(Debug, Serialize)]
pub struct CoverageInfo {
    pub range_from: Option<String>,
    pub range_to: Option<String>,
    pub expected_months: i64,
    pub empty_months: i64,
    pub gap_ratio: f64,
    /// True once a material fraction of the RFC's own declared sync range
    /// has zero CFDIs — a historical rebuild still in progress, or a
    /// coverage hole nothing ever re-triggered, both read as "healthy" to
    /// the missing-XML check above since there are no bad rows, just absent
    /// ones.
    pub material: bool,
}

/// Same conservative-margin philosophy as MATERIAL_THRESHOLD, tuned for a
/// different failure shape: one or two genuinely slow months is normal
/// business variance, but a tenth of the RFC's own declared range sitting
/// completely empty means the sync itself hasn't caught up, not that the
/// business went quiet.
const COVERAGE_MATERIAL_THRESHOLD: f64 = 0.10;

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
        coverage: coverage(pool, rfc).await,
    })
}

fn year_month(date: &str) -> Option<(i64, i64)> {
    if date.len() < 7 {
        return None;
    }
    let y = date[0..4].parse().ok()?;
    let m = date[5..7].parse().ok()?;
    Some((y, m))
}

async fn coverage(pool: &DbPool, rfc: &str) -> CoverageInfo {
    let empty = CoverageInfo {
        range_from: None,
        range_to: None,
        expected_months: 0,
        empty_months: 0,
        gap_ratio: 0.0,
        material: false,
    };

    let Ok(Some((from, to))) = crate::db::jobs::rfc_job_range(pool, rfc).await else {
        return empty;
    };
    let (Some((fy, fm)), Some((ty, tm))) = (year_month(&from), year_month(&to)) else {
        return CoverageInfo { range_from: Some(from), range_to: Some(to), ..empty };
    };
    let months_with_data = crate::db::cfdis::months_with_data(pool, rfc).await.unwrap_or_default();

    let from_abs = fy * 12 + fm;
    let to_abs = ty * 12 + tm;
    let expected_months = (to_abs - from_abs + 1).max(0);
    let mut empty_months = 0i64;
    let mut abs = from_abs;
    while abs <= to_abs {
        let year = (abs - 1) / 12;
        let month = ((abs - 1) % 12) + 1;
        if !months_with_data.contains(&(year, month)) {
            empty_months += 1;
        }
        abs += 1;
    }
    let gap_ratio = if expected_months > 0 {
        empty_months as f64 / expected_months as f64
    } else {
        0.0
    };

    CoverageInfo {
        range_from: Some(from),
        range_to: Some(to),
        expected_months,
        empty_months,
        gap_ratio,
        material: gap_ratio > COVERAGE_MATERIAL_THRESHOLD,
    }
}
