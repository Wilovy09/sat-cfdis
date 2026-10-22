use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct XmlBreakdownRow {
    pub tipo_comprobante: String,
    pub estado_sat: String,
    pub xml_available: i64,
    pub total: i64,
}

#[derive(Debug, Serialize)]
pub struct XmlBreakdownResponse {
    pub rows: Vec<XmlBreakdownRow>,
    pub grand_total: i64,
    // L10-11 / AUD-100: the full universe's own date range and how much of it sits before
    // the analysis window -- "XMLs disponibles" used to be computed from the already-
    // windowed emitidos/recibidos summaries, so it always agreed with "Período consultado"
    // by construction and could never tell an analyst there's more the platform hasn't
    // analyzed. Both come from this same unfiltered query (rfc_emisor OR rfc_receptor, no
    // tipo/cancelación/exclusión/ventana filter), same as `grand_total` above.
    pub min_fecha: Option<String>,
    pub max_fecha: Option<String>,
    pub out_of_window_count: i64,
}

// DEC-054: the analysis window starts here by design (three full ejercicios plus the
// current one) -- fixed, not derived from today's date. Matches the frontend's own
// `defaultFrom = '2023-01'`.
const ANALYSIS_WINDOW_START: &str = "2023-01-01";

pub async fn get(pool: &DbPool, rfc: &str) -> anyhow::Result<XmlBreakdownResponse> {
    let rows = sqlx::query(
        r#"
        SELECT
            COALESCE(tipo_comprobante, '?')  AS tipo_comprobante,
            COALESCE(estado_sat, 'vigente')  AS estado_sat,
            xml_available::bigint            AS xml_available,
            COUNT(*)::bigint                 AS total
        FROM pulso.cfdis
        WHERE rfc_emisor = $1 OR rfc_receptor = $1
        GROUP BY tipo_comprobante, estado_sat, xml_available
        ORDER BY total DESC
        "#,
    )
    .bind(rfc)
    .fetch_all(pool)
    .await?;

    let mut result: Vec<XmlBreakdownRow> = Vec::new();
    let mut grand_total: i64 = 0;

    for row in rows {
        let total: i64 = row.try_get("total").unwrap_or(0);
        grand_total += total;
        result.push(XmlBreakdownRow {
            tipo_comprobante: row.try_get("tipo_comprobante").unwrap_or_default(),
            estado_sat: row.try_get("estado_sat").unwrap_or_default(),
            xml_available: row.try_get("xml_available").unwrap_or(0),
            total,
        });
    }

    let range_row = sqlx::query(
        r#"
        SELECT
            MIN(fecha_emision::date)::text AS min_fecha,
            MAX(fecha_emision::date)::text AS max_fecha,
            COUNT(*) FILTER (WHERE fecha_emision::date < $2::date)::bigint AS out_of_window
        FROM pulso.cfdis
        WHERE rfc_emisor = $1 OR rfc_receptor = $1
        "#,
    )
    .bind(rfc)
    .bind(ANALYSIS_WINDOW_START)
    .fetch_one(pool)
    .await?;

    Ok(XmlBreakdownResponse {
        rows: result,
        grand_total,
        min_fecha: range_row.try_get("min_fecha").ok().flatten(),
        max_fecha: range_row.try_get("max_fecha").ok().flatten(),
        out_of_window_count: range_row.try_get("out_of_window").unwrap_or(0),
    })
}
