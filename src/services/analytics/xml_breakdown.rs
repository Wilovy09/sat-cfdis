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
}

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

    Ok(XmlBreakdownResponse {
        rows: result,
        grand_total,
    })
}
