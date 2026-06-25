use super::summary::{dl_type_filter, rfc_column};
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct XmlCountResponse {
    pub dl_type: String,
    pub total: i64,
    pub available: i64,
    pub unavailable: i64,
    pub pending: i64,
}

pub async fn get(pool: &DbPool, rfc: &str, dl_type: &str) -> anyhow::Result<XmlCountResponse> {
    let q = if dl_type == "ambos" {
        // UNION: emitidos (rfc_emisor) + recibidos (rfc_receptor)
        "SELECT \
            COUNT(*)::bigint                                            AS total, \
            COUNT(*) FILTER (WHERE xml_available = 1)::bigint          AS available, \
            COUNT(*) FILTER (WHERE xml_available = -1)::bigint         AS unavailable, \
            COUNT(*) FILTER (WHERE xml_available = 0)::bigint          AS pending \
         FROM ( \
             SELECT xml_available FROM pulso.cfdis \
             WHERE rfc_emisor = $1 AND dl_type IN ('emitidos','ambos') \
               AND NOT is_cancelled \
             UNION ALL \
             SELECT xml_available FROM pulso.cfdis \
             WHERE rfc_receptor = $1 AND dl_type IN ('recibidos','ambos') \
               AND NOT is_cancelled \
         ) u".to_string()
    } else {
        let owner_col = rfc_column(dl_type);
        let dl_filter = dl_type_filter(dl_type);
        format!(
            "SELECT \
                COUNT(*)::bigint                                            AS total, \
                COUNT(*) FILTER (WHERE xml_available = 1)::bigint          AS available, \
                COUNT(*) FILTER (WHERE xml_available = -1)::bigint         AS unavailable, \
                COUNT(*) FILTER (WHERE xml_available = 0)::bigint          AS pending \
             FROM pulso.cfdis \
             WHERE {owner_col} = $1 AND {dl_filter} \
               AND NOT is_cancelled"
        )
    };

    let row = sqlx::query(&q).bind(rfc).fetch_one(pool).await?;

    Ok(XmlCountResponse {
        dl_type: dl_type.to_string(),
        total: row.try_get("total").unwrap_or(0),
        available: row.try_get("available").unwrap_or(0),
        unavailable: row.try_get("unavailable").unwrap_or(0),
        pending: row.try_get("pending").unwrap_or(0),
    })
}
