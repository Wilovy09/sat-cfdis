use super::summary::{dl_type_filter, parse_ym, rfc_column};
/// Geography: breakdown by lugar_expedicion (postal code) and state.
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize)]
pub struct GeographyResponse {
    pub by_state: Vec<StateRow>,
    pub by_postal_code: Vec<PostalCodeRow>,
    pub unknown_pct: f64,
}

#[derive(Debug, Serialize)]
pub struct StateRow {
    pub state_code: String,
    pub state_name: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub unique_cp: i64,
    pub pct_of_total: f64,
}

#[derive(Debug, Serialize)]
pub struct PostalCodeRow {
    pub postal_code: String,
    pub state_code: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<GeographyResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_col = if dl_type == "recibidos" { "rfc_emisor" } else { "rfc_receptor" };

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            COALESCE(lugar_expedicion, 'UNKNOWN') AS cp,
            {cp_col}                              AS counterparty_rfc,
            SUM(COALESCE(total_mxn,0)::float8)::float8 AS total,
            COUNT(*)::bigint                      AS cnt
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY cp, {cp_col}
        ORDER BY total DESC
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let grand_total: f64 = rows
        .iter()
        .map(|r| r.try_get::<f64, _>("total").unwrap_or(0.0))
        .sum();

    // state_code → (total_mxn, invoice_count, unique_cp_rfcs)
    let mut state_map: HashMap<String, (f64, i64, HashSet<String>)> = Default::default();
    let mut by_postal_code = Vec::new();
    let mut unknown_total = 0.0f64;

    for r in &rows {
        let cp: String = r.try_get("cp").unwrap_or_default();
        let counterparty_rfc: String = r.try_get("counterparty_rfc").unwrap_or_default();
        let total: f64 = r.try_get("total").unwrap_or(0.0);
        let cnt: i64 = r.try_get("cnt").unwrap_or(0);

        if cp == "UNKNOWN" {
            unknown_total += total;
            continue;
        }

        let state = postal_to_state(&cp).to_string();
        let e = state_map.entry(state.clone()).or_insert((0.0, 0, HashSet::new()));
        e.0 += total;
        e.1 += cnt;
        e.2.insert(counterparty_rfc);

        by_postal_code.push(PostalCodeRow {
            postal_code: cp,
            state_code: state,
            total_mxn: total,
            invoice_count: cnt,
        });
    }

    let mut by_state: Vec<StateRow> = state_map
        .into_iter()
        .map(|(code, (total, cnt, rfcs))| StateRow {
            state_name: state_name(&code).to_string(),
            pct_of_total: if grand_total > 0.0 {
                total / grand_total * 100.0
            } else {
                0.0
            },
            state_code: code,
            total_mxn: total,
            invoice_count: cnt,
            unique_cp: rfcs.len() as i64,
        })
        .collect();
    by_state.sort_by(|a, b| b.total_mxn.partial_cmp(&a.total_mxn).unwrap());

    Ok(GeographyResponse {
        by_state,
        by_postal_code,
        unknown_pct: if grand_total > 0.0 {
            unknown_total / grand_total * 100.0
        } else {
            0.0
        },
    })
}

/// Map Mexican postal code prefix → state code.
fn postal_to_state(cp: &str) -> &'static str {
    let prefix: u32 = cp[..2.min(cp.len())].parse().unwrap_or(99);
    match prefix {
        0..=16 => "CDMX",
        20 => "AGS",
        21..=22 => "BCN",
        23 => "BCS",
        24 => "CAM",
        25..=26 => "COA",
        27..=28 => "COL",
        29..=30 => "CHP",
        31..=33 => "CHI",
        34..=35 => "DGO",
        36..=38 => "GTO",
        39..=41 => "GRO",
        42..=43 => "HGO",
        44..=49 => "JAL",
        50..=57 => "MEX",
        58..=61 => "MIC",
        62 => "MOR",
        63 => "NAY",
        64..=67 => "NLE",
        68..=71 => "OAX",
        72..=75 => "PUE",
        76 => "QRO",
        77 => "ROO",
        78..=79 => "SLP",
        80..=82 => "SIN",
        83..=85 => "SON",
        86 => "TAB",
        87..=89 => "TAM",
        90..=91 => "TLA",
        92..=93 => "VER",
        97 => "YUC",
        98..=99 => "ZAC",
        _ => "OTR",
    }
}

fn state_name(code: &str) -> &'static str {
    match code {
        "AGS" => "Aguascalientes",
        "BCN" => "Baja California",
        "BCS" => "Baja California Sur",
        "CAM" => "Campeche",
        "CHP" => "Chiapas",
        "CHI" => "Chihuahua",
        "CDMX" => "Ciudad de México",
        "COA" => "Coahuila",
        "COL" => "Colima",
        "DGO" => "Durango",
        "GTO" => "Guanajuato",
        "GRO" => "Guerrero",
        "HGO" => "Hidalgo",
        "JAL" => "Jalisco",
        "MEX" => "Estado de México",
        "MIC" => "Michoacán",
        "MOR" => "Morelos",
        "NAY" => "Nayarit",
        "NLE" => "Nuevo León",
        "OAX" => "Oaxaca",
        "PUE" => "Puebla",
        "QRO" => "Querétaro",
        "ROO" => "Quintana Roo",
        "SLP" => "San Luis Potosí",
        "SIN" => "Sinaloa",
        "SON" => "Sonora",
        "TAB" => "Tabasco",
        "TAM" => "Tamaulipas",
        "TLA" => "Tlaxcala",
        "VER" => "Veracruz",
        "YUC" => "Yucatán",
        "ZAC" => "Zacatecas",
        _ => "Otro",
    }
}
