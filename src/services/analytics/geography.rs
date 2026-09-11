use super::summary::{
    RFC_EXTRANJERO_GENERICO, cp_key_expr, dl_type_filter, get_f64, parse_ym, rfc_column,
};
/// Geography: breakdown by lugar_expedicion (postal code) and state.
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize)]
pub struct GeographyResponse {
    pub by_state: Vec<StateRow>,
    pub by_postal_code: Vec<PostalCodeRow>,
    pub total_mxn: f64,
    // L11-31: distinct counterparty RFCs across the WHOLE universe (states + unassigned +
    // extranjero) -- a client billing from two states is one client here, not two. Summing
    // each state row's own `unique_counterparties` over-counts by exactly that overlap.
    pub total_unique_counterparties: i64,
    // L11-29 / DEC-063 / L11-R3: invoices with no resolvable counterparty state -- either no
    // CP at all, or a CP whose prefix doesn't parse as a number. Always returned (no 5%
    // threshold): the frontend closes the table with this as its own row, unconditionally.
    pub unassigned_mxn: f64,
    pub unassigned_invoice_count: i64,
    pub unassigned_unique_counterparties: i64,
    pub unassigned_pct: f64,
    // L11-30 / DEC-062 / DEC-068: XEXX010101000 (extranjero) is classified by RFC, never by
    // CP -- even when domicilio_fiscal_receptor happens to carry a Mexican CP (11560) or is
    // empty. Excluded from `by_state` (and therefore from the map, which is Mexico-only).
    pub extranjero_mxn: f64,
    pub extranjero_invoice_count: i64,
    pub extranjero_unique_counterparties: i64,
    pub extranjero_pct: f64,
}

#[derive(Debug, Serialize)]
pub struct StateRow {
    pub state_code: String,
    pub state_name: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub unique_cp: i64,             // distinct postal codes
    pub unique_counterparties: i64, // distinct counterparty RFCs
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

    // C13-04/DEC-080: 'ambos' rows can be either direction, so the counterparty RFC/name
    // pair fed to cp_key_expr must mirror the same per-row CASE the raw counterparty_rfc
    // (now counterparty_key) column already used -- otherwise a generic RFC (XAXX/XEXX)
    // would group by the bare RFC here while every other module groups it by
    // RFC||NORMALIZED_NAME, undercounting real distinct counterparties.
    let cp_col_expr = "(CASE WHEN rfc_emisor = $1 THEN rfc_receptor ELSE rfc_emisor END)";
    let cp_name_col_expr =
        "(CASE WHEN rfc_emisor = $1 THEN nombre_receptor ELSE nombre_emisor END)";
    let counterparty_key_expr = cp_key_expr(cp_col_expr, cp_name_col_expr);

    // AUD-005: geographic grouping must reflect the counterparty's location, not the
    // owner's own lugar_expedicion. Resolved per-row against $1 (not against the
    // requested dl_type) so 'ambos' rows are each classified by their own direction:
    // rows where the owner is the emisor use the receptor's domicilio fiscal, rows
    // where the owner is the receptor use the emisor's lugar_expedicion (unchanged).
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            COALESCE(
                CASE WHEN rfc_emisor = $1 THEN domicilio_fiscal_receptor ELSE lugar_expedicion END,
                'UNKNOWN'
            )                                                                AS cp,
            ({counterparty_key_expr})                                       AS counterparty_key,
            SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS total,
            COUNT(*)::bigint                      AS cnt
        FROM pulso.cfdis_ajustado c
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N','T')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
          AND NOT is_cancelled
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        GROUP BY 1, 2
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

    let grand_total: f64 = rows.iter().map(|r| get_f64(r, "total")).sum();

    // state_code → (total_mxn, invoice_count, unique_cp_rfcs)
    // state_code → (total_mxn, invoice_count, unique_counterparty_rfcs, unique_postal_codes)
    let mut state_map: HashMap<String, (f64, i64, HashSet<String>, HashSet<String>)> =
        Default::default();
    let mut by_postal_code = Vec::new();
    let mut all_rfcs: HashSet<String> = HashSet::new();

    let mut unassigned_total = 0.0f64;
    let mut unassigned_count: i64 = 0;
    let mut unassigned_rfcs: HashSet<String> = HashSet::new();

    let mut extranjero_total = 0.0f64;
    let mut extranjero_count: i64 = 0;
    let mut extranjero_rfcs: HashSet<String> = HashSet::new();

    for r in &rows {
        let cp: String = r.try_get("cp").unwrap_or_default();
        let counterparty_rfc: String = r.try_get("counterparty_key").unwrap_or_default();
        let total: f64 = get_f64(r, "total");
        let cnt: i64 = r.try_get("cnt").unwrap_or(0);

        all_rfcs.insert(counterparty_rfc.clone());

        // C13-04/DEC-080: counterparty_rfc may be a composite "RFC||NORMALIZED_NAME" (see
        // cp_key_expr) -- split it back apart before comparing against the bare generic
        // RFC, or a real foreign counterparty hiding behind XEXX010101000 would fail this
        // match and fall through to "Sin estado asignado" instead of "Extranjero".
        let (base_rfc, _) = counterparty_rfc
            .split_once("||")
            .unwrap_or((&counterparty_rfc, ""));

        // L11-30 trap 3: extranjero is classified by RFC, not CP -- even the rows that come
        // in with no CP at all still go to "Extranjero", never to "Sin estado asignado".
        if base_rfc == RFC_EXTRANJERO_GENERICO {
            extranjero_total += total;
            extranjero_count += cnt;
            extranjero_rfcs.insert(counterparty_rfc);
            continue;
        }

        if cp == "UNKNOWN" {
            unassigned_total += total;
            unassigned_count += cnt;
            unassigned_rfcs.insert(counterparty_rfc);
            continue;
        }

        // L11-29 trap 2: a CP whose prefix doesn't parse as a number used to default to 99
        // (Zacatecas) -- a silent real-state misattribution. It now falls to "unassigned",
        // same as a missing CP. A CP that parses but maps to no known range ("OTR"/"Otro")
        // is a different, legitimate bucket and is left alone (trap 3).
        let Some(state) = postal_to_state(&cp) else {
            unassigned_total += total;
            unassigned_count += cnt;
            unassigned_rfcs.insert(counterparty_rfc);
            continue;
        };
        let state = state.to_string();

        let e = state_map
            .entry(state.clone())
            .or_insert((0.0, 0, HashSet::new(), HashSet::new()));
        e.0 += total;
        e.1 += cnt;
        e.2.insert(counterparty_rfc);
        e.3.insert(cp.clone());

        by_postal_code.push(PostalCodeRow {
            postal_code: cp,
            state_code: state,
            total_mxn: total,
            invoice_count: cnt,
        });
    }

    let mut by_state: Vec<StateRow> = state_map
        .into_iter()
        .map(|(code, (total, cnt, rfcs, cps))| StateRow {
            state_name: state_name(&code).to_string(),
            pct_of_total: if grand_total > 0.0 {
                total / grand_total * 100.0
            } else {
                0.0
            },
            state_code: code,
            total_mxn: total,
            invoice_count: cnt,
            unique_cp: cps.len() as i64,
            unique_counterparties: rfcs.len() as i64,
        })
        .collect();
    by_state.sort_by(|a, b| b.total_mxn.partial_cmp(&a.total_mxn).unwrap());

    let pct_of = |v: f64| {
        if grand_total > 0.0 {
            v / grand_total * 100.0
        } else {
            0.0
        }
    };

    Ok(GeographyResponse {
        by_state,
        by_postal_code,
        total_mxn: grand_total,
        total_unique_counterparties: all_rfcs.len() as i64,
        unassigned_mxn: unassigned_total,
        unassigned_invoice_count: unassigned_count,
        unassigned_unique_counterparties: unassigned_rfcs.len() as i64,
        unassigned_pct: pct_of(unassigned_total),
        extranjero_mxn: extranjero_total,
        extranjero_invoice_count: extranjero_count,
        extranjero_unique_counterparties: extranjero_rfcs.len() as i64,
        extranjero_pct: pct_of(extranjero_total),
    })
}

/// Map Mexican postal code prefix → state code. `None` when the prefix isn't a parseable
/// number at all (L11-29 trap 2) -- distinct from `Some("OTR")`, a prefix that parses fine
/// but maps to no known range (trap 3: that's a real "Otro" bucket, not "sin asignar").
fn postal_to_state(cp: &str) -> Option<&'static str> {
    let prefix: u32 = cp[..2.min(cp.len())].parse().ok()?;
    Some(match prefix {
        0..=16 => "CDMX",
        20 => "AGS",
        21..=22 => "BCN",
        23 => "BCS",
        24 => "CAM",
        25..=27 => "COA",
        28 => "COL",
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
        90 => "TLA",
        91..=96 => "VER",
        97 => "YUC",
        98..=99 => "ZAC",
        _ => "OTR",
    })
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
