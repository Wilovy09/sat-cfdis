//! Lote 12: hallazgos de Egresos que no tienen gemelo en Ingresos -- la pregunta no aplica
//! del lado de ventas. Comparten una sola regla (`DEC-071`, la misma del `L11-02`): el
//! hallazgo describe el hecho y no lo califica. No hay "nivel" de riesgo aquí a propósito --
//! eso es lo que distingue esta familia de `hallazgos.rs` (H1-H9), que sí trae un semáforo.
use super::summary::{
    RFC_EXTRANJERO_GENERICO, RFC_PUBLICO_GENERAL, cp_key_expr, current_month_yyyymm, get_f64,
};
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub struct EgresosHallazgosResponse {
    pub h_e1: Option<HE1>,
    pub h_e2: Option<HE2>,
    pub h_e3: Vec<ProveedorEvento>,
}

// ---------------------------------------------------------------------------
// H-E1 -- Gasto pagado a personas físicas
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct HE1 {
    pub pct_personas_fisicas: f64,
    pub gasto_personas_fisicas_mxn: f64,
    pub count_personas_fisicas: i64,
    pub count_proveedores_total: i64,
}

/// DEC-074: el umbral (15%) se calibró sobre las 7 empresas cargadas -- cae en mitad de un
/// vacío de casi 40 puntos entre 44.07-44.49% (3 empresas) y 0.45-4.34% (4 empresas), así
/// que no hay corte mejor puesto. Se omite por debajo, igual que H1/H8 en hallazgos.rs.
const HE1_THRESHOLD_PCT: f64 = 15.0;

async fn compute_h_e1(pool: &DbPool, rfc: &str, cutoff: i64) -> anyhow::Result<Option<HE1>> {
    let cp_key = cp_key_expr("rfc_emisor", "nombre_emisor");
    // DEC-072: el tipo de persona se deduce de la longitud del RFC (13 = física, 12 =
    // moral) -- no hay que traer datos nuevos ni cruzar nada. XAXX/XEXX tienen 13
    // caracteres y no son personas físicas: se excluyen explícitamente del numerador
    // (trampa 1). Ningún nombre entra a esta consulta: sólo se agrega monto y conteo.
    let row = sqlx::query(&format!(
        r#"
        WITH prov AS (
            SELECT ({cp_key}) AS cp_key,
                   MAX(rfc_emisor) AS rfc_emisor,
                   SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS monto
            FROM pulso.cfdis_ajustado c
            WHERE rfc_receptor = $1
              AND dl_type IN ('recibidos','ambos')
              AND tipo_comprobante NOT IN ('P','N','T')
              AND NOT is_cancelled
              AND year * 100 + month <= $2
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY ({cp_key})
        )
        SELECT
            COALESCE(SUM(monto), 0)::float8 AS total_gasto,
            COALESCE(SUM(monto) FILTER (
                WHERE LENGTH(rfc_emisor) = 13
                  AND rfc_emisor NOT IN ('{RFC_PUBLICO_GENERAL}', '{RFC_EXTRANJERO_GENERICO}')
            ), 0)::float8 AS gasto_pf,
            COUNT(*) FILTER (
                WHERE LENGTH(rfc_emisor) = 13
                  AND rfc_emisor NOT IN ('{RFC_PUBLICO_GENERAL}', '{RFC_EXTRANJERO_GENERICO}')
            )::bigint AS count_pf,
            COUNT(*)::bigint AS count_total
        FROM prov
        "#
    ))
    .bind(rfc)
    .bind(cutoff)
    .fetch_one(pool)
    .await?;

    let total_gasto = get_f64(&row, "total_gasto");
    if total_gasto <= 0.0 {
        return Ok(None);
    }
    let gasto_pf = get_f64(&row, "gasto_pf");
    let pct = gasto_pf / total_gasto * 100.0;
    if pct < HE1_THRESHOLD_PCT {
        return Ok(None);
    }

    Ok(Some(HE1 {
        pct_personas_fisicas: pct,
        gasto_personas_fisicas_mxn: gasto_pf,
        count_personas_fisicas: row.try_get("count_pf").unwrap_or(0),
        count_proveedores_total: row.try_get("count_total").unwrap_or(0),
    }))
}

// ---------------------------------------------------------------------------
// H-E2 -- Contrapartes que son cliente y proveedor a la vez
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct HE2Row {
    pub rfc: String,
    pub nombre: String,
    pub gasto_mxn: f64,
    pub pct_gasto: f64,
    pub venta_mxn: f64,
    pub pct_venta: f64,
}

#[derive(Debug, Serialize)]
pub struct HE2 {
    pub rows: Vec<HE2Row>,
    pub total_gasto_mxn: f64,
    pub total_pct_gasto: f64,
    pub total_venta_mxn: f64,
    pub total_pct_venta: f64,
}

/// El umbral (contraparte pesa >= 1% de cualquiera de los dos lados) se calibró sobre 5
/// empresas: dispara en 4 de 5, de 0 a 4 casos por empresa -- volumen correcto para una
/// sección de hallazgos.
const HE2_THRESHOLD_PCT: f64 = 1.0;

async fn compute_h_e2(pool: &DbPool, rfc: &str, cutoff: i64) -> anyhow::Result<Option<HE2>> {
    // Misma llave normalizada (L11's cp_key_expr) en los dos universos -- una llave por RFC
    // real cruzando emitidos/recibidos, distinta sólo cuando el RFC es genérico (XAXX/XEXX),
    // caso en el que no debería cruzar de todos modos (trampa 2).
    let venta_key = cp_key_expr("rfc_receptor", "nombre_receptor");
    let compra_key = cp_key_expr("rfc_emisor", "nombre_emisor");
    let rows = sqlx::query(&format!(
        r#"
        WITH ventas AS (
            SELECT ({venta_key}) AS cp_key, MAX(nombre_receptor) AS nombre,
                   SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS venta_mxn
            FROM pulso.cfdis_ajustado c
            WHERE rfc_emisor = $1
              AND dl_type IN ('emitidos','ambos')
              AND tipo_comprobante NOT IN ('P','N','T')
              AND NOT is_cancelled
              AND year * 100 + month <= $2
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY ({venta_key})
        ),
        compras AS (
            SELECT ({compra_key}) AS cp_key, MAX(nombre_emisor) AS nombre,
                   SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS gasto_mxn
            FROM pulso.cfdis_ajustado c
            WHERE rfc_receptor = $1
              AND dl_type IN ('recibidos','ambos')
              AND tipo_comprobante NOT IN ('P','N','T')
              AND NOT is_cancelled
              AND year * 100 + month <= $2
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY ({compra_key})
        )
        SELECT co.cp_key AS rfc, COALESCE(co.nombre, ve.nombre) AS nombre,
               co.gasto_mxn, ve.venta_mxn
        FROM compras co
        JOIN ventas ve ON ve.cp_key = co.cp_key
        "#
    ))
    .bind(rfc)
    .bind(cutoff)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    // Denominadores: el universo completo de cada lado, no sólo lo cruzado (trampa 1 -- no
    // se suman los dos lados en una sola cifra, cada uno lleva su propio % con su propia
    // base).
    let total_venta_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(COALESCE(total_neto_mxn_ajustado,0)::float8), 0)::float8 AS total
        FROM pulso.cfdis_ajustado c
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND year * 100 + month <= $2
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        "#,
    )
    .bind(rfc)
    .bind(cutoff)
    .fetch_one(pool)
    .await?;
    let total_venta = get_f64(&total_venta_row, "total");

    let total_gasto_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(COALESCE(total_neto_mxn_ajustado,0)::float8), 0)::float8 AS total
        FROM pulso.cfdis_ajustado c
        WHERE rfc_receptor = $1
          AND dl_type IN ('recibidos','ambos')
          AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND year * 100 + month <= $2
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        "#,
    )
    .bind(rfc)
    .bind(cutoff)
    .fetch_one(pool)
    .await?;
    let total_gasto = get_f64(&total_gasto_row, "total");

    if total_venta <= 0.0 || total_gasto <= 0.0 {
        return Ok(None);
    }

    let mut candidates: Vec<HE2Row> = rows
        .iter()
        .map(|r| {
            let gasto_mxn = get_f64(r, "gasto_mxn");
            let venta_mxn = get_f64(r, "venta_mxn");
            HE2Row {
                rfc: r.try_get("rfc").unwrap_or_default(),
                nombre: r.try_get("nombre").unwrap_or_default(),
                gasto_mxn,
                pct_gasto: gasto_mxn / total_gasto * 100.0,
                venta_mxn,
                pct_venta: venta_mxn / total_venta * 100.0,
            }
        })
        .filter(|r| r.pct_gasto >= HE2_THRESHOLD_PCT || r.pct_venta >= HE2_THRESHOLD_PCT)
        .collect();

    if candidates.is_empty() {
        return Ok(None);
    }

    candidates.sort_by(|a, b| b.pct_gasto.partial_cmp(&a.pct_gasto).unwrap());

    let total_gasto_mxn: f64 = candidates.iter().map(|r| r.gasto_mxn).sum();
    let total_venta_mxn: f64 = candidates.iter().map(|r| r.venta_mxn).sum();

    Ok(Some(HE2 {
        total_pct_gasto: total_gasto_mxn / total_gasto * 100.0,
        total_pct_venta: total_venta_mxn / total_venta * 100.0,
        total_gasto_mxn,
        total_venta_mxn,
        rows: candidates,
    }))
}

// ---------------------------------------------------------------------------
// H-E3 -- Proveedor relevante que aparece o desaparece de golpe
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct ProveedorEvento {
    pub rfc: String,
    pub nombre: String,
    /// "aparece" | "desaparece" | "arco" (apareció y luego dejó de facturar, un solo evento
    /// -- L11-32 trampa 2 / DEC-078)
    pub tipo: String,
    pub year: i32,
    /// Sólo presente cuando `tipo == "arco"`: el año en que dejó de facturar.
    pub year_siguiente: Option<i32>,
    pub pct_peso: f64,
    pub monto_mxn: f64,
    /// Trampa 4: sin esta columna el hallazgo puede decir lo contrario de lo que pasó (una
    /// compra puntual de 2 meses se lee igual que una relación nueva y estable de 12).
    pub meses_con_factura: i64,
}

const HE3_APARICION_PCT: f64 = 5.0;
const HE3_DESAPARICION_FLOOR_PCT: f64 = 1.0;

#[derive(Clone)]
struct YearStats {
    nombre: String,
    monto: f64,
    pct: f64,
    meses: i64,
}

async fn compute_h_e3(pool: &DbPool, rfc: &str) -> anyhow::Result<Vec<ProveedorEvento>> {
    // Trampa 1: el año en curso no se compara contra un año completo -- sólo años completos
    // entran (2023-2025 hoy). `current_month_yyyymm() / 100` es el año del último mes
    // cerrado; si ese mes es diciembre el año en curso ya está completo y sí entra.
    let cutoff = current_month_yyyymm();
    let last_closed_year = (cutoff / 100) as i32;
    let last_closed_month = (cutoff % 100) as i32;
    let last_complete_year = if last_closed_month == 12 {
        last_closed_year
    } else {
        last_closed_year - 1
    };

    let cp_key = cp_key_expr("rfc_emisor", "nombre_emisor");
    let rows = sqlx::query(&format!(
        r#"
        WITH yearly AS (
            SELECT year, ({cp_key}) AS cp_key, MAX(nombre_emisor) AS nombre,
                   SUM(COALESCE(total_neto_mxn_ajustado,0)::float8)::float8 AS monto,
                   COUNT(DISTINCT month)::bigint AS meses
            FROM pulso.cfdis_ajustado c
            WHERE rfc_receptor = $1
              AND dl_type IN ('recibidos','ambos')
              AND tipo_comprobante NOT IN ('P','N','T')
              AND NOT is_cancelled
              AND year <= $2
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY year, ({cp_key})
        ),
        year_totals AS (
            SELECT year, GREATEST(SUM(monto), 1) AS total FROM yearly GROUP BY year
        )
        SELECT y.year, y.cp_key, y.nombre, y.monto, y.meses,
               y.monto / yt.total * 100 AS pct
        FROM yearly y JOIN year_totals yt ON yt.year = y.year
        "#
    ))
    .bind(rfc)
    .bind(last_complete_year as i64)
    .fetch_all(pool)
    .await?;

    // cp_key -> year -> stats
    let mut by_provider: HashMap<String, HashMap<i32, YearStats>> = HashMap::new();
    for r in &rows {
        let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let key: String = r.try_get("cp_key").unwrap_or_default();
        let stats = YearStats {
            nombre: r.try_get("nombre").unwrap_or_default(),
            monto: get_f64(r, "monto"),
            pct: get_f64(r, "pct"),
            meses: r.try_get("meses").unwrap_or(0),
        };
        by_provider.entry(key).or_default().insert(year, stats);
    }

    let years: Vec<i32> = {
        let start_year = last_complete_year - 2; // three complete years: N-2, N-1, N
        (start_year..=last_complete_year).collect()
    };

    #[derive(Clone, Copy, PartialEq)]
    enum RawTipo {
        Aparece,
        Desaparece,
    }

    struct RawEvent {
        pivot_year: i32,
        tipo: RawTipo,
    }

    let mut raw_by_provider: HashMap<String, Vec<RawEvent>> = HashMap::new();

    let empty_stats = |_year: i32| YearStats {
        nombre: String::new(),
        monto: 0.0,
        pct: 0.0,
        meses: 0,
    };

    for (cp_key, year_map) in &by_provider {
        for pair in years.windows(2) {
            let (y0, y1) = (pair[0], pair[1]);
            let s0 = year_map
                .get(&y0)
                .cloned()
                .unwrap_or_else(|| empty_stats(y0));
            let s1 = year_map
                .get(&y1)
                .cloned()
                .unwrap_or_else(|| empty_stats(y1));
            if s0.pct < HE3_DESAPARICION_FLOOR_PCT && s1.pct >= HE3_APARICION_PCT {
                raw_by_provider
                    .entry(cp_key.clone())
                    .or_default()
                    .push(RawEvent {
                        pivot_year: y1,
                        tipo: RawTipo::Aparece,
                    });
            }
            if s0.pct >= HE3_APARICION_PCT && s1.pct < HE3_DESAPARICION_FLOOR_PCT {
                raw_by_provider
                    .entry(cp_key.clone())
                    .or_default()
                    .push(RawEvent {
                        pivot_year: y0,
                        tipo: RawTipo::Desaparece,
                    });
            }
        }
    }

    let mut eventos: Vec<ProveedorEvento> = Vec::new();
    for (cp_key, raw_events) in &raw_by_provider {
        let year_map = &by_provider[cp_key];
        // L11-32 trampa 2 / DEC-078: un proveedor-año que dispara aparición Y desaparición
        // en el mismo año pivote es un solo arco, no dos hallazgos -- agrupa por año pivote.
        let mut by_pivot: HashMap<i32, Vec<RawTipo>> = HashMap::new();
        for ev in raw_events {
            by_pivot.entry(ev.pivot_year).or_default().push(ev.tipo);
        }
        for (pivot_year, tipos) in by_pivot {
            let stats = year_map
                .get(&pivot_year)
                .cloned()
                .unwrap_or_else(|| empty_stats(pivot_year));
            let has_aparece = tipos.contains(&RawTipo::Aparece);
            let has_desaparece = tipos.contains(&RawTipo::Desaparece);
            let (tipo, year_siguiente) = if has_aparece && has_desaparece {
                ("arco".to_string(), Some(pivot_year + 1))
            } else if has_aparece {
                ("aparece".to_string(), None)
            } else {
                ("desaparece".to_string(), None)
            };
            eventos.push(ProveedorEvento {
                rfc: cp_key.clone(),
                nombre: stats.nombre,
                tipo,
                year: pivot_year,
                year_siguiente,
                pct_peso: stats.pct,
                monto_mxn: stats.monto,
                meses_con_factura: stats.meses,
            });
        }
    }

    eventos.sort_by(|a, b| b.pct_peso.partial_cmp(&a.pct_peso).unwrap());
    Ok(eventos)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    to: Option<&str>,
) -> anyhow::Result<EgresosHallazgosResponse> {
    let parse_yyyymm = |s: &str| -> i64 {
        let parts: Vec<&str> = s.splitn(2, '-').collect();
        let y: i64 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
        let m: i64 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(1);
        y * 100 + m
    };
    let closed = current_month_yyyymm();
    let cutoff = to.map(|t| parse_yyyymm(t).min(closed)).unwrap_or(closed);

    let h_e1 = compute_h_e1(pool, rfc, cutoff).await?;
    let h_e2 = compute_h_e2(pool, rfc, cutoff).await?;
    let h_e3 = compute_h_e3(pool, rfc).await?;

    Ok(EgresosHallazgosResponse { h_e1, h_e2, h_e3 })
}
