use super::summary::{current_month_yyyymm, get_f64};
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct HallazgosResponse {
    pub visible: Vec<Hallazgo>,
    pub all: Vec<Hallazgo>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Hallazgo {
    pub id: String,
    pub titulo: String,
    pub familia: String, // "riesgo" | "desempeno"
    pub nivel: String,
    pub metrica_principal: Option<f64>,
    pub cuerpo: String,
    pub interpretacion: String,
    pub disclaimer: Option<String>,
    pub nota_fija: Option<String>,
    pub datos_tabla: Option<Vec<TablaRow>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct TablaRow {
    pub nombre: String,
    // L10-02b: declared contract-start date (fecha_inicio_rel_laboral), not an inferred
    // "primer pago" -- renamed to match what it actually is.
    pub fecha_ingreso: String,
    // L10-02c: an exact date (fecha_final_pago), not a truncated year-month.
    pub ultimo_periodo_pagado: String,
    // L10-02d: last ordinario receipt's gross monthly salary, full precision -- see
    // fmt_mxn_full for why this isn't run through the abbreviating fmt_mxn.
    pub sueldo_mensual: f64,
    // L10-03: lets the analyst see what contract figure backs each row.
    pub tipo_contrato: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn subtract_months(y: i64, m: i64, n: i64) -> (i64, i64) {
    let total = y * 12 + m - 1 - n;
    (total / 12, total % 12 + 1)
}

fn fmt_mxn(v: f64) -> String {
    if v >= 1_000_000.0 {
        format!("${:.1}M MXN", v / 1_000_000.0)
    } else if v >= 1_000.0 {
        format!("${:.0}K MXN", v / 1_000.0)
    } else {
        format!("${:.0} MXN", v)
    }
}

// L10-02d / AUD-086: abbreviating to miles hid a 0.32x-1.47x error in H5B's salary column --
// full precision doesn't fix that on its own, but it's the format that lets an analyst
// actually reconcile the number against a payslip, which "$140K" doesn't.
fn fmt_mxn_full(v: f64) -> String {
    format!("${v:.2} MXN")
}

/// Linear-interpolated percentile of a value already sorted ascending (0.5 = median).
/// Empty input returns 0.0 -- callers only reach this after checking the population.
fn percentile_of_sorted(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = p * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = rank - lo as f64;
        sorted[lo] + (sorted[hi] - sorted[lo]) * frac
    }
}

/// Severity score for ranking — lower = more severe/urgent.
/// Riesgo:    critico=0, alto=1, medio=2, bajo=3, muy_bajo=4
/// Desempeno: muy_negativo=0, negativo=1, neutral=2, positivo=3, muy_positivo=4
fn severity_score(nivel: &str) -> u8 {
    match nivel {
        "critico" | "muy_negativo" => 0,
        "alto" | "negativo" => 1,
        "medio" | "neutral" => 2,
        "bajo" | "positivo" => 3,
        "muy_bajo" | "muy_positivo" => 4,
        _ => 5,
    }
}

fn h_priority(id: &str) -> u8 {
    match id {
        "H1" => 1,
        "H2" => 2,
        "H9" => 3,
        "H8" => 4,
        "H3" => 5,
        "H6" => 6,
        "H7" => 7,
        "H4" => 8,
        "H5A" => 9,
        _ => 10,
    }
}

// ---------------------------------------------------------------------------
// Nivel thresholds + interpretation text
// ---------------------------------------------------------------------------

/// L8-05: shared concentration scale for H1 and H8 -- both compute the same arithmetic
/// (top 3 / base identificable), so they share one scale; the difference between "clientes"
/// and "proveedores" lives in the interpretation text below, not the color. Decided as
/// Opción C: <25% is omitted (the piso común -- H8 used to omit under 15%, H1 never
/// omitted at all), 25-45 bajo, 45-65 medio, 65-80 alto, >=80 crítico. Top 1 is a second
/// disparador on top of the Top 3 band: >=35% forces at least "alto", >=50% forces
/// "crítico" -- covers the shape Top 3 dilutes (many small counterparties plus one that
/// dominates), not "fixing" the data, just reading a different concentration pattern.
fn concentracion_nivel(top3_pct: f64, top1_pct: f64) -> Option<&'static str> {
    if top3_pct < 25.0 {
        return None;
    }
    let base = if top3_pct < 45.0 {
        "bajo"
    } else if top3_pct < 65.0 {
        "medio"
    } else if top3_pct < 80.0 {
        "alto"
    } else {
        "critico"
    };
    let disparador = if top1_pct >= 50.0 {
        Some("critico")
    } else if top1_pct >= 35.0 {
        Some("alto")
    } else {
        None
    };
    // The more severe of the two readings wins (lower severity_score = more severe).
    Some(match disparador {
        Some(d) if severity_score(d) < severity_score(base) => d,
        _ => base,
    })
}

fn h1_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "critico" => {
            "Dependencia extrema — la pérdida o renegociación de cualquiera de estas cuentas tiene impacto directo sobre la viabilidad del negocio."
        }
        "alto" => {
            "Concentración elevada con riesgo de pérdida material ante churn de las cuentas principales."
        }
        "medio" => {
            "Concentración moderada. Revisar recurrencia y antigüedad de las cuentas principales en el módulo de Emitidas."
        }
        _ => "Base de clientes diversificada. Sin concentración crítica observable en LTM.",
    }
}

// L8-06: adopts H9's neutral band (-5% to +5%) instead of its own (0% to +5%) -- the same
// class of twin-hallazgo defect as concentracion_nivel above. A CAGR of two extremes that
// actually rose 20% then fell 17% used to land at -0.22%, just inside H2's old "negativo"
// band, and print "Caída sostenida ... requiere explicación de gestión" for a trajectory
// that wasn't a sustained fall at all.
fn h2_nivel(cagr: f64) -> &'static str {
    if cagr > 15.0 {
        "muy_positivo"
    } else if cagr >= 5.0 {
        "positivo"
    } else if cagr > -5.0 {
        "neutral"
    } else if cagr >= -15.0 {
        "negativo"
    } else {
        "muy_negativo"
    }
}

fn h2_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "muy_negativo" | "negativo" => {
            "Caída sostenida de ingresos en el período analizado. Requiere explicación de gestión antes de cualquier ejercicio de valoración."
        }
        "neutral" => {
            "Crecimiento marginal. Insuficiente para absorber inflación de costos sin compresión de flujo visible."
        }
        "positivo" => "Crecimiento consistente en el período analizado.",
        _ => "Crecimiento sólido y sostenido en el período analizado.",
    }
}

fn h3_nivel(delta_pp: f64) -> &'static str {
    if delta_pp > 10.0 {
        "muy_positivo"
    } else if delta_pp >= 5.0 {
        "positivo"
    } else if delta_pp > -5.0 {
        "neutral"
    } else if delta_pp >= -10.0 {
        "negativo"
    } else {
        "muy_negativo"
    }
}

fn h3_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "muy_negativo" => {
            "Deterioro de flujo visible: los egresos y la nómina crecen más rápido que los ingresos. Revisar drivers de gasto y masa salarial en detalle."
        }
        "negativo" => {
            "Presión creciente sobre el flujo visible. Verificar evolución de egresos y nómina frente a tendencia de ingresos."
        }
        "neutral" => "Relación ingresos/egresos/nómina estable en el período analizado.",
        // L10-07 / AUD-096, trap 1: a large improvement isn't unconditionally good news --
        // it doesn't get a congratulatory text, it gets the same "revisar si es recurrente"
        // framing a deterioration of the same magnitude would. A margin swing this size in
        // the year before a sale is exactly what a buyer questions first.
        "muy_positivo" => {
            "Mejora marcada en la relación ingresos/egresos/nómina. Revisar si responde a un cambio estructural o a movimientos no recurrentes antes de proyectarla hacia adelante."
        }
        _ => "Mejora en la relación ingresos/egresos/nómina visibles.",
    }
}

fn h4_nivel(ratio_pct: f64) -> &'static str {
    if ratio_pct < 5.0 {
        "muy_bajo"
    } else if ratio_pct < 10.0 {
        "bajo"
    } else if ratio_pct < 20.0 {
        "medio"
    } else if ratio_pct < 35.0 {
        "alto"
    } else {
        "critico"
    }
}

fn h4_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "critico" | "alto" => {
            "Pasivo laboral de peso significativo. Requiere análisis detallado de antigüedad, sueldos y estructura de plantilla."
        }
        "medio" => "Pasivo laboral relevante. Considerar en la estructura de la transacción.",
        _ => "Pasivo laboral manejable en relación al nivel de ingresos.",
    }
}

fn h5a_nivel(tasa_pct: f64) -> &'static str {
    if tasa_pct < 10.0 {
        "muy_bajo"
    } else if tasa_pct < 20.0 {
        "bajo"
    } else if tasa_pct < 35.0 {
        "medio"
    } else if tasa_pct < 50.0 {
        "alto"
    } else {
        "critico"
    }
}

fn h5a_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "critico" | "alto" => {
            "Rotación elevada — señal de inestabilidad operativa o condiciones laborales que requieren validación. Revisar distribución por departamento y nivel salarial en módulo de Nómina."
        }
        "medio" => {
            "Rotación moderada. Verificar si se concentra en áreas críticas o corresponde a patrones estacionales."
        }
        _ => "Plantilla estable en el período analizado.",
    }
}

/// C8-04 (opcional): H6 and H7 measure a saldo/ingreso ratio with the same shape now
/// (both con IVA, sin exclusiones -- see compute_h6's comment for why) and had been writing
/// identical thresholds twice. One definition; the interpretation text stays separate below
/// since "cartera de clientes" and "saldo con proveedores" read differently.
fn cartera_pct_nivel(ratio_pct: f64) -> &'static str {
    if ratio_pct < 2.0 {
        "muy_bajo"
    } else if ratio_pct < 5.0 {
        "bajo"
    } else if ratio_pct < 10.0 {
        "medio"
    } else if ratio_pct < 20.0 {
        "alto"
    } else {
        "critico"
    }
}

fn h6_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "critico" | "alto" => {
            "Cartera material en riesgo. Revisar antigüedad y concentración de saldos en el módulo de Cobranza."
        }
        "medio" => {
            "Saldo pendiente relevante. Verificar composición por cliente y buckets de antigüedad."
        }
        _ => "Cobranza eficiente. Cartera pendiente dentro de rangos normales.",
    }
}

fn h7_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "critico" | "alto" => {
            "Saldo relevante con proveedores. Revisar antigüedad, concentración y posible impacto en relaciones comerciales o liquidez."
        }
        "medio" => "Saldo moderado. Verificar composición por proveedor y buckets de vencimiento.",
        _ => "Disciplina de pago sólida. Sin pasivo material observable con proveedores.",
    }
}

fn h8_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "critico" | "alto" => {
            "Concentración elevada en pocos proveedores. Una interrupción en las relaciones principales tendría impacto material sobre la operación. Revisar condiciones contractuales."
        }
        "medio" => {
            "Concentración moderada. Validar diversificación, exclusividad y riesgo de sustitución en el proceso de due diligence."
        }
        _ => "Gasto distribuido entre proveedores. Sin dependencia crítica observable.",
    }
}

fn h9_nivel(delta_pct: f64) -> &'static str {
    if delta_pct > 15.0 {
        "muy_positivo"
    } else if delta_pct >= 5.0 {
        "positivo"
    } else if delta_pct > -5.0 {
        "neutral"
    } else if delta_pct >= -15.0 {
        "negativo"
    } else {
        "muy_negativo"
    }
}

fn h9_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "muy_negativo" => {
            "Caída material en la ventana más reciente. El CAGR histórico puede enmascarar un deterioro acelerado. Contrastar con CAGR histórico y revisar módulo de Emitidas."
        }
        "negativo" => {
            "Desaceleración visible en los últimos 12 meses vs el período anterior. Contrastar con el CAGR histórico para distinguir corrección temporal de deterioro estructural."
        }
        "neutral" => {
            "Ingresos recientes en línea con el LTM anterior. Sin aceleración ni deterioro visible en la ventana más reciente."
        }
        "positivo" => "Ingresos recientes por encima del LTM anterior. Momentum favorable.",
        _ => {
            "Aceleración de ingresos en la ventana más reciente. Señal positiva de momentum comercial."
        }
    }
}

// ---------------------------------------------------------------------------
// LTM ingreso helper (emitidos tipo I only)
// ---------------------------------------------------------------------------

async fn compute_ltm_ingreso(
    pool: &DbPool,
    rfc: &str,
    from_y: i64,
    from_m: i64,
    to_y: i64,
    to_m: i64,
) -> anyhow::Result<f64> {
    // L8-03 / DEC-041: same four properties as H1's query above. Feeds H9, called once for
    // the current LTM window and once for LTM-12.
    let row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(COALESCE(total_neto_mxn_ajustado,0))::float8, 0) AS total
        FROM pulso.cfdis_ajustado c
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        "#,
    )
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_one(pool)
    .await?;
    Ok(get_f64(&row, "total"))
}

// ---------------------------------------------------------------------------
// H1 — Concentración de clientes
// ---------------------------------------------------------------------------

async fn compute_h1(
    pool: &DbPool,
    rfc: &str,
    ltm_start_y: i64,
    ltm_start_m: i64,
    ltm_end_y: i64,
    ltm_end_m: i64,
) -> anyhow::Result<Option<Hallazgo>> {
    // L8-03 / DEC-041: base del Resumen -- cfdis_ajustado, total_neto_mxn_ajustado,
    // exclusiones, tipo_comprobante NOT IN ('P','N','T') instead of ='I' (con IVA, sin
    // exclusiones, notas de crédito descartadas del todo). Same four properties on every
    // one of H1/H2/H8/H9's six feeder queries.
    let rows = sqlx::query(
        r#"
        SELECT c.rfc_receptor, MAX(c.nombre_receptor) AS nombre,
               SUM(COALESCE(c.total_neto_mxn_ajustado,0))::float8 AS ltm_mxn
        FROM pulso.cfdis_ajustado c
        WHERE c.rfc_emisor = $1
          AND c.dl_type IN ('emitidos','ambos')
          AND c.tipo_comprobante NOT IN ('P','N','T')
          AND NOT c.is_cancelled
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        GROUP BY c.rfc_receptor
        ORDER BY ltm_mxn DESC
        "#,
    )
    .bind(rfc)
    .bind(ltm_start_y)
    .bind(ltm_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    struct ClientRow {
        rfc: String,
        #[allow(dead_code)]
        nombre: String,
        mxn: f64,
    }

    let clients: Vec<ClientRow> = rows
        .iter()
        .map(|r| ClientRow {
            rfc: r.try_get("rfc_receptor").unwrap_or_default(),
            nombre: r.try_get("nombre").unwrap_or_default(),
            mxn: get_f64(r, "ltm_mxn"),
        })
        .collect();

    let total_ltm: f64 = clients.iter().map(|c| c.mxn).sum();
    if total_ltm <= 0.0 {
        return Ok(None);
    }

    // PeG share
    let peg_mxn: f64 = clients
        .iter()
        .filter(|c| c.rfc == super::summary::RFC_PUBLICO_GENERAL)
        .map(|c| c.mxn)
        .sum();
    let peg_pct = peg_mxn / total_ltm * 100.0;

    // Top 3 excluding PeG (take first 3 non-XAXX clients). L8-03: with credit notes now
    // netted in, a counterparty can land with a negative total_neto_mxn_ajustado --
    // excluded here, not just from the top-3 pick (rows sort DESC so a negative total
    // wouldn't be picked anyway) but from the concentration denominator too, or a negative
    // contributor drags total_excl_peg below top3_mxn and the percentage passes 100.
    // L10-10 / AUD-099, trap 3: also excludes the "residente extranjero" generic RFC, so
    // H1's own "Top 3 clientes" doesn't become a third, differently-filtered concentration
    // figure on the same screen as the KPI and the counterparties table.
    let identifiable: Vec<&ClientRow> = clients
        .iter()
        .filter(|c| {
            c.rfc != super::summary::RFC_PUBLICO_GENERAL
                && c.rfc != super::summary::RFC_EXTRANJERO_GENERICO
                && c.mxn > 0.0
        })
        .collect();

    if identifiable.is_empty() {
        return Ok(None);
    }

    let total_excl_peg: f64 = identifiable.iter().map(|c| c.mxn).sum();
    if total_excl_peg <= 0.0 {
        return Ok(None);
    }

    let top3: Vec<&ClientRow> = identifiable.iter().take(3).copied().collect();
    let top3_mxn: f64 = top3.iter().map(|c| c.mxn).sum();
    let top3_pct = top3_mxn / total_excl_peg * 100.0;
    let top1_pct = top3
        .first()
        .map(|c| c.mxn / total_excl_peg * 100.0)
        .unwrap_or(0.0);
    let n = top3.len();

    let nivel = match concentracion_nivel(top3_pct, top1_pct) {
        Some(n) => n,
        None => return Ok(None), // < 25% Top 3 and < 35% Top 1 -- omit
    };
    let interp = h1_interpretacion(nivel);

    // L8-04: the denominator stays base identificable (concentration among identifiable
    // clients is the right metric) -- what changes is the body saying so explicitly,
    // instead of implying it's a share of the whole LTM ingreso.
    let mut cuerpo = format!(
        "El Top {} cliente{} representa el {:.1}% del ingreso identificable.",
        n,
        if n == 1 { "" } else { "s" },
        top3_pct
    );

    // L8-04: no longer conditioned on >30% -- if there's any ingreso a Público en
    // General, it's always named, with its own percentage.
    if peg_mxn > 0.0 {
        cuerpo.push_str(&format!(
            " Adicionalmente, el {:.1}% del ingreso corresponde a ventas a Público en General.",
            peg_pct
        ));
    }

    Ok(Some(Hallazgo {
        id: "H1".to_string(),
        titulo: "Concentración de clientes".to_string(),
        familia: "riesgo".to_string(),
        nivel: nivel.to_string(),
        metrica_principal: Some(top3_pct),
        cuerpo,
        interpretacion: interp.to_string(),
        disclaimer: None,
        nota_fija: None,
        datos_tabla: None,
    }))
}

// ---------------------------------------------------------------------------
// H5A — Rotación de personal
// ---------------------------------------------------------------------------

/// L6C-03: H5A/H5B's own "last period" -- nómina's own MAX(year, month), not the facturas
/// anchor `get()` resolves for H1/H2/H3/H6 (which excludes tipo_comprobante='N' precisely
/// so nómina can't move it). Nómina is timbrada after facturas, so the facturas anchor is
/// routinely a month ahead of the last real payroll -- for 5 of 7 RFCs measured 2026-09-04,
/// the facturas anchor's month has ZERO nómina rows, so `latest_row`/`term_rows` compared
/// every real employee against an empty ghost month: H5A read 100% rotación (every LTM
/// employee counted as a baja against a month with nobody in it) and H5B marked the entire
/// plantilla as terminada. Resolved by having H5A/H5B query nomina_normalizada's own anchor
/// directly instead of receiving the facturas one from the caller.
/// Nómina bruta por año (total_percepciones, L5-04's bruto redefinition), todo tipo_nomina,
/// grouped by year_devengo -- matches the single definition every other consumer
/// (payroll.rs's by_month/by_year, the bridge's three nomina sources, list_excluded_cfdis)
/// uses since Lote 5. `pub`, not `pub(crate)`: this is the single source H3's `cuerpo` reads
/// from AND the one `tests/consistency_invariants.rs` (an integration test, which only sees
/// the public API) calls to verify H3 against `payroll::monthly_series` -- per Rob's review
/// of L6C-10: the invariant used to re-type this exact query inside the test file, so
/// reverting H3 to `n.year` would leave the test passing against its own untouched copy
/// while the real code silently regressed. Extracted here, both call sites read the same
/// compiled function; there's no second copy left to diverge.
pub async fn nomina_por_year(
    pool: &DbPool,
    rfc: &str,
) -> anyhow::Result<std::collections::HashMap<i64, f64>> {
    let rows = sqlx::query(
        r#"
        SELECT n.year_devengo AS year,
               SUM(n.total_percepciones) AS nomina
        FROM pulso.nomina_normalizada n
        WHERE n.rfc_emisor = $1
          AND NOT n.is_excluded
        GROUP BY n.year_devengo
        "#,
    )
    .bind(rfc)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| {
            (
                r.try_get::<i64, _>("year").unwrap_or(0),
                get_f64(r, "nomina"),
            )
        })
        .collect())
}

async fn nomina_last_period(pool: &DbPool, rfc: &str) -> anyhow::Result<Option<(i64, i64)>> {
    let row = sqlx::query(
        r#"
        SELECT MAX(year_devengo * 100 + month_devengo)::bigint AS max_ym
        FROM pulso.nomina_normalizada
        WHERE rfc_emisor = $1 AND NOT is_excluded
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let max_ym: Option<i64> = row.try_get("max_ym").ok().flatten();
    Ok(max_ym.filter(|ym| *ym > 0).map(|ym| (ym / 100, ym % 100)))
}

async fn compute_h5a(pool: &DbPool, rfc: &str) -> anyhow::Result<Option<Hallazgo>> {
    let Some((ltm_end_y, ltm_end_m)) = nomina_last_period(pool, rfc).await? else {
        return Ok(None);
    };
    let (ltm_start_y, ltm_start_m) = subtract_months(ltm_end_y, ltm_end_m, 11);

    // L10-09 / AUD-098, DEC-053: rotación used to count every termination the same way,
    // blending contratos de obra/tiempo determinado (which end by design) with real
    // attrition of permanent staff -- Compro's 131.7% "crítico" was almost entirely people
    // whose obra contract simply ran out. `emp_tipo` classifies each employee ONCE, from
    // their own last receipt by fecha_pago -- never MAX/MIN/mode: '03' sorts before '01'
    // alphabetically, so MAX would (and once did, in this item's own diagnosis) misclassify
    // permanent staff as temporary. Only tipo_contrato 01/02 (indeterminado/determinado)
    // count as "plantilla permanente"; 03/04/99 are tracked separately below for the
    // context sentence, never silently dropped.
    let month_rows = sqlx::query(
        r#"
        WITH emp_tipo AS (
            SELECT DISTINCT ON (rfc_receptor) rfc_receptor, tipo_contrato
            FROM pulso.nomina_normalizada
            WHERE rfc_emisor = $1 AND NOT is_excluded
            ORDER BY rfc_receptor, fecha_pago DESC
        )
        SELECT n.year_devengo AS year, n.month_devengo AS month, COUNT(DISTINCT n.rfc_receptor)::bigint AS hc
        FROM pulso.nomina_normalizada n
        JOIN emp_tipo et ON et.rfc_receptor = n.rfc_receptor
        WHERE n.rfc_emisor = $1
          AND (n.year_devengo > $2 OR (n.year_devengo = $2 AND n.month_devengo >= $3))
          AND (n.year_devengo < $4 OR (n.year_devengo = $4 AND n.month_devengo <= $5))
          AND NOT n.is_excluded
          AND et.tipo_contrato IN ('01', '02')
        GROUP BY n.year_devengo, n.month_devengo
        "#,
    )
    .bind(rfc)
    .bind(ltm_start_y)
    .bind(ltm_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_all(pool)
    .await?;

    if month_rows.is_empty() {
        return Ok(None);
    }

    let hc_per_month: Vec<i64> = month_rows
        .iter()
        .map(|r| r.try_get::<i64, _>("hc").unwrap_or(0))
        .collect();
    // L10-09: one decimal on the average headcount -- rounding it to a whole number (as
    // before) is what let a reader's own bajas/headcount division disagree with the
    // displayed percentage (L10-06 / AUD-094).
    let avg_hc = hc_per_month.iter().sum::<i64>() as f64 / hc_per_month.len() as f64;

    // P-01 / AUD-075: latest-period headcount and bajas used to be two separate queries,
    // the second a correlated NOT EXISTS that rebuilt the whole nomina_normalizada view
    // once per LTM employee (102 times, measured 5.8s on the RFC grande). Grouping by
    // employee once and counting with FILTER gets both numbers in a single pass. The old
    // `latest_row` query didn't bind the LTM window (it just filtered the literal latest
    // period); this fused version inherits the window from the CTE it's built on, which is
    // harmless here because the latest month is always inside the LTM window by
    // construction -- not a definition change, just noted so it doesn't read as one later.
    let emp_rows = sqlx::query(
        r#"
        WITH emp_tipo AS (
            SELECT DISTINCT ON (rfc_receptor) rfc_receptor, tipo_contrato
            FROM pulso.nomina_normalizada
            WHERE rfc_emisor = $1 AND NOT is_excluded
            ORDER BY rfc_receptor, fecha_pago DESC
        )
        SELECT
            COUNT(*) FILTER (WHERE active_latest AND tipo_contrato IN ('01', '02'))       AS latest_hc,
            COUNT(*) FILTER (WHERE NOT active_latest AND tipo_contrato IN ('01', '02'))   AS bajas_permanentes,
            COUNT(*) FILTER (WHERE NOT active_latest AND tipo_contrato NOT IN ('01', '02')) AS bajas_temporales
        FROM (
            SELECT n.rfc_receptor, et.tipo_contrato,
                   BOOL_OR(n.year_devengo = $4 AND n.month_devengo = $5) AS active_latest
            FROM pulso.nomina_normalizada n
            JOIN emp_tipo et ON et.rfc_receptor = n.rfc_receptor
            WHERE n.rfc_emisor = $1
              AND (n.year_devengo > $2 OR (n.year_devengo = $2 AND n.month_devengo >= $3))
              AND (n.year_devengo < $4 OR (n.year_devengo = $4 AND n.month_devengo <= $5))
              AND NOT n.is_excluded
            GROUP BY n.rfc_receptor, et.tipo_contrato
        ) emp
        "#,
    )
    .bind(rfc)
    .bind(ltm_start_y)
    .bind(ltm_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_one(pool)
    .await?;
    let latest_hc: i64 = emp_rows.try_get("latest_hc").unwrap_or(0);
    let bajas: i64 = emp_rows.try_get("bajas_permanentes").unwrap_or(0);
    let bajas_temporales: i64 = emp_rows.try_get("bajas_temporales").unwrap_or(0);

    if avg_hc <= 0.0 {
        return Ok(None);
    }

    let tasa_pct = bajas as f64 / avg_hc * 100.0;
    let nivel = h5a_nivel(tasa_pct);
    let interp = h5a_interpretacion(nivel);

    // L10-09: one figure, one semáforo -- the by-tipo-de-contrato breakdown belongs in
    // Nómina > Altas y bajas, not here. The second sentence only appears when there's
    // something it would otherwise hide: without it, a low permanent-turnover number could
    // read as "barely anyone left" even when dozens of obra contracts ended.
    let mut cuerpo = format!(
        "La rotación de plantilla permanente en los últimos 12 meses es de {:.1}% ({} baja{} / {:.1} \
         empleados de planta en promedio, {} activo{} en el último periodo).",
        tasa_pct,
        bajas,
        if bajas == 1 { "" } else { "s" },
        avg_hc,
        latest_hc,
        if latest_hc == 1 { "" } else { "s" }
    );
    if bajas_temporales > 0 {
        cuerpo.push_str(&format!(
            " Además terminaron {bajas_temporales} contrato{} por obra o tiempo determinado, que no cuentan como rotación.",
            if bajas_temporales == 1 { "" } else { "s" }
        ));
    }

    Ok(Some(Hallazgo {
        id: "H5A".to_string(),
        titulo: "Rotación de personal".to_string(),
        familia: "riesgo".to_string(),
        nivel: nivel.to_string(),
        metrica_principal: Some(tasa_pct),
        cuerpo,
        interpretacion: interp.to_string(),
        disclaimer: None,
        nota_fija: Some("Estimado a partir de CFDIs de nómina. Puede no reflejar movimientos que no se timbraron, o puede contener errores en el timbrado de CFDIs de nómina.".to_string()),
        datos_tabla: None,
    }))
}

// ---------------------------------------------------------------------------
// H5B — Baja de personal clave (top 10% salarial, últimos 24 meses)
// ---------------------------------------------------------------------------

async fn compute_h5b(pool: &DbPool, rfc: &str) -> anyhow::Result<Option<Hallazgo>> {
    let Some((ltm_end_y, ltm_end_m)) = nomina_last_period(pool, rfc).await? else {
        return Ok(None);
    };
    let (win_start_y, win_start_m) = subtract_months(ltm_end_y, ltm_end_m, 23);

    // L10-03 / AUD-098, DEC-050: reference distribution is the ACTIVE workforce (last devengo
    // month), never the terminated pool -- ranking "most key of the people who left" always
    // finds someone; ranking against the company's own current plantilla is what "clave"
    // actually means. Median salary here also becomes the floor below (trap 1).
    let active_rows = sqlx::query(
        r#"
        WITH base AS MATERIALIZED (
            SELECT n.rfc_receptor, n.year_devengo, n.month_devengo, n.tipo_nomina,
                   n.total_sueldos, n.num_dias_pagados, n.fecha_pago, n.fecha_inicio_rel_laboral
            FROM pulso.nomina_normalizada n
            WHERE n.rfc_emisor = $1 AND NOT n.is_excluded
        ),
        active_emps AS (
            SELECT DISTINCT rfc_receptor FROM base
            WHERE year_devengo = $2 AND month_devengo = $3
        ),
        last_ordinario AS (
            SELECT DISTINCT ON (b.rfc_receptor) b.rfc_receptor,
                (COALESCE(b.total_sueldos, 0)::float8 / NULLIF(b.num_dias_pagados, 0)::float8 * 30.0) AS sueldo
            FROM base b
            WHERE b.rfc_receptor IN (SELECT rfc_receptor FROM active_emps) AND b.tipo_nomina = 'O'
            ORDER BY b.rfc_receptor, b.fecha_pago DESC
        ),
        start_dates AS (
            SELECT b.rfc_receptor,
                COALESCE(
                    MAX(b.fecha_inicio_rel_laboral::date) FILTER (
                        WHERE b.fecha_inicio_rel_laboral::date >= DATE '1980-01-01'
                          AND b.fecha_inicio_rel_laboral::date <= CURRENT_DATE
                    ),
                    MIN(b.fecha_pago::date)
                ) AS start_date
            FROM base b
            WHERE b.rfc_receptor IN (SELECT rfc_receptor FROM active_emps)
            GROUP BY b.rfc_receptor
        )
        SELECT
            lo.sueldo,
            (((date_trunc('month', CURRENT_DATE) - interval '1 day')::date) - sd.start_date) / 365.25 AS antiguedad_years
        FROM last_ordinario lo
        JOIN start_dates sd ON sd.rfc_receptor = lo.rfc_receptor
        WHERE lo.sueldo > 0
        "#,
    )
    .bind(rfc)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_all(pool)
    .await?;

    if active_rows.is_empty() {
        return Ok(None);
    }

    let mut active_sueldos: Vec<f64> = active_rows.iter().map(|r| get_f64(r, "sueldo")).collect();
    let mut active_tenures: Vec<f64> = active_rows
        .iter()
        .map(|r| get_f64(r, "antiguedad_years"))
        .collect();
    active_sueldos.sort_by(|a, b| a.partial_cmp(b).unwrap());
    active_tenures.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let masa_salarial_activa: f64 = active_sueldos.iter().sum();
    let mediana_sueldo_activa = percentile_of_sorted(&active_sueldos, 0.5);

    // percentile_rank(x, sorted) = share of the reference population at or below x -- used
    // both for the sueldo floor above and for each candidate's índice below.
    let percentile_rank = |sorted: &[f64], x: f64| -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let count_le = sorted.iter().filter(|&&v| v <= x).count();
        count_le as f64 / sorted.len() as f64
    };

    // Employees with last payroll in the 24-month window but not in the latest month --
    // still "terminados en la ventana", unchanged from before this item.
    // L10-02a/b/c/d, L10-03 / AUD-083..087, AUD-098: nombre from nombre_receptor (not CURP);
    // fecha de ingreso from fecha_inicio_rel_laboral (not inferred from MIN(fecha_pago));
    // fecha de baja is fecha_final_pago, a real date (not a truncated month); sueldo is the
    // last ordinario receipt regardless of when it falls (a departing employee's very last
    // receipt is almost always a finiquito, tipo_nomina='E'); tipo_contrato taken from each
    // employee's own last receipt by date, never MAX/MIN/mode (L10-09's own warning applies
    // here too -- '03' sorts before '01' alphabetically and MAX would misclassify).
    let term_rows = sqlx::query(
        r#"
        WITH base AS MATERIALIZED (
            SELECT n.rfc_receptor, n.nombre_receptor, n.year_devengo, n.month_devengo,
                   n.tipo_nomina, n.tipo_contrato, n.total_sueldos, n.num_dias_pagados,
                   n.fecha_pago, n.fecha_final_pago, n.fecha_inicio_rel_laboral
            FROM pulso.nomina_normalizada n
            WHERE n.rfc_emisor = $1 AND NOT n.is_excluded
        ),
        term AS (
            SELECT rfc_receptor,
                MAX(year_devengo * 100 + month_devengo)::bigint AS last_period
            FROM base
            WHERE (year_devengo > $2 OR (year_devengo = $2 AND month_devengo >= $3))
              AND (year_devengo < $4 OR (year_devengo = $4 AND month_devengo <= $5))
            GROUP BY rfc_receptor
            HAVING MAX(year_devengo * 100 + month_devengo) < $4 * 100 + $5
        ),
        last_row AS (
            SELECT DISTINCT ON (b.rfc_receptor)
                b.rfc_receptor, b.nombre_receptor, b.tipo_contrato, b.fecha_final_pago
            FROM base b
            JOIN term t ON t.rfc_receptor = b.rfc_receptor
            ORDER BY b.rfc_receptor, b.fecha_pago DESC
        ),
        last_ordinario AS (
            SELECT DISTINCT ON (b.rfc_receptor) b.rfc_receptor,
                (COALESCE(b.total_sueldos, 0)::float8 / NULLIF(b.num_dias_pagados, 0)::float8 * 30.0) AS sueldo
            FROM base b
            JOIN term t ON t.rfc_receptor = b.rfc_receptor
            WHERE b.tipo_nomina = 'O'
            ORDER BY b.rfc_receptor, b.fecha_pago DESC
        ),
        start_dates AS (
            SELECT b.rfc_receptor,
                COALESCE(
                    MAX(b.fecha_inicio_rel_laboral::date) FILTER (
                        WHERE b.fecha_inicio_rel_laboral::date >= DATE '1980-01-01'
                          AND b.fecha_inicio_rel_laboral::date <= CURRENT_DATE
                    ),
                    MIN(b.fecha_pago::date)
                ) AS start_date
            FROM base b
            JOIN term t ON t.rfc_receptor = b.rfc_receptor
            GROUP BY b.rfc_receptor
        )
        SELECT
            t.rfc_receptor AS rfc,
            lr.nombre_receptor AS nombre,
            lr.tipo_contrato,
            sd.start_date::text AS start_date,
            lr.fecha_final_pago::text AS fecha_final_pago,
            lo.sueldo,
            (make_date((t.last_period/100)::int, (t.last_period%100)::int, 1) - sd.start_date) / 365.25 AS antiguedad_al_baja
        FROM term t
        JOIN last_row lr ON lr.rfc_receptor = t.rfc_receptor
        JOIN last_ordinario lo ON lo.rfc_receptor = t.rfc_receptor
        JOIN start_dates sd ON sd.rfc_receptor = t.rfc_receptor
        WHERE lo.sueldo > 0
        "#,
    )
    .bind(rfc)
    .bind(win_start_y)
    .bind(win_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_all(pool)
    .await?;

    if term_rows.is_empty() {
        return Ok(None);
    }

    struct TermRow {
        #[allow(dead_code)]
        rfc: String,
        nombre: String,
        tipo_contrato: String,
        fecha_ingreso: Option<String>,
        fecha_baja: Option<String>,
        sueldo: f64,
        antiguedad_years: f64,
    }

    let terminated: Vec<TermRow> = term_rows
        .iter()
        .map(|r| TermRow {
            rfc: r.try_get("rfc").unwrap_or_default(),
            nombre: r.try_get("nombre").unwrap_or_default(),
            tipo_contrato: r.try_get("tipo_contrato").unwrap_or_default(),
            fecha_ingreso: r.try_get("start_date").ok(),
            fecha_baja: r.try_get("fecha_final_pago").ok(),
            sueldo: get_f64(r, "sueldo"),
            antiguedad_years: get_f64(r, "antiguedad_al_baja"),
        })
        .collect();

    // L10-03 / DEC-050, trap 1/2: contrato indeterminado only, tenure >= 3 years measured AT
    // the moment of the baja (not against today), salary at or above the ACTIVE plantilla's
    // median -- someone paid less than the median employee isn't "personal clave" by pay.
    let key_exits: Vec<&TermRow> = terminated
        .iter()
        .filter(|t| {
            t.tipo_contrato == "01"
                && t.antiguedad_years >= 3.0
                && t.sueldo >= mediana_sueldo_activa
        })
        .collect();

    if key_exits.is_empty() {
        return Ok(None);
    }

    // L10-12 / AUD-101: level decided by what share of the active plantilla's total masa
    // salarial these exits represent, not by a raw headcount threshold -- two exits of
    // $18k each isn't the same signal as one of $234k, and a count-based cutoff can't tell
    // them apart once L10-03's filters shrink the population this much.
    let masa_perdida: f64 = key_exits.iter().map(|e| e.sueldo).sum();
    let pct_masa_perdida = if masa_salarial_activa > 0.0 {
        masa_perdida / masa_salarial_activa
    } else {
        0.0
    };
    let nivel = if pct_masa_perdida >= 0.15 {
        "critico"
    } else {
        "alto"
    };

    // Índice 50/50 en percentiles de la plantilla activa -- ver compute_h5b's own doc comment
    // on why percentiles and not simple ratios (a low-tenure, low-salary reference population
    // makes ratio-based scoring dominated by whichever variable has more spread).
    let mut ranked: Vec<&TermRow> = key_exits.clone();
    ranked.sort_by(|a, b| {
        let ia = 0.5 * percentile_rank(&active_sueldos, a.sueldo)
            + 0.5 * percentile_rank(&active_tenures, a.antiguedad_years);
        let ib = 0.5 * percentile_rank(&active_sueldos, b.sueldo)
            + 0.5 * percentile_rank(&active_tenures, b.antiguedad_years);
        ib.partial_cmp(&ia).unwrap()
    });

    let fmt_date =
        |d: &Option<String>| -> String { d.clone().unwrap_or_else(|| "—".to_string()) };

    let top: Vec<&TermRow> = ranked.iter().take(5).copied().collect();
    let extra = ranked.len().saturating_sub(top.len());

    let (cuerpo, datos_tabla) = if key_exits.len() == 1 {
        let emp = key_exits[0];
        let body = format!(
            "En los últimos 24 meses se detectó la baja de 1 empleado con nivel salarial relevante.\n\
             · Fecha de ingreso: {}\n\
             · Último periodo pagado: {}\n\
             · Último sueldo bruto mensual: {}",
            fmt_date(&emp.fecha_ingreso),
            fmt_date(&emp.fecha_baja),
            fmt_mxn_full(emp.sueldo)
        );
        (body, None)
    } else {
        let mut body = format!(
            "En los últimos 24 meses se detectaron {} bajas de empleados con nivel salarial relevante.",
            key_exits.len()
        );
        if extra > 0 {
            body.push_str(&format!(
                " Se muestran las 5 de mayor índice, y {extra} más."
            ));
        }
        let tabla: Vec<TablaRow> = top
            .iter()
            .map(|emp| TablaRow {
                nombre: emp.nombre.clone(),
                fecha_ingreso: fmt_date(&emp.fecha_ingreso),
                ultimo_periodo_pagado: fmt_date(&emp.fecha_baja),
                sueldo_mensual: emp.sueldo,
                tipo_contrato: emp.tipo_contrato.clone(),
            })
            .collect();
        (body, Some(tabla))
    };

    let interpretacion = if key_exits.len() == 1 {
        "Se recomienda validar si el rol era operativamente crítico y si existe un sustituto o reemplazo."
    } else {
        "Múltiples salidas de perfil senior pueden indicar restructura, conflicto interno o pérdida de talento clave."
    };

    Ok(Some(Hallazgo {
        id: "H5B".to_string(),
        titulo: "Baja de posible personal clave".to_string(),
        familia: "riesgo".to_string(),
        nivel: nivel.to_string(),
        metrica_principal: Some(key_exits.len() as f64),
        cuerpo,
        interpretacion: interpretacion.to_string(),
        disclaimer: None,
        nota_fija: Some("Fechas e ingreso desde CFDIs de nómina (fecha_inicio_rel_laboral / fecha_final_pago). Confirmar con expedientes de Recursos Humanos. Identificadores corresponden a RFC o nombre declarado en el CFDI.".to_string()),
        datos_tabla,
    }))
}

// ---------------------------------------------------------------------------
// H6 — CxC pendiente (emitidos)
// ---------------------------------------------------------------------------

async fn compute_h6(
    pool: &DbPool,
    rfc: &str,
    ltm_start_y: i64,
    ltm_start_m: i64,
    ltm_end_y: i64,
    ltm_end_m: i64,
) -> anyhow::Result<Option<Hallazgo>> {
    // Condition: must have at least one payment complement for emitidos
    let has_pagos_row = sqlx::query(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM pulso.cfdi_payments cp
            JOIN pulso.cfdis c ON c.uuid = cp.payment_uuid
            WHERE c.rfc_emisor = $1
              AND c.tipo_comprobante = 'P'
              AND NOT c.is_cancelled
        ) AS has_pagos
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let has_pagos: bool = has_pagos_row.try_get("has_pagos").unwrap_or(false);
    if !has_pagos {
        return Ok(None);
    }

    // Outstanding = base saldo for PPD invoices (L2-04: shared with payments.rs/counterparties.rs),
    // capped at the last complete calendar month like those two (L7-03 / DEC-039, AUD-011)
    // -- without it this disagreed with the Cobranza tab's own saldo for the same RFC.
    let outstanding_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(c.saldo_mxn), 0)::float8 AS outstanding
        FROM pulso.cfdi_cobro_estado c
        WHERE c.rfc_emisor = $1
          AND c.dl_type IN ('emitidos','ambos')
          AND c.metodo_pago = 'PPD'
          AND (c.year * 100 + c.month) <= $2
        "#,
    )
    .bind(rfc)
    .bind(current_month_yyyymm())
    .fetch_one(pool)
    .await?;
    let outstanding: f64 = get_f64(&outstanding_row, "outstanding");

    // C8-04 / AUD-068: H6's numerator (outstanding, above) is a balance -- con IVA, sin
    // exclusiones. A ratio between a balance and a P&L figure means nothing (DEC-044
    // backwards), so the denominator has to be the same shape, not `compute_ltm_ingreso`
    // (moved to the Resumen base by L8-03 for H9/H4, which are P&L measures and correctly
    // stay net-with-exclusions -- not touched here). Same form H7 already uses for its own
    // ratio, applied to the emitidos side.
    let ltm_ingreso_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(COALESCE(total_mxn,0))::float8, 0) AS total
        FROM pulso.cfdis
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante = 'I'
          AND NOT is_cancelled
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        "#,
    )
    .bind(rfc)
    .bind(ltm_start_y)
    .bind(ltm_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_one(pool)
    .await?;
    let ltm_ingreso: f64 = get_f64(&ltm_ingreso_row, "total");
    if ltm_ingreso <= 0.0 {
        return Ok(None);
    }

    let ratio_pct = outstanding / ltm_ingreso * 100.0;
    let nivel = cartera_pct_nivel(ratio_pct);
    let interp = h6_interpretacion(nivel);

    let cuerpo = format!(
        "El saldo de facturas PPD sin cobrar representa el {:.1}% del ingreso LTM ({}).",
        ratio_pct,
        fmt_mxn(outstanding)
    );

    Ok(Some(Hallazgo {
        id: "H6".to_string(),
        titulo: "Cartera pendiente de cobro".to_string(),
        familia: "riesgo".to_string(),
        nivel: nivel.to_string(),
        metrica_principal: Some(ratio_pct),
        cuerpo,
        interpretacion: interp.to_string(),
        disclaimer: Some("Calculado sobre complementos de pago disponibles. Interpretar como señal analítica, no como saldo de cuentas por cobrar definitivo.".to_string()),
        nota_fija: None,
        datos_tabla: None,
    }))
}

// ---------------------------------------------------------------------------
// H7 — CxP pendiente (recibidos)
// ---------------------------------------------------------------------------

async fn compute_h7(
    pool: &DbPool,
    rfc: &str,
    ltm_start_y: i64,
    ltm_start_m: i64,
    ltm_end_y: i64,
    ltm_end_m: i64,
) -> anyhow::Result<Option<Hallazgo>> {
    // Condition: must have payment complements where RFC is receptor
    let has_pagos_row = sqlx::query(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM pulso.cfdi_payments cp
            JOIN pulso.cfdis c ON c.uuid = cp.payment_uuid
            WHERE c.rfc_receptor = $1
              AND c.tipo_comprobante = 'P'
              AND NOT c.is_cancelled
        ) AS has_pagos
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let has_pagos: bool = has_pagos_row.try_get("has_pagos").unwrap_or(false);
    if !has_pagos {
        return Ok(None);
    }

    // L7-03 / DEC-039, AUD-011: capped at the last complete calendar month, same as H6 and
    // the Cobranza tab.
    let outstanding_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(c.saldo_mxn), 0)::float8 AS outstanding
        FROM pulso.cfdi_cobro_estado c
        WHERE c.rfc_receptor = $1
          AND c.dl_type IN ('recibidos','ambos')
          AND c.metodo_pago = 'PPD'
          AND (c.year * 100 + c.month) <= $2
        "#,
    )
    .bind(rfc)
    .bind(current_month_yyyymm())
    .fetch_one(pool)
    .await?;
    let outstanding: f64 = get_f64(&outstanding_row, "outstanding");

    // LTM gasto recibidos
    let ltm_gasto_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(COALESCE(total_mxn,0))::float8, 0) AS total
        FROM pulso.cfdis
        WHERE rfc_receptor = $1
          AND dl_type IN ('recibidos','ambos')
          AND tipo_comprobante = 'I'
          AND NOT is_cancelled
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        "#,
    )
    .bind(rfc)
    .bind(ltm_start_y)
    .bind(ltm_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_one(pool)
    .await?;
    let ltm_gasto: f64 = get_f64(&ltm_gasto_row, "total");

    if ltm_gasto <= 0.0 {
        return Ok(None);
    }

    let ratio_pct = outstanding / ltm_gasto * 100.0;
    let nivel = cartera_pct_nivel(ratio_pct);
    let interp = h7_interpretacion(nivel);

    let cuerpo = format!(
        "El saldo de facturas PPD sin pagar representa el {:.1}% del gasto LTM ({}).",
        ratio_pct,
        fmt_mxn(outstanding)
    );

    Ok(Some(Hallazgo {
        id: "H7".to_string(),
        titulo: "Cuentas por pagar pendientes".to_string(),
        familia: "riesgo".to_string(),
        nivel: nivel.to_string(),
        metrica_principal: Some(ratio_pct),
        cuerpo,
        interpretacion: interp.to_string(),
        disclaimer: Some(
            "Interpretar como señal analítica, no como saldo de cuentas por pagar definitivo."
                .to_string(),
        ),
        nota_fija: None,
        datos_tabla: None,
    }))
}

// ---------------------------------------------------------------------------
// H8 — Concentración de proveedores
// ---------------------------------------------------------------------------

async fn compute_h8(
    pool: &DbPool,
    rfc: &str,
    ltm_start_y: i64,
    ltm_start_m: i64,
    ltm_end_y: i64,
    ltm_end_m: i64,
) -> anyhow::Result<Option<Hallazgo>> {
    // L8-03 / DEC-041: same four properties as H1's query above.
    let rows = sqlx::query(
        r#"
        SELECT c.rfc_emisor, MAX(c.nombre_emisor) AS nombre,
               SUM(COALESCE(c.total_neto_mxn_ajustado,0))::float8 AS ltm_mxn
        FROM pulso.cfdis_ajustado c
        WHERE c.rfc_receptor = $1
          AND c.dl_type IN ('recibidos','ambos')
          AND c.tipo_comprobante NOT IN ('P','N','T')
          AND NOT c.is_cancelled
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        GROUP BY c.rfc_emisor
        ORDER BY ltm_mxn DESC
        "#,
    )
    .bind(rfc)
    .bind(ltm_start_y)
    .bind(ltm_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    struct SupRow {
        rfc: String,
        nombre: String,
        mxn: f64,
    }

    let suppliers: Vec<SupRow> = rows
        .iter()
        .map(|r| SupRow {
            rfc: r.try_get("rfc_emisor").unwrap_or_default(),
            nombre: r.try_get("nombre").unwrap_or_default(),
            mxn: get_f64(r, "ltm_mxn"),
        })
        .collect();

    // L8-07: exact RFC, not prefix -- IMS.../INF... matched a legitimate manufacturing
    // supplier of the RFC de control ($4,072) whose RFC happened to start the same way.
    // Confirmed by the team, not guessed: IMSS = IMS421231I45, Infonavit = INF7205011ZA.
    // L10-10 / AUD-099, trap 3: also excludes the "residente extranjero" generic RFC now,
    // for the same reason H1's identifiable pool below excludes both generic RFCs -- one
    // definition of "not a real counterparty", shared by clientes and proveedores.
    let is_regulatory = |rfc: &str| {
        rfc == super::summary::RFC_IMSS
            || rfc == super::summary::RFC_INFONAVIT
            || rfc == super::summary::RFC_PUBLICO_GENERAL
            || rfc == super::summary::RFC_EXTRANJERO_GENERICO
    };

    // L8-03: same negative-total edge case as H1 -- see its comment for why.
    let identifiable: Vec<&SupRow> = suppliers
        .iter()
        .filter(|s| !is_regulatory(&s.rfc) && s.mxn > 0.0)
        .collect();
    if identifiable.is_empty() {
        return Ok(None);
    }

    let total_excl: f64 = identifiable.iter().map(|s| s.mxn).sum();
    if total_excl <= 0.0 {
        return Ok(None);
    }

    let top3: Vec<&SupRow> = identifiable.iter().take(3).copied().collect();
    let top3_mxn: f64 = top3.iter().map(|s| s.mxn).sum();
    let top3_pct = top3_mxn / total_excl * 100.0;
    let top1_nombre = top3.first().map(|s| s.nombre.as_str()).unwrap_or("");
    let top1_pct = top3
        .first()
        .map(|s| s.mxn / total_excl * 100.0)
        .unwrap_or(0.0);

    let nivel = match concentracion_nivel(top3_pct, top1_pct) {
        Some(n) => n,
        None => return Ok(None), // < 25% Top 3 and < 35% Top 1 -- omit
    };

    let interp = h8_interpretacion(nivel);

    let cuerpo = format!(
        "El Top 3 proveedores representa el {:.1}% del gasto LTM. El mayor proveedor es {} con el {:.1}% del gasto.",
        top3_pct, top1_nombre, top1_pct
    );

    Ok(Some(Hallazgo {
        id: "H8".to_string(),
        titulo: "Concentración de proveedores".to_string(),
        familia: "riesgo".to_string(),
        nivel: nivel.to_string(),
        metrica_principal: Some(top3_pct),
        cuerpo,
        interpretacion: interp.to_string(),
        disclaimer: None,
        nota_fija: None,
        datos_tabla: None,
    }))
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

pub async fn get(pool: &DbPool, rfc: &str) -> anyhow::Result<HallazgosResponse> {
    // Establish LTM window from emitidos data
    let max_ym_row = sqlx::query(
        r#"
        SELECT MAX(year * 100 + month)::bigint AS max_ym
        FROM pulso.cfdis
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;

    let max_ym: Option<i64> = max_ym_row.try_get("max_ym").ok().flatten();
    // L8-01: anchor at the last CLOSED calendar month, not the last month with any
    // comprobante -- 5 of 6 RFC anchored on the in-progress current month (a handful of
    // days of data) before this. This one line reaches all ten hallazgos below, not just
    // H1/H2/H8/H9: H6/H7 (L7-03) and H3/H5A/H5B (Lote 6) all inherit it too, correctly.
    let (ltm_end_y, ltm_end_m) = match max_ym {
        Some(ym) if ym > 0 => {
            let ym = ym.min(current_month_yyyymm());
            (ym / 100, ym % 100)
        }
        _ => {
            return Ok(HallazgosResponse {
                visible: vec![],
                all: vec![],
            });
        }
    };
    let (ltm_start_y, ltm_start_m) = subtract_months(ltm_end_y, ltm_end_m, 11);

    // LTM-12 window for H9
    let (ltm_prev_end_y, ltm_prev_end_m) = subtract_months(ltm_end_y, ltm_end_m, 12);
    let (ltm_prev_start_y, ltm_prev_start_m) = subtract_months(ltm_end_y, ltm_end_m, 23);

    let mut all: Vec<Hallazgo> = Vec::new();

    // H1 — Concentración de clientes
    if let Some(h) = compute_h1(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m).await? {
        all.push(h);
    }

    // Annual emitidos data — needed for H2 (the `ingreso` figure), and H3 (only the
    // `complete_years` year list -- H3's own ingreso comes from its ing_rows/rec_rows
    // queries below, untouched by this one). L8-03 / DEC-041: H2's ingreso now sums
    // total_neto_mxn_ajustado over cfdis_ajustado with exclusions applied, same four
    // properties as H1's query above -- was con-IVA and counted only tipo_comprobante='I',
    // discarding notas de crédito ('E') entirely instead of netting them.
    let annual_rows = sqlx::query(
        r#"
        SELECT year,
               COUNT(DISTINCT month)::bigint AS month_count,
               SUM(COALESCE(total_neto_mxn_ajustado,0))::float8 AS ingreso
        FROM pulso.cfdis_ajustado c
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        GROUP BY year
        ORDER BY year
        "#,
    )
    .bind(rfc)
    .fetch_all(pool)
    .await?;

    struct AnnualEmitidos {
        year: i64,
        month_count: i64,
        ingreso: f64,
    }

    let annual_emitidos: Vec<AnnualEmitidos> = annual_rows
        .iter()
        .map(|r| AnnualEmitidos {
            year: r.try_get("year").unwrap_or(0),
            month_count: r.try_get("month_count").unwrap_or(0),
            ingreso: get_f64(r, "ingreso"),
        })
        .collect();

    let complete_years: Vec<&AnnualEmitidos> = annual_emitidos
        .iter()
        .filter(|y| y.month_count == 12)
        .collect();

    // H2 — Trayectoria de ingresos (CAGR)
    if complete_years.len() >= 2 {
        let first = &complete_years[0];
        let last = &complete_years[complete_years.len() - 1];
        let n_years = (last.year - first.year) as f64;
        if n_years > 0.0 && first.ingreso > 0.0 {
            let cagr_pct = ((last.ingreso / first.ingreso).powf(1.0 / n_years) - 1.0) * 100.0;
            let nivel = h2_nivel(cagr_pct);
            let interp = h2_interpretacion(nivel);
            all.push(Hallazgo {
                id: "H2".to_string(),
                titulo: "Trayectoria de ingresos".to_string(),
                familia: "desempeno".to_string(),
                nivel: nivel.to_string(),
                metrica_principal: Some(cagr_pct),
                // L8-06: H2 only ever uses complete calendar years -- correct for a CAGR,
                // but silently discarding the in-progress current year read as a
                // contradiction next to H9 (which does use it) with nothing on screen to
                // reconcile the two. Says its own window now instead of assuming it's
                // implied.
                cuerpo: format!(
                    "Los ingresos muestran un CAGR de {:.1}% en el período {}-{} (años calendario completos; no incluye el año en curso).",
                    cagr_pct, first.year, last.year
                ),
                interpretacion: interp.to_string(),
                disclaimer: None,
                nota_fija: None,
                datos_tabla: None,
            });
        }
    }

    // H3 — Evolución del flujo visible
    if complete_years.len() >= 2 {
        // R-34 + AUD-012 + AUD-013: aligned to the "Neto facturado" dashboard measure —
        // con IVA, facturas MENOS notas de crédito (the dashboard reads this as
        // ingreso_con_iva_mxn - egreso_con_iva_mxn from summary.rs, which is exactly
        // SUM(I) - SUM(E) within one direction), nómina neta de caja, and L3-01's shared
        // exclusion base instead of a hand-rolled join (AUD-012's broken AND/OR precedence
        // left the UUID branch of that join with no owner/action guard at all).
        let ing_rows = sqlx::query(
            r#"
            SELECT c.year,
                   SUM(CASE WHEN c.tipo_comprobante = 'I' THEN COALESCE(c.total_mxn,0)
                            WHEN c.tipo_comprobante = 'E' THEN -COALESCE(c.total_mxn,0)
                            ELSE 0 END)::float8 AS ingreso
            FROM pulso.cfdis c
            WHERE c.rfc_emisor = $1
              AND c.dl_type IN ('emitidos','ambos')
              AND c.tipo_comprobante IN ('I','E')
              AND NOT c.is_cancelled
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY c.year
            "#,
        )
        .bind(rfc)
        .fetch_all(pool)
        .await?;
        let ing_h3_map: std::collections::HashMap<i64, f64> = ing_rows
            .iter()
            .map(|r| {
                (
                    r.try_get::<i64, _>("year").unwrap_or(0),
                    get_f64(r, "ingreso"),
                )
            })
            .collect();

        // Recibidos per year (con IVA, menos notas de crédito recibidas)
        let rec_rows = sqlx::query(
            r#"
            SELECT c.year,
                   SUM(CASE WHEN c.tipo_comprobante = 'I' THEN COALESCE(c.total_mxn,0)
                            WHEN c.tipo_comprobante = 'E' THEN -COALESCE(c.total_mxn,0)
                            ELSE 0 END)::float8 AS egreso
            FROM pulso.cfdis c
            WHERE c.rfc_receptor = $1
              AND c.dl_type IN ('recibidos','ambos')
              AND c.tipo_comprobante IN ('I','E')
              AND NOT c.is_cancelled
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
              )
            GROUP BY c.year
            "#,
        )
        .bind(rfc)
        .fetch_all(pool)
        .await?;

        let rec_map: std::collections::HashMap<i64, f64> = rec_rows
            .iter()
            .map(|r| {
                (
                    r.try_get::<i64, _>("year").unwrap_or(0),
                    get_f64(r, "egreso"),
                )
            })
            .collect();

        // Looked up below against `yd.year`, the facturas-side (emisión) annual bucket H1/H2
        // already iterate -- an accepted mismatch of bases at the year grain: measured
        // system-wide, only one year boundary (2025/2026) has any receipts that cross it, for
        // 10,855 pesos total, so this doesn't manufacture a new visible divergence in practice.
        let nom_map = nomina_por_year(pool, rfc).await?;

        struct YearMargin {
            year: i64,
            margin_pct: f64,
        }

        let year_margins: Vec<YearMargin> = complete_years
            .iter()
            .filter_map(|yd| {
                let ingreso = *ing_h3_map.get(&yd.year).unwrap_or(&0.0);
                if ingreso <= 0.0 {
                    return None;
                }
                let egreso = *rec_map.get(&yd.year).unwrap_or(&0.0);
                let nomina = *nom_map.get(&yd.year).unwrap_or(&0.0);
                let flujo = ingreso - egreso - nomina;
                Some(YearMargin {
                    year: yd.year,
                    margin_pct: flujo / ingreso * 100.0,
                })
            })
            .collect();

        if year_margins.len() >= 2 {
            let fm = &year_margins[0];
            let lm = &year_margins[year_margins.len() - 1];
            let delta = lm.margin_pct - fm.margin_pct;
            let nivel = h3_nivel(delta);
            let interp = h3_interpretacion(nivel);
            all.push(Hallazgo {
                id: "H3".to_string(),
                titulo: "Evolución del flujo visible".to_string(),
                familia: "desempeno".to_string(),
                nivel: nivel.to_string(),
                metrica_principal: Some(delta),
                // L10-07 / AUD-095: the text used to name two terms (ingresos, egresos)
                // while the number behind it (margin_pct = flujo/ingreso, flujo = ingreso -
                // egreso - nomina) already subtracts a third. Naming all three here is the
                // whole fix -- the calculation itself doesn't change.
                cuerpo: format!(
                    "La relación ingresos vs egresos y nómina visibles pasó de {:.1}% en {} a {:.1}% en {} ({:+.1}pp).",
                    fm.margin_pct, fm.year, lm.margin_pct, lm.year, delta
                ),
                interpretacion: interp.to_string(),
                disclaimer: Some("Este indicador no representa EBITDA ni flujo de efectivo real. Se construye desde los CFDIs vigentes y puede verse afectado por normalizaciones pendientes, movimientos extraordinarios o compras de activo no reclasificadas.".to_string()),
                nota_fija: None,
                datos_tabla: None,
            });
        }
    }

    // H4, H5A, H5B — payroll hallazgos (conditional on nomina data)
    if let Ok(snap) = super::payroll::get_snapshot(pool, rfc).await
        && snap.has_data
    {
        // H4 — Pasivo laboral relativo
        let ltm_ingreso =
            compute_ltm_ingreso(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m)
                .await
                .unwrap_or(0.0);
        if ltm_ingreso > 0.0 {
            let ratio_pct = snap.pasivo_laboral_estimado_mxn / ltm_ingreso * 100.0;
            let meses_equiv = if snap.run_rate_mensual_ltm_mxn > 0.0 {
                snap.pasivo_laboral_estimado_mxn / snap.run_rate_mensual_ltm_mxn
            } else {
                0.0
            };
            let nivel = h4_nivel(ratio_pct);
            let interp = h4_interpretacion(nivel);
            let cuerpo_h4 = if snap.months_of_data >= 6 {
                format!(
                    "El pasivo laboral estimado asciende a {}, equivalente al {:.1}% del ingreso LTM y a {:.1} meses de nómina ordinaria estimada.",
                    fmt_mxn(snap.pasivo_laboral_estimado_mxn),
                    ratio_pct,
                    meses_equiv
                )
            } else {
                format!(
                    "El pasivo laboral estimado asciende a {}, equivalente al {:.1}% del ingreso LTM.",
                    fmt_mxn(snap.pasivo_laboral_estimado_mxn),
                    ratio_pct,
                )
            };
            all.push(Hallazgo {
                    id: "H4".to_string(),
                    titulo: "Pasivo laboral estimado".to_string(),
                    familia: "riesgo".to_string(),
                    nivel: nivel.to_string(),
                    metrica_principal: Some(ratio_pct),
                    cuerpo: cuerpo_h4,
                    interpretacion: interp.to_string(),
                    disclaimer: None,
                    nota_fija: Some("Estimación con prestaciones de ley: aguinaldo 15 días, vacaciones y prima vacacional según Ley Federal del Trabajo. No constituye un cálculo definitivo.".to_string()),
                    datos_tabla: None,
                });
        }

        // H5A — Rotación
        if let Some(h) = compute_h5a(pool, rfc).await? {
            all.push(h);
        }

        // H5B — Personal clave
        if let Some(h) = compute_h5b(pool, rfc).await? {
            all.push(h);
        }
    }

    // H6 — CxC pendiente
    if let Some(h) = compute_h6(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m).await? {
        all.push(h);
    }

    // H7 — CxP pendiente
    if let Some(h) = compute_h7(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m).await? {
        all.push(h);
    }

    // H8 — Concentración de proveedores
    if let Some(h) = compute_h8(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m).await? {
        all.push(h);
    }

    // H9 — Momentum reciente (≥ 24 months condition). L8-03: same base as the other five --
    // a month excluded entirely shouldn't count toward the 24-month gate either.
    let total_months_row = sqlx::query(
        r#"
        SELECT COUNT(DISTINCT year * 100 + month)::bigint AS cnt
        FROM pulso.cfdis_ajustado c
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante NOT IN ('P','N','T')
          AND NOT is_cancelled
          AND NOT EXISTS (
              SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = $1 AND ex.uuid = c.uuid
          )
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let total_months: i64 = total_months_row.try_get("cnt").unwrap_or(0);

    if total_months >= 24 {
        let ltm_current =
            compute_ltm_ingreso(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m)
                .await
                .unwrap_or(0.0);
        let ltm_prev = compute_ltm_ingreso(
            pool,
            rfc,
            ltm_prev_start_y,
            ltm_prev_start_m,
            ltm_prev_end_y,
            ltm_prev_end_m,
        )
        .await
        .unwrap_or(0.0);

        if ltm_prev > 0.0 {
            let delta_pct = (ltm_current / ltm_prev - 1.0) * 100.0;
            let nivel = h9_nivel(delta_pct);
            let interp = h9_interpretacion(nivel);
            let ltm_label = format!(
                "{}-{:02} a {}-{:02}",
                ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m
            );
            all.push(Hallazgo {
                id: "H9".to_string(),
                titulo: "Momentum reciente de ingresos".to_string(),
                familia: "desempeno".to_string(),
                nivel: nivel.to_string(),
                metrica_principal: Some(delta_pct),
                cuerpo: format!(
                    "El ingreso LTM {} muestra una variación de {:+.1}% vs el LTM anterior.",
                    ltm_label, delta_pct
                ),
                interpretacion: interp.to_string(),
                disclaimer: None,
                nota_fija: Some("Comparación LTM vs LTM-12. No anualizado. Verificar posibles efectos estacionales o eventos puntuales antes de concluir sobre tendencia.".to_string()),
                datos_tabla: None,
            });
        }
    }

    // L8-02 / DEC-043: H4 "Pasivo laboral estimado" doesn't ship in the launch -- dropped
    // here, the one place, so it never reaches the response (not just hidden from the
    // five visible slots).
    all.retain(|h| h.id != "H4");

    // -------------------------------------------------------------------------
    // Ranking & visible selection (max 5)
    // -------------------------------------------------------------------------
    let h5b = all.iter().find(|h| h.id == "H5B").cloned();
    // L10-07 / AUD-096, trap 3: H3 is the only hallazgo that speaks to the business result
    // itself (ingresos vs egresos y nómina) -- reserved a slot the same way H5B already is,
    // so a severity-only ranking (which favors risk-family bad news) can't push out the one
    // performance hallazgo that exists, whether its own news is good or bad.
    let h3 = all.iter().find(|h| h.id == "H3").cloned();
    let mut others: Vec<Hallazgo> = all
        .iter()
        .filter(|h| h.id != "H5B" && h.id != "H3")
        .cloned()
        .collect();

    others.sort_by(|a, b| {
        severity_score(&a.nivel)
            .cmp(&severity_score(&b.nivel))
            .then(h_priority(&a.id).cmp(&h_priority(&b.id)))
    });

    let max_slots = 5usize;
    let reserved_slots = h5b.is_some() as usize + h3.is_some() as usize;
    let remaining = max_slots.saturating_sub(reserved_slots);

    let mut visible: Vec<Hallazgo> = Vec::new();
    if let Some(b) = h5b.clone() {
        visible.push(b);
    }
    if let Some(t) = h3.clone() {
        visible.push(t);
    }
    visible.extend(others.into_iter().take(remaining));

    Ok(HallazgosResponse { visible, all })
}
