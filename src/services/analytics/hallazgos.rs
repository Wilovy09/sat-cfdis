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
    pub fecha_primer_pago: String,
    pub fecha_baja: String,
    pub sueldo_mensual: f64,
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

fn h1_nivel(top3_pct: f64) -> &'static str {
    if top3_pct < 40.0 {
        "muy_bajo"
    } else if top3_pct < 60.0 {
        "bajo"
    } else if top3_pct < 75.0 {
        "medio"
    } else if top3_pct < 85.0 {
        "alto"
    } else {
        "critico"
    }
}

fn h1_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "critico" => "Dependencia extrema — la pérdida o renegociación de cualquiera de estas cuentas tiene impacto directo sobre la viabilidad del negocio.",
        "alto" => "Concentración elevada con riesgo de pérdida material ante churn de las cuentas principales.",
        "medio" => "Concentración moderada. Revisar recurrencia y antigüedad de las cuentas principales en el módulo de Emitidas.",
        _ => "Base de clientes diversificada. Sin concentración crítica observable en LTM.",
    }
}

fn h2_nivel(cagr: f64) -> &'static str {
    if cagr > 15.0 {
        "muy_positivo"
    } else if cagr > 5.0 {
        "positivo"
    } else if cagr >= 0.0 {
        "neutral"
    } else if cagr >= -15.0 {
        "negativo"
    } else {
        "muy_negativo"
    }
}

fn h2_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "muy_negativo" | "negativo" => "Caída sostenida de ingresos en el período analizado. Requiere explicación de gestión antes de cualquier ejercicio de valoración.",
        "neutral" => "Crecimiento marginal. Insuficiente para absorber inflación de costos sin compresión de flujo visible.",
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
        "muy_negativo" => "Deterioro de flujo visible: los egresos crecen más rápido que los ingresos. Revisar drivers de gasto y masa salarial en detalle.",
        "negativo" => "Presión creciente sobre el flujo visible. Verificar evolución de egresos y nómina frente a tendencia de ingresos.",
        "neutral" => "Relación ingresos/egresos estable en el período analizado.",
        _ => "Mejora en la relación ingresos vs egresos visibles.",
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
        "critico" | "alto" => "Pasivo laboral de peso significativo. Requiere análisis detallado de antigüedad, sueldos y estructura de plantilla.",
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
        "critico" | "alto" => "Rotación elevada — señal de inestabilidad operativa o condiciones laborales que requieren validación. Revisar distribución por departamento y nivel salarial en módulo de Nómina.",
        "medio" => "Rotación moderada. Verificar si se concentra en áreas críticas o corresponde a patrones estacionales.",
        _ => "Plantilla estable en el período analizado.",
    }
}

fn h6_nivel(ratio_pct: f64) -> &'static str {
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
        "critico" | "alto" => "Cartera material en riesgo. Revisar antigüedad y concentración de saldos en el módulo de Cobranza.",
        "medio" => "Saldo pendiente relevante. Verificar composición por cliente y buckets de antigüedad.",
        _ => "Cobranza eficiente. Cartera pendiente dentro de rangos normales.",
    }
}

fn h7_nivel(ratio_pct: f64) -> &'static str {
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

fn h7_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "critico" | "alto" => "Saldo relevante con proveedores. Revisar antigüedad, concentración y posible impacto en relaciones comerciales o liquidez.",
        "medio" => "Saldo moderado. Verificar composición por proveedor y buckets de vencimiento.",
        _ => "Disciplina de pago sólida. Sin pasivo material observable con proveedores.",
    }
}

fn h8_nivel(top3_pct: f64) -> Option<&'static str> {
    if top3_pct < 15.0 {
        None // omit
    } else if top3_pct < 25.0 {
        Some("bajo")
    } else if top3_pct < 40.0 {
        Some("medio")
    } else if top3_pct < 60.0 {
        Some("alto")
    } else {
        Some("critico")
    }
}

fn h8_interpretacion(nivel: &str) -> &'static str {
    match nivel {
        "critico" | "alto" => "Concentración elevada en pocos proveedores. Una interrupción en las relaciones principales tendría impacto material sobre la operación. Revisar condiciones contractuales.",
        "medio" => "Concentración moderada. Validar diversificación, exclusividad y riesgo de sustitución en el proceso de due diligence.",
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
        "muy_negativo" => "Caída material en la ventana más reciente. El CAGR histórico puede enmascarar un deterioro acelerado. Contrastar con CAGR histórico y revisar módulo de Emitidas.",
        "negativo" => "Desaceleración visible en los últimos 12 meses vs el período anterior. Contrastar con el CAGR histórico para distinguir corrección temporal de deterioro estructural.",
        "neutral" => "Ingresos recientes en línea con el LTM anterior. Sin aceleración ni deterioro visible en la ventana más reciente.",
        "positivo" => "Ingresos recientes por encima del LTM anterior. Momentum favorable.",
        _ => "Aceleración de ingresos en la ventana más reciente. Señal positiva de momentum comercial.",
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
    let row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(COALESCE(total_mxn,0))::float8, 0) AS total
        FROM pulso.cfdis
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante = 'I'
          AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        "#,
    )
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<f64, _>("total").unwrap_or(0.0))
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
    let rows = sqlx::query(
        r#"
        SELECT rfc_receptor, MAX(nombre_receptor) AS nombre, SUM(COALESCE(total_mxn,0))::float8 AS ltm_mxn
        FROM pulso.cfdis
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante = 'I'
          AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY rfc_receptor
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
            mxn: r.try_get("ltm_mxn").unwrap_or(0.0),
        })
        .collect();

    let total_ltm: f64 = clients.iter().map(|c| c.mxn).sum();
    if total_ltm <= 0.0 {
        return Ok(None);
    }

    // PeG share
    let peg_mxn: f64 = clients
        .iter()
        .filter(|c| c.rfc == "XAXX010101000")
        .map(|c| c.mxn)
        .sum();
    let peg_pct = peg_mxn / total_ltm * 100.0;

    // Top 3 excluding PeG (take first 3 non-XAXX clients)
    let identifiable: Vec<&ClientRow> = clients
        .iter()
        .filter(|c| c.rfc != "XAXX010101000")
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
    let n = top3.len();

    let nivel = h1_nivel(top3_pct);
    let interp = h1_interpretacion(nivel);

    let mut cuerpo = format!(
        "El Top {} cliente{} representa el {:.1}% del ingreso LTM.",
        n,
        if n == 1 { "" } else { "s" },
        top3_pct
    );

    if peg_pct > 30.0 {
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

async fn compute_h5a(
    pool: &DbPool,
    rfc: &str,
    ltm_start_y: i64,
    ltm_start_m: i64,
    ltm_end_y: i64,
    ltm_end_m: i64,
) -> anyhow::Result<Option<Hallazgo>> {
    // Per-month distinct employee count in LTM
    let month_rows = sqlx::query(
        r#"
        SELECT c.year, c.month, COUNT(DISTINCT c.rfc_receptor)::bigint AS hc
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY c.year, c.month
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
    let avg_hc = hc_per_month.iter().sum::<i64>() as f64 / hc_per_month.len() as f64;

    // Employees active in last period
    let latest_row = sqlx::query(
        r#"
        SELECT COUNT(DISTINCT c.rfc_receptor)::bigint AS active
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND c.year = $2 AND c.month = $3
        "#,
    )
    .bind(rfc)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_one(pool)
    .await?;
    let _latest_hc: i64 = latest_row.try_get("active").unwrap_or(0);

    // Employees who appeared in LTM but not in the latest month
    let bajas_row = sqlx::query(
        r#"
        SELECT COUNT(DISTINCT ltm.rfc_receptor)::bigint AS bajas
        FROM (
            SELECT DISTINCT c.rfc_receptor
            FROM pulso.cfdi_nomina n
            JOIN pulso.cfdis c ON c.uuid = n.uuid
            WHERE c.rfc_emisor = $1
              AND c.tipo_comprobante = 'N'
              AND COALESCE(c.estado_sat,'') != 'cancelado'
              AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
              AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        ) ltm
        WHERE NOT EXISTS (
            SELECT 1 FROM pulso.cfdi_nomina n2
            JOIN pulso.cfdis c2 ON c2.uuid = n2.uuid
            WHERE c2.rfc_emisor = $1
              AND c2.tipo_comprobante = 'N'
              AND COALESCE(c2.estado_sat,'') != 'cancelado'
              AND c2.rfc_receptor = ltm.rfc_receptor
              AND c2.year = $4 AND c2.month = $5
        )
        "#,
    )
    .bind(rfc)
    .bind(ltm_start_y)
    .bind(ltm_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_one(pool)
    .await?;
    let bajas: i64 = bajas_row.try_get("bajas").unwrap_or(0);

    if avg_hc <= 0.0 {
        return Ok(None);
    }

    let tasa_pct = bajas as f64 / avg_hc * 100.0;
    let nivel = h5a_nivel(tasa_pct);
    let interp = h5a_interpretacion(nivel);

    let cuerpo = format!(
        "La rotación estimada en los últimos 12 meses es de {:.1}% ({} baja{} / headcount promedio {:.0} empleados).",
        tasa_pct,
        bajas,
        if bajas == 1 { "" } else { "s" },
        avg_hc
    );

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

async fn compute_h5b(
    pool: &DbPool,
    rfc: &str,
    ltm_end_y: i64,
    ltm_end_m: i64,
) -> anyhow::Result<Option<Hallazgo>> {
    let (win_start_y, win_start_m) = subtract_months(ltm_end_y, ltm_end_m, 23);

    // Employees with last payroll in the 24-month window but not in latest month
    let term_rows = sqlx::query(
        r#"
        SELECT
            c.rfc_receptor                                                       AS rfc,
            MAX(COALESCE(n.curp, c.rfc_receptor))                               AS nombre,
            MIN(c.year * 100 + c.month)::bigint                                 AS first_period,
            MAX(c.year * 100 + c.month)::bigint                                 AS last_period,
            AVG(COALESCE(n.salario_diario_integrado, 0)::float8) * 30           AS sueldo_mensual
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY c.rfc_receptor
        HAVING MAX(c.year * 100 + c.month) < $4 * 100 + $5
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
        first_period: i64,
        last_period: i64,
        sueldo: f64,
    }

    let terminated: Vec<TermRow> = term_rows
        .iter()
        .map(|r| TermRow {
            rfc: r.try_get("rfc").unwrap_or_default(),
            nombre: r.try_get("nombre").unwrap_or_default(),
            first_period: r.try_get::<i64, _>("first_period").unwrap_or(0),
            last_period: r.try_get::<i64, _>("last_period").unwrap_or(0),
            sueldo: r.try_get("sueldo_mensual").unwrap_or(0.0),
        })
        .collect();

    if terminated.is_empty() {
        return Ok(None);
    }

    // 90th percentile salary among terminated employees
    let mut sueldos: Vec<f64> = terminated.iter().map(|t| t.sueldo).collect();
    sueldos.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p90_idx = ((sueldos.len() as f64 * 0.9).ceil() as usize).saturating_sub(1);
    let p90 = sueldos[p90_idx];

    // Filter terminated employees in top 10% salary
    let key_exits: Vec<&TermRow> = terminated.iter().filter(|t| t.sueldo >= p90).collect();

    if key_exits.is_empty() {
        return Ok(None);
    }

    let nivel = if key_exits.len() >= 2 { "critico" } else { "alto" };

    let fmt_period = |ym: i64| -> String {
        format!("{}-{:02}", ym / 100, ym % 100)
    };

    let (cuerpo, datos_tabla) = if key_exits.len() == 1 {
        let emp = key_exits[0];
        let body = format!(
            "En los últimos 24 meses se detectó la baja de 1 empleado con nivel salarial relevante.\n\
             · Fecha de inicio de relación: {}\n\
             · Fecha de baja estimada: {}\n\
             · Sueldo mensual promedio: {}",
            fmt_period(emp.first_period),
            fmt_period(emp.last_period),
            fmt_mxn(emp.sueldo)
        );
        (body, None)
    } else {
        let body = format!(
            "En los últimos 24 meses se detectaron {} bajas de empleados con nivel salarial relevante.",
            key_exits.len()
        );
        let tabla: Vec<TablaRow> = key_exits
            .iter()
            .map(|emp| TablaRow {
                nombre: emp.nombre.clone(),
                fecha_primer_pago: fmt_period(emp.first_period),
                fecha_baja: fmt_period(emp.last_period),
                sueldo_mensual: emp.sueldo,
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
        nota_fija: Some("Fechas inferidas desde CFDIs de nómina. Confirmar con expedientes de Recursos Humanos. Identificadores corresponden a RFC o CURP del empleado.".to_string()),
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
              AND UPPER(COALESCE(c.estado_sat,'')) NOT LIKE '%CANCEL%'
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

    // Outstanding = sum of PPD invoices - payments received - credit notes
    let outstanding_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(GREATEST(
            COALESCE(inv.total_mxn, 0)::float8
            - COALESCE((
                SELECT SUM(pd.imp_pagado)
                FROM pulso.cfdi_payment_docs pd
                JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
                WHERE pd.invoice_uuid = inv.uuid
                  AND UPPER(COALESCE(comp.estado_sat,'')) NOT LIKE '%CANCEL%'
            ), 0)
            - COALESCE((
                SELECT SUM(cr_inv.total_mxn)
                FROM pulso.cfdi_relacionados cr
                JOIN pulso.cfdis cr_inv ON cr_inv.uuid = cr.source_uuid
                WHERE cr.related_uuid = inv.uuid
                  AND cr.tipo_relacion = '01'
                  AND cr_inv.tipo_comprobante = 'E'
                  AND UPPER(COALESCE(cr_inv.estado_sat,'')) NOT LIKE '%CANCEL%'
            ), 0),
            0
        )), 0)::float8 AS outstanding
        FROM pulso.cfdis inv
        WHERE inv.rfc_emisor = $1
          AND inv.dl_type IN ('emitidos','ambos')
          AND inv.tipo_comprobante = 'I'
          AND inv.metodo_pago = 'PPD'
          AND UPPER(COALESCE(inv.estado_sat,'')) NOT LIKE '%CANCEL%'
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let outstanding: f64 = outstanding_row.try_get("outstanding").unwrap_or(0.0);

    let ltm_ingreso =
        compute_ltm_ingreso(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m).await?;
    if ltm_ingreso <= 0.0 {
        return Ok(None);
    }

    let ratio_pct = outstanding / ltm_ingreso * 100.0;
    let nivel = h6_nivel(ratio_pct);
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
              AND UPPER(COALESCE(c.estado_sat,'')) NOT LIKE '%CANCEL%'
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

    let outstanding_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(GREATEST(
            COALESCE(inv.total_mxn, 0)::float8
            - COALESCE((
                SELECT SUM(pd.imp_pagado)
                FROM pulso.cfdi_payment_docs pd
                JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
                WHERE pd.invoice_uuid = inv.uuid
                  AND UPPER(COALESCE(comp.estado_sat,'')) NOT LIKE '%CANCEL%'
            ), 0)
            - COALESCE((
                SELECT SUM(cr_inv.total_mxn)
                FROM pulso.cfdi_relacionados cr
                JOIN pulso.cfdis cr_inv ON cr_inv.uuid = cr.source_uuid
                WHERE cr.related_uuid = inv.uuid
                  AND cr.tipo_relacion = '01'
                  AND cr_inv.tipo_comprobante = 'E'
                  AND UPPER(COALESCE(cr_inv.estado_sat,'')) NOT LIKE '%CANCEL%'
            ), 0),
            0
        )), 0)::float8 AS outstanding
        FROM pulso.cfdis inv
        WHERE inv.rfc_receptor = $1
          AND inv.dl_type IN ('recibidos','ambos')
          AND inv.tipo_comprobante = 'I'
          AND inv.metodo_pago = 'PPD'
          AND UPPER(COALESCE(inv.estado_sat,'')) NOT LIKE '%CANCEL%'
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let outstanding: f64 = outstanding_row.try_get("outstanding").unwrap_or(0.0);

    // LTM gasto recibidos
    let ltm_gasto_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(COALESCE(total_mxn,0))::float8, 0) AS total
        FROM pulso.cfdis
        WHERE rfc_receptor = $1
          AND dl_type IN ('recibidos','ambos')
          AND tipo_comprobante = 'I'
          AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
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
    let ltm_gasto: f64 = ltm_gasto_row.try_get("total").unwrap_or(0.0);

    if ltm_gasto <= 0.0 {
        return Ok(None);
    }

    let ratio_pct = outstanding / ltm_gasto * 100.0;
    let nivel = h7_nivel(ratio_pct);
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
        disclaimer: Some("Interpretar como señal analítica, no como saldo de cuentas por pagar definitivo.".to_string()),
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
    let rows = sqlx::query(
        r#"
        SELECT rfc_emisor, MAX(nombre_emisor) AS nombre, SUM(COALESCE(total_mxn,0))::float8 AS ltm_mxn
        FROM pulso.cfdis
        WHERE rfc_receptor = $1
          AND dl_type IN ('recibidos','ambos')
          AND tipo_comprobante = 'I'
          AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY rfc_emisor
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
            mxn: r.try_get("ltm_mxn").unwrap_or(0.0),
        })
        .collect();

    // Exclude regulatory RFCs from concentration calculation
    let is_regulatory = |rfc: &str| {
        rfc.starts_with("IMS") || rfc.starts_with("INF") || rfc == "XAXX010101000"
    };

    let identifiable: Vec<&SupRow> = suppliers.iter().filter(|s| !is_regulatory(&s.rfc)).collect();
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

    let nivel = match h8_nivel(top3_pct) {
        Some(n) => n,
        None => return Ok(None), // < 15% — omit
    };

    let interp = h8_interpretacion(nivel);
    let top1_nombre = top3.first().map(|s| s.nombre.as_str()).unwrap_or("");
    let top1_pct = top3.first().map(|s| s.mxn / total_excl * 100.0).unwrap_or(0.0);

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
          AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;

    let max_ym: Option<i64> = max_ym_row.try_get("max_ym").ok().flatten();
    let (ltm_end_y, ltm_end_m) = match max_ym {
        Some(ym) if ym > 0 => (ym / 100, ym % 100),
        _ => return Ok(HallazgosResponse { visible: vec![], all: vec![] }),
    };
    let (ltm_start_y, ltm_start_m) = subtract_months(ltm_end_y, ltm_end_m, 11);

    // LTM-12 window for H9
    let (ltm_prev_end_y, ltm_prev_end_m) = subtract_months(ltm_end_y, ltm_end_m, 12);
    let (ltm_prev_start_y, ltm_prev_start_m) = subtract_months(ltm_end_y, ltm_end_m, 23);

    let mut all: Vec<Hallazgo> = Vec::new();

    // H1 — Concentración de clientes
    if let Some(h) =
        compute_h1(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m).await?
    {
        all.push(h);
    }

    // Annual emitidos data — needed for H2, H3
    let annual_rows = sqlx::query(
        r#"
        SELECT year,
               COUNT(DISTINCT month)::bigint AS month_count,
               SUM(CASE WHEN tipo_comprobante='I' THEN COALESCE(total_mxn,0) ELSE 0 END)::float8 AS ingreso
        FROM pulso.cfdis
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante NOT IN ('P','N','T')
          AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
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
            ingreso: r.try_get("ingreso").unwrap_or(0.0),
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
                cuerpo: format!(
                    "Los ingresos muestran un CAGR de {:.1}% en el período {}-{}.",
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
        // Recibidos per year
        let rec_rows = sqlx::query(
            r#"
            SELECT year, SUM(COALESCE(total_mxn,0))::float8 AS egreso
            FROM pulso.cfdis
            WHERE rfc_receptor = $1
              AND dl_type IN ('recibidos','ambos')
              AND tipo_comprobante = 'I'
              AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
            GROUP BY year
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
                    r.try_get::<f64, _>("egreso").unwrap_or(0.0),
                )
            })
            .collect();

        // Nomina ordinary per year (excluding extraordinary percepciones)
        let nom_rows = sqlx::query(
            r#"
            SELECT c.year, SUM(COALESCE(p.importe_gravado,0) + COALESCE(p.importe_exento,0))::float8 AS nomina
            FROM pulso.cfdi_nomina_percepciones p
            JOIN pulso.cfdi_nomina n ON n.uuid = p.uuid
            JOIN pulso.cfdis c ON c.uuid = n.uuid
            WHERE c.rfc_emisor = $1
              AND c.tipo_comprobante = 'N'
              AND COALESCE(c.estado_sat,'') != 'cancelado'
              AND p.tipo_percepcion NOT IN ('002','003','022','038','039','044','045')
            GROUP BY c.year
            "#,
        )
        .bind(rfc)
        .fetch_all(pool)
        .await?;

        let nom_map: std::collections::HashMap<i64, f64> = nom_rows
            .iter()
            .map(|r| {
                (
                    r.try_get::<i64, _>("year").unwrap_or(0),
                    r.try_get::<f64, _>("nomina").unwrap_or(0.0),
                )
            })
            .collect();

        struct YearMargin {
            year: i64,
            margin_pct: f64,
        }

        let year_margins: Vec<YearMargin> = complete_years
            .iter()
            .filter_map(|yd| {
                if yd.ingreso <= 0.0 {
                    return None;
                }
                let egreso = *rec_map.get(&yd.year).unwrap_or(&0.0);
                let nomina = *nom_map.get(&yd.year).unwrap_or(&0.0);
                let flujo = yd.ingreso - egreso - nomina;
                Some(YearMargin {
                    year: yd.year,
                    margin_pct: flujo / yd.ingreso * 100.0,
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
                cuerpo: format!(
                    "La relación ingresos vs egresos visibles pasó de {:.1}% en {} a {:.1}% en {} ({:+.1}pp).",
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
    if let Ok(snap) = super::payroll::get_snapshot(pool, rfc).await {
        if snap.has_data {
            // H4 — Pasivo laboral relativo
            let ltm_ingreso = compute_ltm_ingreso(
                pool,
                rfc,
                ltm_start_y,
                ltm_start_m,
                ltm_end_y,
                ltm_end_m,
            )
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
                all.push(Hallazgo {
                    id: "H4".to_string(),
                    titulo: "Pasivo laboral estimado".to_string(),
                    familia: "riesgo".to_string(),
                    nivel: nivel.to_string(),
                    metrica_principal: Some(ratio_pct),
                    cuerpo: format!(
                        "El pasivo laboral estimado asciende a {}, equivalente al {:.1}% del ingreso LTM y a {:.1} meses de nómina ordinaria estimada.",
                        fmt_mxn(snap.pasivo_laboral_estimado_mxn),
                        ratio_pct,
                        meses_equiv
                    ),
                    interpretacion: interp.to_string(),
                    disclaimer: None,
                    nota_fija: Some("Estimación con prestaciones de ley: aguinaldo 15 días, vacaciones y prima vacacional según Ley Federal del Trabajo. No constituye un cálculo definitivo.".to_string()),
                    datos_tabla: None,
                });
            }

            // H5A — Rotación
            if let Some(h) = compute_h5a(
                pool,
                rfc,
                ltm_start_y,
                ltm_start_m,
                ltm_end_y,
                ltm_end_m,
            )
            .await?
            {
                all.push(h);
            }

            // H5B — Personal clave
            if let Some(h) = compute_h5b(pool, rfc, ltm_end_y, ltm_end_m).await? {
                all.push(h);
            }
        }
    }

    // H6 — CxC pendiente
    if let Some(h) =
        compute_h6(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m).await?
    {
        all.push(h);
    }

    // H7 — CxP pendiente
    if let Some(h) =
        compute_h7(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m).await?
    {
        all.push(h);
    }

    // H8 — Concentración de proveedores
    if let Some(h) =
        compute_h8(pool, rfc, ltm_start_y, ltm_start_m, ltm_end_y, ltm_end_m).await?
    {
        all.push(h);
    }

    // H9 — Momentum reciente (≥ 24 months condition)
    let total_months_row = sqlx::query(
        r#"
        SELECT COUNT(DISTINCT year * 100 + month)::bigint AS cnt
        FROM pulso.cfdis
        WHERE rfc_emisor = $1
          AND dl_type IN ('emitidos','ambos')
          AND tipo_comprobante NOT IN ('P','N','T')
          AND UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%'
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let total_months: i64 = total_months_row.try_get("cnt").unwrap_or(0);

    if total_months >= 24 {
        let ltm_current = compute_ltm_ingreso(
            pool,
            rfc,
            ltm_start_y,
            ltm_start_m,
            ltm_end_y,
            ltm_end_m,
        )
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

    // -------------------------------------------------------------------------
    // Ranking & visible selection (max 5)
    // -------------------------------------------------------------------------
    let h5b = all.iter().find(|h| h.id == "H5B").cloned();
    let mut others: Vec<Hallazgo> = all.iter().filter(|h| h.id != "H5B").cloned().collect();

    others.sort_by(|a, b| {
        severity_score(&a.nivel)
            .cmp(&severity_score(&b.nivel))
            .then(h_priority(&a.id).cmp(&h_priority(&b.id)))
    });

    let max_slots = 5usize;
    let h5b_slot = if h5b.is_some() { 1 } else { 0 };
    let remaining = max_slots.saturating_sub(h5b_slot);

    let mut visible: Vec<Hallazgo> = Vec::new();
    if let Some(b) = h5b.clone() {
        visible.push(b);
    }
    visible.extend(others.into_iter().take(remaining));

    Ok(HallazgosResponse { visible, all })
}
