use super::summary::parse_ym;
/// Payroll: employee analytics from CFDI Nómina complements.

// Reusable exclusion filters for payroll queries (alias = table alias for pulso.cfdis, usually "c").
// Checks both employee-level payroll rules and individual CFDI UUID rules.
const NOMINA_EXCL_C: &str = "\
          AND NOT EXISTS (\
\n              SELECT 1 FROM pulso.payroll_normalization_rules pnr\
\n              WHERE pnr.owner_rfc = $1 AND pnr.action = 'exclude'\
\n                AND pnr.employee_rfc = c.rfc_receptor\
\n                AND (pnr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text,2,'0')) >= pnr.period_start)\
\n                AND (pnr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text,2,'0')) <= pnr.period_end)\
\n          )\
\n          AND NOT EXISTS (\
\n              SELECT 1 FROM pulso.normalization_rules nr\
\n              WHERE nr.owner_rfc = $1 AND nr.action = 'exclude'\
\n                AND nr.cfdi_uuid IS NOT NULL AND UPPER(nr.cfdi_uuid) = UPPER(c.uuid)\
\n          )";
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Serialize)]
pub struct PayrollResponse {
    pub summary: PayrollSummary,
    pub by_month: Vec<PayrollMonth>,
    pub by_employee: Vec<EmployeeRow>,
    pub by_tipo_nomina: Vec<TipoNominaRow>,
    pub by_deduccion: Vec<DeduccionRow>,
    pub by_percepcion: Vec<PercepcionRow>,
    pub headcount_by_month: Vec<HeadcountMonth>,
}

#[derive(Debug, Serialize)]
pub struct PayrollSummary {
    pub total_pagado_mxn: f64,
    pub total_percepciones_mxn: f64,
    pub total_deducciones_mxn: f64,
    pub total_otros_pagos_mxn: f64,
    pub total_isr_retenido: f64,
    pub total_employees: i64,
    pub avg_salary_mxn: f64,
    pub avg_sdi: f64,
    pub payrolls_count: i64,
}

#[derive(Debug, Serialize)]
pub struct PayrollMonth {
    pub period: String,
    pub year: i64,
    pub month: i64,
    pub total_pagado: f64,
    pub total_percepciones: f64,
    pub total_deducciones: f64,
    pub employee_count: i64,
    pub payrolls_count: i64,
}

#[derive(Debug, Serialize)]
pub struct EmployeeRow {
    pub rfc: String,
    pub nombre: String,
    pub num_empleado: String,
    pub departamento: String,
    pub puesto: String,
    pub total_pagado_mxn: f64,
    pub total_percepciones: f64,
    pub total_deducciones: f64,
    pub avg_sdi: f64,
    pub payrolls_count: i64,
    pub months_active: i64,
    pub first_payroll: String,
    pub last_payroll: String,
}

#[derive(Debug, Serialize)]
pub struct TipoNominaRow {
    pub tipo_nomina: String,
    pub label: String,
    pub total_mxn: f64,
    pub count: i64,
    pub pct_of_total: f64,
}

#[derive(Debug, Serialize)]
pub struct DeduccionRow {
    pub tipo_deduccion: String,
    pub concepto: String,
    pub total_importe: f64,
    pub count: i64,
}

#[derive(Debug, Serialize)]
pub struct PercepcionRow {
    pub tipo_percepcion: String,
    pub concepto: String,
    pub total_gravado: f64,
    pub total_exento: f64,
    pub total_importe: f64,
    pub count: i64,
}

#[derive(Debug, Serialize)]
pub struct HeadcountMonth {
    pub period: String,
    pub headcount: i64,
    pub new_employees: i64,
    pub departures: i64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str, // employer RFC (rfc_emisor for nomina)
    from: &str,
    to: &str,
) -> anyhow::Result<PayrollResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);

    // Summary (all tipos, to match full payroll spend)
    let summary_row = sqlx::query(&format!(r#"
        SELECT
            SUM(COALESCE(n.total_percepciones,0)::float8 - COALESCE(n.total_deducciones,0)) AS total_pagado,
            SUM(COALESCE(n.total_percepciones,0)::float8)                                    AS total_perc,
            SUM(COALESCE(n.total_deducciones,0)::float8)                                     AS total_ded,
            SUM(COALESCE(n.total_otros_pagos,0)::float8)                                     AS total_otros,
            COUNT(DISTINCT c.rfc_receptor)                                                   AS emp_count,
            AVG(COALESCE(n.salario_diario_integrado,0)::float8)                              AS avg_sdi,
            COUNT(*)                                                                          AS payrolls_count
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
    "#))
    .bind(rfc).bind(from_y).bind(from_m).bind(to_y).bind(to_m)
    .fetch_one(pool)
    .await?;

    let total_pagado: f64 = summary_row.try_get("total_pagado").unwrap_or(0.0);
    let emp_count: i64 = summary_row.try_get("emp_count").unwrap_or(0);
    let payrolls: i64 = summary_row.try_get("payrolls_count").unwrap_or(0);

    // ISR retenido = deducciones tipo '002' (SAT clave ISR)
    let isr_row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(SUM(COALESCE(d.importe, 0)::float8), 0) AS total_isr
        FROM pulso.cfdi_nomina_deducciones d
        JOIN pulso.cfdi_nomina n ON n.uuid = d.uuid
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND d.tipo_deduccion = '002'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        "#,
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_one(pool)
    .await?;
    let total_isr_retenido: f64 = isr_row.try_get("total_isr").unwrap_or(0.0);

    let summary = PayrollSummary {
        total_pagado_mxn: total_pagado,
        total_percepciones_mxn: summary_row.try_get("total_perc").unwrap_or(0.0),
        total_deducciones_mxn: summary_row.try_get("total_ded").unwrap_or(0.0),
        total_otros_pagos_mxn: summary_row.try_get("total_otros").unwrap_or(0.0),
        total_isr_retenido,
        total_employees: emp_count,
        avg_salary_mxn: if emp_count > 0 {
            total_pagado / emp_count as f64
        } else {
            0.0
        },
        avg_sdi: summary_row.try_get("avg_sdi").unwrap_or(0.0),
        payrolls_count: payrolls,
    };

    // By month
    let month_rows = sqlx::query(&format!(
        r#"
        SELECT c.year, c.month,
               SUM(COALESCE(n.total_percepciones,0)::float8 - COALESCE(n.total_deducciones,0)) AS pagado,
               SUM(COALESCE(n.total_percepciones,0)::float8)  AS perc,
               SUM(COALESCE(n.total_deducciones,0)::float8)   AS ded,
               COUNT(DISTINCT c.rfc_receptor)          AS emp_count,
               COUNT(*)                               AS payrolls_count
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        GROUP BY c.year, c.month
        ORDER BY c.year, c.month
    "#,
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let by_month: Vec<PayrollMonth> = month_rows
        .iter()
        .map(|r| {
            let year: i64 = r.try_get("year").unwrap_or(0);
            let month: i64 = r.try_get("month").unwrap_or(0);
            PayrollMonth {
                period: format!("{year}-{month:02}"),
                year,
                month,
                total_pagado: r.try_get("pagado").unwrap_or(0.0),
                total_percepciones: r.try_get("perc").unwrap_or(0.0),
                total_deducciones: r.try_get("ded").unwrap_or(0.0),
                employee_count: r.try_get("emp_count").unwrap_or(0),
                payrolls_count: r.try_get("payrolls_count").unwrap_or(0),
            }
        })
        .collect();

    // By employee (top 100 by total paid)
    let emp_rows = sqlx::query(&format!(
        r#"
        SELECT
            c.rfc_receptor                              AS emp_rfc,
            MAX(c.nombre_receptor)                      AS emp_nombre,
            MAX(n.num_empleado)                         AS num_emp,
            MAX(n.departamento)                         AS dpto,
            MAX(n.puesto)                               AS puesto,
            SUM(COALESCE(n.total_percepciones,0)::float8 - COALESCE(n.total_deducciones,0)) AS pagado,
            SUM(COALESCE(n.total_percepciones,0)::float8)       AS perc,
            SUM(COALESCE(n.total_deducciones,0)::float8)        AS ded,
            AVG(COALESCE(n.salario_diario_integrado,0)::float8) AS avg_sdi,
            COUNT(*)                                    AS payrolls,
            COUNT(DISTINCT c.year * 100 + c.month)      AS months_active,
            MIN(n.fecha_pago)                           AS first_pay,
            MAX(n.fecha_pago)                           AS last_pay
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        GROUP BY c.rfc_receptor
        ORDER BY pagado DESC
        LIMIT 100
    "#,
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let by_employee: Vec<EmployeeRow> = emp_rows
        .iter()
        .map(|r| EmployeeRow {
            rfc: r.try_get("emp_rfc").unwrap_or_default(),
            nombre: r.try_get("emp_nombre").unwrap_or_default(),
            num_empleado: r.try_get("num_emp").unwrap_or_default(),
            departamento: r.try_get("dpto").unwrap_or_default(),
            puesto: r.try_get("puesto").unwrap_or_default(),
            total_pagado_mxn: r.try_get("pagado").unwrap_or(0.0),
            total_percepciones: r.try_get("perc").unwrap_or(0.0),
            total_deducciones: r.try_get("ded").unwrap_or(0.0),
            avg_sdi: r.try_get("avg_sdi").unwrap_or(0.0),
            payrolls_count: r.try_get("payrolls").unwrap_or(0),
            months_active: r.try_get("months_active").unwrap_or(0),
            first_payroll: r.try_get("first_pay").unwrap_or_default(),
            last_payroll: r.try_get("last_pay").unwrap_or_default(),
        })
        .collect();

    // By tipo nomina
    let tipo_rows = sqlx::query(&format!(
        r#"
        SELECT n.tipo_nomina,
               SUM(COALESCE(n.total_percepciones,0)::float8) AS total,
               COUNT(*) AS cnt
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        GROUP BY n.tipo_nomina
    "#,
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let grand_tipo_total: f64 = tipo_rows
        .iter()
        .map(|r| r.try_get::<f64, _>("total").unwrap_or(0.0))
        .sum();
    let by_tipo_nomina: Vec<TipoNominaRow> = tipo_rows
        .iter()
        .map(|r| {
            let tipo: String = r.try_get("tipo_nomina").unwrap_or_default();
            let total: f64 = r.try_get("total").unwrap_or(0.0);
            TipoNominaRow {
                label: tipo_nomina_label(&tipo).to_string(),
                tipo_nomina: tipo,
                total_mxn: total,
                count: r.try_get("cnt").unwrap_or(0),
                pct_of_total: if grand_tipo_total > 0.0 {
                    total / grand_tipo_total * 100.0
                } else {
                    0.0
                },
            }
        })
        .collect();

    // By deduccion type
    let ded_rows = sqlx::query(&format!(
        r#"
        SELECT d.tipo_deduccion,
               MAX(d.concepto) AS concepto,
               SUM(COALESCE(d.importe,0)::float8) AS total,
               COUNT(*) AS cnt
        FROM pulso.cfdi_nomina_deducciones d
        JOIN pulso.cfdi_nomina n ON n.uuid = d.uuid
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        GROUP BY d.tipo_deduccion
        ORDER BY total DESC
    "#,
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let by_deduccion: Vec<DeduccionRow> = ded_rows
        .iter()
        .map(|r| DeduccionRow {
            tipo_deduccion: r.try_get("tipo_deduccion").unwrap_or_default(),
            concepto: r.try_get("concepto").unwrap_or_default(),
            total_importe: r.try_get("total").unwrap_or(0.0),
            count: r.try_get("cnt").unwrap_or(0),
        })
        .collect();

    // By percepcion type
    let per_rows = sqlx::query(&format!(
        r#"
        SELECT p.tipo_percepcion,
               MAX(p.concepto) AS concepto,
               SUM(COALESCE(p.importe_gravado,0)::float8) AS gravado,
               SUM(COALESCE(p.importe_exento,0)::float8)  AS exento,
               COUNT(*) AS cnt
        FROM pulso.cfdi_nomina_percepciones p
        JOIN pulso.cfdi_nomina n ON n.uuid = p.uuid
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        GROUP BY p.tipo_percepcion
        ORDER BY SUM(COALESCE(p.importe_gravado,0)::float8) + SUM(COALESCE(p.importe_exento,0)::float8) DESC
    "#,
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let by_percepcion: Vec<PercepcionRow> = per_rows
        .iter()
        .map(|r| {
            let grav: f64 = r.try_get("gravado").unwrap_or(0.0);
            let exen: f64 = r.try_get("exento").unwrap_or(0.0);
            PercepcionRow {
                tipo_percepcion: r.try_get("tipo_percepcion").unwrap_or_default(),
                concepto: r.try_get("concepto").unwrap_or_default(),
                total_gravado: grav,
                total_exento: exen,
                total_importe: grav + exen,
                count: r.try_get("cnt").unwrap_or(0),
            }
        })
        .collect();

    // New employees per month: first-ever payslip from this employer is within range
    let new_emp_rows = sqlx::query(&format!(
        r#"
        SELECT yr AS year, mo AS month, COUNT(*) AS new_emp
        FROM (
            SELECT rfc_receptor,
                   (MIN(year * 100 + month) / 100)::bigint AS yr,
                   (MIN(year * 100 + month) % 100)::bigint AS mo
            FROM pulso.cfdi_nomina n2
            JOIN pulso.cfdis c2 ON c2.uuid = n2.uuid
            WHERE c2.rfc_emisor = $1
              AND COALESCE(c2.estado_sat,'') != 'cancelado'
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.payroll_normalization_rules pnr
                  WHERE pnr.owner_rfc = $1 AND pnr.action = 'exclude'
                    AND pnr.employee_rfc = c2.rfc_receptor
                    AND (pnr.period_start IS NULL OR (c2.year::text || '-' || LPAD(c2.month::text,2,'0')) >= pnr.period_start)
                    AND (pnr.period_end IS NULL OR (c2.year::text || '-' || LPAD(c2.month::text,2,'0')) <= pnr.period_end)
              )
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.normalization_rules nr
                  WHERE nr.owner_rfc = $1 AND nr.action = 'exclude'
                    AND nr.cfdi_uuid IS NOT NULL AND UPPER(nr.cfdi_uuid) = UPPER(c2.uuid)
              )
            GROUP BY rfc_receptor
        ) sub
        WHERE (yr > $2 OR (yr = $2 AND mo >= $3))
          AND (yr < $4 OR (yr = $4 AND mo <= $5))
        GROUP BY yr, mo
        "#,
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let new_emp_map: std::collections::HashMap<(i64, i64), i64> = new_emp_rows
        .iter()
        .map(|r| {
            let yr: i64 = r.try_get("year").unwrap_or(0);
            let mo: i64 = r.try_get("month").unwrap_or(0);
            let n: i64 = r.try_get("new_emp").unwrap_or(0);
            ((yr, mo), n)
        })
        .collect();

    // All (year, month, rfc_receptor) in range — used to compute departures
    let emp_month_rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT c.year, c.month, c.rfc_receptor
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        ORDER BY c.year, c.month
        "#,
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Build month → set<rfc> and sorted period list
    let mut emp_by_period: std::collections::BTreeMap<(i64, i64), std::collections::HashSet<String>> =
        std::collections::BTreeMap::new();
    for r in &emp_month_rows {
        let yr: i64 = r.try_get("year").unwrap_or(0);
        let mo: i64 = r.try_get("month").unwrap_or(0);
        let emp: String = r.try_get("rfc_receptor").unwrap_or_default();
        emp_by_period.entry((yr, mo)).or_default().insert(emp);
    }

    // Headcount by month (distinct employees per month)
    let hc_rows = sqlx::query(&format!(
        r#"
        SELECT c.year, c.month, COUNT(DISTINCT c.rfc_receptor) AS hc
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        GROUP BY c.year, c.month
        ORDER BY c.year, c.month
    "#,
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let periods: Vec<(i64, i64)> = emp_by_period.keys().cloned().collect();

    let headcount_by_month: Vec<HeadcountMonth> = hc_rows
        .iter()
        .map(|r| {
            let year: i64 = r.try_get("year").unwrap_or(0);
            let month: i64 = r.try_get("month").unwrap_or(0);
            let key = (year, month);

            let prev_key = periods
                .iter()
                .rev()
                .find(|&&k| k < key)
                .cloned();

            let departures = match prev_key {
                Some(pk) => {
                    let prev_set = emp_by_period.get(&pk);
                    let curr_set = emp_by_period.get(&key);
                    match (prev_set, curr_set) {
                        (Some(prev), Some(curr)) => {
                            prev.iter().filter(|e| !curr.contains(*e)).count() as i64
                        }
                        _ => 0,
                    }
                }
                None => 0,
            };

            HeadcountMonth {
                period: format!("{year}-{month:02}"),
                headcount: r.try_get("hc").unwrap_or(0),
                new_employees: *new_emp_map.get(&key).unwrap_or(&0),
                departures,
            }
        })
        .collect();

    Ok(PayrollResponse {
        summary,
        by_month,
        by_employee,
        by_tipo_nomina,
        by_deduccion,
        by_percepcion,
        headcount_by_month,
    })
}

fn tipo_nomina_label(t: &str) -> &str {
    match t {
        "O" => "Ordinaria",
        "E" => "Extraordinaria",
        _ => t,
    }
}

// ---------------------------------------------------------------------------
// Payroll snapshot (C3 dashboard KPIs)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct PayrollSnapshotResponse {
    pub has_data: bool,
    pub headcount_actual: i64,
    pub run_rate_mensual_ltm_mxn: f64,
    pub yoy_masa_salarial_pct: Option<f64>,
    pub pasivo_laboral_estimado_mxn: f64,
    pub months_of_data: i64,
}

pub async fn get_snapshot(pool: &DbPool, rfc: &str) -> anyhow::Result<PayrollSnapshotResponse> {
    let empty = || PayrollSnapshotResponse {
        has_data: false,
        headcount_actual: 0,
        run_rate_mensual_ltm_mxn: 0.0,
        yoy_masa_salarial_pct: None,
        pasivo_laboral_estimado_mxn: 0.0,
        months_of_data: 0,
    };

    // Most recent period with payroll data
    let period_row = sqlx::query(&format!(
        r#"
        SELECT MAX(c.year * 100 + c.month) AS last_period
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
{NOMINA_EXCL_C}
        "#,
    ))
    .bind(rfc)
    .fetch_one(pool)
    .await?;

    let last_period: Option<i64> = period_row.try_get("last_period").ok().flatten();
    let last_period = match last_period {
        None => return Ok(empty()),
        Some(p) => p,
    };

    let last_y = last_period / 100;
    let last_m = last_period % 100;

    // LTM window: 12 months ending at last_period
    let (ltm_from_y, ltm_from_m) = subtract_months(last_y, last_m, 11);
    // Prior 12 months window (for YoY)
    let (prior_to_y, prior_to_m) = subtract_months(last_y, last_m, 12);
    let (prior_from_y, prior_from_m) = subtract_months(prior_to_y, prior_to_m, 11);

    // Headcount in the most recent period
    let hc_row = sqlx::query(&format!(
        r#"
        SELECT COUNT(DISTINCT c.rfc_receptor) AS headcount
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND c.year = $2 AND c.month = $3
{NOMINA_EXCL_C}
        "#,
    ))
    .bind(rfc)
    .bind(last_y)
    .bind(last_m)
    .fetch_one(pool)
    .await?;
    let headcount_actual: i64 = hc_row.try_get("headcount").unwrap_or(0);

    if headcount_actual == 0 {
        return Ok(empty());
    }

    // Run-rate LTM: exclude one-off percepciones (002=aguinaldo, 003=PTU, 022=prima vacacional, 038=indemnización)
    let rr_row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(SUM(
            COALESCE(p.importe_gravado, 0)::float8 + COALESCE(p.importe_exento, 0)::float8
        ), 0) AS total_regular
        FROM pulso.cfdi_nomina_percepciones p
        JOIN pulso.cfdi_nomina n ON n.uuid = p.uuid
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND p.tipo_percepcion NOT IN ('002', '003', '022', '038')
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        "#,
    ))
    .bind(rfc)
    .bind(ltm_from_y)
    .bind(ltm_from_m)
    .bind(last_y)
    .bind(last_m)
    .fetch_one(pool)
    .await?;
    let total_regular: f64 = rr_row.try_get("total_regular").unwrap_or(0.0);
    let run_rate_mensual_ltm_mxn = total_regular / 12.0;

    // YoY masa salarial: total percepciones LTM vs prior 12 months
    let ltm_row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(SUM(COALESCE(n.total_percepciones, 0)::float8), 0) AS total
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        "#,
    ))
    .bind(rfc)
    .bind(ltm_from_y)
    .bind(ltm_from_m)
    .bind(last_y)
    .bind(last_m)
    .fetch_one(pool)
    .await?;
    let ltm_masa: f64 = ltm_row.try_get("total").unwrap_or(0.0);

    let prior_row = sqlx::query(&format!(
        r#"
        SELECT COALESCE(SUM(COALESCE(n.total_percepciones, 0)::float8), 0) AS total
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
{NOMINA_EXCL_C}
        "#,
    ))
    .bind(rfc)
    .bind(prior_from_y)
    .bind(prior_from_m)
    .bind(prior_to_y)
    .bind(prior_to_m)
    .fetch_one(pool)
    .await?;
    let prior_masa: f64 = prior_row.try_get("total").unwrap_or(0.0);
    let yoy_masa_salarial_pct = if prior_masa > 0.0 {
        Some((ltm_masa - prior_masa) / prior_masa * 100.0)
    } else {
        None
    };

    // Labor liability (pasivo laboral): per active employee
    // SDI = salario diario integrado (daily rate for benefit provisioning)
    // Tenure from first payroll ever recorded for this employer
    let emp_rows = sqlx::query(&format!(
        r#"
        WITH active_emps AS (
            SELECT DISTINCT c.rfc_receptor
            FROM pulso.cfdi_nomina n
            JOIN pulso.cfdis c ON c.uuid = n.uuid
            WHERE c.rfc_emisor = $1
              AND c.tipo_comprobante = 'N'
              AND COALESCE(c.estado_sat,'') != 'cancelado'
              AND c.year = $2 AND c.month = $3
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.payroll_normalization_rules pnr
                  WHERE pnr.owner_rfc = $1 AND pnr.action = 'exclude'
                    AND pnr.employee_rfc = c.rfc_receptor
                    AND (pnr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text,2,'0')) >= pnr.period_start)
                    AND (pnr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text,2,'0')) <= pnr.period_end)
              )
              AND NOT EXISTS (
                  SELECT 1 FROM pulso.normalization_rules nr
                  WHERE nr.owner_rfc = $1 AND nr.action = 'exclude'
                    AND nr.cfdi_uuid IS NOT NULL AND UPPER(nr.cfdi_uuid) = UPPER(c.uuid)
              )
        )
        SELECT
            c.rfc_receptor,
            AVG(COALESCE(n.salario_diario_integrado, 0)::float8) AS sdi,
            COALESCE((CURRENT_DATE - MIN(n.fecha_pago)::date)::integer, 0) AS tenure_days
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
          AND c.rfc_receptor IN (SELECT rfc_receptor FROM active_emps)
{NOMINA_EXCL_C}
        GROUP BY c.rfc_receptor
        "#,
    ))
    .bind(rfc)
    .bind(last_y)
    .bind(last_m)
    .fetch_all(pool)
    .await?;

    // last_m used as "current month of period" for aguinaldo proportion
    let period_month = last_m as f64;

    let pasivo_laboral_estimado_mxn: f64 = emp_rows
        .iter()
        .map(|r| {
            let sdi: f64 = r.try_get("sdi").unwrap_or(0.0);
            let tenure_days: i32 = r.try_get("tenure_days").unwrap_or(0);
            if sdi <= 0.0 {
                return 0.0;
            }
            let tenure_years = tenure_days as f64 / 365.25;
            let vac_days = lft_vacation_days(tenure_years);
            let vac_pend = vac_days * sdi;
            let prima_vac = vac_pend * 0.25;
            let aguinaldo = sdi * 15.0 * (period_month / 12.0);
            let ptu = (sdi * 30.0) * 0.1 / 12.0;
            vac_pend + prima_vac + aguinaldo + ptu
        })
        .sum();

    let months_row = sqlx::query(
        r#"
        SELECT COUNT(DISTINCT c.year * 100 + c.month) AS cnt
        FROM pulso.cfdis c
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND COALESCE(c.estado_sat,'') != 'cancelado'
        "#,
    )
    .bind(rfc)
    .fetch_one(pool)
    .await?;
    let months_of_data: i64 = months_row.try_get("cnt").unwrap_or(0);

    Ok(PayrollSnapshotResponse {
        has_data: true,
        headcount_actual,
        run_rate_mensual_ltm_mxn,
        yoy_masa_salarial_pct,
        pasivo_laboral_estimado_mxn,
        months_of_data,
    })
}

fn lft_vacation_days(tenure_years: f64) -> f64 {
    if tenure_years < 1.0 {
        return 12.0 * tenure_years;
    }
    match tenure_years.floor() as i64 {
        1 => 12.0,
        2 => 14.0,
        3 => 16.0,
        4 => 18.0,
        5..=9 => 20.0,
        10..=14 => 22.0,
        15..=19 => 24.0,
        20..=24 => 26.0,
        25..=29 => 28.0,
        _ => 30.0,
    }
}

fn subtract_months(y: i64, m: i64, n: i64) -> (i64, i64) {
    let total = y * 12 + m - n;
    let ry = total / 12;
    let rm = total % 12;
    if rm == 0 { (ry - 1, 12) } else { (ry, rm) }
}
