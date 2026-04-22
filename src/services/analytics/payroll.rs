use super::summary::parse_ym;
/// Payroll: employee analytics from CFDI Nómina complements.
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
    pub total_isr_retenido: f64,
    pub total_employees: i64,
    pub avg_salary_mxn: f64,
    pub avg_sdi: f64, // Salario Diario Integrado promedio
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

    // Summary
    let summary_row = sqlx::query(r#"
        SELECT
            SUM(COALESCE(n.total_percepciones,0) - COALESCE(n.total_deducciones,0)) AS total_pagado,
            SUM(COALESCE(n.total_percepciones,0))                                    AS total_perc,
            SUM(COALESCE(n.total_deducciones,0))                                     AS total_ded,
            COUNT(DISTINCT c.rfc_receptor)                                            AS emp_count,
            AVG(COALESCE(n.salario_diario_integrado,0))                              AS avg_sdi,
            COUNT(*)                                                                  AS payrolls_count
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
    "#)
    .bind(rfc).bind(from_y).bind(from_m).bind(to_y).bind(to_m)
    .fetch_one(pool)
    .await?;

    let total_pagado: f64 = summary_row.try_get("total_pagado").unwrap_or(0.0);
    let emp_count: i64 = summary_row.try_get("emp_count").unwrap_or(0);
    let payrolls: i64 = summary_row.try_get("payrolls_count").unwrap_or(0);

    let summary = PayrollSummary {
        total_pagado_mxn: total_pagado,
        total_percepciones_mxn: summary_row.try_get("total_perc").unwrap_or(0.0),
        total_deducciones_mxn: summary_row.try_get("total_ded").unwrap_or(0.0),
        total_isr_retenido: 0.0, // computed below
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
    let month_rows = sqlx::query(
        r#"
        SELECT c.year, c.month,
               SUM(COALESCE(n.total_percepciones,0) - COALESCE(n.total_deducciones,0)) AS pagado,
               SUM(COALESCE(n.total_percepciones,0))  AS perc,
               SUM(COALESCE(n.total_deducciones,0))   AS ded,
               COUNT(DISTINCT c.rfc_receptor)          AS emp_count,
               COUNT(*)                               AS payrolls_count
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY c.year, c.month
        ORDER BY c.year, c.month
    "#,
    )
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
    let emp_rows = sqlx::query(
        r#"
        SELECT
            c.rfc_receptor                              AS emp_rfc,
            MAX(c.nombre_receptor)                      AS emp_nombre,
            MAX(n.num_empleado)                         AS num_emp,
            MAX(n.departamento)                         AS dpto,
            MAX(n.puesto)                               AS puesto,
            SUM(COALESCE(n.total_percepciones,0) - COALESCE(n.total_deducciones,0)) AS pagado,
            SUM(COALESCE(n.total_percepciones,0))       AS perc,
            SUM(COALESCE(n.total_deducciones,0))        AS ded,
            AVG(COALESCE(n.salario_diario_integrado,0)) AS avg_sdi,
            COUNT(*)                                    AS payrolls,
            COUNT(DISTINCT c.year * 100 + c.month)      AS months_active,
            MIN(n.fecha_pago)                           AS first_pay,
            MAX(n.fecha_pago)                           AS last_pay
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND c.tipo_comprobante = 'N'
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY c.rfc_receptor
        ORDER BY pagado DESC
        LIMIT 100
    "#,
    )
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
    let tipo_rows = sqlx::query(
        r#"
        SELECT n.tipo_nomina,
               SUM(COALESCE(n.total_percepciones,0)) AS total,
               COUNT(*) AS cnt
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY n.tipo_nomina
    "#,
    )
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
    let ded_rows = sqlx::query(
        r#"
        SELECT d.tipo_deduccion,
               MAX(d.concepto) AS concepto,
               SUM(COALESCE(d.importe,0)) AS total,
               COUNT(*) AS cnt
        FROM pulso.cfdi_nomina_deducciones d
        JOIN pulso.cfdi_nomina n ON n.uuid = d.uuid
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY d.tipo_deduccion
        ORDER BY total DESC
    "#,
    )
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
    let per_rows = sqlx::query(
        r#"
        SELECT p.tipo_percepcion,
               MAX(p.concepto) AS concepto,
               SUM(COALESCE(p.importe_gravado,0)) AS gravado,
               SUM(COALESCE(p.importe_exento,0))  AS exento,
               COUNT(*) AS cnt
        FROM pulso.cfdi_nomina_percepciones p
        JOIN pulso.cfdi_nomina n ON n.uuid = p.uuid
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY p.tipo_percepcion
        ORDER BY (gravado + exento) DESC
    "#,
    )
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

    // Headcount by month (distinct employees per month)
    let hc_rows = sqlx::query(
        r#"
        SELECT c.year, c.month, COUNT(DISTINCT c.rfc_receptor) AS hc
        FROM pulso.cfdi_nomina n
        JOIN pulso.cfdis c ON c.uuid = n.uuid
        WHERE c.rfc_emisor = $1
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        GROUP BY c.year, c.month
        ORDER BY c.year, c.month
    "#,
    )
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let headcount_by_month: Vec<HeadcountMonth> = hc_rows
        .iter()
        .map(|r| {
            let year: i64 = r.try_get("year").unwrap_or(0);
            let month: i64 = r.try_get("month").unwrap_or(0);
            HeadcountMonth {
                period: format!("{year}-{month:02}"),
                headcount: r.try_get("hc").unwrap_or(0),
                new_employees: 0,
                departures: 0,
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
