/// Normalization rules CRUD: counterparty grouping/exclusion and payroll adjustments.
use crate::db::DbPool;
use crate::services::analytics::summary::rfc_column;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Counterparty normalization
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct NormalizationRule {
    pub id: String,
    pub owner_rfc: String,
    pub dl_type: String,
    pub source_rfc: Option<String>,
    pub source_name: Option<String>,
    pub group_name: Option<String>,
    pub action: String,
    pub label: Option<String>,
    pub rule_name: Option<String>,
    pub cfdi_uuid: Option<String>,
    // V2 fields
    pub accounting_line: Option<String>,
    pub motivo: Option<String>,
    pub impacts_ebitda: Option<bool>,
    pub capex_estimate_dep: Option<bool>,
    pub capex_asset_type: Option<String>,
    pub capex_useful_life_years: Option<f64>,
    pub capex_annual_dep_mxn: Option<f64>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateRuleRequest {
    pub dl_type: String,
    pub source_rfc: Option<String>,
    pub source_name: Option<String>,
    pub group_name: Option<String>,
    pub action: String, // group|exclude
    pub label: Option<String>,
    pub rule_name: Option<String>,
    pub cfdi_uuid: Option<String>,
    // V2 fields
    pub accounting_line: Option<String>,
    pub motivo: Option<String>,
    pub impacts_ebitda: Option<bool>,
    pub capex_estimate_dep: Option<bool>,
    pub capex_asset_type: Option<String>,
    pub capex_useful_life_years: Option<f64>,
    pub capex_annual_dep_mxn: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct PayrollNormRule {
    pub id: String,
    pub owner_rfc: String,
    pub rule_family: String,
    pub employee_rfc: Option<String>,
    pub employee_name: Option<String>,
    pub action: String,
    pub value_pct: Option<f64>,
    pub value_mxn: Option<f64>,
    pub period_start: Option<String>,
    pub period_end: Option<String>,
    pub notes: Option<String>,
    pub label: Option<String>,
    pub rule_name: Option<String>,
    pub excluded_cfdi_uuids: Option<Vec<String>>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreatePayrollRuleRequest {
    pub rule_family: String,
    pub employee_rfc: Option<String>,
    pub employee_name: Option<String>,
    pub action: String,
    pub value_pct: Option<f64>,
    pub value_mxn: Option<f64>,
    pub period_start: Option<String>,
    pub period_end: Option<String>,
    pub notes: Option<String>,
    pub label: Option<String>,
    pub rule_name: Option<String>,
    pub excluded_cfdi_uuids: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct PayrollEmployeeRow {
    pub employee_rfc: String,
    pub employee_name: Option<String>,
    pub first_month: Option<String>,
    pub last_month: Option<String>,
    pub active_months: i64,
    pub historical_cost_mxn: f64,
    pub run_rate_mensual_mxn: f64,
    pub cfdi_count: i64,
}

#[derive(Debug, Serialize)]
pub struct EbitdaBridgeRow {
    pub concepto: String,
    pub seccion: String,
    pub rule_name: Option<String>,
    pub is_subtotal: bool,
    pub is_bold: bool,
    pub is_pct: bool,
    pub is_section_header: bool,
    pub amounts: std::collections::HashMap<String, f64>,
}

#[derive(Debug, Serialize)]
pub struct ExcludedCfdi {
    pub rule_id: String,
    pub rule_type: String,
    pub rule_name: Option<String>,
    pub label: Option<String>,
    pub cfdi_uuid: String,
    pub rfc_emisor: String,
    pub rfc_receptor: String,
    pub nombre_emisor: Option<String>,
    pub nombre_receptor: Option<String>,
    pub tipo_comprobante: String,
    pub fecha_emision: Option<String>,
    pub total_mxn: f64,
    pub period: String,
}

pub async fn list_rules(pool: &DbPool, owner_rfc: &str) -> anyhow::Result<Vec<NormalizationRule>> {
    let rows = sqlx::query(
        "SELECT id, owner_rfc, dl_type, source_rfc, source_name, group_name, action, label,
                rule_name, cfdi_uuid,
                accounting_line, motivo, impacts_ebitda, capex_estimate_dep,
                capex_asset_type, capex_useful_life_years, capex_annual_dep_mxn,
                created_at, updated_at
         FROM pulso.normalization_rules WHERE owner_rfc = $1 ORDER BY created_at DESC"
    )
    .bind(owner_rfc)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| NormalizationRule {
            id: r.try_get("id").unwrap_or_default(),
            owner_rfc: r.try_get("owner_rfc").unwrap_or_default(),
            dl_type: r.try_get("dl_type").unwrap_or_default(),
            source_rfc: r.try_get("source_rfc").ok(),
            source_name: r.try_get("source_name").ok(),
            group_name: r.try_get("group_name").ok(),
            action: r.try_get("action").unwrap_or_default(),
            label: r.try_get("label").ok(),
            rule_name: r.try_get("rule_name").ok(),
            cfdi_uuid: r.try_get("cfdi_uuid").ok(),
            accounting_line: r.try_get("accounting_line").ok(),
            motivo: r.try_get("motivo").ok(),
            impacts_ebitda: r.try_get("impacts_ebitda").ok(),
            capex_estimate_dep: r.try_get("capex_estimate_dep").ok(),
            capex_asset_type: r.try_get("capex_asset_type").ok(),
            capex_useful_life_years: r.try_get("capex_useful_life_years").ok(),
            capex_annual_dep_mxn: r.try_get("capex_annual_dep_mxn").ok(),
            created_at: r.try_get("created_at").unwrap_or_default(),
            updated_at: r.try_get("updated_at").unwrap_or_default(),
        })
        .collect())
}

pub async fn create_rule(
    pool: &DbPool,
    owner_rfc: &str,
    req: &CreateRuleRequest,
) -> anyhow::Result<NormalizationRule> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = utc_now();

    sqlx::query(
        r#"INSERT INTO pulso.normalization_rules
            (id, owner_rfc, dl_type, source_rfc, source_name, group_name, action, label,
             rule_name, cfdi_uuid, accounting_line, motivo, impacts_ebitda,
             capex_estimate_dep, capex_asset_type, capex_useful_life_years,
             capex_annual_dep_mxn, created_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)"#
    )
    .bind(&id)
    .bind(owner_rfc)
    .bind(&req.dl_type)
    .bind(&req.source_rfc)
    .bind(&req.source_name)
    .bind(&req.group_name)
    .bind(&req.action)
    .bind(&req.label)
    .bind(&req.rule_name)
    .bind(&req.cfdi_uuid)
    .bind(&req.accounting_line)
    .bind(&req.motivo)
    .bind(&req.impacts_ebitda)
    .bind(&req.capex_estimate_dep)
    .bind(&req.capex_asset_type)
    .bind(&req.capex_useful_life_years)
    .bind(&req.capex_annual_dep_mxn)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

    Ok(NormalizationRule {
        id,
        owner_rfc: owner_rfc.to_string(),
        dl_type: req.dl_type.clone(),
        source_rfc: req.source_rfc.clone(),
        source_name: req.source_name.clone(),
        group_name: req.group_name.clone(),
        action: req.action.clone(),
        label: req.label.clone(),
        rule_name: req.rule_name.clone(),
        cfdi_uuid: req.cfdi_uuid.clone(),
        accounting_line: req.accounting_line.clone(),
        motivo: req.motivo.clone(),
        impacts_ebitda: req.impacts_ebitda,
        capex_estimate_dep: req.capex_estimate_dep,
        capex_asset_type: req.capex_asset_type.clone(),
        capex_useful_life_years: req.capex_useful_life_years,
        capex_annual_dep_mxn: req.capex_annual_dep_mxn,
        created_at: now.clone(),
        updated_at: now,
    })
}

pub async fn delete_rule(pool: &DbPool, id: &str, owner_rfc: &str) -> anyhow::Result<bool> {
    let result =
        sqlx::query("DELETE FROM pulso.normalization_rules WHERE id = $1 AND owner_rfc = $2")
            .bind(id)
            .bind(owner_rfc)
            .execute(pool)
            .await?;

    Ok(result.rows_affected() > 0)
}

// ---------------------------------------------------------------------------
// Payroll normalization
// ---------------------------------------------------------------------------

pub async fn list_payroll_rules(
    pool: &DbPool,
    owner_rfc: &str,
) -> anyhow::Result<Vec<PayrollNormRule>> {
    let rows = sqlx::query(
        "SELECT id, owner_rfc, rule_family, employee_rfc, employee_name, action,
                value_pct, value_mxn, period_start, period_end, notes, label, rule_name,
                excluded_cfdi_uuids, created_at, updated_at
         FROM pulso.payroll_normalization_rules WHERE owner_rfc = $1 ORDER BY created_at DESC",
    )
    .bind(owner_rfc)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| PayrollNormRule {
            id: r.try_get("id").unwrap_or_default(),
            owner_rfc: r.try_get("owner_rfc").unwrap_or_default(),
            rule_family: r.try_get("rule_family").unwrap_or_default(),
            employee_rfc: r.try_get("employee_rfc").ok(),
            employee_name: r.try_get("employee_name").ok(),
            action: r.try_get("action").unwrap_or_default(),
            value_pct: r.try_get("value_pct").ok(),
            value_mxn: r.try_get("value_mxn").ok(),
            period_start: r.try_get("period_start").ok(),
            period_end: r.try_get("period_end").ok(),
            notes: r.try_get("notes").ok(),
            label: r.try_get("label").ok(),
            rule_name: r.try_get("rule_name").ok(),
            excluded_cfdi_uuids: r.try_get::<Option<Vec<String>>, _>("excluded_cfdi_uuids").ok().flatten(),
            created_at: r.try_get("created_at").unwrap_or_default(),
            updated_at: r.try_get("updated_at").unwrap_or_default(),
        })
        .collect())
}

pub async fn create_payroll_rule(
    pool: &DbPool,
    owner_rfc: &str,
    req: &CreatePayrollRuleRequest,
) -> anyhow::Result<PayrollNormRule> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = utc_now();

    sqlx::query(
        r#"INSERT INTO pulso.payroll_normalization_rules
            (id, owner_rfc, rule_family, employee_rfc, employee_name, action,
             value_pct, value_mxn, period_start, period_end, notes, label, rule_name,
             excluded_cfdi_uuids, created_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)"#,
    )
    .bind(&id)
    .bind(owner_rfc)
    .bind(&req.rule_family)
    .bind(&req.employee_rfc)
    .bind(&req.employee_name)
    .bind(&req.action)
    .bind(&req.value_pct)
    .bind(&req.value_mxn)
    .bind(&req.period_start)
    .bind(&req.period_end)
    .bind(&req.notes)
    .bind(&req.label)
    .bind(&req.rule_name)
    .bind(&req.excluded_cfdi_uuids)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

    Ok(PayrollNormRule {
        id,
        owner_rfc: owner_rfc.to_string(),
        rule_family: req.rule_family.clone(),
        employee_rfc: req.employee_rfc.clone(),
        employee_name: req.employee_name.clone(),
        action: req.action.clone(),
        value_pct: req.value_pct,
        value_mxn: req.value_mxn,
        period_start: req.period_start.clone(),
        period_end: req.period_end.clone(),
        notes: req.notes.clone(),
        label: req.label.clone(),
        rule_name: req.rule_name.clone(),
        excluded_cfdi_uuids: req.excluded_cfdi_uuids.clone(),
        created_at: now.clone(),
        updated_at: now,
    })
}

pub async fn delete_payroll_rule(pool: &DbPool, id: &str, owner_rfc: &str) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "DELETE FROM pulso.payroll_normalization_rules WHERE id = $1 AND owner_rfc = $2",
    )
    .bind(id)
    .bind(owner_rfc)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn list_excluded_cfdis(
    pool: &DbPool,
    owner_rfc: &str,
) -> anyhow::Result<Vec<ExcludedCfdi>> {
    let counterparty_rows = sqlx::query(
        r#"SELECT nr.id AS rule_id, 'counterparty' AS rule_type, nr.rule_name, nr.label,
                  c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
                  c.tipo_comprobante, c.fecha_emision, COALESCE(c.total_mxn, 0)::float8 AS total_mxn,
                  c.year::text || '-' || LPAD(c.month::text, 2, '0') AS period
           FROM pulso.normalization_rules nr
           JOIN pulso.cfdis c ON (
               (nr.dl_type IN ('emitidos','ambos') AND nr.source_rfc = c.rfc_receptor AND c.rfc_emisor = nr.owner_rfc)
               OR (nr.dl_type IN ('recibidos','ambos') AND nr.source_rfc = c.rfc_emisor AND c.rfc_receptor = nr.owner_rfc)
           )
           WHERE nr.owner_rfc = $1 AND nr.action = 'exclude'"#,
    )
    .bind(owner_rfc)
    .fetch_all(pool)
    .await?;

    let payroll_rows = sqlx::query(
        r#"SELECT pnr.id AS rule_id, 'payroll' AS rule_type, pnr.rule_name, pnr.label,
                  c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
                  c.tipo_comprobante, c.fecha_emision, COALESCE(c.total_mxn, 0)::float8 AS total_mxn,
                  c.year::text || '-' || LPAD(c.month::text, 2, '0') AS period
           FROM pulso.payroll_normalization_rules pnr
           JOIN pulso.cfdis c ON c.rfc_emisor = pnr.owner_rfc AND c.rfc_receptor = pnr.employee_rfc
               AND c.tipo_comprobante = 'N'
               AND (pnr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text,2,'0')) >= pnr.period_start)
               AND (pnr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text,2,'0')) <= pnr.period_end)
           WHERE pnr.owner_rfc = $1 AND pnr.action = 'exclude'"#,
    )
    .bind(owner_rfc)
    .fetch_all(pool)
    .await?;

    let cfdi_uuid_rows = sqlx::query(
        r#"SELECT nr.id AS rule_id, 'cfdi' AS rule_type, nr.rule_name, nr.label,
                  c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
                  c.tipo_comprobante, c.fecha_emision, COALESCE(c.total_mxn, 0)::float8 AS total_mxn,
                  c.year::text || '-' || LPAD(c.month::text, 2, '0') AS period
           FROM pulso.normalization_rules nr
           JOIN pulso.cfdis c ON UPPER(c.uuid) = UPPER(nr.cfdi_uuid)
           WHERE nr.owner_rfc = $1 AND nr.action = 'exclude' AND nr.cfdi_uuid IS NOT NULL"#,
    )
    .bind(owner_rfc)
    .fetch_all(pool)
    .await?;

    let mut results: Vec<ExcludedCfdi> = counterparty_rows
        .iter()
        .map(map_excluded_cfdi_row)
        .collect();
    results.extend(payroll_rows.iter().map(map_excluded_cfdi_row));
    results.extend(cfdi_uuid_rows.iter().map(map_excluded_cfdi_row));

    // A single CFDI can match multiple query paths (e.g. both a counterparty rule
    // and a cfdi_uuid rule). Deduplicate by (uuid, rule_id) to avoid phantom rows.
    let mut seen = std::collections::HashSet::new();
    results.retain(|r| seen.insert((r.cfdi_uuid.to_uppercase(), r.rule_id.clone())));

    Ok(results)
}

// ---------------------------------------------------------------------------
// Individual CFDI listing for normalization UI
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct NormCfdiRow {
    pub uuid: String,
    pub rfc_contraparte: String,
    pub nombre_contraparte: String,
    pub tipo_comprobante: String,
    pub fecha_emision: String,
    pub total_mxn: f64,
    pub period: String,
    pub concepto: String,
    pub is_excluded: bool,
    pub rule_id: Option<String>,
    pub label: Option<String>,
}

pub async fn list_cfdis_for_normalization(
    pool: &DbPool,
    owner_rfc: &str,
    dl_type: &str,
    from_y: i64,
    from_m: i64,
    to_y: i64,
    to_m: i64,
    limit: i64,
) -> anyhow::Result<Vec<NormCfdiRow>> {
    let is_nomina = dl_type == "nomina";
    let rfc_col = if is_nomina { "rfc_emisor" } else { rfc_column(dl_type) };
    let dl_filter = match dl_type {
        "recibidos" => "c.dl_type IN ('recibidos', 'ambos')",
        "ambos" | "nomina" => "1=1",
        _ => "c.dl_type IN ('emitidos', 'ambos')",
    };
    let tipo_filter = if is_nomina {
        "c.tipo_comprobante = 'N'"
    } else {
        "c.tipo_comprobante NOT IN ('P','N')"
    };

    let sql = format!(
        r#"SELECT c.uuid,
               CASE WHEN c.rfc_emisor = $1 THEN c.rfc_receptor ELSE c.rfc_emisor END AS rfc_contraparte,
               CASE WHEN c.rfc_emisor = $1 THEN COALESCE(c.nombre_receptor,'') ELSE COALESCE(c.nombre_emisor,'') END AS nombre_contraparte,
               c.tipo_comprobante,
               COALESCE(c.fecha_emision::text, '') AS fecha_emision,
               COALESCE(c.total_mxn, 0)::float8 AS total_mxn,
               c.year::text || '-' || LPAD(c.month::text, 2, '0') AS period,
               COALESCE((SELECT cc.descripcion FROM pulso.cfdi_concepts cc WHERE cc.uuid = c.uuid LIMIT 1), '') AS concepto,
               CASE WHEN nr.id IS NOT NULL THEN true ELSE false END AS is_excluded,
               nr.id AS rule_id,
               nr.label
        FROM pulso.cfdis c
        LEFT JOIN pulso.normalization_rules nr ON UPPER(nr.cfdi_uuid) = UPPER(c.uuid)
            AND nr.owner_rfc = $1 AND nr.action = 'exclude'
        WHERE c.{rfc_col} = $1
          AND {dl_filter}
          AND {tipo_filter}
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
        ORDER BY c.fecha_emision DESC
        LIMIT $6"#
    );

    let rows = sqlx::query(&sql)
        .bind(owner_rfc)
        .bind(from_y)
        .bind(from_m)
        .bind(to_y)
        .bind(to_m)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    Ok(rows
        .iter()
        .map(|r| NormCfdiRow {
            uuid: r.try_get("uuid").unwrap_or_default(),
            rfc_contraparte: r.try_get("rfc_contraparte").unwrap_or_default(),
            nombre_contraparte: r.try_get("nombre_contraparte").unwrap_or_default(),
            tipo_comprobante: r.try_get("tipo_comprobante").unwrap_or_default(),
            fecha_emision: r.try_get("fecha_emision").unwrap_or_default(),
            total_mxn: r.try_get("total_mxn").unwrap_or(0.0),
            period: r.try_get("period").unwrap_or_default(),
            concepto: r.try_get("concepto").unwrap_or_default(),
            is_excluded: r.try_get::<bool, _>("is_excluded").unwrap_or(false),
            rule_id: r.try_get("rule_id").ok(),
            label: r.try_get("label").ok(),
        })
        .collect())
}

fn map_excluded_cfdi_row(r: &sqlx::postgres::PgRow) -> ExcludedCfdi {
    ExcludedCfdi {
        rule_id: r.try_get("rule_id").unwrap_or_default(),
        rule_type: r.try_get("rule_type").unwrap_or_default(),
        rule_name: r.try_get("rule_name").ok(),
        label: r.try_get("label").ok(),
        cfdi_uuid: r.try_get("uuid").unwrap_or_default(),
        rfc_emisor: r.try_get("rfc_emisor").unwrap_or_default(),
        rfc_receptor: r.try_get("rfc_receptor").unwrap_or_default(),
        nombre_emisor: r.try_get("nombre_emisor").ok(),
        nombre_receptor: r.try_get("nombre_receptor").ok(),
        tipo_comprobante: r.try_get("tipo_comprobante").unwrap_or_default(),
        fecha_emision: r.try_get("fecha_emision").ok(),
        total_mxn: r.try_get("total_mxn").unwrap_or(0.0),
        period: r.try_get("period").unwrap_or_default(),
    }
}

fn utc_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_ts(secs)
}

fn format_ts(secs: u64) -> String {
    let s = secs % 86400;
    let days = secs / 86400;
    let (y, mo, d) = days_to_ymd(days);
    let h = s / 3600;
    let mi = (s % 3600) / 60;
    let sec = s % 60;
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{sec:02}Z")
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let mut y = 1970u64;
    let mut rem = days;
    loop {
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let dy = if leap { 366 } else { 365 };
        if rem < dy {
            break;
        }
        rem -= dy;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let months = [
        31u64,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut mo = 1u64;
    for &dm in &months {
        if rem < dm {
            break;
        }
        rem -= dm;
        mo += 1;
    }
    (y, mo, rem + 1)
}

// ---------------------------------------------------------------------------
// Counterparty list for normalization UI
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct NormCounterpartyRow {
    pub rfc: String,
    pub nombre: String,
    pub year_amounts: std::collections::HashMap<String, f64>,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub is_excluded: bool,
    pub rule_id: Option<String>,
}

/// Returns one row per counterparty with per-year totals and exclusion status.
/// RFC-level exclusion rule (cfdi_uuid IS NULL, source_rfc = counterparty) sets is_excluded=true.
pub async fn list_counterparties_for_normalization(
    pool: &DbPool,
    owner_rfc: &str,
    dl_type: &str,
    from_y: i64,
    from_m: i64,
    to_y: i64,
    to_m: i64,
) -> anyhow::Result<Vec<NormCounterpartyRow>> {
    let rfc_col = rfc_column(dl_type);
    let dl_filter = match dl_type {
        "recibidos" => "c.dl_type IN ('recibidos', 'ambos')",
        _ => "c.dl_type IN ('emitidos', 'ambos')",
    };

    let sql = format!(
        r#"SELECT
               CASE WHEN c.rfc_emisor = $1 THEN c.rfc_receptor ELSE c.rfc_emisor END AS rfc_cp,
               CASE WHEN c.rfc_emisor = $1 THEN COALESCE(c.nombre_receptor,'') ELSE COALESCE(c.nombre_emisor,'') END AS nombre_cp,
               c.year,
               SUM(COALESCE(c.total_neto_mxn, c.total_mxn, 0))::float8 AS year_total,
               COUNT(*)::bigint AS year_count
           FROM pulso.cfdis c
           WHERE c.{rfc_col} = $1
             AND {dl_filter}
             AND c.tipo_comprobante NOT IN ('P','N','T')
             AND UPPER(COALESCE(c.estado_sat,'')) NOT LIKE '%CANCEL%'
             AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
             AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
           GROUP BY rfc_cp, nombre_cp, c.year
           ORDER BY rfc_cp, c.year"#
    );

    let rows = sqlx::query(&sql)
        .bind(owner_rfc)
        .bind(from_y)
        .bind(from_m)
        .bind(to_y)
        .bind(to_m)
        .fetch_all(pool)
        .await?;

    // Aggregate per counterparty RFC
    let mut map: std::collections::HashMap<String, NormCounterpartyRow> = std::collections::HashMap::new();
    for r in &rows {
        let rfc: String = r.try_get("rfc_cp").unwrap_or_default();
        let nombre: String = r.try_get("nombre_cp").unwrap_or_default();
        let year: i32 = r.try_get("year").unwrap_or(0);
        let year_total: f64 = r.try_get("year_total").unwrap_or(0.0);
        let year_count: i64 = r.try_get("year_count").unwrap_or(0);

        let entry = map.entry(rfc.clone()).or_insert_with(|| NormCounterpartyRow {
            rfc: rfc.clone(),
            nombre: nombre.clone(),
            year_amounts: std::collections::HashMap::new(),
            total_mxn: 0.0,
            invoice_count: 0,
            is_excluded: false,
            rule_id: None,
        });
        entry.year_amounts.insert(year.to_string(), year_total);
        entry.total_mxn += year_total;
        entry.invoice_count += year_count;
    }

    // Look up RFC-level exclusion rules for each counterparty
    let dl_rule_filter = match dl_type {
        "recibidos" => "nr.dl_type IN ('recibidos','ambos')",
        _ => "nr.dl_type IN ('emitidos','ambos')",
    };
    let rule_sql = format!(
        r#"SELECT nr.id, nr.source_rfc
           FROM pulso.normalization_rules nr
           WHERE nr.owner_rfc = $1 AND nr.action = 'exclude'
             AND nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL
             AND {dl_rule_filter}"#
    );
    let rule_rows = sqlx::query(&rule_sql)
        .bind(owner_rfc)
        .fetch_all(pool)
        .await?;

    for r in &rule_rows {
        let source_rfc: String = r.try_get("source_rfc").unwrap_or_default();
        let rule_id: String = r.try_get("id").unwrap_or_default();
        if let Some(entry) = map.get_mut(&source_rfc.to_uppercase()) {
            entry.is_excluded = true;
            entry.rule_id = Some(rule_id);
        }
    }

    let mut result: Vec<NormCounterpartyRow> = map.into_values().collect();
    result.sort_by(|a, b| b.total_mxn.partial_cmp(&a.total_mxn).unwrap_or(std::cmp::Ordering::Equal));
    Ok(result)
}

// ---------------------------------------------------------------------------
// Individual CFDIs for a specific counterparty (normalization drill-down)
// ---------------------------------------------------------------------------

/// Returns CFDIs for a specific counterparty RFC, with per-CFDI exclusion status.
/// Marks CFDIs excluded either by UUID-level or by RFC-level rule.
pub async fn list_cfdis_for_counterparty(
    pool: &DbPool,
    owner_rfc: &str,
    counterparty_rfc: &str,
    dl_type: &str,
    from_y: i64,
    from_m: i64,
    to_y: i64,
    to_m: i64,
    limit: i64,
) -> anyhow::Result<Vec<NormCfdiRow>> {
    let rfc_col = rfc_column(dl_type);
    let dl_filter = match dl_type {
        "recibidos" => "c.dl_type IN ('recibidos', 'ambos')",
        _ => "c.dl_type IN ('emitidos', 'ambos')",
    };
    let cp_col = match dl_type {
        "recibidos" => "c.rfc_emisor",
        _ => "c.rfc_receptor",
    };

    let sql = format!(
        r#"SELECT c.uuid,
               {cp_col} AS rfc_contraparte,
               CASE WHEN c.rfc_emisor = $1 THEN COALESCE(c.nombre_receptor,'') ELSE COALESCE(c.nombre_emisor,'') END AS nombre_contraparte,
               c.tipo_comprobante,
               COALESCE(c.fecha_emision::text, '') AS fecha_emision,
               COALESCE(c.total_neto_mxn, c.total_mxn, 0)::float8 AS total_mxn,
               c.year::text || '-' || LPAD(c.month::text, 2, '0') AS period,
               COALESCE((SELECT cc.descripcion FROM pulso.cfdi_concepts cc WHERE cc.uuid = c.uuid LIMIT 1), '') AS concepto,
               CASE WHEN nr.id IS NOT NULL THEN true
                    WHEN rfc_nr.id IS NOT NULL THEN true
                    ELSE false END AS is_excluded,
               COALESCE(nr.id, rfc_nr.id) AS rule_id,
               COALESCE(nr.label, rfc_nr.label) AS label
           FROM pulso.cfdis c
           LEFT JOIN pulso.normalization_rules nr
               ON UPPER(nr.cfdi_uuid) = UPPER(c.uuid)
               AND nr.owner_rfc = $1 AND nr.action = 'exclude'
           LEFT JOIN pulso.normalization_rules rfc_nr
               ON rfc_nr.cfdi_uuid IS NULL
               AND rfc_nr.source_rfc = $2
               AND rfc_nr.owner_rfc = $1 AND rfc_nr.action = 'exclude'
               AND rfc_nr.dl_type IN ({dl_filter_rfc})
           WHERE c.{rfc_col} = $1
             AND {cp_col} = $2
             AND {dl_filter}
             AND c.tipo_comprobante NOT IN ('P','N','T')
             AND (c.year > $3 OR (c.year = $3 AND c.month >= $4))
             AND (c.year < $5 OR (c.year = $5 AND c.month <= $6))
           ORDER BY c.fecha_emision DESC
           LIMIT $7"#,
        dl_filter_rfc = match dl_type {
            "recibidos" => "'recibidos','ambos'",
            _ => "'emitidos','ambos'",
        }
    );

    let rows = sqlx::query(&sql)
        .bind(owner_rfc)
        .bind(counterparty_rfc.to_uppercase())
        .bind(from_y)
        .bind(from_m)
        .bind(to_y)
        .bind(to_m)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    Ok(rows
        .iter()
        .map(|r| NormCfdiRow {
            uuid: r.try_get("uuid").unwrap_or_default(),
            rfc_contraparte: r.try_get("rfc_contraparte").unwrap_or_default(),
            nombre_contraparte: r.try_get("nombre_contraparte").unwrap_or_default(),
            tipo_comprobante: r.try_get("tipo_comprobante").unwrap_or_default(),
            fecha_emision: r.try_get("fecha_emision").unwrap_or_default(),
            total_mxn: r.try_get("total_mxn").unwrap_or(0.0),
            period: r.try_get("period").unwrap_or_default(),
            concepto: r.try_get("concepto").unwrap_or_default(),
            is_excluded: r.try_get::<bool, _>("is_excluded").unwrap_or(false),
            rule_id: r.try_get("rule_id").ok(),
            label: r.try_get("label").ok(),
        })
        .collect())
}

// ---------------------------------------------------------------------------
// GET /normalization/payroll/employees
// ---------------------------------------------------------------------------

pub async fn list_payroll_employees(
    pool: &DbPool,
    owner_rfc: &str,
    from_y: i64,
    from_m: i64,
    to_y: i64,
    to_m: i64,
) -> anyhow::Result<Vec<PayrollEmployeeRow>> {
    let rows = sqlx::query(
        r#"
        WITH monthly AS (
            SELECT
                c.rfc_receptor                                          AS employee_rfc,
                MAX(c.nombre_receptor)                                  AS employee_name,
                c.year::text || '-' || LPAD(c.month::text, 2, '0')     AS month_key,
                SUM(COALESCE(c.total_mxn, 0))                           AS month_total
            FROM pulso.cfdis c
            WHERE c.rfc_emisor = $1
              AND c.tipo_comprobante = 'N'
              AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
              AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
              AND (c.estado_sat IS NULL OR c.estado_sat != 'cancelado')
              AND c.rfc_receptor IS NOT NULL
              AND c.rfc_receptor != ''
            GROUP BY c.rfc_receptor, month_key
        )
        SELECT
            employee_rfc,
            MAX(employee_name)                      AS employee_name,
            MIN(month_key)                          AS first_month,
            MAX(month_key)                          AS last_month,
            COUNT(DISTINCT month_key)               AS active_months,
            SUM(month_total)                        AS historical_cost_mxn,
            AVG(month_total)                        AS run_rate_mensual_mxn,
            COUNT(*)                                AS cfdi_count
        FROM monthly
        GROUP BY employee_rfc
        ORDER BY historical_cost_mxn DESC NULLS LAST
        "#,
    )
    .bind(owner_rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| PayrollEmployeeRow {
            employee_rfc: r.try_get("employee_rfc").unwrap_or_default(),
            employee_name: r.try_get("employee_name").ok(),
            first_month: r.try_get("first_month").ok(),
            last_month: r.try_get("last_month").ok(),
            active_months: r.try_get::<i64, _>("active_months").unwrap_or(0),
            historical_cost_mxn: r.try_get::<f64, _>("historical_cost_mxn").unwrap_or(0.0),
            run_rate_mensual_mxn: r.try_get::<f64, _>("run_rate_mensual_mxn").unwrap_or(0.0),
            cfdi_count: r.try_get::<i64, _>("cfdi_count").unwrap_or(0),
        })
        .collect())
}

// ---------------------------------------------------------------------------
// GET /normalization/ebitda-bridge
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct EbitdaBridgeAdjustment {
    pub rule_id: String,
    pub rule_name: Option<String>,
    pub accounting_line: Option<String>,
    pub motivo: Option<String>,
    pub impacts_ebitda: Option<bool>,
    pub dl_type: String,
    pub capex_estimate_dep: Option<bool>,
    pub capex_asset_type: Option<String>,
    pub capex_useful_life_years: Option<f64>,
    pub capex_annual_dep_mxn: Option<f64>,
    pub amounts_by_year: std::collections::HashMap<String, f64>,
    pub total_mxn: f64,
}

pub async fn list_ebitda_bridge_adjustments(
    pool: &DbPool,
    owner_rfc: &str,
    from_y: i64,
    from_m: i64,
    to_y: i64,
    to_m: i64,
) -> anyhow::Result<Vec<EbitdaBridgeAdjustment>> {
    let rows = sqlx::query(
        r#"
        SELECT
            nr.id, nr.rule_name, nr.accounting_line, nr.motivo, nr.impacts_ebitda,
            nr.dl_type, nr.capex_estimate_dep, nr.capex_asset_type,
            nr.capex_useful_life_years, nr.capex_annual_dep_mxn,
            c.year,
            SUM(COALESCE(c.total_mxn, 0))::float8 AS year_total
        FROM pulso.normalization_rules nr
        JOIN pulso.cfdis c ON (
            (nr.cfdi_uuid IS NOT NULL AND c.uuid = nr.cfdi_uuid)
            OR (nr.source_rfc IS NOT NULL AND c.rfc_emisor = nr.source_rfc AND nr.dl_type IN ('recibidos','ambos'))
            OR (nr.source_rfc IS NOT NULL AND c.rfc_receptor = nr.source_rfc AND nr.dl_type IN ('emitidos','ambos'))
        )
        WHERE nr.owner_rfc = $1
          AND nr.accounting_line IS NOT NULL
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
          AND (c.estado_sat IS NULL OR c.estado_sat != 'cancelado')
        GROUP BY nr.id, nr.rule_name, nr.accounting_line, nr.motivo, nr.impacts_ebitda,
                 nr.dl_type, nr.capex_estimate_dep, nr.capex_asset_type,
                 nr.capex_useful_life_years, nr.capex_annual_dep_mxn, c.year
        ORDER BY nr.id, c.year
        "#,
    )
    .bind(owner_rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let mut map: std::collections::HashMap<String, EbitdaBridgeAdjustment> =
        std::collections::HashMap::new();

    for row in &rows {
        let rule_id: String = row.try_get("id").unwrap_or_default();
        let year: i64 = row.try_get("year").unwrap_or(0);
        let year_total: f64 = row.try_get("year_total").unwrap_or(0.0);

        let entry = map.entry(rule_id.clone()).or_insert_with(|| EbitdaBridgeAdjustment {
            rule_id: rule_id.clone(),
            rule_name: row.try_get("rule_name").ok(),
            accounting_line: row.try_get("accounting_line").ok(),
            motivo: row.try_get("motivo").ok(),
            impacts_ebitda: row.try_get("impacts_ebitda").ok(),
            dl_type: row.try_get("dl_type").unwrap_or_default(),
            capex_estimate_dep: row.try_get("capex_estimate_dep").ok(),
            capex_asset_type: row.try_get("capex_asset_type").ok(),
            capex_useful_life_years: row.try_get("capex_useful_life_years").ok(),
            capex_annual_dep_mxn: row.try_get("capex_annual_dep_mxn").ok(),
            amounts_by_year: std::collections::HashMap::new(),
            total_mxn: 0.0,
        });

        entry.amounts_by_year.insert(year.to_string(), year_total);
        entry.total_mxn += year_total;
    }

    let mut result: Vec<EbitdaBridgeAdjustment> = map.into_values().collect();
    result.sort_by(|a, b| a.rule_id.cmp(&b.rule_id));
    Ok(result)
}
