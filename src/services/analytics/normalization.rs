/// Normalization rules CRUD: counterparty grouping/exclusion and payroll adjustments.
use crate::db::DbPool;
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
    pub period_start: Option<String>,
    pub period_end: Option<String>,
    pub notes: Option<String>,
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
    pub period_start: Option<String>,
    pub period_end: Option<String>,
    pub notes: Option<String>,
}

pub async fn list_rules(pool: &DbPool, owner_rfc: &str) -> anyhow::Result<Vec<NormalizationRule>> {
    let rows = sqlx::query(
        "SELECT id, owner_rfc, dl_type, source_rfc, source_name, group_name, action, created_at, updated_at
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
            (id, owner_rfc, dl_type, source_rfc, source_name, group_name, action, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#
    )
    .bind(&id)
    .bind(owner_rfc)
    .bind(&req.dl_type)
    .bind(&req.source_rfc)
    .bind(&req.source_name)
    .bind(&req.group_name)
    .bind(&req.action)
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
        created_at: now.clone(),
        updated_at: now,
    })
}

pub async fn delete_rule(pool: &DbPool, id: &str, owner_rfc: &str) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM pulso.normalization_rules WHERE id = $1 AND owner_rfc = $2")
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
                value_pct, period_start, period_end, notes, created_at, updated_at
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
            period_start: r.try_get("period_start").ok(),
            period_end: r.try_get("period_end").ok(),
            notes: r.try_get("notes").ok(),
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
             value_pct, period_start, period_end, notes, created_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"#,
    )
    .bind(&id)
    .bind(owner_rfc)
    .bind(&req.rule_family)
    .bind(&req.employee_rfc)
    .bind(&req.employee_name)
    .bind(&req.action)
    .bind(&req.value_pct)
    .bind(&req.period_start)
    .bind(&req.period_end)
    .bind(&req.notes)
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
        period_start: req.period_start.clone(),
        period_end: req.period_end.clone(),
        notes: req.notes.clone(),
        created_at: now.clone(),
        updated_at: now,
    })
}

pub async fn delete_payroll_rule(pool: &DbPool, id: &str, owner_rfc: &str) -> anyhow::Result<bool> {
    let result =
        sqlx::query("DELETE FROM pulso.payroll_normalization_rules WHERE id = $1 AND owner_rfc = $2")
            .bind(id)
            .bind(owner_rfc)
            .execute(pool)
            .await?;

    Ok(result.rows_affected() > 0)
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
