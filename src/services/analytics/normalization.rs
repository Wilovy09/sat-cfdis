/// Normalization rules CRUD: counterparty grouping/exclusion and payroll adjustments.
use crate::db::DbPool;
use crate::services::analytics::summary::{
    cp_key_expr, cp_nombre_expr, get_f64, get_f64_opt, rfc_column,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::collections::HashMap;
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
    // DEC-032: optional validity period, "YYYY-MM". NULL on either end = unbounded.
    // Meaningless (and ignored by pulso.cfdi_exclusion) on a cfdi_uuid-level rule.
    pub period_start: Option<String>,
    pub period_end: Option<String>,
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
    // DEC-032
    pub period_start: Option<String>,
    pub period_end: Option<String>,
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
    // DEC-030: same Egresos "Línea del P&L" / Motivo catalog the comprobante-level rules
    // use, so L4-02's EBITDA bridge has something to group a nómina adjustment by.
    // L5-12: motivo no longer reuses that catalog -- see PAYROLL_MOTIVOS -- but the field
    // itself is still shared storage for both rule families.
    pub accounting_line: Option<String>,
    pub motivo: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    // L5-14: re-evaluated at list time (not just at creation) against whatever real
    // percepciones exist now -- empty means the factor is still in range. Informational
    // only, never blocks. Only ever non-empty for rule_family = adjust_to_amount_mxn,
    // the only family whose factor can drift as new receipts arrive.
    pub factor_warnings: Vec<FactorWarning>,
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
    // DEC-030
    pub accounting_line: Option<String>,
    pub motivo: Option<String>,
    // L4-04/L4-12: set true to proceed anyway after a low-factor warning was already
    // shown once for this same request. Absent/false on the first attempt.
    pub confirmed: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct PayrollEmployeeRow {
    pub employee_rfc: String,
    pub employee_name: Option<String>,
    // L5-02: the employee's REAL first-to-last devengo month, never clamped/truncated by
    // whatever date window the caller happens to have selected elsewhere in the app --
    // this is the one place the rule-creation form's period picker reads its bounds from.
    pub first_month: Option<String>,
    pub last_month: Option<String>,
    pub active_months: i64,
    pub historical_cost_mxn: f64,
    pub run_rate_mensual_mxn: f64,
    pub cfdi_count: i64,
    // L5-02: this catalog is no longer filtered by exclusion -- an excluded employee still
    // appears here (it's the only screen that can undo their exclusion), just marked.
    pub is_excluded: bool,
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
    // L3-14: the rules-list keeps showing a rule on a cancelled comprobante (it must stay
    // visible/deletable), but the frontend needs this to mark it as cancelled rather than
    // implying it's still live.
    pub is_cancelled: bool,
}

pub async fn list_rules(pool: &DbPool, owner_rfc: &str) -> anyhow::Result<Vec<NormalizationRule>> {
    let rows = sqlx::query(
        // L5-01: capex_useful_life_years/capex_annual_dep_mxn are NUMERIC -- sqlx's f64
        // only decodes FLOAT8, so an uncast read here silently failed and .ok() turned it
        // into a null the frontend could never tell apart from "not captured".
        "SELECT id, owner_rfc, dl_type, source_rfc, source_name, group_name, action, label,
                rule_name, cfdi_uuid,
                accounting_line, motivo, impacts_ebitda, capex_estimate_dep,
                capex_asset_type, capex_useful_life_years::float8 AS capex_useful_life_years,
                capex_annual_dep_mxn::float8 AS capex_annual_dep_mxn,
                period_start, period_end,
                created_at, updated_at
         FROM pulso.normalization_rules WHERE owner_rfc = $1 ORDER BY created_at DESC",
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
            capex_useful_life_years: get_f64_opt(r, "capex_useful_life_years"),
            capex_annual_dep_mxn: get_f64_opt(r, "capex_annual_dep_mxn"),
            period_start: r.try_get("period_start").ok(),
            period_end: r.try_get("period_end").ok(),
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
             capex_annual_dep_mxn, period_start, period_end, created_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)"#,
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
    .bind(&req.period_start)
    .bind(&req.period_end)
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
        period_start: req.period_start.clone(),
        period_end: req.period_end.clone(),
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

// L5-10: same fields, same validations as create_rule (enforced by the route handler,
// which runs the same accounting_line/motivo checks for both verbs) -- this used to be
// delete + recreate only, which loses the rule's created_at and its id (every consumer
// that keyed off the id, e.g. an EBITDA bridge row, would silently start from zero).
pub async fn update_rule(
    pool: &DbPool,
    id: &str,
    owner_rfc: &str,
    req: &CreateRuleRequest,
) -> anyhow::Result<Option<NormalizationRule>> {
    let now = utc_now();

    let row = sqlx::query(
        r#"UPDATE pulso.normalization_rules
           SET dl_type = $1, source_rfc = $2, source_name = $3, group_name = $4,
               action = $5, label = $6, rule_name = $7, cfdi_uuid = $8,
               accounting_line = $9, motivo = $10, impacts_ebitda = $11,
               capex_estimate_dep = $12, capex_asset_type = $13,
               capex_useful_life_years = $14, capex_annual_dep_mxn = $15,
               period_start = $16, period_end = $17, updated_at = $18
           WHERE id = $19 AND owner_rfc = $20
           RETURNING created_at"#,
    )
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
    .bind(&req.period_start)
    .bind(&req.period_end)
    .bind(&now)
    .bind(id)
    .bind(owner_rfc)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };
    let created_at: String = row.try_get("created_at").unwrap_or_default();

    Ok(Some(NormalizationRule {
        id: id.to_string(),
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
        period_start: req.period_start.clone(),
        period_end: req.period_end.clone(),
        created_at,
        updated_at: now,
    }))
}

// ---------------------------------------------------------------------------
// Payroll normalization
// ---------------------------------------------------------------------------

pub async fn list_payroll_rules(
    pool: &DbPool,
    owner_rfc: &str,
) -> anyhow::Result<Vec<PayrollNormRule>> {
    let rows = sqlx::query(
        // L5-01: value_pct/value_mxn are NUMERIC -- see list_rules' comment above on the
        // same sqlx/f64 decode gap (this is one of the two columns the audit confirmed).
        "SELECT id, owner_rfc, rule_family, employee_rfc, employee_name, action,
                value_pct::float8 AS value_pct, value_mxn::float8 AS value_mxn,
                period_start, period_end, notes, label, rule_name,
                excluded_cfdi_uuids, accounting_line, motivo, created_at, updated_at
         FROM pulso.payroll_normalization_rules WHERE owner_rfc = $1 ORDER BY created_at DESC",
    )
    .bind(owner_rfc)
    .fetch_all(pool)
    .await?;

    // L6-04: the L5-14 re-evaluation below used to call compute_adjust_factor_warnings once
    // per adjust_to_amount_mxn rule -- one network round trip per rule (measured: 60 seeded
    // rows took ~9.7s end to end, an order of magnitude over the 1s performance budget).
    // Every candidate employee's monthly percepciones are fetched once here instead, and
    // classified against each rule's own period bounds in memory below.
    let mut adjust_employee_rfcs: Vec<String> = Vec::new();
    for r in &rows {
        let rule_family: String = r.try_get("rule_family").unwrap_or_default();
        if rule_family != "adjust_to_amount_mxn" {
            continue;
        }
        if let Ok(Some(employee_rfc)) = r.try_get::<Option<String>, _>("employee_rfc") {
            adjust_employee_rfcs.push(employee_rfc);
        }
    }
    let adjust_sources =
        batch_adjust_factor_sources(pool, owner_rfc, &adjust_employee_rfcs).await?;
    let no_sources: Vec<(i64, i64, f64)> = Vec::new();

    let mut result = Vec::with_capacity(rows.len());
    for r in &rows {
        let rule_family: String = r.try_get("rule_family").unwrap_or_default();
        let employee_rfc: Option<String> = r.try_get("employee_rfc").ok();
        let value_mxn = get_f64_opt(r, "value_mxn");
        let period_start: Option<String> = r.try_get("period_start").ok();
        let period_end: Option<String> = r.try_get("period_end").ok();

        // L5-14: the same factor check that gates creation, re-run here against whatever
        // real percepciones exist now -- a rule saved as reasonable can drift out of range
        // as new receipts arrive, and nothing used to re-check it after the fact. Purely
        // informational: it marks the row, it never removes it from the list.
        let factor_warnings = match (rule_family.as_str(), employee_rfc.as_deref(), value_mxn) {
            ("adjust_to_amount_mxn", Some(employee_rfc), Some(value_mxn)) => {
                let monthly = adjust_sources.get(employee_rfc).unwrap_or(&no_sources);
                let (high, low) = classify_adjust_factor_warnings(
                    monthly,
                    value_mxn,
                    period_start.as_deref(),
                    period_end.as_deref(),
                );
                high.into_iter().chain(low).collect()
            }
            _ => Vec::new(),
        };

        result.push(PayrollNormRule {
            id: r.try_get("id").unwrap_or_default(),
            owner_rfc: r.try_get("owner_rfc").unwrap_or_default(),
            rule_family,
            employee_rfc,
            employee_name: r.try_get("employee_name").ok(),
            action: r.try_get("action").unwrap_or_default(),
            value_pct: get_f64_opt(r, "value_pct"),
            value_mxn,
            period_start,
            period_end,
            notes: r.try_get("notes").ok(),
            label: r.try_get("label").ok(),
            rule_name: r.try_get("rule_name").ok(),
            excluded_cfdi_uuids: r
                .try_get::<Option<Vec<String>>, _>("excluded_cfdi_uuids")
                .ok()
                .flatten(),
            accounting_line: r.try_get("accounting_line").ok(),
            motivo: r.try_get("motivo").ok(),
            created_at: r.try_get("created_at").unwrap_or_default(),
            updated_at: r.try_get("updated_at").unwrap_or_default(),
            factor_warnings,
        });
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// L4-04 / L4-12 / L5-08 / L5-12 / L5-14: payroll rule validation
// ---------------------------------------------------------------------------

/// One month where an `adjust_to_amount_mxn` rule's factor is far enough from 1 to be
/// worth surfacing: at creation/edit time (L4-12, before the rule is saved) and again at
/// list time (L5-14, re-evaluated against whatever real percepciones exist now).
#[derive(Debug, Serialize)]
pub struct FactorWarning {
    pub period: String,
    pub factor: f64,
    pub real_percepciones: f64,
}

pub enum PayrollRuleCheck {
    Ok,
    /// Hard failure -- the rule cannot be saved as submitted. Message explains why.
    Rejected(String),
    /// At least one covered month produces a suspiciously low factor (L4-12). Not an
    /// error: the caller re-submits with `confirmed: true` to proceed anyway.
    NeedsConfirmation(Vec<FactorWarning>),
}

// L4-04's own suggested starting point ("10x", explicitly calibrate later with real
// data). L4-12 doesn't suggest a number for the low end; a factor below this means the
// adjustment removes more than half of the month's real cost, which is the point past
// which "normalizing" starts looking like "erasing an extraordinary payment."
const FACTOR_REJECT_ABOVE: f64 = 10.0;
const FACTOR_WARN_BELOW: f64 = 0.5;

/// L5-12 / DEC-033: nómina's own reason catalog. It used to share Egresos' ("escalar o
/// ajustar un sueldo no es excluirlo" -- the one real rule captured so far had to pick
/// "Otro" because nothing else applied). Frontend catalog to match:
/// pulso-adquiere/src/constants/normalizationCatalogs.ts.
pub const PAYROLL_MOTIVOS: &[&str] = &[
    "Sueldo por encima de mercado",
    "Sueldo del dueño o accionista",
    "Puesto que no continúa tras la transacción",
    "Plaza vacante o no reemplazada",
    "Compensación extraordinaria",
    "Gasto personal del accionista",
    "Otro",
];

/// An "active exclusion" per `excl_emp` in the `pulso.nomina_normalizada` view definition
/// (migration 066): rule_family in the exclusion family AND action = 'exclude'. Checked
/// the same way here so C1 rejects exactly the combinations the view would actually treat
/// as an exclusion, not a superset or subset of it.
fn is_active_exclusion(rule_family: &str, action: &str) -> bool {
    (rule_family == "exclude_employee" || rule_family == "exclusion") && action == "exclude"
}

fn is_dimensioning_family(rule_family: &str) -> bool {
    // L5-08 C2 note: the future concepts family (L5-17) is deliberately NOT dimensioning
    // -- it coexists with a scale/adjust rule by design -- so it must never be added here.
    rule_family == "scale_employee_pct" || rule_family == "adjust_to_amount_mxn"
}

/// Do periods `[a_start, a_end]` and `[b_start, b_end]` overlap? NULL on either end of
/// either period means unbounded in that direction -- mirrors the frontend's own
/// `periodsOverlap` (PayrollRuleForm.vue) so "vigente" means the same thing server-side
/// as it already does in the capture form.
fn periods_overlap(
    a_start: Option<&str>,
    a_end: Option<&str>,
    b_start: Option<&str>,
    b_end: Option<&str>,
) -> bool {
    let a_s = a_start.unwrap_or("0000-00");
    let a_e = a_end.unwrap_or("9999-99");
    let b_s = b_start.unwrap_or("0000-00");
    let b_e = b_end.unwrap_or("9999-99");
    a_s <= b_e && b_s <= a_e
}

/// L5-08 C1/C2, enforced server-side: a candidate rule (`rule_family`/`period_start`/
/// `period_end`) for `employee_rfc` is checked against every other ACTIVE rule that
/// employee already has (i.e. one whose period overlaps -- C3's spirit: non-overlapping
/// periods are always allowed, no friction). `exclude_rule_id` is the rule's own id on an
/// update, so it doesn't conflict with itself.
///
/// C1: an employee with an active exclusion admits no other rule, in either direction.
/// C2: at most one scale-or-adjust rule active per employee-period, across BOTH families
/// (today they can silently overlap and the more recently created one wins at read time --
/// this closes that at the write layer instead).
async fn check_payroll_rule_locks(
    pool: &DbPool,
    owner_rfc: &str,
    employee_rfc: &str,
    req: &CreatePayrollRuleRequest,
    exclude_rule_id: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let rows = sqlx::query(
        r#"SELECT id, rule_family, action, period_start, period_end
           FROM pulso.payroll_normalization_rules
           WHERE owner_rfc = $1 AND employee_rfc = $2"#,
    )
    .bind(owner_rfc)
    .bind(employee_rfc)
    .fetch_all(pool)
    .await?;

    let new_is_exclusion = is_active_exclusion(&req.rule_family, &req.action);
    let new_is_dimensioning = is_dimensioning_family(&req.rule_family);

    for r in &rows {
        let existing_id: String = r.try_get("id").unwrap_or_default();
        if Some(existing_id.as_str()) == exclude_rule_id {
            continue;
        }
        let existing_family: String = r.try_get("rule_family").unwrap_or_default();
        let existing_action: String = r.try_get("action").unwrap_or_default();
        let existing_start: Option<String> = r.try_get("period_start").ok();
        let existing_end: Option<String> = r.try_get("period_end").ok();
        if !periods_overlap(
            req.period_start.as_deref(),
            req.period_end.as_deref(),
            existing_start.as_deref(),
            existing_end.as_deref(),
        ) {
            continue;
        }

        let existing_is_exclusion = is_active_exclusion(&existing_family, &existing_action);
        let existing_is_dimensioning = is_dimensioning_family(&existing_family);

        if new_is_exclusion && !existing_is_exclusion {
            return Ok(Some(
                "C1: este empleado ya tiene una regla activa en un periodo que se traslapa. \
                 Un empleado excluido no admite ninguna otra regla."
                    .to_string(),
            ));
        }
        if !new_is_exclusion && existing_is_exclusion {
            return Ok(Some(
                "C1: este empleado ya tiene una regla de exclusión activa en un periodo que \
                 se traslapa. Un empleado excluido no admite ninguna otra regla."
                    .to_string(),
            ));
        }
        if new_is_dimensioning && existing_is_dimensioning {
            return Ok(Some(
                "C2: ya existe una regla de escala o ajuste vigente para este empleado en un \
                 periodo que se traslapa. Solo puede haber una regla de dimensionamiento \
                 activa por periodo, sin importar la familia."
                    .to_string(),
            ));
        }
    }

    Ok(None)
}

/// Per-month factor for a candidate `adjust_to_amount_mxn` value against real percepciones,
/// split into months above `FACTOR_REJECT_ABOVE` and below `FACTOR_WARN_BELOW`. Used by
/// `check_payroll_rule` for the one value a create/update request is proposing -- a single
/// call, not looped, so querying per-employee here is fine. `list_payroll_rules` (L5-14:
/// re-evaluating every already-saved rule) instead uses the batched
/// `batch_adjust_factor_sources` + `classify_adjust_factor_warnings` pair below (L6-04),
/// which apply the exact same math without one query per rule.
async fn compute_adjust_factor_warnings(
    pool: &DbPool,
    owner_rfc: &str,
    employee_rfc: &str,
    value_mxn: f64,
    period_start: Option<&str>,
    period_end: Option<&str>,
) -> anyhow::Result<(Vec<FactorWarning>, Vec<FactorWarning>)> {
    let rows = sqlx::query(
        r#"SELECT c.year, c.month, SUM(COALESCE(n.total_percepciones, 0))::float8 AS perc
           FROM pulso.cfdis c
           JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
           WHERE c.rfc_emisor = $1 AND c.rfc_receptor = $2
             AND c.tipo_comprobante = 'N' AND NOT c.is_cancelled
             AND ($3::text IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= $3)
             AND ($4::text IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= $4)
           GROUP BY c.year, c.month"#,
    )
    .bind(owner_rfc)
    .bind(employee_rfc)
    .bind(period_start)
    .bind(period_end)
    .fetch_all(pool)
    .await?;

    let mut high: Vec<FactorWarning> = Vec::new();
    let mut low: Vec<FactorWarning> = Vec::new();
    for r in &rows {
        let perc = get_f64(r, "perc");
        if perc <= 0.0 {
            continue;
        }
        let year: i64 = r.try_get("year").unwrap_or(0);
        let month: i64 = r.try_get("month").unwrap_or(0);
        let factor = value_mxn / perc;
        let warning = FactorWarning {
            period: format!("{year}-{month:02}"),
            factor,
            real_percepciones: perc,
        };
        if factor > FACTOR_REJECT_ABOVE {
            high.push(warning);
        } else if factor < FACTOR_WARN_BELOW {
            low.push(warning);
        }
    }
    Ok((high, low))
}

/// L6-04: every `adjust_to_amount_mxn` candidate employee's monthly percepciones, fetched
/// in one query instead of `list_payroll_rules` running `compute_adjust_factor_warnings`
/// once per rule. No period filtering here -- `classify_adjust_factor_warnings` applies
/// each rule's own bounds afterward, since two rules can share an employee with different
/// periods.
async fn batch_adjust_factor_sources(
    pool: &DbPool,
    owner_rfc: &str,
    employee_rfcs: &[String],
) -> anyhow::Result<HashMap<String, Vec<(i64, i64, f64)>>> {
    if employee_rfcs.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"SELECT c.rfc_receptor AS employee_rfc, c.year, c.month,
                  SUM(COALESCE(n.total_percepciones, 0))::float8 AS perc
           FROM pulso.cfdis c
           JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
           WHERE c.rfc_emisor = $1 AND c.rfc_receptor = ANY($2)
             AND c.tipo_comprobante = 'N' AND NOT c.is_cancelled
           GROUP BY c.rfc_receptor, c.year, c.month"#,
    )
    .bind(owner_rfc)
    .bind(employee_rfcs)
    .fetch_all(pool)
    .await?;

    let mut by_employee: HashMap<String, Vec<(i64, i64, f64)>> = HashMap::new();
    for r in &rows {
        let employee_rfc: String = r.try_get("employee_rfc").unwrap_or_default();
        let year: i64 = r.try_get("year").unwrap_or(0);
        let month: i64 = r.try_get("month").unwrap_or(0);
        by_employee
            .entry(employee_rfc)
            .or_default()
            .push((year, month, get_f64(r, "perc")));
    }
    Ok(by_employee)
}

/// Same threshold/period logic as `compute_adjust_factor_warnings`, applied in memory
/// against a pre-fetched (year, month, percepciones) set instead of running its own query
/// per rule -- see `batch_adjust_factor_sources`.
fn classify_adjust_factor_warnings(
    monthly_percepciones: &[(i64, i64, f64)],
    value_mxn: f64,
    period_start: Option<&str>,
    period_end: Option<&str>,
) -> (Vec<FactorWarning>, Vec<FactorWarning>) {
    let mut high: Vec<FactorWarning> = Vec::new();
    let mut low: Vec<FactorWarning> = Vec::new();
    for &(year, month, perc) in monthly_percepciones {
        if perc <= 0.0 {
            continue;
        }
        let period = format!("{year}-{month:02}");
        if period_start.is_some_and(|s| period.as_str() < s)
            || period_end.is_some_and(|e| period.as_str() > e)
        {
            continue;
        }
        let factor = value_mxn / perc;
        let warning = FactorWarning {
            period,
            factor,
            real_percepciones: perc,
        };
        if factor > FACTOR_REJECT_ABOVE {
            high.push(warning);
        } else if factor < FACTOR_WARN_BELOW {
            low.push(warning);
        }
    }
    (high, low)
}

/// Validates a payroll rule before it's written: required fields and the nómina motivo
/// catalog (L5-12), the C1/C2 mutual-exclusivity and single-dimensioning-rule locks
/// (L5-08, `exclude_rule_id` is Some on an update so the rule doesn't conflict with
/// itself), range-checks on `value_pct`/`value_mxn` (L4-04), and for
/// `adjust_to_amount_mxn`, the factor-vs-real-percepciones check (L4-12).
pub async fn check_payroll_rule(
    pool: &DbPool,
    owner_rfc: &str,
    req: &CreatePayrollRuleRequest,
    exclude_rule_id: Option<&str>,
) -> anyhow::Result<PayrollRuleCheck> {
    if req
        .accounting_line
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .is_empty()
    {
        return Ok(PayrollRuleCheck::Rejected(
            "accounting_line es obligatorio: selecciona la línea del P&L de la que sale este \
             ajuste"
                .to_string(),
        ));
    }
    let motivo = req.motivo.as_deref().map(str::trim).unwrap_or("");
    if motivo.is_empty() {
        return Ok(PayrollRuleCheck::Rejected(
            "motivo es obligatorio: documenta por qué se ajusta o excluye a este empleado"
                .to_string(),
        ));
    }
    if !PAYROLL_MOTIVOS.contains(&motivo) {
        return Ok(PayrollRuleCheck::Rejected(format!(
            "motivo inválido: debe ser uno de: {}",
            PAYROLL_MOTIVOS.join(", ")
        )));
    }

    if req.rule_family == "scale_employee_pct" {
        match req.value_pct {
            Some(p) if (0.0..=100.0).contains(&p) => {}
            _ => {
                return Ok(PayrollRuleCheck::Rejected(
                    "value_pct debe ser un porcentaje entre 0 y 100".to_string(),
                ));
            }
        }
    }

    let value_mxn = if req.rule_family == "adjust_to_amount_mxn" {
        match req.value_mxn {
            Some(m) if m > 0.0 => Some(m),
            _ => {
                return Ok(PayrollRuleCheck::Rejected(
                    "value_mxn debe ser un monto positivo".to_string(),
                ));
            }
        }
    } else {
        None
    };

    if let Some(employee_rfc) = req.employee_rfc.as_deref() {
        if let Some(reason) =
            check_payroll_rule_locks(pool, owner_rfc, employee_rfc, req, exclude_rule_id).await?
        {
            return Ok(PayrollRuleCheck::Rejected(reason));
        }
    }

    if let Some(value_mxn) = value_mxn {
        let Some(employee_rfc) = req.employee_rfc.as_deref() else {
            return Ok(PayrollRuleCheck::Rejected(
                "employee_rfc es obligatorio para ajustar a monto".to_string(),
            ));
        };

        let (high, low) = compute_adjust_factor_warnings(
            pool,
            owner_rfc,
            employee_rfc,
            value_mxn,
            req.period_start.as_deref(),
            req.period_end.as_deref(),
        )
        .await?;

        if !high.is_empty() {
            let months: Vec<String> = high.iter().map(|w| w.period.clone()).collect();
            return Ok(PayrollRuleCheck::Rejected(format!(
                "El monto produce un factor mayor a {FACTOR_REJECT_ABOVE}x en: {}. \
                 Acota el periodo para excluir esos meses o ajusta el monto.",
                months.join(", ")
            )));
        }

        if !low.is_empty() && !req.confirmed.unwrap_or(false) {
            return Ok(PayrollRuleCheck::NeedsConfirmation(low));
        }
    }

    Ok(PayrollRuleCheck::Ok)
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
             excluded_cfdi_uuids, accounting_line, motivo, created_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)"#,
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
    .bind(&req.accounting_line)
    .bind(&req.motivo)
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
        accounting_line: req.accounting_line.clone(),
        motivo: req.motivo.clone(),
        created_at: now.clone(),
        updated_at: now,
        // L5-14: nothing to re-evaluate yet -- check_payroll_rule already validated this
        // exact value against current data as a precondition for this INSERT running.
        factor_warnings: Vec::new(),
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

// L5-10: same fields/validations as create_payroll_rule (the route handler runs the same
// check_payroll_rule -- including the new C1/C2 locks and the L5-12 motivo catalog --
// against this update, passing this rule's own id so it doesn't conflict with itself).
pub async fn update_payroll_rule(
    pool: &DbPool,
    id: &str,
    owner_rfc: &str,
    req: &CreatePayrollRuleRequest,
) -> anyhow::Result<Option<PayrollNormRule>> {
    let now = utc_now();

    let row = sqlx::query(
        r#"UPDATE pulso.payroll_normalization_rules
           SET rule_family = $1, employee_rfc = $2, employee_name = $3, action = $4,
               value_pct = $5, value_mxn = $6, period_start = $7, period_end = $8,
               notes = $9, label = $10, rule_name = $11, excluded_cfdi_uuids = $12,
               accounting_line = $13, motivo = $14, updated_at = $15
           WHERE id = $16 AND owner_rfc = $17
           RETURNING created_at"#,
    )
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
    .bind(&req.accounting_line)
    .bind(&req.motivo)
    .bind(&now)
    .bind(id)
    .bind(owner_rfc)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };
    let created_at: String = row.try_get("created_at").unwrap_or_default();

    Ok(Some(PayrollNormRule {
        id: id.to_string(),
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
        accounting_line: req.accounting_line.clone(),
        motivo: req.motivo.clone(),
        created_at,
        updated_at: now,
        factor_warnings: Vec::new(),
    }))
}

pub async fn list_excluded_cfdis(
    pool: &DbPool,
    owner_rfc: &str,
) -> anyhow::Result<Vec<ExcludedCfdi>> {
    // L3-01: both the counterparty-rule and cfdi_uuid-rule branches now come from the
    // shared exclusion base (which also carries L3-02's generic-RFC name-key match), then
    // join back to normalization_rules for the metadata this listing needs.
    let counterparty_rows = sqlx::query(
        r#"SELECT nr.id AS rule_id,
                  CASE WHEN nr.cfdi_uuid IS NOT NULL THEN 'cfdi' ELSE 'counterparty' END AS rule_type,
                  nr.rule_name, nr.label,
                  c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
                  c.tipo_comprobante, c.fecha_emision, COALESCE(c.total_mxn, 0)::float8 AS total_mxn,
                  c.year::text || '-' || LPAD(c.month::text, 2, '0') AS period,
                  c.is_cancelled
           FROM pulso.cfdi_exclusion ex
           JOIN pulso.normalization_rules nr ON nr.id = ex.rule_id
           JOIN pulso.cfdis c ON c.uuid = ex.uuid
           WHERE ex.owner_rfc = $1"#,
    )
    .bind(owner_rfc)
    .fetch_all(pool)
    .await?;

    // L3-16: which receipts a payroll rule excluded now comes straight from the shared
    // base's employee_rule_id (populated by the same excl_emp match the base already
    // computes for is_excluded), instead of re-deriving the owner/employee/period match here.
    let payroll_rows = sqlx::query(
        r#"SELECT pnr.id AS rule_id, 'payroll' AS rule_type, pnr.rule_name, pnr.label,
                  n.uuid, n.rfc_emisor, n.rfc_receptor, n.nombre_emisor, n.nombre_receptor,
                  'N' AS tipo_comprobante, n.fecha_emision, n.total_percepciones AS total_mxn,
                  n.year::text || '-' || LPAD(n.month::text, 2, '0') AS period,
                  -- pulso.nomina_normalizada already filters out cancelled receipts.
                  false AS is_cancelled
           FROM pulso.nomina_normalizada n
           JOIN pulso.payroll_normalization_rules pnr ON pnr.id = n.employee_rule_id
           WHERE n.rfc_emisor = $1"#,
    )
    .bind(owner_rfc)
    .fetch_all(pool)
    .await?;

    let mut results: Vec<ExcludedCfdi> = counterparty_rows
        .iter()
        .map(map_excluded_cfdi_row)
        .collect();
    results.extend(payroll_rows.iter().map(map_excluded_cfdi_row));

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
        total_mxn: get_f64(r, "total_mxn"),
        period: r.try_get("period").unwrap_or_default(),
        is_cancelled: r.try_get::<bool, _>("is_cancelled").unwrap_or(false),
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
    let cp_col = if dl_type == "recibidos" {
        "c.rfc_emisor"
    } else {
        "c.rfc_receptor"
    };
    let cp_name_col = if dl_type == "recibidos" {
        "c.nombre_emisor"
    } else {
        "c.nombre_receptor"
    };
    // L3-02: group by the same composite RFC||NORMALIZED_NAME key counterparties.rs uses,
    // so the 24 real companies behind a generic SAT RFC show as 24 rows here too, not one.
    let cp_key = cp_key_expr(cp_col, cp_name_col);
    let cp_nombre = cp_nombre_expr(cp_col, cp_name_col);

    let sql = format!(
        r#"SELECT
               ({cp_key})    AS rfc_cp,
               ({cp_nombre}) AS nombre_cp,
               c.year,
               SUM(COALESCE(c.total_neto_mxn_ajustado, c.total_mxn, 0))::float8 AS year_total,
               COUNT(*)::bigint AS year_count
           FROM pulso.cfdis_ajustado c
           WHERE c.{rfc_col} = $1
             AND {dl_filter}
             AND c.tipo_comprobante NOT IN ('P','N','T')
             AND NOT c.is_cancelled
             AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
             AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
           GROUP BY ({cp_key}), c.year
           ORDER BY ({cp_key}), c.year"#
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
    let mut map: std::collections::HashMap<String, NormCounterpartyRow> =
        std::collections::HashMap::new();
    for r in &rows {
        let rfc: String = r.try_get("rfc_cp").unwrap_or_default();
        let nombre: String = r.try_get("nombre_cp").unwrap_or_default();
        let year: i32 = r.try_get("year").unwrap_or(0);
        let year_total: f64 = get_f64(r, "year_total");
        let year_count: i64 = r.try_get("year_count").unwrap_or(0);

        let entry = map
            .entry(rfc.clone())
            .or_insert_with(|| NormCounterpartyRow {
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

    // Look up RFC-level exclusion rules for each counterparty, keyed the same way the
    // map above is: bare RFC for an ordinary rule, RFC||NAME_KEY for a generic-RFC rule
    // narrowed by L3-02 (a generic-RFC rule with no name key still matches the bare key,
    // i.e. every counterparty behind that RFC -- same as before L3-02 existed).
    let dl_rule_filter = match dl_type {
        "recibidos" => "nr.dl_type IN ('recibidos','ambos')",
        _ => "nr.dl_type IN ('emitidos','ambos')",
    };
    let rule_sql = format!(
        r#"SELECT nr.id,
                  CASE WHEN nr.source_name_key IS NOT NULL
                       THEN UPPER(nr.source_rfc) || '||' || nr.source_name_key
                       ELSE UPPER(nr.source_rfc) END AS cp_key
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
        let cp_key: String = r.try_get("cp_key").unwrap_or_default();
        let rule_id: String = r.try_get("id").unwrap_or_default();
        if let Some(entry) = map.get_mut(&cp_key) {
            entry.is_excluded = true;
            entry.rule_id = Some(rule_id);
        }
    }

    let mut result: Vec<NormCounterpartyRow> = map.into_values().collect();
    result.sort_by(|a, b| {
        b.total_mxn
            .partial_cmp(&a.total_mxn)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
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
    let cp_name_col = match dl_type {
        "recibidos" => "c.nombre_emisor",
        _ => "c.nombre_receptor",
    };

    // L3-02: counterparty_rfc may be the composite "GENERIC_RFC||NORMALIZED_NAME" key
    // list_counterparties_for_normalization now returns for a generic SAT RFC. Split it
    // back apart so the drill-down narrows to that one real counterparty instead of every
    // invoice sharing the generic RFC. Ordinary RFCs never contain "||", so name_filter
    // is empty and the added predicate is a no-op.
    let (base_rfc, name_filter): (&str, &str) = counterparty_rfc
        .split_once("||")
        .unwrap_or((counterparty_rfc, ""));
    let name_filter_expr = crate::services::analytics::summary::normalized_name_expr(cp_name_col);

    let sql = format!(
        r#"SELECT c.uuid,
               {cp_col} AS rfc_contraparte,
               COALESCE({cp_name_col}, '') AS nombre_contraparte,
               c.tipo_comprobante,
               COALESCE(c.fecha_emision::text, '') AS fecha_emision,
               COALESCE(c.total_neto_mxn_ajustado, c.total_mxn, 0)::float8 AS total_mxn,
               c.year::text || '-' || LPAD(c.month::text, 2, '0') AS period,
               COALESCE((SELECT cc.descripcion FROM pulso.cfdi_concepts cc WHERE cc.uuid = c.uuid LIMIT 1), '') AS concepto,
               CASE WHEN ex.rule_id IS NOT NULL THEN true ELSE false END AS is_excluded,
               ex.rule_id,
               nr.label
           FROM pulso.cfdis_ajustado c
           LEFT JOIN pulso.cfdi_exclusion ex ON ex.owner_rfc = $1 AND ex.uuid = c.uuid
           LEFT JOIN pulso.normalization_rules nr ON nr.id = ex.rule_id
           WHERE c.{rfc_col} = $1
             AND {cp_col} = $2 AND ($8 = '' OR {name_filter_expr} = $8)
             AND {dl_filter}
             AND c.tipo_comprobante NOT IN ('P','N','T')
             -- L3-14: this drill-down doubles as the picker for building a per-CFDI rule,
             -- so a cancelled comprobante (already excluded from every total) is not
             -- offered as something to build a new rule on. The rules-list endpoint
             -- (list_excluded_cfdis) is untouched -- an existing rule on a cancelled
             -- document must stay visible and deletable there.
             AND NOT c.is_cancelled
             AND (c.year > $3 OR (c.year = $3 AND c.month >= $4))
             AND (c.year < $5 OR (c.year = $5 AND c.month <= $6))
           ORDER BY c.fecha_emision DESC
           LIMIT $7"#
    );

    let rows = sqlx::query(&sql)
        .bind(owner_rfc)
        .bind(base_rfc.to_uppercase())
        .bind(from_y)
        .bind(from_m)
        .bind(to_y)
        .bind(to_m)
        .bind(limit)
        .bind(name_filter)
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
            total_mxn: get_f64(r, "total_mxn"),
            period: r.try_get("period").unwrap_or_default(),
            concepto: r.try_get("concepto").unwrap_or_default(),
            is_excluded: r.try_get::<bool, _>("is_excluded").unwrap_or(false),
            rule_id: r.try_get("rule_id").ok(),
            label: r.try_get("label").ok(),
        })
        .collect())
}

// ---------------------------------------------------------------------------
// L6-11: individual (cfdi_uuid-specific) rule ids a counterparty-wide rule would make
// redundant
// ---------------------------------------------------------------------------

/// Every ALREADY-SAVED individual (`cfdi_uuid IS NOT NULL`) exclusion rule whose target
/// comprobante falls inside the population a counterparty-wide rule (matching
/// `owner_rfc`/`source_rfc`/`dl_type`, optionally narrowed by `source_name_key` and a
/// period) would exclude -- so the caller can delete them and avoid double-counting the
/// same receipt twice (L5-07's fix at the read layer only holds if these get cleaned up).
///
/// Same three match predicates `pulso.cfdi_exclusion`'s counterparty branches use
/// (migration 062): direction (dl_type vs. which side owner_rfc/source_rfc sit on),
/// the optional generic-RFC name key, and the optional period window. Driven off
/// `pulso.normalization_rules` (an owner's individual rules number in the tens at most)
/// joined to `pulso.cfdis` by UUID (primary-key lookup) -- cost scales with how many
/// individual rules this owner has, never with how many comprobantes the counterparty
/// relationship has (the 500-row cap L5-15's client-side version was bound by, and which
/// a 4,599-comprobante relationship already exceeds 9x over). No comprobante rows
/// travel over the wire, no page limit, and no 2023-or-later floor.
pub async fn list_individual_rule_ids_for_counterparty(
    pool: &DbPool,
    owner_rfc: &str,
    source_rfc: &str,
    dl_type: &str,
    source_name_key: Option<&str>,
    period_start: Option<&str>,
    period_end: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query(
        r#"SELECT nr.id
           FROM pulso.normalization_rules nr
           JOIN pulso.cfdis c ON UPPER(nr.cfdi_uuid) = UPPER(c.uuid)
           WHERE nr.owner_rfc = $1
             AND nr.cfdi_uuid IS NOT NULL
             AND nr.action = 'exclude'
             AND (
                   ($3 IN ('emitidos', 'ambos') AND c.rfc_emisor = $1 AND c.rfc_receptor = $2
                    AND ($4::text IS NULL OR $4::text =
                         REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_receptor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g')))
                OR ($3 IN ('recibidos', 'ambos') AND c.rfc_receptor = $1 AND c.rfc_emisor = $2
                    AND ($4::text IS NULL OR $4::text =
                         REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_emisor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g')))
             )
             AND ($5::text IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= $5)
             AND ($6::text IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= $6)"#,
    )
    .bind(owner_rfc)
    .bind(source_rfc)
    .bind(dl_type)
    .bind(source_name_key)
    .bind(period_start)
    .bind(period_end)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| r.try_get::<String, _>("id").unwrap_or_default())
        .collect())
}

// ---------------------------------------------------------------------------
// GET /normalization/payroll/employees
// ---------------------------------------------------------------------------

// L5-02: this is the employee catalog for the payroll rule-creation selector and its
// "periodo real del empleado" -- it used to filter `NOT is_excluded` (so excluding an
// employee made them vanish from the only screen that could undo it) and was windowed by
// the caller's dashboard date range (so a 3-year employee could offer a 1-month period if
// that's what the dashboard happened to be showing). Neither applies here anymore: no
// exclusion filter (is_excluded is returned instead, per row), no date-range parameters
// (first_month/last_month is always this employee's REAL full history), and grouped by
// devengo (year_devengo/month_devengo, L5-04) rather than emisión.
pub async fn list_payroll_employees(
    pool: &DbPool,
    owner_rfc: &str,
) -> anyhow::Result<Vec<PayrollEmployeeRow>> {
    let rows = sqlx::query(
        r#"
        WITH monthly AS (
            SELECT
                n.rfc_receptor                                              AS employee_rfc,
                MAX(n.nombre_receptor)                                      AS employee_name,
                n.year_devengo::text || '-' || LPAD(n.month_devengo::text, 2, '0') AS month_key,
                SUM(n.total_percepciones)                                   AS month_total,
                BOOL_OR(n.is_excluded)                                      AS month_excluded
            FROM pulso.nomina_normalizada n
            WHERE n.rfc_emisor = $1
              AND n.rfc_receptor IS NOT NULL
              AND n.rfc_receptor != ''
            GROUP BY n.rfc_receptor, month_key
        )
        SELECT
            employee_rfc,
            MAX(employee_name)                      AS employee_name,
            MIN(month_key)                          AS first_month,
            MAX(month_key)                          AS last_month,
            COUNT(DISTINCT month_key)               AS active_months,
            SUM(month_total)                        AS historical_cost_mxn,
            AVG(month_total)                        AS run_rate_mensual_mxn,
            COUNT(*)                                AS cfdi_count,
            BOOL_OR(month_excluded)                 AS is_excluded
        FROM monthly
        GROUP BY employee_rfc
        ORDER BY historical_cost_mxn DESC NULLS LAST
        "#,
    )
    .bind(owner_rfc)
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
            historical_cost_mxn: get_f64(r, "historical_cost_mxn"),
            run_rate_mensual_mxn: get_f64(r, "run_rate_mensual_mxn"),
            cfdi_count: r.try_get::<i64, _>("cfdi_count").unwrap_or(0),
            is_excluded: r.try_get::<bool, _>("is_excluded").unwrap_or(false),
        })
        .collect())
}

// ---------------------------------------------------------------------------
// GET /normalization/payroll/employees/{employee_rfc}/receipts
// ---------------------------------------------------------------------------

// L3-15 familia 4 ("excluir CFDIs específicos"): routed to the mechanism that already
// exists for every other comprobante -- a cfdi_uuid-level pulso.normalization_rules
// exclude rule, created the same way the counterparty drill-down creates one (see
// routes/analytics.rs's create_normalization). This listing exists only so the Nómina
// module has individual receipt UUIDs to point that existing mechanism at; it does not
// read or write pulso.payroll_normalization_rules.excluded_cfdi_uuids, which stays unused.
#[derive(Debug, Serialize)]
pub struct NomReceiptRow {
    pub uuid: String,
    pub period: String,
    pub fecha_emision: Option<String>,
    pub total_percepciones: f64,
    pub is_excluded: bool,
}

pub async fn list_nomina_receipts_for_employee(
    pool: &DbPool,
    owner_rfc: &str,
    employee_rfc: &str,
) -> anyhow::Result<Vec<NomReceiptRow>> {
    let rows = sqlx::query(
        r#"SELECT uuid,
                  year::text || '-' || LPAD(month::text, 2, '0') AS period,
                  fecha_emision::text AS fecha_emision,
                  total_percepciones,
                  is_excluded
           FROM pulso.nomina_normalizada
           WHERE rfc_emisor = $1 AND rfc_receptor = $2
           ORDER BY fecha_emision DESC"#,
    )
    .bind(owner_rfc)
    .bind(employee_rfc)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| NomReceiptRow {
            uuid: r.try_get("uuid").unwrap_or_default(),
            period: r.try_get("period").unwrap_or_default(),
            fecha_emision: r.try_get("fecha_emision").ok(),
            total_percepciones: get_f64(r, "total_percepciones"),
            is_excluded: r.try_get::<bool, _>("is_excluded").unwrap_or(false),
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
    // L3-04/L3-05/L3-18: reads the shared exclusion base (owner- and action-scoped,
    // case-insensitive UUID match, L3-02's generic-RFC name key -- this used to be a
    // hand-rolled join with none of that, which is how it leaked Adquiere Latam's own
    // payroll into a rule about receiving invoices from Adquiere Latam) and sums the
    // net-of-IVA measure (total_neto_mxn_ajustado, same as the resumen) instead of con-IVA.
    // The sign is L3-18: a comprobante on the owner's emisor side keeps its contribution
    // to net income and the adjustment is its negative (excluding income lowers EBITDA);
    // on the receptor side the adjustment is the raw value (excluding an expense raises
    // EBITDA) -- resolved per comprobante, not per rule, so an 'ambos' rule signs its two
    // sides independently.
    let rows = sqlx::query(
        r#"
        SELECT
            nr.id, nr.rule_name, nr.accounting_line, nr.motivo, nr.impacts_ebitda,
            nr.dl_type, nr.capex_estimate_dep, nr.capex_asset_type,
            -- L5-01: these are NUMERIC; without the cast, decoding as f64 silently failed
            -- and .ok() turned a captured capex value into a null on the bridge.
            nr.capex_useful_life_years::float8 AS capex_useful_life_years,
            nr.capex_annual_dep_mxn::float8 AS capex_annual_dep_mxn,
            c.year,
            SUM(
                CASE WHEN c.rfc_emisor = nr.owner_rfc THEN -COALESCE(c.total_neto_mxn_ajustado, 0)
                     ELSE COALESCE(c.total_neto_mxn_ajustado, 0) END
            )::float8 AS year_total
        FROM pulso.cfdi_exclusion ex
        JOIN pulso.normalization_rules nr ON nr.id = ex.rule_id
        JOIN pulso.cfdis_ajustado c ON c.uuid = ex.uuid
        WHERE ex.owner_rfc = $1
          -- L3-13: a rule without an accounting_line still surfaces here (grouped under
          -- "Sin clasificar" by the frontend) instead of silently vanishing from the bridge.
          AND c.tipo_comprobante IN ('I', 'E')
          AND NOT c.is_cancelled
          AND (c.year > $2 OR (c.year = $2 AND c.month >= $3))
          AND (c.year < $4 OR (c.year = $4 AND c.month <= $5))
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

    // L4-02: nómina is always a cost from the owner's perspective -- unlike a comprobante-
    // level rule (which can sit on either the income or the expense side depending on
    // dl_type), there's no "which side" ambiguity, so every nómina-sourced row below enters
    // straight positive when it REDUCES cost (raises EBITDA) and negative when it increases
    // cost. Three sources, each a separate query because each attributes to a different
    // rule table / join shape; merged into the same `map` as the comprobante-level rows
    // above via `merge_bridge_row`, keyed by rule id exactly like they are.

    // Source 1: employee-level exclusion (`exclude_employee`, via nomina_normalizada's
    // employee_rule_id). The excluded receipt's cost disappears from the P&L entirely, so
    // it's added back positive. Uses the RAW (pre-factor) cost from cfdi_nomina, not
    // nomina_normalizada's already-factored total_percepciones -- an excluded receipt's
    // factor is irrelevant, its whole cost comes back. L5-03: nómina cost is
    // total_percepciones alone -- deducciones are withheld on the employee's behalf, still
    // the employer's cost, not a discount to it; otros_pagos is immaterial and mostly the
    // generic SAT bucket, not an employer cost.
    // L6-08: windowed and grouped by devengo (nn.year_devengo/nn.month_devengo), matching
    // payroll.rs's by_month/by_year -- grouping by emision while payroll.rs groups by
    // devengo put a receipt whose emision and devengo straddle a year boundary in a
    // different year bucket on the bridge than in the P&L for that same receipt.
    let employee_excl_rows = sqlx::query(
        r#"
        SELECT
            pr.id, pr.rule_name, pr.accounting_line, pr.motivo,
            nn.year_devengo AS year,
            SUM(cn.total_percepciones::float8) AS year_total
        FROM pulso.nomina_normalizada nn
        JOIN pulso.payroll_normalization_rules pr ON pr.id = nn.employee_rule_id
        JOIN pulso.cfdi_nomina cn ON cn.uuid = nn.uuid
        WHERE nn.rfc_emisor = $1
          AND (nn.year_devengo > $2 OR (nn.year_devengo = $2 AND nn.month_devengo >= $3))
          AND (nn.year_devengo < $4 OR (nn.year_devengo = $4 AND nn.month_devengo <= $5))
        GROUP BY pr.id, pr.rule_name, pr.accounting_line, pr.motivo, nn.year_devengo
        "#,
    )
    .bind(owner_rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Source 2: a comprobante-level rule (pulso.normalization_rules, via cfdi_exclusion)
    // that happens to target a tipo_comprobante='N' receipt -- reachable via NominaView's
    // "Excluir un recibo específico". These already carry accounting_line (that form
    // requires it) but are invisible to the query above because it filters
    // tipo_comprobante IN ('I','E'). Same raw-cost measure as source 1 (total_neto_mxn_
    // ajustado is meaningless for a payroll complement), no sign flip (nómina has no
    // "side"). L5-03: total_percepciones only, same reasoning as source 1.
    // L5-06: a receipt covered by BOTH an employee-level rule and a comprobante-level rule
    // is excluded via nomina_normalizada.employee_rule_id already (source 1) -- joining
    // back to the view and requiring employee_rule_id IS NULL keeps this source from
    // double-counting it.
    // L6-08: windowed and grouped by devengo (nn.year_devengo/nn.month_devengo) rather than
    // c.year/c.month (emision) -- same fix as source 1, for the same reason: this source
    // already joins nn (added in L5-06 for the employee-rule-wins fix above), so the devengo
    // columns are available directly without an extra join.
    let receipt_excl_rows = sqlx::query(
        r#"
        SELECT
            nr.id, nr.rule_name, nr.accounting_line, nr.motivo, nr.dl_type,
            nr.impacts_ebitda, nr.capex_estimate_dep, nr.capex_asset_type,
            -- L5-01: see the analogous cast on the source-1 query above.
            nr.capex_useful_life_years::float8 AS capex_useful_life_years,
            nr.capex_annual_dep_mxn::float8 AS capex_annual_dep_mxn,
            nn.year_devengo AS year,
            SUM(n.total_percepciones::float8) AS year_total
        FROM pulso.cfdi_exclusion ex
        JOIN pulso.normalization_rules nr ON nr.id = ex.rule_id
        JOIN pulso.cfdis c ON c.uuid = ex.uuid
        JOIN pulso.cfdi_nomina n ON n.uuid = ex.uuid
        JOIN pulso.nomina_normalizada nn ON nn.uuid = ex.uuid AND nn.rfc_emisor = ex.owner_rfc
        WHERE ex.owner_rfc = $1
          AND c.tipo_comprobante = 'N'
          AND NOT c.is_cancelled
          AND nn.employee_rule_id IS NULL
          AND (nn.year_devengo > $2 OR (nn.year_devengo = $2 AND nn.month_devengo >= $3))
          AND (nn.year_devengo < $4 OR (nn.year_devengo = $4 AND nn.month_devengo <= $5))
        GROUP BY nr.id, nr.rule_name, nr.accounting_line, nr.motivo, nr.dl_type,
                 nr.impacts_ebitda, nr.capex_estimate_dep, nr.capex_asset_type,
                 nr.capex_useful_life_years, nr.capex_annual_dep_mxn, nn.year_devengo
        "#,
    )
    .bind(owner_rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Source 3: the scale/adjust factor difference. `costo real - costo normalizado` per
    // receipt (cfdi_nomina's raw total_percepciones minus nomina_normalizada's already-
    // factored total_percepciones) sums linearly to the same per-employee-month difference
    // the `factors` CTE computes, because `factor` is constant across every receipt of that
    // employee-month -- no separate month-level aggregation needed here. Attributed to
    // whichever rule produced the factor via migration 058's factor_rule_id. Signed
    // correctly by construction: factor < 1 (scale down, or adjust-to-amount below real)
    // makes this positive (cost went down, EBITDA up); factor > 1 makes it negative.
    // L5-05: an employee who is both excluded and has a scale/adjust rule must not also
    // contribute a factor difference here -- their cost is already fully out of the P&L
    // via the exclusion (source 1), so this would be a phantom adjustment on top of it.
    // L6-08: windowed and grouped by devengo, same fix and same reason as source 1.
    let factor_diff_rows = sqlx::query(
        r#"
        SELECT
            pr.id, pr.rule_name, pr.accounting_line, pr.motivo,
            nn.year_devengo AS year,
            SUM(cn.total_percepciones::float8 - nn.total_percepciones)::float8 AS year_total
        FROM pulso.nomina_normalizada nn
        JOIN pulso.payroll_normalization_rules pr ON pr.id = nn.factor_rule_id
        JOIN pulso.cfdi_nomina cn ON cn.uuid = nn.uuid
        WHERE nn.rfc_emisor = $1
          AND NOT nn.is_excluded
          AND (nn.year_devengo > $2 OR (nn.year_devengo = $2 AND nn.month_devengo >= $3))
          AND (nn.year_devengo < $4 OR (nn.year_devengo = $4 AND nn.month_devengo <= $5))
        GROUP BY pr.id, pr.rule_name, pr.accounting_line, pr.motivo, nn.year_devengo
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

    // dl_type has no natural value for a nómina-sourced row (nómina is always a cost, never
    // "emitidos"/"recibidos"/"ambos") -- "nomina" is a fixed sentinel the frontend renders
    // as a small mono tag under the rule name, same spot dl_type shows for comprobante rows.
    for row in &rows {
        merge_bridge_row(&mut map, row, "id", None);
    }
    for row in &employee_excl_rows {
        merge_bridge_row(&mut map, row, "id", Some("nomina"));
    }
    for row in &receipt_excl_rows {
        merge_bridge_row(&mut map, row, "id", None);
    }
    for row in &factor_diff_rows {
        merge_bridge_row(&mut map, row, "id", Some("nomina"));
    }

    let mut result: Vec<EbitdaBridgeAdjustment> = map.into_values().collect();
    result.sort_by(|a, b| a.rule_id.cmp(&b.rule_id));
    Ok(result)
}

/// Merges one grouped (rule, year) row into the bridge's accumulator map. `dl_type_default`
/// is used when the row's own query has no dl_type column (the three nómina sources above);
/// pass None to read it from the row instead (the comprobante-level query). Accumulates
/// rather than overwrites amounts_by_year/total_mxn so a rule id that legitimately appears
/// in more than one source query (e.g. a counterparty rule broad enough to also match a
/// tipo_comprobante='N' receipt) doesn't lose one source's contribution to the other.
fn merge_bridge_row(
    map: &mut std::collections::HashMap<String, EbitdaBridgeAdjustment>,
    row: &sqlx::postgres::PgRow,
    id_col: &str,
    dl_type_default: Option<&str>,
) {
    let rule_id: String = row.try_get(id_col).unwrap_or_default();
    let year: i64 = row.try_get("year").unwrap_or(0);
    let year_total: f64 = get_f64(row, "year_total");

    let entry = map
        .entry(rule_id.clone())
        .or_insert_with(|| EbitdaBridgeAdjustment {
            rule_id: rule_id.clone(),
            rule_name: row.try_get("rule_name").ok(),
            accounting_line: row.try_get("accounting_line").ok(),
            motivo: row.try_get("motivo").ok(),
            impacts_ebitda: row.try_get("impacts_ebitda").ok(),
            dl_type: dl_type_default
                .map(|d| d.to_string())
                .or_else(|| row.try_get("dl_type").ok())
                .unwrap_or_default(),
            capex_estimate_dep: row.try_get("capex_estimate_dep").ok(),
            capex_asset_type: row.try_get("capex_asset_type").ok(),
            // Not L5-01's get_f64_opt here on purpose: two of this fn's four callers
            // (the nómina-sourced queries) never select these columns at all -- that's an
            // expected "column absent by query shape", not a decode failure, and logging a
            // warning on every such row would be noise, not signal. The two callers that DO
            // select these columns now cast them (`::float8`) at the SQL level instead.
            capex_useful_life_years: row.try_get("capex_useful_life_years").ok(),
            capex_annual_dep_mxn: row.try_get("capex_annual_dep_mxn").ok(),
            amounts_by_year: std::collections::HashMap::new(),
            total_mxn: 0.0,
        });

    *entry.amounts_by_year.entry(year.to_string()).or_insert(0.0) += year_total;
    entry.total_mxn += year_total;
}

#[cfg(test)]
mod payroll_lock_tests {
    use super::*;

    #[test]
    fn periods_overlap_detects_overlap() {
        assert!(periods_overlap(
            Some("2026-01"),
            Some("2026-06"),
            Some("2026-06"),
            Some("2026-12")
        ));
    }

    #[test]
    fn periods_overlap_rejects_adjacent_non_overlap() {
        // C3's spirit: genuinely non-overlapping periods must never be flagged.
        assert!(!periods_overlap(
            Some("2026-01"),
            Some("2026-03"),
            Some("2026-04"),
            Some("2026-06")
        ));
    }

    #[test]
    fn periods_overlap_unbounded_end_always_overlaps_later_start() {
        assert!(periods_overlap(
            Some("2026-01"),
            None,
            Some("2030-01"),
            Some("2030-06")
        ));
    }

    #[test]
    fn periods_overlap_both_fully_unbounded() {
        assert!(periods_overlap(None, None, None, None));
    }

    #[test]
    fn is_active_exclusion_requires_both_family_and_action() {
        assert!(is_active_exclusion("exclude_employee", "exclude"));
        assert!(is_active_exclusion("exclusion", "exclude"));
        assert!(!is_active_exclusion("exclude_employee", "normalize"));
        assert!(!is_active_exclusion("scale_employee_pct", "exclude"));
    }

    #[test]
    fn is_dimensioning_family_covers_scale_and_adjust_only() {
        assert!(is_dimensioning_family("scale_employee_pct"));
        assert!(is_dimensioning_family("adjust_to_amount_mxn"));
        assert!(!is_dimensioning_family("exclude_employee"));
        // L5-17's future concepts family must never become dimensioning (C2 note).
        assert!(!is_dimensioning_family("nonrecurring_concept"));
    }

    #[test]
    fn payroll_motivos_has_exactly_the_seven_dec_033_values() {
        assert_eq!(PAYROLL_MOTIVOS.len(), 7);
        assert!(PAYROLL_MOTIVOS.contains(&"Otro"));
    }
}
