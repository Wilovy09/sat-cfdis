use super::summary::{dl_type_filter, parse_ym, rfc_column};
use crate::db::DbPool;
use serde::Serialize;
use sqlx::Row;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub struct CounterpartiesResponse {
    pub top: Vec<CounterpartyRow>,
    pub total_counterparties: i64,
    pub hhi: f64,       // Herfindahl-Hirschman Index (concentration)
    pub top10_pct: f64, // % of total from top 10
}

#[derive(Debug, Serialize)]
pub struct CounterpartyRow {
    pub rfc: String,
    pub nombre: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub avg_invoice_mxn: f64,
    pub first_invoice: String,
    pub last_invoice: String,
    pub pct_of_total: f64,
    pub months_active: i64,
}

pub async fn get(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
    limit: i64,
) -> anyhow::Result<CounterpartiesResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_col = if dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };
    let cp_name_col = if dl_type == "recibidos" {
        "nombre_emisor"
    } else {
        "nombre_receptor"
    };

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            {cp_col}                                                AS cp_rfc,
            MAX({cp_name_col})                                      AS cp_nombre,
            SUM(COALESCE(total_mxn,0)::float8)::float8                             AS total,
            COUNT(*)                                               AS cnt,
            MIN(fecha_emision)                                     AS first_inv,
            MAX(fecha_emision)                                     AS last_inv,
            COUNT(DISTINCT year * 100 + month)                     AS months_active
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY {cp_col}
        ORDER BY total DESC
        LIMIT $6
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    // Total for percentage calc
    let total_row = sqlx::query(&format!(
        r#"
        SELECT SUM(COALESCE(total_mxn,0)::float8)::float8 AS total, COUNT(DISTINCT {cp_col}) AS cp_count
        FROM pulso.cfdis
        WHERE {owner_col} = $1
          AND {dl_filter}
          AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_one(pool)
    .await?;

    let grand_total: f64 = total_row.try_get("total").unwrap_or(0.0);
    let cp_count: i64 = total_row.try_get("cp_count").unwrap_or(0);

    let top: Vec<CounterpartyRow> = rows
        .iter()
        .map(|r| {
            let total: f64 = r.try_get("total").unwrap_or(0.0);
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            CounterpartyRow {
                rfc: r.try_get("cp_rfc").unwrap_or_default(),
                nombre: r.try_get("cp_nombre").unwrap_or_default(),
                total_mxn: total,
                invoice_count: cnt,
                avg_invoice_mxn: if cnt > 0 { total / cnt as f64 } else { 0.0 },
                first_invoice: r.try_get("first_inv").unwrap_or_default(),
                last_invoice: r.try_get("last_inv").unwrap_or_default(),
                pct_of_total: if grand_total > 0.0 {
                    total / grand_total * 100.0
                } else {
                    0.0
                },
                months_active: r.try_get("months_active").unwrap_or(0),
            }
        })
        .collect();

    // HHI: sum of (share_i)^2 — use top results as approximation
    let hhi: f64 = top
        .iter()
        .map(|r| (r.pct_of_total / 100.0).powi(2))
        .sum::<f64>()
        * 10_000.0;

    let top10_total: f64 = top.iter().take(10).map(|r| r.total_mxn).sum();
    let top10_pct = if grand_total > 0.0 {
        top10_total / grand_total * 100.0
    } else {
        0.0
    };

    Ok(CounterpartiesResponse {
        top,
        total_counterparties: cp_count,
        hhi,
        top10_pct,
    })
}

// ---------------------------------------------------------------------------
// Evolution
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct EvolutionResponse {
    pub rows: Vec<CpEvolutionRow>,
    pub years: Vec<i32>,
}

#[derive(Debug, Serialize)]
pub struct CpEvolutionRow {
    pub rfc: String,
    pub nombre: String,
    pub years: HashMap<String, f64>,
    pub total_acumulado: f64,
    pub cagr_pct: Option<f64>,
    pub tendencia: String,
}

pub async fn get_evolution(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<EvolutionResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_col = if dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };
    let cp_name_col = if dl_type == "recibidos" {
        "nombre_emisor"
    } else {
        "nombre_receptor"
    };

    let rows = sqlx::query(&format!(
        r#"
        SELECT {cp_col} AS cp_rfc, MAX({cp_name_col}) AS cp_nombre, year,
               SUM(COALESCE(total_mxn,0)::float8)::float8 AS yr_total
        FROM pulso.cfdis
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY {cp_col}, year
        ORDER BY {cp_col}, year
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Group by cp_rfc
    let mut cp_map: HashMap<String, (String, HashMap<i32, f64>)> = HashMap::new();
    let mut all_years: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();

    for row in &rows {
        let cp_rfc: String = row.try_get("cp_rfc").unwrap_or_default();
        let cp_nombre: String = row.try_get("cp_nombre").unwrap_or_default();
        let year: i32 = row.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let yr_total: f64 = row.try_get("yr_total").unwrap_or(0.0);

        all_years.insert(year);
        let entry = cp_map
            .entry(cp_rfc.clone())
            .or_insert_with(|| (cp_nombre.clone(), HashMap::new()));
        entry.0 = cp_nombre;
        entry.1.insert(year, yr_total);
    }

    let years_sorted: Vec<i32> = all_years.into_iter().collect();

    // Build rows
    let mut evolution_rows: Vec<CpEvolutionRow> = cp_map
        .into_iter()
        .map(|(cp_rfc, (cp_nombre, year_map))| {
            let total_acumulado: f64 = year_map.values().sum();

            // Sorted years with non-zero values
            let mut nonzero_years: Vec<(i32, f64)> = year_map
                .iter()
                .filter(|&(_, &v)| v > 0.0)
                .map(|(&y, &v)| (y, v))
                .collect();
            nonzero_years.sort_by_key(|(y, _)| *y);

            let cagr_pct = if nonzero_years.len() >= 2 {
                let first_val = nonzero_years.first().unwrap().1;
                let last_val = nonzero_years.last().unwrap().1;
                let n_years =
                    (nonzero_years.last().unwrap().0 - nonzero_years.first().unwrap().0) as f64;
                if first_val > 0.0 && n_years > 0.0 {
                    Some(((last_val / first_val).powf(1.0 / n_years) - 1.0) * 100.0)
                } else {
                    None
                }
            } else {
                None
            };

            let tendencia = if nonzero_years.len() <= 1 {
                "Nuevo".to_string()
            } else {
                let first_val = nonzero_years.first().unwrap().1;
                let last_val = nonzero_years.last().unwrap().1;
                if last_val > first_val {
                    "↑ Crecimiento".to_string()
                } else if last_val < first_val * 0.5 {
                    "↓ En declive".to_string()
                } else if last_val < first_val * 0.95 {
                    "↓ Deterioro".to_string()
                } else {
                    "Estable".to_string()
                }
            };

            let years_str: HashMap<String, f64> = year_map
                .into_iter()
                .map(|(y, v)| (y.to_string(), v))
                .collect();

            CpEvolutionRow {
                rfc: cp_rfc,
                nombre: cp_nombre,
                years: years_str,
                total_acumulado,
                cagr_pct,
                tendencia,
            }
        })
        .collect();

    evolution_rows.sort_by(|a, b| {
        b.total_acumulado
            .partial_cmp(&a.total_acumulado)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    evolution_rows.truncate(20);

    Ok(EvolutionResponse {
        rows: evolution_rows,
        years: years_sorted,
    })
}

// ---------------------------------------------------------------------------
// LTM Comparison
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct LtmComparisonResponse {
    pub rows: Vec<LtmRow>,
    pub ltm_total: f64,
    pub ltm_prev_total: f64,
}

#[derive(Debug, Serialize)]
pub struct LtmRow {
    pub rfc: String,
    pub nombre: String,
    pub ltm_mxn: f64,
    pub prev_ltm_mxn: f64,
    pub delta_mxn: f64,
    pub delta_pct: Option<f64>,
    pub share_ltm_pct: f64,
    pub months_active: i64,
    pub invoice_count: i64,
    pub status: String,
}

pub async fn get_ltm_comparison(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    to: &str,
) -> anyhow::Result<LtmComparisonResponse> {
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_col = if dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };
    let cp_name_col = if dl_type == "recibidos" {
        "nombre_emisor"
    } else {
        "nombre_receptor"
    };

    // Compute LTM window: [to_y/to_m - 11 months ... to_y/to_m]
    let ltm_end_y = to_y;
    let ltm_end_m = to_m;
    let ltm_start_total_months = (to_y * 12 + to_m - 1) - 11;
    let ltm_start_y = ltm_start_total_months / 12;
    let ltm_start_m = ltm_start_total_months % 12 + 1;

    // PrevLTM window: [to_y/to_m - 23 months ... to_y/to_m - 12 months]
    let prev_end_total_months = ltm_start_total_months - 1;
    let prev_end_y = prev_end_total_months / 12;
    let prev_end_m = prev_end_total_months % 12 + 1;
    let prev_start_total_months = prev_end_total_months - 11;
    let prev_start_y = prev_start_total_months / 12;
    let prev_start_m = prev_start_total_months % 12 + 1;

    let ltm_rows = sqlx::query(&format!(
        r#"
        SELECT {cp_col} AS cp_rfc,
               MAX({cp_name_col}) AS cp_nombre,
               SUM(COALESCE(total_mxn,0)::float8)::float8 AS ltm_total,
               COUNT(DISTINCT year * 100 + month) AS months_active,
               COUNT(*) AS invoice_count
        FROM pulso.cfdis
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY {cp_col}
        "#
    ))
    .bind(rfc)
    .bind(ltm_start_y)
    .bind(ltm_start_m)
    .bind(ltm_end_y)
    .bind(ltm_end_m)
    .fetch_all(pool)
    .await?;

    let prev_rows = sqlx::query(&format!(
        r#"
        SELECT {cp_col} AS cp_rfc,
               SUM(COALESCE(total_mxn,0)::float8)::float8 AS prev_total
        FROM pulso.cfdis
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY {cp_col}
        "#
    ))
    .bind(rfc)
    .bind(prev_start_y)
    .bind(prev_start_m)
    .bind(prev_end_y)
    .bind(prev_end_m)
    .fetch_all(pool)
    .await?;

    let mut prev_map: HashMap<String, f64> = HashMap::new();
    for row in &prev_rows {
        let cp_rfc: String = row.try_get("cp_rfc").unwrap_or_default();
        let prev_total: f64 = row.try_get("prev_total").unwrap_or(0.0);
        prev_map.insert(cp_rfc, prev_total);
    }

    let ltm_grand_total: f64 = ltm_rows
        .iter()
        .map(|r| r.try_get::<f64, _>("ltm_total").unwrap_or(0.0))
        .sum();
    let prev_grand_total: f64 = prev_map.values().sum();

    let mut rows: Vec<LtmRow> = ltm_rows
        .iter()
        .map(|r| {
            let cp_rfc: String = r.try_get("cp_rfc").unwrap_or_default();
            let cp_nombre: String = r.try_get("cp_nombre").unwrap_or_default();
            let ltm_mxn: f64 = r.try_get("ltm_total").unwrap_or(0.0);
            let prev_ltm_mxn: f64 = *prev_map.get(&cp_rfc).unwrap_or(&0.0);
            let delta_mxn = ltm_mxn - prev_ltm_mxn;
            let delta_pct = if prev_ltm_mxn > 0.0 {
                Some(delta_mxn / prev_ltm_mxn * 100.0)
            } else {
                None
            };
            let share_ltm_pct = if ltm_grand_total > 0.0 {
                ltm_mxn / ltm_grand_total * 100.0
            } else {
                0.0
            };
            let months_active: i64 = r.try_get("months_active").unwrap_or(0);
            let invoice_count: i64 = r.try_get("invoice_count").unwrap_or(0);
            let status = if prev_ltm_mxn == 0.0 {
                "Nueva en LTM".to_string()
            } else {
                "Retenida".to_string()
            };

            LtmRow {
                rfc: cp_rfc,
                nombre: cp_nombre,
                ltm_mxn,
                prev_ltm_mxn,
                delta_mxn,
                delta_pct,
                share_ltm_pct,
                months_active,
                invoice_count,
                status,
            }
        })
        .collect();

    rows.sort_by(|a, b| {
        b.ltm_mxn
            .partial_cmp(&a.ltm_mxn)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows.truncate(20);

    Ok(LtmComparisonResponse {
        rows,
        ltm_total: ltm_grand_total,
        ltm_prev_total: prev_grand_total,
    })
}

// ---------------------------------------------------------------------------
// Payments Detail
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct PaymentsDetailResponse {
    pub rows: Vec<CpPaymentRow>,
}

#[derive(Debug, Serialize)]
pub struct CpPaymentRow {
    pub rfc: String,
    pub nombre: String,
    pub facturado_mxn: f64,
    pub cobrado_mxn: f64,
    pub saldo_pendiente_mxn: f64,
    pub pct_cobrado: f64,
    pub facturas_ppd: i64,
    pub facturas_abiertas: i64,
    pub dias_cobro_ppd: f64,
    pub monto_riesgo_180d: f64,
}

pub async fn get_payments_detail(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<PaymentsDetailResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_col = if dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };
    let cp_name_col = if dl_type == "recibidos" {
        "nombre_emisor"
    } else {
        "nombre_receptor"
    };

    let rows = sqlx::query(&format!(
        r#"
        SELECT inv.{cp_col} AS cp_rfc,
               MAX(inv.{cp_name_col}) AS cp_nombre,
               SUM(COALESCE(inv.total_mxn,0)::float8)::float8 AS facturado,
               COALESCE(SUM(pd.imp_pagado)::float8, 0) AS cobrado,
               COUNT(DISTINCT inv.uuid) AS facturas_ppd,
               COUNT(DISTINCT CASE WHEN (COALESCE(inv.total_mxn,0) - COALESCE(paid_per_inv.paid,0)) > 1.0 THEN inv.uuid END) AS facturas_abiertas,
               COALESCE(AVG(CASE WHEN cp.fecha_pago IS NOT NULL THEN (cp.fecha_pago::date - inv.fecha_emision::date)::float8 END), 0) AS dias_cobro,
               COALESCE(SUM(CASE WHEN (COALESCE(inv.total_mxn,0) - COALESCE(paid_per_inv.paid,0)) > 1.0
                                  AND inv.fecha_emision::date < CURRENT_DATE - INTERVAL '180 days'
                             THEN (COALESCE(inv.total_mxn,0) - COALESCE(paid_per_inv.paid,0)) END)::float8, 0) AS monto_riesgo
        FROM pulso.cfdis inv
        LEFT JOIN pulso.cfdi_payment_docs pd ON pd.invoice_uuid = inv.uuid
        LEFT JOIN pulso.cfdi_payments cp ON cp.payment_uuid = pd.payment_uuid AND cp.pago_num = pd.pago_num
        LEFT JOIN (
            SELECT invoice_uuid, SUM(imp_pagado)::float8 AS paid FROM pulso.cfdi_payment_docs GROUP BY invoice_uuid
        ) paid_per_inv ON paid_per_inv.invoice_uuid = inv.uuid
        WHERE inv.{owner_col} = $1 AND inv.{dl_filter}
          AND inv.tipo_comprobante = 'I' AND inv.metodo_pago = 'PPD'
          AND (inv.year > $2 OR (inv.year = $2 AND inv.month >= $3))
          AND (inv.year < $4 OR (inv.year = $4 AND inv.month <= $5))
        GROUP BY inv.{cp_col}
        ORDER BY facturado DESC
        LIMIT 50
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let payment_rows: Vec<CpPaymentRow> = rows
        .iter()
        .map(|r| {
            let facturado: f64 = r.try_get("facturado").unwrap_or(0.0);
            let cobrado: f64 = r.try_get("cobrado").unwrap_or(0.0);
            let saldo_pendiente = facturado - cobrado;
            let pct_cobrado = if facturado > 0.0 {
                cobrado / facturado * 100.0
            } else {
                0.0
            };
            CpPaymentRow {
                rfc: r.try_get("cp_rfc").unwrap_or_default(),
                nombre: r.try_get("cp_nombre").unwrap_or_default(),
                facturado_mxn: facturado,
                cobrado_mxn: cobrado,
                saldo_pendiente_mxn: saldo_pendiente,
                pct_cobrado,
                facturas_ppd: r.try_get("facturas_ppd").unwrap_or(0),
                facturas_abiertas: r.try_get("facturas_abiertas").unwrap_or(0),
                dias_cobro_ppd: r.try_get("dias_cobro").unwrap_or(0.0),
                monto_riesgo_180d: r.try_get("monto_riesgo").unwrap_or(0.0),
            }
        })
        .collect();

    Ok(PaymentsDetailResponse { rows: payment_rows })
}

// ---------------------------------------------------------------------------
// Atypical
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct AtypicalResponse {
    pub rows: Vec<AtypicalRow>,
}

#[derive(Debug, Serialize)]
pub struct AtypicalRow {
    pub rfc: String,
    pub nombre: String,
    pub period: String,
    pub total_mxn: f64,
    pub median_mxn: f64,
    pub multiple: f64,
    pub pct_of_cp_total: f64,
}

pub async fn get_atypical(
    pool: &DbPool,
    rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<AtypicalResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_col = if dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };
    let cp_name_col = if dl_type == "recibidos" {
        "nombre_emisor"
    } else {
        "nombre_receptor"
    };

    let rows = sqlx::query(&format!(
        r#"
        WITH monthly AS (
            SELECT {cp_col} AS cp_rfc, MAX({cp_name_col}) AS cp_nombre,
                   year, month,
                   year::text || '-' || LPAD(month::text, 2, '0') AS period,
                   SUM(COALESCE(total_mxn,0)::float8)::float8 AS mo_total
            FROM pulso.cfdis
            WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
              AND (year > $2 OR (year = $2 AND month >= $3))
              AND (year < $4 OR (year = $4 AND month <= $5))
            GROUP BY {cp_col}, year, month
        ),
        stats AS (
            SELECT cp_rfc,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY mo_total) AS median_amt,
                   SUM(mo_total) AS cp_total
            FROM monthly
            GROUP BY cp_rfc
            HAVING COUNT(*) >= 3
        )
        SELECT m.cp_rfc, m.cp_nombre, m.period, m.mo_total,
               s.median_amt, s.cp_total,
               m.mo_total / NULLIF(s.median_amt, 0) AS multiple
        FROM monthly m
        JOIN stats s ON s.cp_rfc = m.cp_rfc
        WHERE s.median_amt > 0 AND m.mo_total > s.median_amt * 2.5
          AND m.mo_total / NULLIF(s.cp_total, 0) >= 0.03
        ORDER BY multiple DESC
        LIMIT 20
        "#
    ))
    .bind(rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let atypical_rows: Vec<AtypicalRow> = rows
        .iter()
        .map(|r| {
            let mo_total: f64 = r.try_get("mo_total").unwrap_or(0.0);
            let cp_total: f64 = r.try_get("cp_total").unwrap_or(0.0);
            let pct_of_cp_total = if cp_total > 0.0 {
                mo_total / cp_total * 100.0
            } else {
                0.0
            };
            AtypicalRow {
                rfc: r.try_get("cp_rfc").unwrap_or_default(),
                nombre: r.try_get("cp_nombre").unwrap_or_default(),
                period: r.try_get("period").unwrap_or_default(),
                total_mxn: mo_total,
                median_mxn: r.try_get("median_amt").unwrap_or(0.0),
                multiple: r.try_get("multiple").unwrap_or(0.0),
                pct_of_cp_total,
            }
        })
        .collect();

    Ok(AtypicalResponse {
        rows: atypical_rows,
    })
}

// ---------------------------------------------------------------------------
// Individual
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct CpIndividualResponse {
    pub rfc: String,
    pub nombre: String,
    pub yearly_totals: Vec<CpYearRow>,
    pub by_month_by_year: Vec<CpMonthRow>,
    pub top_concepts: Vec<CpConceptRow>,
    pub pct_of_year: HashMap<String, f64>,
}

#[derive(Debug, Serialize)]
pub struct CpYearRow {
    pub year: i32,
    pub total_mxn: f64,
    pub invoice_count: i64,
    pub crecimiento_pct: Option<f64>,
    pub cagr_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct CpMonthRow {
    pub year: i32,
    pub month: i32,
    pub period: String,
    pub total_mxn: f64,
    pub invoice_count: i64,
}

#[derive(Debug, Serialize)]
pub struct CpConceptRow {
    pub descripcion: String,
    pub year_amounts: HashMap<String, f64>,
    pub year_counts: HashMap<String, i64>,
    pub total_mxn: f64,
}

pub async fn get_individual(
    pool: &DbPool,
    owner_rfc: &str,
    cp_rfc: &str,
    dl_type: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<CpIndividualResponse> {
    let (from_y, from_m) = parse_ym(from);
    let (to_y, to_m) = parse_ym(to);
    let dl_filter = dl_type_filter(dl_type);
    let owner_col = rfc_column(dl_type);
    let cp_col = if dl_type == "recibidos" {
        "rfc_emisor"
    } else {
        "rfc_receptor"
    };
    let cp_name_col = if dl_type == "recibidos" {
        "nombre_emisor"
    } else {
        "nombre_receptor"
    };

    // 1. Yearly totals for this counterparty
    let yearly_rows = sqlx::query(&format!(
        r#"
        SELECT year,
               SUM(COALESCE(total_mxn,0)::float8)::float8 AS yr_total,
               COUNT(*) AS cnt
        FROM pulso.cfdis
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
          AND {cp_col} = $2
          AND (year > $3 OR (year = $3 AND month >= $4))
          AND (year < $5 OR (year = $5 AND month <= $6))
        GROUP BY year
        ORDER BY year
        "#
    ))
    .bind(owner_rfc)
    .bind(cp_rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Get nombre from yearly query (fallback to monthly)
    let cp_nombre_row = sqlx::query(&format!(
        r#"
        SELECT MAX({cp_name_col}) AS cp_nombre
        FROM pulso.cfdis
        WHERE {owner_col} = $1 AND {cp_col} = $2 AND {dl_filter}
        "#
    ))
    .bind(owner_rfc)
    .bind(cp_rfc)
    .fetch_optional(pool)
    .await?;

    let cp_nombre: String = cp_nombre_row
        .as_ref()
        .and_then(|r| r.try_get("cp_nombre").ok())
        .unwrap_or_else(|| cp_rfc.to_string());

    let mut raw_years: Vec<(i32, f64, i64)> = yearly_rows
        .iter()
        .map(|r| {
            let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
            let total: f64 = r.try_get("yr_total").unwrap_or(0.0);
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            (year, total, cnt)
        })
        .collect();
    raw_years.sort_by_key(|(y, _, _)| *y);

    let yearly_totals: Vec<CpYearRow> = raw_years
        .iter()
        .enumerate()
        .map(|(i, &(year, total_mxn, invoice_count))| {
            let crecimiento_pct = if i > 0 {
                let prev_total = raw_years[i - 1].1;
                if prev_total > 0.0 {
                    Some((total_mxn - prev_total) / prev_total * 100.0)
                } else {
                    None
                }
            } else {
                None
            };

            // CAGR from first year to this year
            let cagr_pct = if i > 0 {
                let first_total = raw_years[0].1;
                let n_years = i as f64;
                if first_total > 0.0 && total_mxn > 0.0 {
                    Some(((total_mxn / first_total).powf(1.0 / n_years) - 1.0) * 100.0)
                } else {
                    None
                }
            } else {
                None
            };

            CpYearRow {
                year,
                total_mxn,
                invoice_count,
                crecimiento_pct,
                cagr_pct,
            }
        })
        .collect();

    // 2. Monthly breakdown
    let monthly_rows = sqlx::query(&format!(
        r#"
        SELECT year, month,
               year::text || '-' || LPAD(month::text, 2, '0') AS period,
               SUM(COALESCE(total_mxn,0)::float8)::float8 AS mo_total,
               COUNT(*) AS cnt
        FROM pulso.cfdis
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
          AND {cp_col} = $2
          AND (year > $3 OR (year = $3 AND month >= $4))
          AND (year < $5 OR (year = $5 AND month <= $6))
        GROUP BY year, month
        ORDER BY year, month
        "#
    ))
    .bind(owner_rfc)
    .bind(cp_rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let by_month_by_year: Vec<CpMonthRow> = monthly_rows
        .iter()
        .map(|r| CpMonthRow {
            year: r.try_get::<i64, _>("year").unwrap_or(0) as i32,
            month: r.try_get::<i64, _>("month").unwrap_or(0) as i32,
            period: r.try_get("period").unwrap_or_default(),
            total_mxn: r.try_get("mo_total").unwrap_or(0.0),
            invoice_count: r.try_get("cnt").unwrap_or(0),
        })
        .collect();

    // 3. Top concepts
    let concept_rows = sqlx::query(&format!(
        r#"
        SELECT SUBSTRING(cc.descripcion, 1, 80) AS desc_key,
               c.year,
               SUM(COALESCE(cc.importe, 0)::float8)::float8 AS yr_amount,
               COUNT(*) AS yr_count
        FROM pulso.cfdi_concepts cc
        JOIN pulso.cfdis c ON c.uuid = cc.uuid
        WHERE c.{owner_col} = $1 AND c.{dl_filter} AND c.tipo_comprobante NOT IN ('P','N')
          AND c.{cp_col} = $2
          AND (c.year > $3 OR (c.year = $3 AND c.month >= $4))
          AND (c.year < $5 OR (c.year = $5 AND c.month <= $6))
        GROUP BY SUBSTRING(cc.descripcion, 1, 80), c.year
        "#
    ))
    .bind(owner_rfc)
    .bind(cp_rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    // Aggregate concepts
    let mut concept_map: HashMap<String, (HashMap<String, f64>, HashMap<String, i64>, f64)> =
        HashMap::new();
    for row in &concept_rows {
        let desc: String = row.try_get("desc_key").unwrap_or_default();
        let year: i32 = row.try_get::<i64, _>("year").unwrap_or(0) as i32;
        let yr_amount: f64 = row.try_get("yr_amount").unwrap_or(0.0);
        let yr_count: i64 = row.try_get("yr_count").unwrap_or(0);
        let year_key = year.to_string();

        let entry = concept_map
            .entry(desc)
            .or_insert_with(|| (HashMap::new(), HashMap::new(), 0.0));
        *entry.0.entry(year_key.clone()).or_insert(0.0) += yr_amount;
        *entry.1.entry(year_key).or_insert(0) += yr_count;
        entry.2 += yr_amount;
    }

    let mut top_concepts: Vec<CpConceptRow> = concept_map
        .into_iter()
        .map(
            |(desc, (year_amounts, year_counts, total_mxn))| CpConceptRow {
                descripcion: desc,
                year_amounts,
                year_counts,
                total_mxn,
            },
        )
        .collect();
    top_concepts.sort_by(|a, b| {
        b.total_mxn
            .partial_cmp(&a.total_mxn)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    top_concepts.truncate(10);

    // 4. pct_of_year: for each year, what % of owner's total does this cp represent
    let owner_yearly_rows = sqlx::query(&format!(
        r#"
        SELECT year,
               SUM(COALESCE(total_mxn,0)::float8)::float8 AS yr_total
        FROM pulso.cfdis
        WHERE {owner_col} = $1 AND {dl_filter} AND tipo_comprobante NOT IN ('P','N')
          AND (year > $2 OR (year = $2 AND month >= $3))
          AND (year < $4 OR (year = $4 AND month <= $5))
        GROUP BY year
        "#
    ))
    .bind(owner_rfc)
    .bind(from_y)
    .bind(from_m)
    .bind(to_y)
    .bind(to_m)
    .fetch_all(pool)
    .await?;

    let owner_year_map: HashMap<i32, f64> = owner_yearly_rows
        .iter()
        .map(|r| {
            let year: i32 = r.try_get::<i64, _>("year").unwrap_or(0) as i32;
            let total: f64 = r.try_get("yr_total").unwrap_or(0.0);
            (year, total)
        })
        .collect();

    let pct_of_year: HashMap<String, f64> = raw_years
        .iter()
        .map(|&(year, cp_total, _)| {
            let owner_total = *owner_year_map.get(&year).unwrap_or(&0.0);
            let pct = if owner_total > 0.0 {
                cp_total / owner_total * 100.0
            } else {
                0.0
            };
            (year.to_string(), pct)
        })
        .collect();

    Ok(CpIndividualResponse {
        rfc: cp_rfc.to_string(),
        nombre: cp_nombre,
        yearly_totals,
        by_month_by_year,
        top_concepts,
        pct_of_year,
    })
}
