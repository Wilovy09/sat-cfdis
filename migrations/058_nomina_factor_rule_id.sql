-- Migration 058: L4-02 -- attribute pulso.nomina_normalizada's `factor` to the specific
-- payroll_normalization_rules row that produced it.
--
-- Today `factors` computes a single `factor` via COALESCE(adjust_subquery, scale_subquery,
-- 1.0), two independent scalar subqueries with no way to know which one (if either) won.
-- The EBITDA bridge (normalization.rs's list_ebitda_bridge_adjustments) needs to group the
-- "costo real - costo normalizado" difference a scale/adjust rule produces by that rule's
-- own accounting_line/motivo (DEC-030), so it needs the winning rule's id, not just the
-- resulting number.
--
-- Restructured as two LEFT JOIN LATERAL subqueries (adjust, then scale), each selecting
-- both `rule_id` and the computed `factor`, then COALESCE'd together -- same precedence as
-- before (adjust wins over scale, L3-15), now carrying the id alongside the number.
--
-- `factor_rule_id` is appended as the last column in the SELECT list; every other column
-- keeps its exact name, order, and type, so this is CREATE OR REPLACE VIEW, not DROP+CREATE
-- (Postgres allows appending columns to a view via OR REPLACE; it only refuses when an
-- existing column's name, position, or type would change). The ~26+ existing call sites in
-- payroll.rs/hallazgos.rs read columns by name via sqlx's try_get, not by position, so the
-- new trailing column is invisible to them -- none of them are touched by this migration.
CREATE OR REPLACE VIEW pulso.nomina_normalizada AS
WITH excl_emp AS (
    SELECT id AS rule_id, owner_rfc, employee_rfc, period_start, period_end
    FROM pulso.payroll_normalization_rules
    WHERE action = 'exclude' AND rule_family IN ('exclude_employee', 'exclusion')
),
scale_rules AS (
    SELECT id AS rule_id, owner_rfc, employee_rfc, period_start, period_end, value_pct
    FROM pulso.payroll_normalization_rules
    WHERE rule_family = 'scale_employee_pct' AND value_pct IS NOT NULL
),
adjust_rules AS (
    SELECT id AS rule_id, owner_rfc, employee_rfc, period_start, period_end, value_mxn
    FROM pulso.payroll_normalization_rules
    WHERE rule_family = 'adjust_to_amount_mxn' AND value_mxn IS NOT NULL
),
monthly AS (
    SELECT c.rfc_emisor AS owner_rfc, c.rfc_receptor AS employee_rfc, c.year, c.month,
           SUM(COALESCE(n.total_percepciones, 0))::float8 AS month_percepciones
    FROM pulso.cfdis c
    JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
    WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled
    GROUP BY 1, 2, 3, 4
),
factors AS (
    SELECT m.owner_rfc, m.employee_rfc, m.year, m.month,
           COALESCE(adj.factor, scl.factor, 1.0) AS factor,
           COALESCE(adj.rule_id, scl.rule_id) AS factor_rule_id
    FROM monthly m
    LEFT JOIN LATERAL (
        SELECT ar.rule_id, ar.value_mxn::float8 / m.month_percepciones AS factor
        FROM adjust_rules ar
        WHERE ar.owner_rfc = m.owner_rfc AND ar.employee_rfc = m.employee_rfc
          AND m.month_percepciones > 0
          AND (ar.period_start IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) >= ar.period_start)
          AND (ar.period_end IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) <= ar.period_end)
        LIMIT 1
    ) adj ON true
    LEFT JOIN LATERAL (
        SELECT sr.rule_id, sr.value_pct::float8 / 100.0 AS factor
        FROM scale_rules sr
        WHERE sr.owner_rfc = m.owner_rfc AND sr.employee_rfc = m.employee_rfc
          AND (sr.period_start IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) >= sr.period_start)
          AND (sr.period_end IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) <= sr.period_end)
        LIMIT 1
    ) scl ON true
)
SELECT
    c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
    c.year, c.month, c.fecha_emision,
    n.tipo_nomina, n.fecha_pago, n.fecha_inicial_pago, n.fecha_final_pago, n.num_dias_pagados,
    n.curp, n.tipo_contrato, n.tipo_regimen, n.num_empleado, n.departamento, n.puesto,
    n.tipo_jornada, n.fecha_inicio_rel_laboral, n.antiguedad, n.periodicidad_pago,
    n.salario_base_cot_apor, n.salario_diario_integrado,
    COALESCE(f.factor, 1.0) AS factor,
    (
        (EXISTS (
            SELECT 1 FROM excl_emp e
            WHERE e.owner_rfc = c.rfc_emisor AND e.employee_rfc = c.rfc_receptor
              AND (e.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= e.period_start)
              AND (e.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= e.period_end)
        ))
        OR ex.rule_id IS NOT NULL
    ) AS is_excluded,
    COALESCE(n.total_percepciones, 0)::float8 * COALESCE(f.factor, 1.0) AS total_percepciones,
    COALESCE(n.total_deducciones, 0)::float8  * COALESCE(f.factor, 1.0) AS total_deducciones,
    COALESCE(n.total_otros_pagos, 0)::float8  * COALESCE(f.factor, 1.0) AS total_otros_pagos,
    COALESCE(n.total_sueldos, 0)::float8      * COALESCE(f.factor, 1.0) AS total_sueldos,
    COALESCE(n.total_gravado, 0)::float8      * COALESCE(f.factor, 1.0) AS total_gravado,
    COALESCE(n.total_exento, 0)::float8       * COALESCE(f.factor, 1.0) AS total_exento,
    (
        SELECT e.rule_id FROM excl_emp e
        WHERE e.owner_rfc = c.rfc_emisor AND e.employee_rfc = c.rfc_receptor
          AND (e.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= e.period_start)
          AND (e.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= e.period_end)
        LIMIT 1
    ) AS employee_rule_id,
    -- L4-02 source 3: which scale/adjust rule (if any) produced this row's `factor`, so the
    -- bridge can group "costo real - costo normalizado" under that rule's own
    -- accounting_line/motivo. NULL when factor is the 1.0 default (no rule matched).
    f.factor_rule_id AS factor_rule_id
FROM pulso.cfdis c
JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
LEFT JOIN factors f
    ON f.owner_rfc = c.rfc_emisor AND f.employee_rfc = c.rfc_receptor
   AND f.year = c.year AND f.month = c.month
LEFT JOIN pulso.cfdi_exclusion ex
    ON ex.owner_rfc = c.rfc_emisor AND ex.uuid = c.uuid
WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled;
