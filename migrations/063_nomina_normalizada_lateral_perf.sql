-- Migration 063: live incident, continued -- pulso.nomina_normalizada's factor lookup.
--
-- After 062 fixed cfdi_exclusion's plan, CES100706U65's payroll queries were STILL
-- taking 500+ seconds. EXPLAIN ANALYZE showed why: the `factors` CTE pre-aggregates
-- month_percepciones for every (owner, employee, year, month) combination the owner
-- has ANY nómina for, then LEFT JOINs that whole relation back onto every receipt by
-- (rfc_receptor, year, month) -- a condition Postgres can't hash/index because the left
-- side is a computed aggregate, not a table. The planner fell back to a nested loop:
-- for CES (10,684 nómina receipts x ~10,926 factor rows) that's ~29,400,000 row
-- comparisons, re-running two payroll_normalization_rules probes on every single one.
--
-- Fixed by dropping the pre-aggregated `factors`/`monthly` relation entirely and
-- replacing it with two LATERAL subqueries evaluated per receipt, correlated directly
-- against pulso.payroll_normalization_rules -- a tiny table (rule count, not receipt
-- count; 2 rows platform-wide today) -- instead of joining a large derived relation.
-- For an owner with zero scale/adjust rules (every RFC except NUB170623KI3 today),
-- each LATERAL probe is now a near-instant no-match lookup against ~2 rows, run once
-- per receipt, instead of a cross join against thousands of precomputed factor rows.
-- The adjust-to-amount factor's own month_percepciones sum is now computed inline,
-- scoped to the one employee-month it actually needs -- only paid when a matching rule
-- exists at all, which is the rare case.
--
-- Same columns, same semantics (adjust wins over scale, per L3-15; a month with zero
-- percepciones leaves the adjust factor undefined via NULLIF, falling through to scale
-- or 1.0, per DEC-028/"no se inventa el monto donde no hubo recibo"); only the
-- evaluation strategy changed. factor_rule_id (migration 058) is preserved.
CREATE OR REPLACE VIEW pulso.nomina_normalizada AS
WITH excl_emp AS (
    SELECT id AS rule_id, owner_rfc, employee_rfc, period_start, period_end
    FROM pulso.payroll_normalization_rules
    WHERE action = 'exclude' AND rule_family IN ('exclude_employee', 'exclusion')
)
SELECT
    c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
    c.year, c.month, c.fecha_emision,
    n.tipo_nomina, n.fecha_pago, n.fecha_inicial_pago, n.fecha_final_pago, n.num_dias_pagados,
    n.curp, n.tipo_contrato, n.tipo_regimen, n.num_empleado, n.departamento, n.puesto,
    n.tipo_jornada, n.fecha_inicio_rel_laboral, n.antiguedad, n.periodicidad_pago,
    n.salario_base_cot_apor, n.salario_diario_integrado,
    COALESCE(adj.factor, scl.factor, 1.0) AS factor,
    (
        (EXISTS (
            SELECT 1 FROM excl_emp e
            WHERE e.owner_rfc = c.rfc_emisor AND e.employee_rfc = c.rfc_receptor
              AND (e.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= e.period_start)
              AND (e.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= e.period_end)
        ))
        OR ex.rule_id IS NOT NULL
    ) AS is_excluded,
    COALESCE(n.total_percepciones, 0)::float8 * COALESCE(adj.factor, scl.factor, 1.0) AS total_percepciones,
    COALESCE(n.total_deducciones, 0)::float8  * COALESCE(adj.factor, scl.factor, 1.0) AS total_deducciones,
    COALESCE(n.total_otros_pagos, 0)::float8  * COALESCE(adj.factor, scl.factor, 1.0) AS total_otros_pagos,
    COALESCE(n.total_sueldos, 0)::float8      * COALESCE(adj.factor, scl.factor, 1.0) AS total_sueldos,
    COALESCE(n.total_gravado, 0)::float8      * COALESCE(adj.factor, scl.factor, 1.0) AS total_gravado,
    COALESCE(n.total_exento, 0)::float8       * COALESCE(adj.factor, scl.factor, 1.0) AS total_exento,
    (
        SELECT e.rule_id FROM excl_emp e
        WHERE e.owner_rfc = c.rfc_emisor AND e.employee_rfc = c.rfc_receptor
          AND (e.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= e.period_start)
          AND (e.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= e.period_end)
        LIMIT 1
    ) AS employee_rule_id,
    COALESCE(adj.rule_id, scl.rule_id) AS factor_rule_id
FROM pulso.cfdis c
JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
LEFT JOIN LATERAL (
    SELECT ar.id AS rule_id,
           ar.value_mxn::float8 / NULLIF((
               SELECT SUM(COALESCE(n2.total_percepciones, 0))::float8
               FROM pulso.cfdis c2
               JOIN pulso.cfdi_nomina n2 ON n2.uuid = c2.uuid
               WHERE c2.rfc_emisor = c.rfc_emisor AND c2.rfc_receptor = c.rfc_receptor
                 AND c2.year = c.year AND c2.month = c.month
                 AND c2.tipo_comprobante = 'N' AND NOT c2.is_cancelled
           ), 0) AS factor
    FROM pulso.payroll_normalization_rules ar
    WHERE ar.owner_rfc = c.rfc_emisor AND ar.employee_rfc = c.rfc_receptor
      AND ar.rule_family = 'adjust_to_amount_mxn' AND ar.value_mxn IS NOT NULL
      AND (ar.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= ar.period_start)
      AND (ar.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= ar.period_end)
    LIMIT 1
) adj ON true
LEFT JOIN LATERAL (
    SELECT sr.id AS rule_id, sr.value_pct::float8 / 100.0 AS factor
    FROM pulso.payroll_normalization_rules sr
    WHERE sr.owner_rfc = c.rfc_emisor AND sr.employee_rfc = c.rfc_receptor
      AND sr.rule_family = 'scale_employee_pct' AND sr.value_pct IS NOT NULL
      AND (sr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= sr.period_start)
      AND (sr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= sr.period_end)
    LIMIT 1
) scl ON true
LEFT JOIN pulso.cfdi_exclusion ex
    ON ex.owner_rfc = c.rfc_emisor AND ex.uuid = c.uuid
WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled;
