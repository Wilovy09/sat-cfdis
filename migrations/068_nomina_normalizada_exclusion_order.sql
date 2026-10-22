-- Migration 068: L6-10 C7 -- of the view's four rule-lookups, two (the adj/scl LATERAL
-- factor subqueries) already got ORDER BY created_at DESC in migration 066, making
-- "most recent wins" deterministic. The other two didn't: excl_emp's SELECT doesn't even
-- carry created_at, and the employee_rule_id scalar subquery that reads from it has no
-- ORDER BY before its LIMIT 1 -- if an employee ever ends up with more than one
-- overlapping active exclusion (nothing at the DB level stops that; only the C1 lock in
-- check_payroll_rule_locks discourages it going forward), which one "wins" was whatever
-- order Postgres happened to return rows in, not a deliberate choice.
--
-- is_excluded's EXISTS clause is untouched -- it only tests presence, so ordering the
-- rows it scans changes nothing about the boolean it produces.
--
-- Column order in the SELECT list is unchanged (append-only past this point -- see
-- migration 065's own comment on why: Postgres forbids reordering or removing columns
-- from a view without dropping it first).
CREATE OR REPLACE VIEW pulso.nomina_normalizada AS
WITH excl_emp AS (
    SELECT id AS rule_id, owner_rfc, employee_rfc, period_start, period_end, created_at
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
        ORDER BY e.created_at DESC
        LIMIT 1
    ) AS employee_rule_id,
    COALESCE(adj.rule_id, scl.rule_id) AS factor_rule_id,
    EXTRACT(YEAR FROM COALESCE(
        NULLIF(NULLIF(TRIM(COALESCE(n.fecha_final_pago, '')), ''), '0000-00-00')::date,
        c.fecha_emision::date
    ))::bigint AS year_devengo,
    EXTRACT(MONTH FROM COALESCE(
        NULLIF(NULLIF(TRIM(COALESCE(n.fecha_final_pago, '')), ''), '0000-00-00')::date,
        c.fecha_emision::date
    ))::bigint AS month_devengo
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
    ORDER BY ar.created_at DESC
    LIMIT 1
) adj ON true
LEFT JOIN LATERAL (
    SELECT sr.id AS rule_id, sr.value_pct::float8 / 100.0 AS factor
    FROM pulso.payroll_normalization_rules sr
    WHERE sr.owner_rfc = c.rfc_emisor AND sr.employee_rfc = c.rfc_receptor
      AND sr.rule_family = 'scale_employee_pct' AND sr.value_pct IS NOT NULL
      AND (sr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= sr.period_start)
      AND (sr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= sr.period_end)
    ORDER BY sr.created_at DESC
    LIMIT 1
) scl ON true
LEFT JOIN LATERAL (
    SELECT ex1.rule_id
    FROM pulso.cfdi_exclusion ex1
    WHERE ex1.owner_rfc = c.rfc_emisor AND ex1.uuid = c.uuid
    LIMIT 1
) ex ON true
WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled;
