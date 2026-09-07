-- Migration 070: L6C-12 -- DEC-038, Rob 2026-09-04: normalization applies by devengo month,
-- not emisión. Base for the migration is 068 (verbatim in migration 069), NOT 065 -- 065 is
-- two revisions behind (no created_at on excl_emp, LIMIT 1 without ORDER BY on the three
-- rule lookups); starting from it would revert L6-10 (C7) and L5-07 in silence.
--
-- Five spots move from emisión (c.year/c.month) to devengo (year_devengo/month_devengo) for
-- period-overlap comparison against a rule's period_start/period_end:
--   1. is_excluded's EXISTS over excl_emp
--   2. employee_rule_id's scalar subquery (same excl_emp lookup, different projection)
--   3. the adj lateral's own period-overlap (is this adjust_to_amount_mxn rule in force here)
--   4. the adj lateral's DENOMINATOR -- today sums the employee's OTHER receipts in the same
--      emisión month; moves to the same devengo month instead
--   5. the scl lateral's own period-overlap (scale_employee_pct)
--
-- year_devengo/month_devengo can't be referenced from this SELECT's own WHERE or LATERAL
-- joins -- they're this same query's OUTPUT columns, not real columns yet at that point in
-- evaluation. Resolved with a `base` CTE that computes them once (identical FROM/JOIN/WHERE
-- shape as before, just two extra computed columns) so every other part of the view
-- references them as plain columns instead of repeating the COALESCE/EXTRACT expression by
-- hand five times -- a correctness bet on getting one expression right once, not a
-- performance change: `base` is exactly the `cfdis JOIN cfdi_nomina` pair the view already
-- had, nothing added to what it scans.
--
-- Out of scope, confirmed empty: cfdi_exclusion (comprobante-level exclusion) matches by
-- UUID, not period -- never touches year/month at all, devengo or otherwise. AUD-042.
DROP VIEW pulso.nomina_normalizada;

CREATE VIEW pulso.nomina_normalizada AS
WITH base AS (
    SELECT
        c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
        c.year, c.month, c.fecha_emision,
        n.tipo_nomina, n.fecha_pago, n.fecha_inicial_pago, n.fecha_final_pago, n.num_dias_pagados,
        n.curp, n.tipo_contrato, n.tipo_regimen, n.num_empleado, n.departamento, n.puesto,
        n.tipo_jornada, n.fecha_inicio_rel_laboral, n.antiguedad, n.periodicidad_pago,
        n.salario_base_cot_apor, n.salario_diario_integrado,
        n.total_percepciones, n.total_deducciones, n.total_otros_pagos,
        n.total_sueldos, n.total_gravado, n.total_exento,
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
    WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled
),
excl_emp AS (
    SELECT id AS rule_id, owner_rfc, employee_rfc, period_start, period_end, created_at
    FROM pulso.payroll_normalization_rules
    WHERE action = 'exclude' AND rule_family IN ('exclude_employee', 'exclusion')
),
-- The adj lateral's denominator needs "this employee's total for this devengo month" --
-- grouped ONCE here (one pass over `base`) instead of a correlated subquery re-scanning
-- `base` from scratch for every receipt of every employee an adjust_to_amount_mxn rule
-- names. Confirmed the difference is not theoretical: a single synthetic rule against one
-- real employee (188 receipts, RFC grande) measured 439ms (no rule) -> 2146ms with a plain
-- (non-materialized) CTE here -- Postgres inlined it into the LATERAL, so it re-scanned
-- `base` (18,597 rows) fresh for all 188 receipts anyway, same shape as a raw correlated
-- subquery. `AS MATERIALIZED` (explicit, Postgres 12+) forces it computed exactly once
-- instead: confirmed via EXPLAIN ANALYZE, `monthly_totals`'s own HashAggregate runs
-- `loops=1`, and every one of the 188 receipts becomes a cheap scan over its 6,311-row
-- result (still no index, but a 6,311-row table beats re-deriving from an 18,597-row
-- source 188 times) -- 439ms -> 771ms, not 2146ms. The two incidents migrations 062/063
-- document for this same lateral were exactly this shape at real production scale; this is
-- the fix, not just a measurement.
monthly_totals AS MATERIALIZED (
    SELECT rfc_emisor, rfc_receptor, year_devengo, month_devengo,
           SUM(COALESCE(total_percepciones, 0))::float8 AS month_percepciones
    FROM base
    GROUP BY rfc_emisor, rfc_receptor, year_devengo, month_devengo
)
SELECT
    b.uuid, b.rfc_emisor, b.rfc_receptor, b.nombre_emisor, b.nombre_receptor,
    b.year, b.month, b.fecha_emision,
    b.tipo_nomina, b.fecha_pago, b.fecha_inicial_pago, b.fecha_final_pago, b.num_dias_pagados,
    b.curp, b.tipo_contrato, b.tipo_regimen, b.num_empleado, b.departamento, b.puesto,
    b.tipo_jornada, b.fecha_inicio_rel_laboral, b.antiguedad, b.periodicidad_pago,
    b.salario_base_cot_apor, b.salario_diario_integrado,
    COALESCE(adj.factor, scl.factor, 1.0) AS factor,
    (
        (EXISTS (
            SELECT 1 FROM excl_emp e
            WHERE e.owner_rfc = b.rfc_emisor AND e.employee_rfc = b.rfc_receptor
              AND (e.period_start IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) >= e.period_start)
              AND (e.period_end IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) <= e.period_end)
        ))
        OR ex.rule_id IS NOT NULL
    ) AS is_excluded,
    COALESCE(b.total_percepciones, 0)::float8 * COALESCE(adj.factor, scl.factor, 1.0) AS total_percepciones,
    COALESCE(b.total_deducciones, 0)::float8  * COALESCE(adj.factor, scl.factor, 1.0) AS total_deducciones,
    COALESCE(b.total_otros_pagos, 0)::float8  * COALESCE(adj.factor, scl.factor, 1.0) AS total_otros_pagos,
    COALESCE(b.total_sueldos, 0)::float8      * COALESCE(adj.factor, scl.factor, 1.0) AS total_sueldos,
    COALESCE(b.total_gravado, 0)::float8      * COALESCE(adj.factor, scl.factor, 1.0) AS total_gravado,
    COALESCE(b.total_exento, 0)::float8       * COALESCE(adj.factor, scl.factor, 1.0) AS total_exento,
    (
        SELECT e.rule_id FROM excl_emp e
        WHERE e.owner_rfc = b.rfc_emisor AND e.employee_rfc = b.rfc_receptor
          AND (e.period_start IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) >= e.period_start)
          AND (e.period_end IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) <= e.period_end)
        ORDER BY e.created_at DESC
        LIMIT 1
    ) AS employee_rule_id,
    COALESCE(adj.rule_id, scl.rule_id) AS factor_rule_id,
    b.year_devengo,
    b.month_devengo
FROM base b
LEFT JOIN LATERAL (
    SELECT ar.id AS rule_id, ar.value_mxn::float8 / NULLIF(mt.month_percepciones, 0) AS factor
    FROM pulso.payroll_normalization_rules ar
    LEFT JOIN monthly_totals mt
      ON mt.rfc_emisor = b.rfc_emisor AND mt.rfc_receptor = b.rfc_receptor
     AND mt.year_devengo = b.year_devengo AND mt.month_devengo = b.month_devengo
    WHERE ar.owner_rfc = b.rfc_emisor AND ar.employee_rfc = b.rfc_receptor
      AND ar.rule_family = 'adjust_to_amount_mxn' AND ar.value_mxn IS NOT NULL
      AND (ar.period_start IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) >= ar.period_start)
      AND (ar.period_end IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) <= ar.period_end)
    ORDER BY ar.created_at DESC
    LIMIT 1
) adj ON true
LEFT JOIN LATERAL (
    SELECT sr.id AS rule_id, sr.value_pct::float8 / 100.0 AS factor
    FROM pulso.payroll_normalization_rules sr
    WHERE sr.owner_rfc = b.rfc_emisor AND sr.employee_rfc = b.rfc_receptor
      AND sr.rule_family = 'scale_employee_pct' AND sr.value_pct IS NOT NULL
      AND (sr.period_start IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) >= sr.period_start)
      AND (sr.period_end IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) <= sr.period_end)
    ORDER BY sr.created_at DESC
    LIMIT 1
) scl ON true
LEFT JOIN LATERAL (
    SELECT ex1.rule_id
    FROM pulso.cfdi_exclusion ex1
    WHERE ex1.owner_rfc = b.rfc_emisor AND ex1.uuid = b.uuid
    LIMIT 1
) ex ON true;
