-- Migration 071: shared devengo-date function + restore RFC pushdown, per Rob's review of
-- migration 070. Two independent fixes landed together because both touch the same view.
--
-- Fix 1: compute_adjust_factor_warnings/batch_adjust_factor_sources (normalization.rs)
-- needed year_devengo/month_devengo but only had it available by joining
-- pulso.nomina_normalizada -- which, to read two computed columns, forces Postgres to build
-- the WHOLE view (three per-row LATERAL joins for factor/exclusion) for however many rows
-- match. Measured against a real employee: 7,317ms through the view vs 19ms computing
-- devengo directly from cfdi_nomina.fecha_final_pago (fallback cfdis.fecha_emision) without
-- touching the view at all -- same 7 rows either way.
--
-- The view join existed for a real reason (one definition of devengo, not two), so the fix
-- isn't to re-inline the expression at yet another call site -- that's a fourth copy, not a
-- fix. `pulso.devengo_date` is the single definition now; the view and both queries in
-- normalization.rs call it. IMMUTABLE (same inputs always give the same output, no table
-- access) so Postgres can inline it freely -- confirmed this doesn't reintroduce the
-- migration 070 cost: calling it directly against cfdi_nomina/cfdis is still just column
-- reads, no join to the view.
--
-- Fix 2: migration 070's `base` CTE, referenced twice (the main SELECT's FROM, and
-- monthly_totals' own FROM), defaulted to MATERIALIZED -- Postgres's rule for a CTE named
-- more than once. That froze `base` (all platforms' nómina, unfiltered) before rfc_emisor
-- could be pushed into cfdis' own idx_cfdis_rfc_emisor index: confirmed via EXPLAIN ANALYZE,
-- a single-client query scanned all 19,009 platform-wide N-type receipts and discarded
-- 8,005, instead of the ~11,004-14,954 idx_cfdis_rfc_emisor already narrows the client to.
-- `NOT MATERIALIZED` on `base` forces it inlined at both sites independently, so each can
-- push its own predicate down again -- confirmed directly (this migration, not the
-- simplified replica the review measured against): EXPLAIN ANALYZE shows the
-- Bitmap Index Scan on idx_cfdis_rfc_emisor back (rows=14,954, not 19,009), 594ms with zero
-- adjust_to_amount_mxn rules platform-wide (down from migration 070's post-regression cost,
-- and monthly_totals shows `never executed` since nothing needs it), 1,113ms with one real
-- synthetic rule seeded and measured directly (deleted after) -- `monthly_totals` stays
-- `MATERIALIZED` and still runs exactly once (confirmed: `loops=1` on its HashAggregate),
-- so fix 2 does not reintroduce the migration 070-fixed per-row-rescan bug fix 2 shares this
-- migration with. Same 7 RFCs' row counts/totals/exclusions confirmed unchanged before/after.
CREATE FUNCTION pulso.devengo_date(fecha_final_pago text, fecha_emision text)
RETURNS date
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(
        NULLIF(NULLIF(TRIM(COALESCE(fecha_final_pago, '')), ''), '0000-00-00')::date,
        fecha_emision::date
    )
$$;

-- Rebuild nomina_normalizada's `base` CTE to call the function instead of repeating the
-- expression -- same output, single definition. Rest of the view (excl_emp, monthly_totals,
-- the three laterals) is verbatim from migration 070.
DROP VIEW pulso.nomina_normalizada;

CREATE VIEW pulso.nomina_normalizada AS
WITH base AS NOT MATERIALIZED (
    SELECT
        c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
        c.year, c.month, c.fecha_emision,
        n.tipo_nomina, n.fecha_pago, n.fecha_inicial_pago, n.fecha_final_pago, n.num_dias_pagados,
        n.curp, n.tipo_contrato, n.tipo_regimen, n.num_empleado, n.departamento, n.puesto,
        n.tipo_jornada, n.fecha_inicio_rel_laboral, n.antiguedad, n.periodicidad_pago,
        n.salario_base_cot_apor, n.salario_diario_integrado,
        n.total_percepciones, n.total_deducciones, n.total_otros_pagos,
        n.total_sueldos, n.total_gravado, n.total_exento,
        EXTRACT(YEAR FROM pulso.devengo_date(n.fecha_final_pago, c.fecha_emision))::bigint AS year_devengo,
        EXTRACT(MONTH FROM pulso.devengo_date(n.fecha_final_pago, c.fecha_emision))::bigint AS month_devengo
    FROM pulso.cfdis c
    JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
    WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled
),
excl_emp AS (
    SELECT id AS rule_id, owner_rfc, employee_rfc, period_start, period_end, created_at
    FROM pulso.payroll_normalization_rules
    WHERE action = 'exclude' AND rule_family IN ('exclude_employee', 'exclusion')
),
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
