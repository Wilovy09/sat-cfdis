-- Migration 072: P-02 (Bloque de desempeño) / AUD-076 -- nomina_normalizada's exclusion
-- check stops being a per-row correlated LATERAL and becomes an ordinary join.
--
-- The view's `ex` LATERAL re-ran pulso.cfdi_exclusion (itself a 3-way UNION joining cfdis
-- and pulso.normalization_rules) once per row of `base`, even for an RFC with zero
-- normalization rules at all -- confirmed via EXPLAIN ANALYZE, the RFC de control's
-- 10,592-row COUNT(*) paid 293.9ms doing this, none of it useful (every LATERAL evaluation
-- returned zero rows). `adj`/`scl` (the two factor LATERALs against
-- payroll_normalization_rules directly) are untouched -- they're a much cheaper single-table
-- lookup, not chained through cfdi_exclusion's own joins, and the diagnosis this migration
-- is based on measured the exclusion check specifically as the added cost on top of them.
--
-- Fix: `excl_uuid`, a new CTE, resolves pulso.cfdi_exclusion once -- deduplicated by
-- (owner_rfc, uuid), since a cfdi can match more than one branch of cfdi_exclusion's UNION
-- and `is_excluded` below only checks `rule_id IS NOT NULL` (which rule_id survives the
-- MIN() dedup doesn't matter). An ordinary (non-lateral) LEFT JOIN against it lets the
-- planner build one hash table over the exclusion set and probe it per `base` row, instead
-- of re-planning and re-running the whole exclusion query per row.
--
-- Verified before touching the live view: seeded one synthetic pulso.normalization_rules
-- exclude row against a real nomina CFDI of the RFC de control (deleted immediately after
-- measuring -- no client data in this file, no client data left in the database), then
-- compared the current view's output against this migration's proposed SELECT, row by row,
-- on every exposed column, for that RFC and then for the whole platform (19,022 nomina
-- rows): zero rows differed either direction (`EXCEPT` both ways empty) before this was
-- applied. CREATE OR REPLACE VIEW used instead of DROP+CREATE -- the column list is
-- unchanged, so this is a same-shape swap with no window where the view doesn't exist for
-- the six background workers and three consumer modules (payroll.rs, hallazgos.rs,
-- normalization.rs) that share it.
--
-- Measured after: COUNT(*) for the RFC de control (zero normalization rules) drops from
-- 294ms to 75.1ms. Nomina totals by year for the one RFC with a real payroll_normalization_
-- rules rule (scale_employee_pct) are unchanged -- guaranteed by the zero-diff comparison
-- above, not re-measured separately.
CREATE OR REPLACE VIEW pulso.nomina_normalizada AS
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
),
excl_uuid AS (
    SELECT owner_rfc, uuid, MIN(rule_id) AS rule_id
    FROM pulso.cfdi_exclusion
    GROUP BY owner_rfc, uuid
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
LEFT JOIN excl_uuid ex ON ex.owner_rfc = b.rfc_emisor AND ex.uuid = b.uuid;
