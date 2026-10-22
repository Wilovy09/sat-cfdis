-- Migration 056: L4-09 + L4-10 -- move nómina money columns off REAL (single precision).
--
-- Twelve columns (nine in pulso.cfdi_nomina, three across its detail tables) have been
-- REAL since the original schema. Single precision on money accumulates real, measurable
-- error (1,839.83 MXN across today's 78M pesos of payroll, growing with volume) and is the
-- most likely cause of L4-10 ("Escalar por porcentaje" fails to save: value_pct is the only
-- payroll rule value column still REAL, unlike value_mxn which was added later as NUMERIC
-- and works). No Rust-side change is needed: every write path already binds Option<f64> and
-- either lets Postgres apply the assignment cast for the INSERT target column, or explicitly
-- casts to ::float8[] before a bulk UNNEST insert (see db/cfdis.rs) -- both continue to work
-- once the target column is NUMERIC instead of REAL. Every read path already casts to
-- ::float8 before aggregating, so query results are unaffected in shape, only in accuracy.
--
-- pulso.nomina_normalizada (migration 054) reads both cfdi_nomina's money columns and
-- payroll_normalization_rules.value_pct, so Postgres refuses the ALTERs while it exists.
-- Dropped and recreated verbatim around them -- its own `factor` column stays float8
-- (COALESCE's common type with the 1.0 literal and the adjust branch's float8 division),
-- confirmed against the query planner before writing this migration.
DROP VIEW pulso.nomina_normalizada;

ALTER TABLE pulso.cfdi_nomina
    ALTER COLUMN num_dias_pagados         TYPE NUMERIC(8,2)  USING num_dias_pagados::numeric,
    ALTER COLUMN total_percepciones       TYPE NUMERIC(18,2) USING total_percepciones::numeric,
    ALTER COLUMN total_deducciones        TYPE NUMERIC(18,2) USING total_deducciones::numeric,
    ALTER COLUMN total_otros_pagos        TYPE NUMERIC(18,2) USING total_otros_pagos::numeric,
    ALTER COLUMN salario_base_cot_apor    TYPE NUMERIC(18,2) USING salario_base_cot_apor::numeric,
    ALTER COLUMN salario_diario_integrado TYPE NUMERIC(18,2) USING salario_diario_integrado::numeric,
    ALTER COLUMN total_sueldos            TYPE NUMERIC(18,2) USING total_sueldos::numeric,
    ALTER COLUMN total_gravado            TYPE NUMERIC(18,2) USING total_gravado::numeric,
    ALTER COLUMN total_exento             TYPE NUMERIC(18,2) USING total_exento::numeric;

ALTER TABLE pulso.cfdi_nomina_percepciones
    ALTER COLUMN importe_gravado TYPE NUMERIC(18,2) USING importe_gravado::numeric,
    ALTER COLUMN importe_exento  TYPE NUMERIC(18,2) USING importe_exento::numeric;

ALTER TABLE pulso.cfdi_nomina_deducciones
    ALTER COLUMN importe TYPE NUMERIC(18,2) USING importe::numeric;

-- L4-10: value_pct is a percentage (0-100 once L4-04's validation lands), not a peso amount.
-- No rows carry it today (zero scale_employee_pct rules loaded), so there is nothing to
-- convert -- confirmed before writing this migration.
ALTER TABLE pulso.payroll_normalization_rules
    ALTER COLUMN value_pct TYPE NUMERIC(5,2) USING value_pct::numeric;

CREATE VIEW pulso.nomina_normalizada AS
WITH excl_emp AS (
    SELECT id AS rule_id, owner_rfc, employee_rfc, period_start, period_end
    FROM pulso.payroll_normalization_rules
    WHERE action = 'exclude' AND rule_family IN ('exclude_employee', 'exclusion')
),
scale_rules AS (
    SELECT owner_rfc, employee_rfc, period_start, period_end, value_pct
    FROM pulso.payroll_normalization_rules
    WHERE rule_family = 'scale_employee_pct' AND value_pct IS NOT NULL
),
adjust_rules AS (
    SELECT owner_rfc, employee_rfc, period_start, period_end, value_mxn
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
           COALESCE(
               (SELECT ar.value_mxn::float8 / m.month_percepciones
                FROM adjust_rules ar
                WHERE ar.owner_rfc = m.owner_rfc AND ar.employee_rfc = m.employee_rfc
                  AND m.month_percepciones > 0
                  AND (ar.period_start IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) >= ar.period_start)
                  AND (ar.period_end IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) <= ar.period_end)
                LIMIT 1),
               (SELECT sr.value_pct::float8 / 100.0
                FROM scale_rules sr
                WHERE sr.owner_rfc = m.owner_rfc AND sr.employee_rfc = m.employee_rfc
                  AND (sr.period_start IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) >= sr.period_start)
                  AND (sr.period_end IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) <= sr.period_end)
                LIMIT 1),
               1.0
           ) AS factor
    FROM monthly m
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
        OR EXISTS (
            SELECT 1 FROM pulso.cfdi_exclusion ex WHERE ex.owner_rfc = c.rfc_emisor AND ex.uuid = c.uuid
        )
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
    ) AS employee_rule_id
FROM pulso.cfdis c
JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
LEFT JOIN factors f
    ON f.owner_rfc = c.rfc_emisor AND f.employee_rfc = c.rfc_receptor
   AND f.year = c.year AND f.month = c.month
WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled;
