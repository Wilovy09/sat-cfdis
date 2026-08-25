-- Migration 054: L3-16 -- shared payroll-normalization base.
--
-- Per PULSO_Correcciones_Lote3: today's payroll normalization is a FILTER (drop excluded
-- receipts), repeated correctly in 28 places via payroll.rs's NOMINA_EXCL_C constant.
-- Scaling and adjusting are a TRANSFORMATION (change the amounts), and a transformation
-- copy-pasted across 19 money queries is exactly the bug class migration 052 fixed for
-- collections. One base, everyone reads it.
--
-- pulso.nomina_normalizada: one row per (owner_rfc, non-cancelled tipo_comprobante='N'
-- recibo), carrying:
--   - is_excluded: true if an "exclude_employee" rule covers this employee+month, OR a
--     cfdi_uuid-level rule (pulso.cfdi_exclusion, L3-01) excludes this specific receipt.
--     Consumers filter WHERE NOT is_excluded -- rows are NOT dropped here, because two of
--     the hallazgos (rotación, baja de personal clave) need to count/list who got excluded,
--     not just skip them silently.
--   - factor: 1.0 unless a scale/adjust rule covers this employee's (owner, employee,
--     month). Money columns below are already multiplied by it, so a query that today
--     does SUM(n.total_percepciones) is a mechanical FROM-clause swap, not a rewrite.
--     Adjust-to-amount wins over scale (L3-15 precedence); if the employee had zero
--     percepciones that month, adjust-to-amount is undefined and factor stays 1 --
--     "no se inventa el monto donde no hubo recibo".
--   - "Month" is fixed to the comprobante's own (year, month) -- not fecha_final_pago --
--     matching the window the exclusion rules already use, so exclusion and scaling agree
--     on which month a receipt belongs to (11.8% of receipts fall in a different month
--     under the other attribution; see the anexo's control query).
--   - SDI, antigüedad, and every non-money attribute pass through unscaled (DEC-028): an
--     employee scaled to 50% still counts as one employee with their real base salary.
CREATE OR REPLACE VIEW pulso.nomina_normalizada AS
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
               (SELECT ar.value_mxn / m.month_percepciones
                FROM adjust_rules ar
                WHERE ar.owner_rfc = m.owner_rfc AND ar.employee_rfc = m.employee_rfc
                  AND m.month_percepciones > 0
                  AND (ar.period_start IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) >= ar.period_start)
                  AND (ar.period_end IS NULL OR (m.year::text || '-' || LPAD(m.month::text, 2, '0')) <= ar.period_end)
                LIMIT 1),
               (SELECT sr.value_pct / 100.0
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
        -- Exposed only so list_excluded_cfdis (normalization.rs) can name which specific
        -- employee-level rule excluded this receipt without re-deriving the period-bound
        -- match itself -- the same reason cfdi_exclusion carries rule_id.
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

-- L3-15 familia 1 naming mismatch: the Nómina-module form wrote 'exclusion', the
-- Normalización-tab form wrote 'exclude_employee'. Canonicalize on 'exclude_employee' so
-- L3-11's unified form (and the view above) only ever has to recognize one string; the
-- view's excl_emp CTE still accepts both during rollout, but existing rows migrate now,
-- not later, so nothing depends on the old string surviving.
UPDATE pulso.payroll_normalization_rules
SET rule_family = 'exclude_employee'
WHERE rule_family = 'exclusion';
