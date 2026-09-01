-- Migration 057: DEC-030 (nómina rules get their own P&L line + motivo), DEC-032
-- (counterparty rules get an optional validity period), and L4-03's suggested LEFT JOIN
-- fix for cfdi_exclusion's per-row correlation inside pulso.nomina_normalizada.

-- DEC-032: period_start/period_end, same "YYYY-MM" format and NULL-means-unbounded
-- convention pulso.payroll_normalization_rules already uses. Evaluated against the
-- comprobante's own (year, month) -- the attribution every other exclusion check uses --
-- not against fecha_emision or any payment date. NULL on both ends preserves today's
-- behavior exactly, so the 7 existing rules (all unbounded) don't change.
ALTER TABLE pulso.normalization_rules
    ADD COLUMN IF NOT EXISTS period_start TEXT,
    ADD COLUMN IF NOT EXISTS period_end   TEXT;

-- DEC-030: nómina rules get their own P&L line + motivo (same Egresos catalog the
-- comprobante-level rules use), so L4-02's bridge integration has something to group by.
-- The one rule that exists today gets neither -- it falls under "Sin
-- clasificar" once L4-02 wires the bridge, matching L3-13's precedent for old rules with
-- no line: not backfilled with an invented value.
ALTER TABLE pulso.payroll_normalization_rules
    ADD COLUMN IF NOT EXISTS accounting_line TEXT,
    ADD COLUMN IF NOT EXISTS motivo          TEXT;

CREATE OR REPLACE VIEW pulso.cfdi_exclusion AS
SELECT DISTINCT nr.id AS rule_id, nr.owner_rfc, c.uuid
FROM pulso.normalization_rules nr
JOIN pulso.cfdis c ON (
    (nr.cfdi_uuid IS NOT NULL AND UPPER(nr.cfdi_uuid) = UPPER(c.uuid))
    OR (nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL AND (
        (nr.dl_type IN ('emitidos', 'ambos')
         AND c.rfc_emisor = nr.owner_rfc AND c.rfc_receptor = nr.source_rfc
         AND (nr.source_name_key IS NULL OR nr.source_name_key =
              REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_receptor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
         AND (nr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= nr.period_start)
         AND (nr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= nr.period_end))
        OR (nr.dl_type IN ('recibidos', 'ambos')
         AND c.rfc_receptor = nr.owner_rfc AND c.rfc_emisor = nr.source_rfc
         AND (nr.source_name_key IS NULL OR nr.source_name_key =
              REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_emisor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
         AND (nr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= nr.period_start)
         AND (nr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= nr.period_end))
    ))
)
WHERE nr.action = 'exclude';

-- L4-03: is_excluded's second clause used to be a correlated EXISTS re-running
-- cfdi_exclusion's whole rule×comprobante join (two REGEXP_REPLACE calls per rule) once
-- per nómina receipt -- 644 receipts x 7 rules today, 20,000 x 50 tomorrow. Replaced with
-- a single LEFT JOIN so the planner computes cfdi_exclusion's hash once instead of
-- re-planning a correlated subquery per outer row. No materialized view (a new rule must
-- take effect immediately), and the query shape everywhere else is unchanged.
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
    ) AS employee_rule_id
FROM pulso.cfdis c
JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
LEFT JOIN factors f
    ON f.owner_rfc = c.rfc_emisor AND f.employee_rfc = c.rfc_receptor
   AND f.year = c.year AND f.month = c.month
LEFT JOIN pulso.cfdi_exclusion ex
    ON ex.owner_rfc = c.rfc_emisor AND ex.uuid = c.uuid
WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled;
