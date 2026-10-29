-- PULSO_Plan_Mejoras_SQL.md, punto #6: pulso.nomina_normalizada is a plain VIEW,
-- referenced from 31 call sites across hallazgos.rs/payroll.rs, each one paying its
-- full nested-loop-left-join cost from scratch -- confirmed via EXPLAIN (ANALYZE,
-- BUFFERS): 372.9ms warm, for one RFC, one call. This is the root cause behind
-- "Nomina carga lento".
--
-- Fix: same name, same SELECT, same output -- only the relkind changes, from VIEW to
-- MATERIALIZED VIEW. Every one of the 31 call sites keeps working unchanged (Postgres
-- doesn't distinguish "SELECT ... FROM x" over a view vs a materialized view). The
-- SELECT body below is copied verbatim from `pg_get_viewdef('pulso.nomina_normalizada')`
-- -- nothing in the calculation itself changes, only when it's computed.
--
-- Tradeoff this accepts (explicit, chosen over a per-RFC targeted-refresh table): reads
-- become eventually consistent instead of always-live. A payroll_normalization_rules or
-- normalization_rules edit, or a fresh ETL sync, won't show up in nomina_normalizada
-- until the next refresh cycle -- see services/nomina_refresh.rs for the worker that
-- runs REFRESH MATERIALIZED VIEW CONCURRENTLY on an interval.
DROP VIEW pulso.nomina_normalizada;

CREATE MATERIALIZED VIEW pulso.nomina_normalizada AS
WITH base AS NOT MATERIALIZED (
    SELECT c.uuid,
        c.rfc_emisor,
        c.rfc_receptor,
        c.nombre_emisor,
        c.nombre_receptor,
        c.year,
        c.month,
        c.fecha_emision,
        n.tipo_nomina,
        n.fecha_pago,
        n.fecha_inicial_pago,
        n.fecha_final_pago,
        n.num_dias_pagados,
        n.curp,
        n.tipo_contrato,
        n.tipo_regimen,
        n.num_empleado,
        n.departamento,
        n.puesto,
        n.tipo_jornada,
        n.fecha_inicio_rel_laboral,
        n.antiguedad,
        n.periodicidad_pago,
        n.salario_base_cot_apor,
        n.salario_diario_integrado,
        n.total_percepciones,
        n.total_deducciones,
        n.total_otros_pagos,
        n.total_sueldos,
        n.total_gravado,
        n.total_exento,
        EXTRACT(year FROM pulso.devengo_date(n.fecha_final_pago, c.fecha_emision))::bigint AS year_devengo,
        EXTRACT(month FROM pulso.devengo_date(n.fecha_final_pago, c.fecha_emision))::bigint AS month_devengo
    FROM pulso.cfdis c
    JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
    WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled
), excl_emp AS (
    SELECT payroll_normalization_rules.id AS rule_id,
        payroll_normalization_rules.owner_rfc,
        payroll_normalization_rules.employee_rfc,
        payroll_normalization_rules.period_start,
        payroll_normalization_rules.period_end,
        payroll_normalization_rules.created_at
    FROM pulso.payroll_normalization_rules
    WHERE payroll_normalization_rules.action = 'exclude'
        AND (payroll_normalization_rules.rule_family = ANY (ARRAY['exclude_employee', 'exclusion']))
), rule_rfcs AS NOT MATERIALIZED (
    SELECT DISTINCT payroll_normalization_rules.owner_rfc AS rfc_emisor
    FROM pulso.payroll_normalization_rules
    WHERE payroll_normalization_rules.rule_family = 'adjust_to_amount_mxn'
        AND payroll_normalization_rules.value_mxn IS NOT NULL
), monthly_totals AS MATERIALIZED (
    SELECT bt.rfc_emisor,
        bt.rfc_receptor,
        bt.year_devengo,
        bt.month_devengo,
        bt.month_percepciones
    FROM rule_rfcs br,
        LATERAL (
            SELECT base.rfc_emisor,
                base.rfc_receptor,
                base.year_devengo,
                base.month_devengo,
                sum(COALESCE(base.total_percepciones, 0))::double precision AS month_percepciones
            FROM base
            WHERE base.rfc_emisor = br.rfc_emisor
            GROUP BY base.rfc_emisor, base.rfc_receptor, base.year_devengo, base.month_devengo
        ) bt
), excl_uuid AS (
    SELECT cfdi_exclusion.owner_rfc,
        cfdi_exclusion.uuid,
        min(cfdi_exclusion.rule_id) AS rule_id
    FROM pulso.cfdi_exclusion
    GROUP BY cfdi_exclusion.owner_rfc, cfdi_exclusion.uuid
)
SELECT b.uuid,
    b.rfc_emisor,
    b.rfc_receptor,
    b.nombre_emisor,
    b.nombre_receptor,
    b.year,
    b.month,
    b.fecha_emision,
    b.tipo_nomina,
    b.fecha_pago,
    b.fecha_inicial_pago,
    b.fecha_final_pago,
    b.num_dias_pagados,
    b.curp,
    b.tipo_contrato,
    b.tipo_regimen,
    b.num_empleado,
    b.departamento,
    b.puesto,
    b.tipo_jornada,
    b.fecha_inicio_rel_laboral,
    b.antiguedad,
    b.periodicidad_pago,
    b.salario_base_cot_apor,
    b.salario_diario_integrado,
    COALESCE(adj.factor, scl.factor, 1.0) AS factor,
    (EXISTS (
        SELECT 1 FROM excl_emp e
        WHERE e.owner_rfc = b.rfc_emisor AND e.employee_rfc = b.rfc_receptor
            AND (e.period_start IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) >= e.period_start)
            AND (e.period_end IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) <= e.period_end)
    )) OR ex.rule_id IS NOT NULL AS is_excluded,
    COALESCE(b.total_percepciones, 0)::double precision * COALESCE(adj.factor, scl.factor, 1.0) AS total_percepciones,
    COALESCE(b.total_deducciones, 0)::double precision * COALESCE(adj.factor, scl.factor, 1.0) AS total_deducciones,
    COALESCE(b.total_otros_pagos, 0)::double precision * COALESCE(adj.factor, scl.factor, 1.0) AS total_otros_pagos,
    COALESCE(b.total_sueldos, 0)::double precision * COALESCE(adj.factor, scl.factor, 1.0) AS total_sueldos,
    COALESCE(b.total_gravado, 0)::double precision * COALESCE(adj.factor, scl.factor, 1.0) AS total_gravado,
    COALESCE(b.total_exento, 0)::double precision * COALESCE(adj.factor, scl.factor, 1.0) AS total_exento,
    (
        SELECT e.rule_id FROM excl_emp e
        WHERE e.owner_rfc = b.rfc_emisor AND e.employee_rfc = b.rfc_receptor
            AND (e.period_start IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) >= e.period_start)
            AND (e.period_end IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) <= e.period_end)
        ORDER BY e.created_at DESC LIMIT 1
    ) AS employee_rule_id,
    COALESCE(adj.rule_id, scl.rule_id) AS factor_rule_id,
    b.year_devengo,
    b.month_devengo
FROM base b
LEFT JOIN LATERAL (
    SELECT ar.id AS rule_id,
        ar.value_mxn::double precision / NULLIF(mt.month_percepciones, 0) AS factor
    FROM pulso.payroll_normalization_rules ar
    LEFT JOIN monthly_totals mt ON mt.rfc_emisor = b.rfc_emisor AND mt.rfc_receptor = b.rfc_receptor
        AND mt.year_devengo = b.year_devengo AND mt.month_devengo = b.month_devengo
    WHERE ar.owner_rfc = b.rfc_emisor AND ar.employee_rfc = b.rfc_receptor
        AND ar.rule_family = 'adjust_to_amount_mxn' AND ar.value_mxn IS NOT NULL
        AND (ar.period_start IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) >= ar.period_start)
        AND (ar.period_end IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) <= ar.period_end)
    ORDER BY ar.created_at DESC LIMIT 1
) adj ON true
LEFT JOIN LATERAL (
    SELECT sr.id AS rule_id,
        sr.value_pct::double precision / 100.0 AS factor
    FROM pulso.payroll_normalization_rules sr
    WHERE sr.owner_rfc = b.rfc_emisor AND sr.employee_rfc = b.rfc_receptor
        AND sr.rule_family = 'scale_employee_pct' AND sr.value_pct IS NOT NULL
        AND (sr.period_start IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) >= sr.period_start)
        AND (sr.period_end IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) <= sr.period_end)
    ORDER BY sr.created_at DESC LIMIT 1
) scl ON true
LEFT JOIN excl_uuid ex ON ex.owner_rfc = b.rfc_emisor AND ex.uuid = b.uuid;

-- Needed for REFRESH MATERIALIZED VIEW CONCURRENTLY (services/nomina_refresh.rs) --
-- without a unique index, a concurrent refresh isn't possible and every refresh would
-- lock out readers for its full duration.
CREATE UNIQUE INDEX nomina_normalizada_uuid_idx ON pulso.nomina_normalizada (uuid);

-- Every one of the 31 call sites filters by rfc_emisor first -- carry over the same
-- access pattern the old view relied on via its underlying cfdis/cfdi_nomina indexes,
-- now needed explicitly since this is a physical relation with its own storage.
CREATE INDEX idx_nomina_normalizada_rfc_emisor ON pulso.nomina_normalizada (rfc_emisor);
