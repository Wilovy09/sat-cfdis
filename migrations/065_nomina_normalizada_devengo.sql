-- Lote 5, L5-04: una sola definicion de mes de devengo.
--
-- DEC-034 fija el mes de un recibo de nomina como el mes de FechaFinalPago (con
-- respaldo a fecha de emision cuando viene vacio). payroll.rs ya implementa ese
-- calculo en 13 consultas -- inline, repetido caracter por caracter -- pero
-- by_month/by_year/by_month_ordinaria lo usan solo para el GROUP BY y siguen
-- filtrando la ventana WHERE por mes de EMISION. Un recibo timbrado en enero por
-- trabajo de diciembre entra por la ventana de enero y se agrupa en diciembre: el
-- total de un mes cambia segun la ventana consultada.
--
-- Se agrega year_devengo/month_devengo a la vista, con el MISMO calculo que ya usa
-- payroll.rs (COALESCE de fecha_final_pago normalizada a fecha_emision), para que
-- exista una sola definicion y los consumidores dejen de repetirla inline. Los
-- consumidores que deben seguir en mes de emision (first_pay/last_pay, headcount,
-- la ventana de 92 dias de plantilla vigente, la vigencia de reglas) no cambian --
-- siguen usando year/month, que conserva su significado de emision sin tocar.
--
-- pulso.cfdi_exclusion NO lleva devengo (ver L5-04 del documento): se construye
-- sobre normalization_rules x cfdis y no ve cfdi_nomina; ademas nomina_normalizada
-- la consume, asi que meterle devengo seria una dependencia circular. La vigencia
-- de una regla de comprobante sobre un recibo de nomina se evalua aqui, donde ya
-- hay devengo.
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
