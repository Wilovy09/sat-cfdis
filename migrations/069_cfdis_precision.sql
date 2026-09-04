-- Migration 069: L6-12 -- move pulso.cfdis' money columns off REAL (single precision).
--
-- Same defect L4-09 (migration 056) already fixed on the nómina side: REAL accumulates
-- real, measurable error over a SUM, and this is the tolerance L6-02's "puente lado
-- comprobantes" contract test has to declare (+/-2 pesos) instead of asserting exact
-- equality -- the same population, cast identically, sums to -14,303,149.33 or
-- -14,303,149.34 depending purely on grouping order. This is that tolerance's root cause,
-- on the largest table in the system (65k+ rows and growing with every sync).
--
-- Six columns: subtotal, descuento, total, total_mxn, total_neto_mxn -> NUMERIC(18,2),
-- matching migration 056's precedent for peso amounts. tipo_cambio is an exchange RATE, not
-- a peso amount -- confirmed against live data (max 20.850000381469727, REAL's own
-- precision ceiling showing through) -- NUMERIC(18,2) would round a real rate like 17.1234
-- down to 17.12 and manufacture a NEW error on every foreign-currency total this column
-- feeds into. Given NUMERIC(18,6) instead, matching SAT's own CFDI schema allowance for
-- TipoCambio.
--
-- No Rust-side change needed: every write path already binds Option<f64> (Postgres applies
-- the assignment cast on INSERT/UPDATE); every read path already casts to ::float8 before
-- aggregating (confirmed by static_invariants.rs's f64_decode_without_cast_or_helper test,
-- which passes both before and after this migration -- the cast site doesn't move, only
-- what it converts FROM does).
--
-- Five views read from pulso.cfdis and must be dropped before the ALTER, then recreated:
-- nomina_normalizada depends on cfdi_exclusion (drop/create nomina_normalizada around it,
-- not the other way -- confirmed via pg_depend before writing this, no other inter-view
-- dependency exists among the five). cfdi_exclusion, nomina_normalizada, and
-- rfc_as_of_cutoff never reference cfdis' money columns at all (confirmed by reading each
-- definition) -- recreated verbatim, byte-identical to their live definitions (062, 068,
-- and 052 respectively). cfdi_cobro_estado (055) already casts every cfdis money column to
-- ::float8 before use -- also verbatim, unaffected by the underlying column's type.
-- cfdis_ajustado is the one that needs an actual change: its CASE expression's WHEN branch
-- returns a literal `0::real` to match total_neto_mxn's old type in the ELSE branch: once
-- total_neto_mxn is NUMERIC, that literal must become `0::numeric` too, or the CASE's
-- branches no longer share a common type.
-- total_neto_mxn turned out to be a GENERATED (STORED) column -- not visible in Tabularis's
-- own describe_table output (it reported is_generated: false for every column, including
-- this one; information_schema.columns' own is_generated is what actually caught it) --
-- generated from subtotal/descuento/tipo_cambio via a CASE (sign flips for tipo_comprobante
-- 'E'). Postgres refuses `ALTER COLUMN ... TYPE ... USING` on a generated column outright,
-- and separately refuses to alter subtotal/descuento/tipo_cambio while ANY generated column
-- still depends on them. Confirmed live, in this order, before settling on the fix below:
-- attempt 1 (alter total_neto_mxn with USING) -> "cannot specify USING when altering type
-- of generated column"; attempt 2 (alter the three base columns first) -> "cannot alter
-- type of a column used by a generated column". Resolved by dropping total_neto_mxn (its
-- index, idx_cfdis_total_neto_mxn, goes with it), altering the five ordinary columns, then
-- re-adding total_neto_mxn as GENERATED with the identical expression (only the target type
-- and the two 0.0/1.0 literals' now-NUMERIC context change -- the CASE logic itself is
-- byte-identical to what information_schema.columns reported live before this ran), and
-- recreating the index last.
DROP VIEW pulso.nomina_normalizada;
DROP VIEW pulso.cfdi_exclusion;
DROP VIEW pulso.cfdis_ajustado;
DROP VIEW pulso.cfdi_cobro_estado;
DROP VIEW pulso.rfc_as_of_cutoff;

ALTER TABLE pulso.cfdis DROP COLUMN total_neto_mxn;

ALTER TABLE pulso.cfdis
    ALTER COLUMN subtotal    TYPE NUMERIC(18,2) USING subtotal::numeric,
    ALTER COLUMN descuento   TYPE NUMERIC(18,2) USING descuento::numeric,
    ALTER COLUMN total       TYPE NUMERIC(18,2) USING total::numeric,
    ALTER COLUMN tipo_cambio TYPE NUMERIC(18,6) USING tipo_cambio::numeric,
    ALTER COLUMN total_mxn   TYPE NUMERIC(18,2) USING total_mxn::numeric;

ALTER TABLE pulso.cfdis ADD COLUMN total_neto_mxn NUMERIC(18,2)
    GENERATED ALWAYS AS (
        CASE
            WHEN tipo_comprobante = 'E' THEN (-(COALESCE(subtotal, 0.0) - COALESCE(descuento, 0.0))) * COALESCE(tipo_cambio, 1.0)
            ELSE (COALESCE(subtotal, 0.0) - COALESCE(descuento, 0.0)) * COALESCE(tipo_cambio, 1.0)
        END
    ) STORED;

CREATE INDEX idx_cfdis_total_neto_mxn ON pulso.cfdis USING btree (total_neto_mxn);

-- Verbatim from migration 062 (cfdi_exclusion_union_perf) -- no money column referenced.
CREATE OR REPLACE VIEW pulso.cfdi_exclusion AS
SELECT nr.id AS rule_id, nr.owner_rfc, c.uuid
FROM pulso.normalization_rules nr
JOIN pulso.cfdis c ON UPPER(nr.cfdi_uuid) = UPPER(c.uuid)
WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NOT NULL

UNION

SELECT nr.id AS rule_id, nr.owner_rfc, c.uuid
FROM pulso.normalization_rules nr
JOIN pulso.cfdis c
  ON c.rfc_emisor = nr.owner_rfc AND c.rfc_receptor = nr.source_rfc
WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL
  AND nr.dl_type IN ('emitidos', 'ambos')
  AND (nr.source_name_key IS NULL OR nr.source_name_key =
       REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_receptor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
  AND (nr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= nr.period_start)
  AND (nr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= nr.period_end)

UNION

SELECT nr.id AS rule_id, nr.owner_rfc, c.uuid
FROM pulso.normalization_rules nr
JOIN pulso.cfdis c
  ON c.rfc_receptor = nr.owner_rfc AND c.rfc_emisor = nr.source_rfc
WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL
  AND nr.dl_type IN ('recibidos', 'ambos')
  AND (nr.source_name_key IS NULL OR nr.source_name_key =
       REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_emisor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
  AND (nr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= nr.period_start)
  AND (nr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= nr.period_end);

-- Verbatim from migration 068 (nomina_normalizada_exclusion_order) -- no cfdis money
-- column referenced (only uuid/rfc_emisor/rfc_receptor/nombre_*/year/month/fecha_emision).
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

-- Verbatim from migration 052 -- no cfdis money column referenced.
CREATE OR REPLACE VIEW pulso.rfc_as_of_cutoff AS
WITH monthly AS (
    SELECT rfc_emisor AS owner_rfc, 'emitidos' AS direccion, year, month, COUNT(*) AS cnt
    FROM pulso.cfdis
    WHERE tipo_comprobante = 'I' AND NOT is_cancelled AND dl_type IN ('emitidos', 'ambos')
    GROUP BY 1, 3, 4
    UNION ALL
    SELECT rfc_receptor, 'recibidos', year, month, COUNT(*)
    FROM pulso.cfdis
    WHERE tipo_comprobante = 'I' AND NOT is_cancelled AND dl_type IN ('recibidos', 'ambos')
    GROUP BY 1, 3, 4
),
baseline AS (
    SELECT owner_rfc, direccion, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cnt)::float8 AS median
    FROM monthly GROUP BY owner_rfc, direccion
),
density_cutoff AS (
    SELECT m.owner_rfc, m.direccion, MAX(m.year * 100 + m.month) AS density_ym
    FROM monthly m
    JOIN baseline b ON b.owner_rfc = m.owner_rfc AND b.direccion = m.direccion
    WHERE m.cnt::float8 >= GREATEST(b.median * 0.15, 3.0)
    GROUP BY m.owner_rfc, m.direccion
),
last_complete_month AS (
    SELECT (EXTRACT(YEAR FROM date_trunc('month', CURRENT_DATE) - interval '1 month')::int * 100
          + EXTRACT(MONTH FROM date_trunc('month', CURRENT_DATE) - interval '1 month')::int) AS ym
)
SELECT dc.owner_rfc, dc.direccion,
       LEAST(dc.density_ym, lcm.ym) AS as_of_ym
FROM density_cutoff dc, last_complete_month lcm;

-- Verbatim from migration 055 (tc3_currency_fix) -- every cfdis money column it reads is
-- already cast to ::float8 before use, so the underlying column type change is invisible
-- to this view's logic.
CREATE OR REPLACE VIEW pulso.cfdi_cobro_estado AS
SELECT
    inv.uuid,
    inv.rfc_emisor,
    inv.rfc_receptor,
    inv.dl_type,
    inv.year,
    inv.month,
    inv.fecha_emision,
    inv.metodo_pago,
    COALESCE(inv.total_mxn, 0)::float8 AS total_mxn,
    pago.pagado_mxn,
    pago.acreditado_mxn,
    GREATEST(COALESCE(inv.total_mxn, 0)::float8 - pago.pagado_mxn - pago.acreditado_mxn, 0) AS saldo_mxn,
    (
        SELECT MAX(cp.fecha_pago::date)
        FROM pulso.cfdi_payment_docs pd
        JOIN pulso.cfdi_payments cp
          ON cp.payment_uuid = pd.payment_uuid AND cp.pago_num = pd.pago_num
        JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
        WHERE pd.invoice_uuid = inv.uuid AND NOT comp.is_cancelled
          AND cp.fecha_pago IS NOT NULL AND cp.fecha_pago::date >= inv.fecha_emision::date
    ) AS ultimo_pago_fecha,
    ((date_trunc('month', CURRENT_DATE) - interval '1 day')::date - inv.fecha_emision::date) AS dias_antiguedad
FROM pulso.cfdis inv
CROSS JOIN LATERAL (
    SELECT
        CASE WHEN COALESCE(inv.metodo_pago, 'PUE') != 'PPD' THEN COALESCE(inv.total_mxn, 0)::float8
             ELSE COALESCE((
                 SELECT SUM(pd.imp_pagado::float8
                            / COALESCE(NULLIF(pd.tipo_cambio_dr::float8, 0), 1)
                            * (CASE
                                 WHEN cp.moneda_p IS NOT NULL AND cp.moneda_p <> 'MXN'
                                      AND COALESCE(NULLIF(cp.tipo_cambio_p::float8, 0), 1) = 1
                                      AND EXISTS (
                                          SELECT 1 FROM pulso.cfdi_payment_docs tc3d
                                          WHERE tc3d.payment_uuid = cp.payment_uuid AND tc3d.moneda_dr = cp.moneda_p
                                      )
                                 THEN COALESCE((
                                     SELECT SUM(tc3d.imp_pagado::float8 * COALESCE(NULLIF(tc3i.tipo_cambio::float8, 0), 1))
                                            / NULLIF(SUM(tc3d.imp_pagado::float8), 0)
                                     FROM pulso.cfdi_payment_docs tc3d
                                     JOIN pulso.cfdis tc3i ON tc3i.uuid = tc3d.invoice_uuid
                                     WHERE tc3d.payment_uuid = cp.payment_uuid AND tc3d.moneda_dr = cp.moneda_p
                                 ), 1)
                                 ELSE COALESCE(NULLIF(cp.tipo_cambio_p::float8, 0), 1)
                               END))
                 FROM pulso.cfdi_payment_docs pd
                 JOIN pulso.cfdi_payments cp
                   ON cp.payment_uuid = pd.payment_uuid AND cp.pago_num = pd.pago_num
                 JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
                 WHERE pd.invoice_uuid = inv.uuid AND NOT comp.is_cancelled
             ), 0)
        END AS pagado_mxn,
        CASE WHEN COALESCE(inv.metodo_pago, 'PUE') != 'PPD' THEN 0::float8
             ELSE COALESCE((
                 SELECT SUM(COALESCE(nc.total_mxn, 0)::float8)
                 FROM pulso.cfdi_relacionados cr
                 JOIN pulso.cfdis nc ON nc.uuid = cr.source_uuid
                 WHERE cr.related_uuid = inv.uuid AND cr.tipo_relacion IN ('01', '03')
                   AND nc.tipo_comprobante = 'E' AND NOT nc.is_cancelled
             ), 0)
        END AS acreditado_mxn
) pago
WHERE inv.tipo_comprobante = 'I' AND NOT inv.is_cancelled;

-- The one view whose logic actually changes: the WHEN branch's literal must match
-- total_neto_mxn's new NUMERIC type so the CASE has one common branch type.
CREATE OR REPLACE VIEW pulso.cfdis_ajustado AS
SELECT
    uuid, job_id, rfc_emisor, nombre_emisor, regimen_fiscal_emisor, rfc_receptor,
    nombre_receptor, uso_cfdi, domicilio_fiscal_receptor, regimen_fiscal_receptor,
    fecha_emision, year, month, tipo_comprobante, subtotal, descuento, total, moneda,
    tipo_cambio, total_mxn, metodo_pago, forma_pago, lugar_expedicion, estado_sat, dl_type,
    xml_available, created_at, total_neto_mxn, is_cancelled, estado_sat_checked_at,
    estado_sat_check_attempts, xml_redownload_attempts,
    CASE
        WHEN tipo_comprobante = 'E' AND EXISTS (
            SELECT 1 FROM pulso.cfdi_relacionados r
            WHERE r.source_uuid = c.uuid AND r.tipo_relacion IN ('02', '07')
        ) THEN 0::numeric
        ELSE total_neto_mxn
    END AS total_neto_mxn_ajustado
FROM pulso.cfdis c;
