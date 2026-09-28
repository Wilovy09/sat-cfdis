-- PULSO_Lote18_Seguridad.md, L18-06 punto 1: hoy, registrar un RFC sin dueno activo da
-- acceso inmediato a los CFDIs que Pulso ya tenia de el como contraparte -- sin validar la
-- CIEC contra el SAT. La CIEC se sigue sin validar directamente (validarla exigiria
-- replicar el login del SAT fuera de un sync job), pero un RFC recien registrado no debe
-- mostrar NADA hasta que un sync job con esas credenciales pase la autenticacion real.
--
-- Mecanismo: en vez de tocar cada uno de los 47+ sitios que hacen "FROM pulso.cfdis" (alto
-- riesgo de dejar alguno sin cubrir, y sin forma de probarlo todo en esta sesion), se
-- renombra la tabla fisica a cfdis_raw y se crea una vista pulso.cfdis filtrada por si el
-- dueno activo del RFC (emisor o receptor) ya fue verificado. Los 47+ sitios siguen
-- diciendo "pulso.cfdis" sin cambiar una linea -- ahora leen la vista, no la tabla.
--
-- Excepciones deliberadas, resueltas en el codigo Rust (db/cfdis.rs, db/jobs.rs,
-- routes/users.rs), que siguen apuntando a cfdis_raw:
--   - Todas las escrituras (unico archivo que escribe: db/cfdis.rs).
--   - Bookkeeping interno por uuid ya conocido (estado SAT, reintentos de descarga,
--     deteccion de huecos, reprocesamiento de admin): necesitan ver la verdad completa
--     para no corromper su propio pipeline.
--   - Las 4 rutas admin-only (admin_list_rfcs, admin_user_rfcs, admin_rfc_xml_years,
--     admin_rfc_xml_days): un admin ya tiene acceso total por diseno.
-- months_with_data/months_with_data_direction (grilla de cobertura del usuario) y
-- get_user_rfcs_with_nombre/role SI quedan sobre la vista filtrada a proposito -- son de
-- cara al usuario, y un RFC no verificado debe verse "como uno nuevo que se esta
-- sincronizando", no con huecos raros.

-- Columna nueva: cuando el dueno activo de un RFC quedo verificado (un sync job con SUS
-- credenciales -- no una FIEL preexistente de un dueno anterior -- avanzo mas alla del
-- login del SAT). NULL = no verificado todavia.
ALTER TABLE pulso.users ADD COLUMN verified_at TIMESTAMPTZ;

-- Grandfathering: sin esto, TODOS los clientes reales de hoy pierden acceso a sus propios
-- datos en el momento del deploy. Se asume que toda fila activa hoy es legitima (ya fue
-- verificada implicitamente por meses/anos de uso real).
UPDATE pulso.users SET verified_at = created_at WHERE deleted_at IS NULL;

-- Las vistas que dependen de cfdis se recrean mas abajo para que su referencia directa
-- quede atada (por OID) a la vista filtrada, no a la tabla renombrada. nomina_normalizada
-- se debe soltar primero porque depende de cfdi_exclusion.
DROP MATERIALIZED VIEW pulso.nomina_normalizada;

ALTER TABLE pulso.cfdis RENAME TO cfdis_raw;

CREATE VIEW pulso.cfdis AS
SELECT c.* FROM pulso.cfdis_raw c
WHERE EXISTS (
    SELECT 1 FROM pulso.users u
    WHERE u.rfc = c.rfc_emisor AND u.deleted_at IS NULL AND u.verified_at IS NOT NULL
) OR EXISTS (
    SELECT 1 FROM pulso.users u
    WHERE u.rfc = c.rfc_receptor AND u.deleted_at IS NULL AND u.verified_at IS NOT NULL
);

-- Las 3 vistas siguientes usan CREATE OR REPLACE con el texto IDENTICO al que ya teniam
-- (pg_get_viewdef, confirmado contra adquiere-test el mismo dia de esta migracion) -- lo
-- unico que cambia es que "pulso.cfdis" ahora resuelve a la vista filtrada de arriba, no a
-- la tabla renombrada.

CREATE OR REPLACE VIEW pulso.cfdi_exclusion AS
 SELECT nr.id AS rule_id,
    nr.owner_rfc,
    c.uuid
   FROM pulso.normalization_rules nr
     JOIN pulso.cfdis c ON upper(nr.cfdi_uuid) = upper(c.uuid)
  WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NOT NULL
UNION
 SELECT nr.id AS rule_id,
    nr.owner_rfc,
    c.uuid
   FROM pulso.normalization_rules nr
     JOIN pulso.cfdis c ON c.rfc_emisor = nr.owner_rfc AND c.rfc_receptor = nr.source_rfc
  WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL
    AND (nr.dl_type = ANY (ARRAY['emitidos', 'ambos']))
    AND (nr.source_name_key IS NULL OR nr.source_name_key = regexp_replace(regexp_replace(TRIM(BOTH FROM upper(COALESCE(c.nombre_receptor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
    AND (nr.period_start IS NULL OR ((c.year::text || '-') || lpad(c.month::text, 2, '0')) >= nr.period_start)
    AND (nr.period_end IS NULL OR ((c.year::text || '-') || lpad(c.month::text, 2, '0')) <= nr.period_end)
UNION
 SELECT nr.id AS rule_id,
    nr.owner_rfc,
    c.uuid
   FROM pulso.normalization_rules nr
     JOIN pulso.cfdis c ON c.rfc_receptor = nr.owner_rfc AND c.rfc_emisor = nr.source_rfc
  WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL
    AND (nr.dl_type = ANY (ARRAY['recibidos', 'ambos']))
    AND (nr.source_name_key IS NULL OR nr.source_name_key = regexp_replace(regexp_replace(TRIM(BOTH FROM upper(COALESCE(c.nombre_emisor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
    AND (nr.period_start IS NULL OR ((c.year::text || '-') || lpad(c.month::text, 2, '0')) >= nr.period_start)
    AND (nr.period_end IS NULL OR ((c.year::text || '-') || lpad(c.month::text, 2, '0')) <= nr.period_end);

CREATE OR REPLACE VIEW pulso.cfdis_ajustado AS
 SELECT uuid, job_id, rfc_emisor, nombre_emisor, regimen_fiscal_emisor, rfc_receptor,
    nombre_receptor, uso_cfdi, domicilio_fiscal_receptor, regimen_fiscal_receptor,
    fecha_emision, year, month, tipo_comprobante, subtotal, descuento, total, moneda,
    tipo_cambio, total_mxn, metodo_pago, forma_pago, lugar_expedicion, estado_sat, dl_type,
    xml_available, created_at, total_neto_mxn, is_cancelled, estado_sat_checked_at,
    estado_sat_check_attempts, xml_redownload_attempts,
    CASE
        WHEN tipo_comprobante = 'E' AND (EXISTS (
            SELECT 1 FROM pulso.cfdi_relacionados r
            WHERE r.source_uuid = c.uuid AND (r.tipo_relacion = ANY (ARRAY['02', '07']))
        )) THEN 0::numeric
        ELSE total_neto_mxn
    END AS total_neto_mxn_ajustado
   FROM pulso.cfdis c;

CREATE OR REPLACE VIEW pulso.cfdi_cobro_estado AS
 SELECT inv.uuid, inv.rfc_emisor, inv.rfc_receptor, inv.dl_type, inv.year, inv.month,
    inv.fecha_emision, inv.metodo_pago,
    COALESCE(inv.total_mxn, 0::numeric)::double precision AS total_mxn,
    pago.pagado_mxn, pago.acreditado_mxn,
    GREATEST(COALESCE(inv.total_mxn, 0::numeric)::double precision - pago.pagado_mxn - pago.acreditado_mxn, 0::double precision) AS saldo_mxn,
    (SELECT max(cp.fecha_pago::date) AS max
        FROM pulso.cfdi_payment_docs pd
        JOIN pulso.cfdi_payments cp ON cp.payment_uuid = pd.payment_uuid AND cp.pago_num = pd.pago_num
        JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
        WHERE pd.invoice_uuid = inv.uuid AND NOT comp.is_cancelled AND cp.fecha_pago IS NOT NULL AND cp.fecha_pago::date >= inv.fecha_emision::date) AS ultimo_pago_fecha,
    (date_trunc('month', CURRENT_DATE::timestamp with time zone) - '1 day'::interval)::date - inv.fecha_emision::date AS dias_antiguedad
   FROM pulso.cfdis inv
     CROSS JOIN LATERAL (
        SELECT
            CASE
                WHEN COALESCE(inv.metodo_pago, 'PUE') <> 'PPD' THEN COALESCE(inv.total_mxn, 0::numeric)::double precision
                ELSE COALESCE((
                    SELECT sum(pd.imp_pagado::double precision / COALESCE(NULLIF(pd.tipo_cambio_dr::double precision, 0::double precision), 1::double precision) *
                        CASE
                            WHEN cp.moneda_p IS NOT NULL AND cp.moneda_p <> 'MXN' AND COALESCE(NULLIF(cp.tipo_cambio_p::double precision, 0::double precision), 1::double precision) = 1::double precision AND (EXISTS (
                                SELECT 1 FROM pulso.cfdi_payment_docs tc3d
                                WHERE tc3d.payment_uuid = cp.payment_uuid AND tc3d.moneda_dr = cp.moneda_p
                            )) THEN COALESCE((
                                SELECT sum(tc3d.imp_pagado::double precision * COALESCE(NULLIF(tc3i.tipo_cambio::double precision, 0::double precision), 1::double precision)) / NULLIF(sum(tc3d.imp_pagado::double precision), 0::double precision)
                                FROM pulso.cfdi_payment_docs tc3d
                                JOIN pulso.cfdis tc3i ON tc3i.uuid = tc3d.invoice_uuid
                                WHERE tc3d.payment_uuid = cp.payment_uuid AND tc3d.moneda_dr = cp.moneda_p
                            ), 1::double precision)
                            ELSE COALESCE(NULLIF(cp.tipo_cambio_p::double precision, 0::double precision), 1::double precision)
                        END) AS sum
                    FROM pulso.cfdi_payment_docs pd
                    JOIN pulso.cfdi_payments cp ON cp.payment_uuid = pd.payment_uuid AND cp.pago_num = pd.pago_num
                    JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
                    WHERE pd.invoice_uuid = inv.uuid AND NOT comp.is_cancelled), 0::double precision)
            END AS pagado_mxn,
            CASE
                WHEN COALESCE(inv.metodo_pago, 'PUE') <> 'PPD' THEN 0::double precision
                ELSE COALESCE((
                    SELECT sum(COALESCE(nc.total_mxn, 0::numeric)::double precision) AS sum
                    FROM pulso.cfdi_relacionados cr
                    JOIN pulso.cfdis nc ON nc.uuid = cr.source_uuid
                    WHERE cr.related_uuid = inv.uuid AND (cr.tipo_relacion = ANY (ARRAY['01', '03'])) AND nc.tipo_comprobante = 'E' AND NOT nc.is_cancelled), 0::double precision)
            END AS acreditado_mxn) pago
  WHERE inv.tipo_comprobante = 'I' AND NOT inv.is_cancelled;

-- nomina_normalizada (migracion 086): recreada con el mismo cuerpo exacto -- su CTE `base`
-- referencia pulso.cfdis directamente, no solo via cfdi_exclusion, asi que tambien
-- necesita recrearse para que esa referencia quede atada a la vista filtrada.
CREATE MATERIALIZED VIEW pulso.nomina_normalizada AS
WITH base AS NOT MATERIALIZED (
    SELECT c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
        c.year, c.month, c.fecha_emision, n.tipo_nomina, n.fecha_pago, n.fecha_inicial_pago,
        n.fecha_final_pago, n.num_dias_pagados, n.curp, n.tipo_contrato, n.tipo_regimen,
        n.num_empleado, n.departamento, n.puesto, n.tipo_jornada, n.fecha_inicio_rel_laboral,
        n.antiguedad, n.periodicidad_pago, n.salario_base_cot_apor, n.salario_diario_integrado,
        n.total_percepciones, n.total_deducciones, n.total_otros_pagos, n.total_sueldos,
        n.total_gravado, n.total_exento,
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
    SELECT bt.rfc_emisor, bt.rfc_receptor, bt.year_devengo, bt.month_devengo, bt.month_percepciones
    FROM rule_rfcs br,
        LATERAL (
            SELECT base.rfc_emisor, base.rfc_receptor, base.year_devengo, base.month_devengo,
                sum(COALESCE(base.total_percepciones, 0))::double precision AS month_percepciones
            FROM base WHERE base.rfc_emisor = br.rfc_emisor
            GROUP BY base.rfc_emisor, base.rfc_receptor, base.year_devengo, base.month_devengo
        ) bt
), excl_uuid AS (
    SELECT cfdi_exclusion.owner_rfc, cfdi_exclusion.uuid, min(cfdi_exclusion.rule_id) AS rule_id
    FROM pulso.cfdi_exclusion GROUP BY cfdi_exclusion.owner_rfc, cfdi_exclusion.uuid
)
SELECT b.uuid, b.rfc_emisor, b.rfc_receptor, b.nombre_emisor, b.nombre_receptor, b.year,
    b.month, b.fecha_emision, b.tipo_nomina, b.fecha_pago, b.fecha_inicial_pago,
    b.fecha_final_pago, b.num_dias_pagados, b.curp, b.tipo_contrato, b.tipo_regimen,
    b.num_empleado, b.departamento, b.puesto, b.tipo_jornada, b.fecha_inicio_rel_laboral,
    b.antiguedad, b.periodicidad_pago, b.salario_base_cot_apor, b.salario_diario_integrado,
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
    b.year_devengo, b.month_devengo
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

CREATE UNIQUE INDEX nomina_normalizada_uuid_idx ON pulso.nomina_normalizada (uuid);
CREATE INDEX idx_nomina_normalizada_rfc_emisor ON pulso.nomina_normalizada (rfc_emisor);
