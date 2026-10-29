-- Un mismo RFC en varias cuentas, cada una con su propio análisis (decisión de producto,
-- 29-oct-2026; docs/Pulso_Feature_Liga_Credenciales.md regla 6). Los CFDIs, la descarga y
-- la conciliación siguen siendo uno por RFC; lo que es de cada cuenta es su análisis: reglas
-- de normalización y de nómina, la nómina normalizada y los resultados en caché.
--
-- Llave de análisis (pulso.users.analysis_key): el RFC mismo para la cuenta que ya lo tenía
-- (nada de lo existente cambia ni se migra), y RFC#xxxxxxxx para las demás. Las reglas
-- guardan esa llave en owner_rfc.
--
-- Una sesión trabaja con un análisis vía la variable pulso.analysis (la fija el pool de esa
-- cuenta en Rust). pulso.analysis_key(rfc) devuelve la llave vigente para ese RFC: la de la
-- sesión si es de ese RFC, si no el RFC mismo. Así cfdi_exclusion y nomina_normalizada
-- siguen exponiendo owner_rfc = RFC de los datos y ninguna consulta de análisis cambia; sin
-- la variable (workers, ETL, admin) se comportan exactamente como antes.

-- 1. Llave de análisis por cuenta dueña.
ALTER TABLE pulso.users ADD COLUMN analysis_key text;
UPDATE pulso.users SET analysis_key = rfc;
-- Una baja lógica de otra cuenta junto a un dueño activo no puede compartir su llave (al
-- restaurarse chocaría): recibe una propia.
UPDATE pulso.users u SET analysis_key = u.rfc || '#' || left(u.id, 8)
WHERE u.deleted_at IS NOT NULL
  AND EXISTS (SELECT 1 FROM pulso.users a
              WHERE a.rfc = u.rfc AND a.deleted_at IS NULL AND a.user_id <> u.user_id);
ALTER TABLE pulso.users ALTER COLUMN analysis_key SET NOT NULL;

DROP INDEX pulso.users_rfc_global_unique;
CREATE UNIQUE INDEX users_analysis_key_active_unique
    ON pulso.users (analysis_key) WHERE deleted_at IS NULL;

-- 2. La llave vigente de un RFC en esta sesión.
CREATE FUNCTION pulso.analysis_key(rfc text) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN split_part(coalesce(current_setting('pulso.analysis', true), ''), '#', 1) = rfc
                THEN current_setting('pulso.analysis', true)
                ELSE rfc END
$$;

-- 3. Exclusiones de todos los análisis; cfdi_exclusion, solo las del análisis vigente.
DROP MATERIALIZED VIEW pulso.nomina_normalizada;

CREATE VIEW pulso.cfdi_exclusion_all AS
 SELECT nr.id AS rule_id,
    nr.owner_rfc AS analysis_key,
    split_part(nr.owner_rfc, '#', 1) AS owner_rfc,
    c.uuid
   FROM pulso.normalization_rules nr
     JOIN pulso.cfdis c ON upper(nr.cfdi_uuid) = upper(c.uuid)
  WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NOT NULL
UNION
 SELECT nr.id AS rule_id,
    nr.owner_rfc AS analysis_key,
    split_part(nr.owner_rfc, '#', 1) AS owner_rfc,
    c.uuid
   FROM pulso.normalization_rules nr
     JOIN pulso.cfdis c ON c.rfc_emisor = split_part(nr.owner_rfc, '#', 1) AND c.rfc_receptor = nr.source_rfc
  WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL
    AND (nr.dl_type = ANY (ARRAY['emitidos', 'ambos']))
    AND (nr.source_name_key IS NULL OR nr.source_name_key = regexp_replace(regexp_replace(TRIM(BOTH FROM upper(COALESCE(c.nombre_receptor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
    AND (nr.period_start IS NULL OR ((c.year::text || '-') || lpad(c.month::text, 2, '0')) >= nr.period_start)
    AND (nr.period_end IS NULL OR ((c.year::text || '-') || lpad(c.month::text, 2, '0')) <= nr.period_end)
UNION
 SELECT nr.id AS rule_id,
    nr.owner_rfc AS analysis_key,
    split_part(nr.owner_rfc, '#', 1) AS owner_rfc,
    c.uuid
   FROM pulso.normalization_rules nr
     JOIN pulso.cfdis c ON c.rfc_receptor = split_part(nr.owner_rfc, '#', 1) AND c.rfc_emisor = nr.source_rfc
  WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL
    AND (nr.dl_type = ANY (ARRAY['recibidos', 'ambos']))
    AND (nr.source_name_key IS NULL OR nr.source_name_key = regexp_replace(regexp_replace(TRIM(BOTH FROM upper(COALESCE(c.nombre_emisor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
    AND (nr.period_start IS NULL OR ((c.year::text || '-') || lpad(c.month::text, 2, '0')) >= nr.period_start)
    AND (nr.period_end IS NULL OR ((c.year::text || '-') || lpad(c.month::text, 2, '0')) <= nr.period_end);

CREATE OR REPLACE VIEW pulso.cfdi_exclusion AS
 SELECT rule_id, owner_rfc, uuid
   FROM pulso.cfdi_exclusion_all
  WHERE analysis_key = pulso.analysis_key(owner_rfc);

-- 4. Nómina normalizada: una fila por CFDI y análisis del RFC emisor (para un RFC con una
-- sola cuenta, las mismas filas de siempre). Misma definición que la 086/091 con las reglas
-- unidas por llave de análisis en vez de por RFC.
CREATE MATERIALIZED VIEW pulso.nomina_normalizada_mv AS
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
        ), analyses AS (
         -- El análisis del RFC mismo siempre, más el de cada cuenta adicional.
         SELECT DISTINCT base.rfc_emisor, base.rfc_emisor AS analysis_key FROM base
        UNION
         SELECT u.rfc, u.analysis_key FROM pulso.users u
          WHERE u.deleted_at IS NULL AND u.analysis_key <> u.rfc
        ), excl_emp AS (
         SELECT r.id AS rule_id, r.owner_rfc, r.employee_rfc, r.period_start, r.period_end, r.created_at
           FROM pulso.payroll_normalization_rules r
          WHERE r.action = 'exclude' AND (r.rule_family = ANY (ARRAY['exclude_employee', 'exclusion']))
        ), rule_rfcs AS NOT MATERIALIZED (
         SELECT DISTINCT split_part(r.owner_rfc, '#', 1) AS rfc_emisor
           FROM pulso.payroll_normalization_rules r
          WHERE r.rule_family = 'adjust_to_amount_mxn' AND r.value_mxn IS NOT NULL
        ), monthly_totals AS MATERIALIZED (
         SELECT bt.rfc_emisor, bt.rfc_receptor, bt.year_devengo, bt.month_devengo, bt.month_percepciones
           FROM rule_rfcs br,
            LATERAL ( SELECT base.rfc_emisor, base.rfc_receptor, base.year_devengo, base.month_devengo,
                    sum(COALESCE(base.total_percepciones, 0::numeric))::double precision AS month_percepciones
                   FROM base
                  WHERE base.rfc_emisor = br.rfc_emisor
                  GROUP BY base.rfc_emisor, base.rfc_receptor, base.year_devengo, base.month_devengo) bt
        ), excl_uuid AS (
         SELECT ea.analysis_key, ea.uuid, min(ea.rule_id) AS rule_id
           FROM pulso.cfdi_exclusion_all ea
          GROUP BY ea.analysis_key, ea.uuid
        )
 SELECT b.uuid, a.analysis_key, b.rfc_emisor, b.rfc_receptor, b.nombre_emisor, b.nombre_receptor,
    b.year, b.month, b.fecha_emision, b.tipo_nomina, b.fecha_pago, b.fecha_inicial_pago,
    b.fecha_final_pago, b.num_dias_pagados, b.curp, b.tipo_contrato, b.tipo_regimen,
    b.num_empleado, b.departamento, b.puesto, b.tipo_jornada, b.fecha_inicio_rel_laboral,
    b.antiguedad, b.periodicidad_pago, b.salario_base_cot_apor, b.salario_diario_integrado,
    COALESCE(adj.factor, scl.factor, 1.0::double precision) AS factor,
    (EXISTS ( SELECT 1
           FROM excl_emp e
          WHERE e.owner_rfc = a.analysis_key AND e.employee_rfc = b.rfc_receptor
            AND (e.period_start IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) >= e.period_start)
            AND (e.period_end IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) <= e.period_end)))
      OR ex.rule_id IS NOT NULL AS is_excluded,
    COALESCE(b.total_percepciones, 0::numeric)::double precision * COALESCE(adj.factor, scl.factor, 1.0::double precision) AS total_percepciones,
    COALESCE(b.total_deducciones, 0::numeric)::double precision * COALESCE(adj.factor, scl.factor, 1.0::double precision) AS total_deducciones,
    COALESCE(b.total_otros_pagos, 0::numeric)::double precision * COALESCE(adj.factor, scl.factor, 1.0::double precision) AS total_otros_pagos,
    COALESCE(b.total_sueldos, 0::numeric)::double precision * COALESCE(adj.factor, scl.factor, 1.0::double precision) AS total_sueldos,
    COALESCE(b.total_gravado, 0::numeric)::double precision * COALESCE(adj.factor, scl.factor, 1.0::double precision) AS total_gravado,
    COALESCE(b.total_exento, 0::numeric)::double precision * COALESCE(adj.factor, scl.factor, 1.0::double precision) AS total_exento,
    ( SELECT e.rule_id
           FROM excl_emp e
          WHERE e.owner_rfc = a.analysis_key AND e.employee_rfc = b.rfc_receptor
            AND (e.period_start IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) >= e.period_start)
            AND (e.period_end IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) <= e.period_end)
          ORDER BY e.created_at DESC
         LIMIT 1) AS employee_rule_id,
    COALESCE(adj.rule_id, scl.rule_id) AS factor_rule_id,
    b.year_devengo,
    b.month_devengo
   FROM base b
     JOIN analyses a ON a.rfc_emisor = b.rfc_emisor
     LEFT JOIN LATERAL ( SELECT ar.id AS rule_id,
            ar.value_mxn::double precision / NULLIF(mt.month_percepciones, 0::double precision) AS factor
           FROM pulso.payroll_normalization_rules ar
             LEFT JOIN monthly_totals mt ON mt.rfc_emisor = b.rfc_emisor AND mt.rfc_receptor = b.rfc_receptor AND mt.year_devengo = b.year_devengo AND mt.month_devengo = b.month_devengo
          WHERE ar.owner_rfc = a.analysis_key AND ar.employee_rfc = b.rfc_receptor AND ar.rule_family = 'adjust_to_amount_mxn' AND ar.value_mxn IS NOT NULL
            AND (ar.period_start IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) >= ar.period_start)
            AND (ar.period_end IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) <= ar.period_end)
          ORDER BY ar.created_at DESC
         LIMIT 1) adj ON true
     LEFT JOIN LATERAL ( SELECT sr.id AS rule_id,
            sr.value_pct::double precision / 100.0::double precision AS factor
           FROM pulso.payroll_normalization_rules sr
          WHERE sr.owner_rfc = a.analysis_key AND sr.employee_rfc = b.rfc_receptor AND sr.rule_family = 'scale_employee_pct' AND sr.value_pct IS NOT NULL
            AND (sr.period_start IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) >= sr.period_start)
            AND (sr.period_end IS NULL OR ((b.year_devengo::text || '-') || lpad(b.month_devengo::text, 2, '0')) <= sr.period_end)
          ORDER BY sr.created_at DESC
         LIMIT 1) scl ON true
     LEFT JOIN excl_uuid ex ON ex.analysis_key = a.analysis_key AND ex.uuid = b.uuid;

-- REFRESH ... CONCURRENTLY necesita un índice único.
CREATE UNIQUE INDEX nomina_normalizada_mv_uuid_analysis_idx ON pulso.nomina_normalizada_mv (uuid, analysis_key);
CREATE INDEX nomina_normalizada_mv_rfc_analysis_idx ON pulso.nomina_normalizada_mv (rfc_emisor, analysis_key);

-- Las mismas columnas que la vista materializada de antes, del análisis vigente.
CREATE VIEW pulso.nomina_normalizada AS
 SELECT uuid, rfc_emisor, rfc_receptor, nombre_emisor, nombre_receptor, year, month,
    fecha_emision, tipo_nomina, fecha_pago, fecha_inicial_pago, fecha_final_pago,
    num_dias_pagados, curp, tipo_contrato, tipo_regimen, num_empleado, departamento, puesto,
    tipo_jornada, fecha_inicio_rel_laboral, antiguedad, periodicidad_pago,
    salario_base_cot_apor, salario_diario_integrado, factor, is_excluded, total_percepciones,
    total_deducciones, total_otros_pagos, total_sueldos, total_gravado, total_exento,
    employee_rule_id, factor_rule_id, year_devengo, month_devengo
   FROM pulso.nomina_normalizada_mv
  WHERE analysis_key = pulso.analysis_key(rfc_emisor);

-- 5. Un cambio en las reglas de cualquier análisis invalida la caché del RFC de los datos.
CREATE OR REPLACE FUNCTION pulso.trg_bump_rfc_data_version()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pulso.bump_rfc_data_version(split_part(OLD.owner_rfc, '#', 1));
        RETURN OLD;
    END IF;

    PERFORM pulso.bump_rfc_data_version(split_part(NEW.owner_rfc, '#', 1));
    IF TG_OP = 'UPDATE' AND OLD.owner_rfc IS DISTINCT FROM NEW.owner_rfc THEN
        PERFORM pulso.bump_rfc_data_version(split_part(OLD.owner_rfc, '#', 1));
    END IF;
    RETURN NEW;
END;
$function$;
