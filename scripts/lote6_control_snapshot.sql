-- L6-05: rehacer la foto de control, fuera de una migracion.
--
-- La foto de 064 (Lote 5) tenia tres problemas medidos en PULSO_Correcciones_Lote6.md:
--   1. "sin normalizar" y "no excluida" sumaban la MISMA columna ya factorizada
--      (nomina_normalizada.total_percepciones) -- ninguna llegaba al bruto real
--      (suma directa de cfdi_nomina, sin factor). Corregido: bruto real ahora sale de
--      cfdis JOIN cfdi_nomina directo, igual que number_contract.rs's row 1.
--   2. El control del puente usaba total con IVA y signo plano. Corregido: usa
--      total_neto_mxn_ajustado con el signo por lado (emisor = negativo, receptor =
--      positivo), exactamente la formula del "direct_row" en number_contract.rs's row 6.
--   3. Las dos RFC de referencia estaban escritas a mano. Corregido: el universo de RFC
--      con nomina sale de una consulta (DISTINCT rfc_emisor con recibos tipo N no
--      cancelados), no de una lista fija -- hoy son 6, sea cual sea el numero manana
--      esta consulta lo cubre.
--
-- Este es un script de una sola vez: se corre a mano contra la base (Tabularis), NUNCA se
-- registra en _sqlx_migrations ni vive en migrations/. Una migracion se re-ejecuta segun
-- el estado de _sqlx_migrations; una foto de control por definicion no debe volver a
-- correr jamas -- si se re-ejecutara, sus INSERT agregarian al conjunto congelado los
-- comprobantes que hayan llegado despues de este corte, exactamente el riesgo que L6-01
-- existe para prevenir en el caso de una migracion real. Reutiliza las tablas de 064
-- (pulso.lote5_snapshot_uuid / pulso.lote5_snapshot_value, ya aplicadas y registradas)
-- bajo un snapshot_label nuevo, 'lote6_pre' -- las filas 'lote5_pre' quedan intactas como
-- registro historico de lo que se midio (mal) en el lote anterior.

-- Paso 1: universo de RFC con nomina, resuelto por consulta.
INSERT INTO pulso.lote5_snapshot_uuid (snapshot_label, scope, owner_rfc, uuid)
SELECT 'lote6_pre', 'nomina_receipts', n.rfc_emisor, n.uuid
FROM pulso.nomina_normalizada n
WHERE n.rfc_emisor IN (
    SELECT DISTINCT c.rfc_emisor
    FROM pulso.cfdis c
    JOIN pulso.cfdi_nomina cn ON cn.uuid = c.uuid
    WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled
)
ON CONFLICT DO NOTHING;

INSERT INTO pulso.lote5_snapshot_uuid (snapshot_label, scope, owner_rfc, uuid)
SELECT 'lote6_pre', 'cfdi_exclusion', ex.owner_rfc, ex.uuid
FROM pulso.cfdi_exclusion ex
WHERE ex.owner_rfc IN (
    SELECT DISTINCT c.rfc_emisor
    FROM pulso.cfdis c
    JOIN pulso.cfdi_nomina cn ON cn.uuid = c.uuid
    WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled
)
ON CONFLICT DO NOTHING;

-- Paso 2: nomina bruta real -- SIN factor, directo de cfdi_nomina, acotado al UUID congelado.
INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote6_pre', 'nomina_bruta_real', c.rfc_emisor, SUM(n2.total_percepciones)
FROM pulso.cfdis c
JOIN pulso.cfdi_nomina n2 ON n2.uuid = c.uuid
JOIN pulso.lote5_snapshot_uuid s
  ON s.snapshot_label = 'lote6_pre' AND s.scope = 'nomina_receipts'
 AND s.owner_rfc = c.rfc_emisor AND s.uuid = c.uuid
GROUP BY c.rfc_emisor
ON CONFLICT DO NOTHING;

-- Paso 3: nomina normalizada no excluida -- CON factor, a traves de la vista.
INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote6_pre', 'nomina_normalizada_no_excluida', n.rfc_emisor, SUM(n.total_percepciones)
FROM pulso.nomina_normalizada n
JOIN pulso.lote5_snapshot_uuid s
  ON s.snapshot_label = 'lote6_pre' AND s.scope = 'nomina_receipts'
 AND s.owner_rfc = n.rfc_emisor AND s.uuid = n.uuid
WHERE NOT n.is_excluded
GROUP BY n.rfc_emisor
ON CONFLICT DO NOTHING;

-- Paso 4: filas de la vista de nomina, acotadas al UUID congelado.
INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote6_pre', 'nomina_filas_vista', n.rfc_emisor, COUNT(*)
FROM pulso.nomina_normalizada n
JOIN pulso.lote5_snapshot_uuid s
  ON s.snapshot_label = 'lote6_pre' AND s.scope = 'nomina_receipts'
 AND s.owner_rfc = n.rfc_emisor AND s.uuid = n.uuid
GROUP BY n.rfc_emisor
ON CONFLICT DO NOTHING;

-- Paso 5: filas de la base de exclusiones, acotadas al UUID congelado.
INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote6_pre', 'cfdi_exclusion_filas', s.owner_rfc, COUNT(*)
FROM pulso.lote5_snapshot_uuid s
WHERE s.snapshot_label = 'lote6_pre' AND s.scope = 'cfdi_exclusion'
GROUP BY s.owner_rfc
ON CONFLICT DO NOTHING;

-- Paso 6: puente lado comprobantes -- misma formula exacta que number_contract.rs's row 6
-- (total_neto_mxn_ajustado, signo por lado, sin agrupar por regla/anio), acotada al UUID
-- congelado de cfdi_exclusion. NO usa total con IVA ni signo plano -- ese fue el problema 2.
-- El ::float8 en cada rama del CASE no es cosmetico: total_neto_mxn_ajustado es REAL
-- (precision simple, L6-12), y sin el cast la propia SUM() de Postgres agrega en REAL --
-- probado en vivo, la diferencia fue de 50 pesos sobre 14.3 millones (-14303200 sin cast
-- vs -14303149.33 con cast, y este ultimo es el que coincide con number_contract.rs).
INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote6_pre', 'puente_lado_comprobantes', s.owner_rfc,
       SUM(
           CASE WHEN c.rfc_emisor = s.owner_rfc THEN -COALESCE(c.total_neto_mxn_ajustado, 0)::float8
                ELSE COALESCE(c.total_neto_mxn_ajustado, 0)::float8 END
       )
FROM pulso.lote5_snapshot_uuid s
JOIN pulso.cfdis_ajustado c ON c.uuid = s.uuid
WHERE s.snapshot_label = 'lote6_pre' AND s.scope = 'cfdi_exclusion'
  AND c.tipo_comprobante IN ('I', 'E') AND NOT c.is_cancelled
GROUP BY s.owner_rfc
ON CONFLICT DO NOTHING;
