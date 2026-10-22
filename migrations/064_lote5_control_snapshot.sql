-- Lote 5, "Antes de tocar nada": foto de control acotada por UUID, tomada antes de
-- cualquier cambio de este lote. La base crece con cada sincronizacion (el propio
-- documento midio +238,036.48 en 48 horas para el RFC de prueba), asi que un rango de
-- fechas abierto contaminaria la reconciliacion con CFDIs que simplemente llegaron
-- despues. Se congela el conjunto de UUIDs, no una ventana de tiempo.
--
-- Dos scopes por RFC de referencia (el RFC de prueba y el RFC grande del documento,
-- confirmados contra sus valores de control):
--   'nomina_receipts'  -- todo pulso.nomina_normalizada.uuid del owner, para reconciliar
--                          nomina bruta / excluida / filas de la vista.
--   'cfdi_exclusion'   -- todo pulso.cfdi_exclusion.uuid del owner, para reconciliar el
--                          lado de comprobantes del puente EBITDA.
--
-- Los valores de control se calculan una sola vez aqui, con el codigo de HOY (antes de
-- L5-01..L5-21), y quedan fijos. La reconciliacion final (ultima tarea del lote) vuelve
-- a calcular los mismos control_key pero restringidos al MISMO conjunto de UUIDs
-- congelado aqui, y compara.
CREATE TABLE IF NOT EXISTS pulso.lote5_snapshot_uuid (
    snapshot_label text NOT NULL,
    scope text NOT NULL,
    owner_rfc text NOT NULL,
    uuid text NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_label, scope, owner_rfc, uuid)
);

CREATE TABLE IF NOT EXISTS pulso.lote5_snapshot_value (
    snapshot_label text NOT NULL,
    control_key text NOT NULL,
    owner_rfc text NOT NULL,
    value numeric,
    captured_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_label, control_key, owner_rfc)
);

INSERT INTO pulso.lote5_snapshot_uuid (snapshot_label, scope, owner_rfc, uuid)
SELECT 'lote5_pre', 'nomina_receipts', rfc_emisor, uuid
FROM pulso.nomina_normalizada
WHERE rfc_emisor IN ('NUB170623KI3', 'CES100706U65')
ON CONFLICT DO NOTHING;

INSERT INTO pulso.lote5_snapshot_uuid (snapshot_label, scope, owner_rfc, uuid)
SELECT 'lote5_pre', 'cfdi_exclusion', owner_rfc, uuid
FROM pulso.cfdi_exclusion
WHERE owner_rfc IN ('NUB170623KI3', 'CES100706U65')
ON CONFLICT DO NOTHING;

INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote5_pre', 'nomina_bruta_percepciones_no_excluida', n.rfc_emisor, SUM(n.total_percepciones)
FROM pulso.nomina_normalizada n
JOIN pulso.lote5_snapshot_uuid s
  ON s.snapshot_label = 'lote5_pre' AND s.scope = 'nomina_receipts'
 AND s.owner_rfc = n.rfc_emisor AND s.uuid = n.uuid
WHERE NOT n.is_excluded
GROUP BY n.rfc_emisor
ON CONFLICT DO NOTHING;

INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote5_pre', 'nomina_bruta_percepciones_sin_normalizar', n.rfc_emisor, SUM(n.total_percepciones)
FROM pulso.nomina_normalizada n
JOIN pulso.lote5_snapshot_uuid s
  ON s.snapshot_label = 'lote5_pre' AND s.scope = 'nomina_receipts'
 AND s.owner_rfc = n.rfc_emisor AND s.uuid = n.uuid
GROUP BY n.rfc_emisor
ON CONFLICT DO NOTHING;

INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote5_pre', 'nomina_filas_normalizadas', n.rfc_emisor, COUNT(*)
FROM pulso.nomina_normalizada n
JOIN pulso.lote5_snapshot_uuid s
  ON s.snapshot_label = 'lote5_pre' AND s.scope = 'nomina_receipts'
 AND s.owner_rfc = n.rfc_emisor AND s.uuid = n.uuid
GROUP BY n.rfc_emisor
ON CONFLICT DO NOTHING;

INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote5_pre', 'puente_lado_comprobantes_total', ex.owner_rfc, -SUM(c.total)
FROM pulso.cfdi_exclusion ex
JOIN pulso.lote5_snapshot_uuid s
  ON s.snapshot_label = 'lote5_pre' AND s.scope = 'cfdi_exclusion'
 AND s.owner_rfc = ex.owner_rfc AND s.uuid = ex.uuid
JOIN pulso.cfdis c ON c.uuid = ex.uuid
GROUP BY ex.owner_rfc
ON CONFLICT DO NOTHING;

INSERT INTO pulso.lote5_snapshot_value (snapshot_label, control_key, owner_rfc, value)
SELECT 'lote5_pre', 'cfdi_exclusion_filas', owner_rfc, COUNT(*)
FROM pulso.lote5_snapshot_uuid
WHERE snapshot_label = 'lote5_pre' AND scope = 'cfdi_exclusion'
GROUP BY owner_rfc
ON CONFLICT DO NOTHING;
