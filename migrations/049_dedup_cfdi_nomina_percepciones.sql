-- NOM-6 (PULSO_auditoria_y_correcciones_v2.md): cfdi_nomina_percepciones
-- already has a BIGSERIAL `id` PRIMARY KEY, but that's a surrogate — it
-- never collides, so `ON CONFLICT DO NOTHING` in insert_nomina() has never
-- actually protected against a re-run duplicating the same percepcion row.
-- Only CES100706U65 is affected (razón 1.751); every other RFC's table is
-- already 1:1 clean. Verified every duplicate group here is exactly 2 deep
-- (never 3+) before deleting anything — the ETL ran twice, not a
-- coincidental pair of genuinely distinct rows.
DELETE FROM pulso.cfdi_nomina_percepciones a
USING pulso.cfdi_nomina_percepciones b
WHERE a.ctid > b.ctid
  AND a.uuid = b.uuid
  AND a.tipo_percepcion  IS NOT DISTINCT FROM b.tipo_percepcion
  AND a.clave            IS NOT DISTINCT FROM b.clave
  AND a.concepto         IS NOT DISTINCT FROM b.concepto
  AND a.importe_gravado  IS NOT DISTINCT FROM b.importe_gravado
  AND a.importe_exento   IS NOT DISTINCT FROM b.importe_exento;

CREATE UNIQUE INDEX cfdi_nomina_percepciones_uq
  ON pulso.cfdi_nomina_percepciones (uuid, tipo_percepcion, clave, concepto);
