-- Hermana de NOM-6, no reportada en el audit v2 pero es el mismo bug:
-- cfdi_nomina_deducciones también tiene solo un PK surrogate (BIGSERIAL id,
-- nunca colisiona) y ON CONFLICT DO NOTHING sin target real. Encontrada al
-- corregir la tabla vecina cfdi_nomina_percepciones (NOM-6) — mismo alcance
-- exacto: solo el RFC grande (59,513 filas / 43,482 firmas), y cada grupo
-- duplicado aparece exactamente 2 veces, nunca 3+.
DELETE FROM pulso.cfdi_nomina_deducciones a
USING pulso.cfdi_nomina_deducciones b
WHERE a.ctid > b.ctid
  AND a.uuid = b.uuid
  AND a.tipo_deduccion IS NOT DISTINCT FROM b.tipo_deduccion
  AND a.clave          IS NOT DISTINCT FROM b.clave
  AND a.concepto       IS NOT DISTINCT FROM b.concepto
  AND a.importe        IS NOT DISTINCT FROM b.importe;

CREATE UNIQUE INDEX cfdi_nomina_deducciones_uq
  ON pulso.cfdi_nomina_deducciones (uuid, tipo_deduccion, clave, concepto);
