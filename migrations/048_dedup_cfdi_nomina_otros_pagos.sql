-- NOM-5 (PULSO_auditoria_y_correcciones_v2.md): cfdi_nomina_otros_pagos has
-- no primary key or unique index, so `ON CONFLICT DO NOTHING` in
-- insert_nomina() has nothing to collide against — every re-run of the ETL
-- over the same UUID inserts the same rows again. NRS04's "Otros pagos por
-- clave SAT" table was showing exactly double the real total in every year
-- of Axented's data.
--
-- Verified before deleting anything: every duplicate group platform-wide
-- (ADC101206334, ALA2409253U7, CCO210630GE6, CES100706U65 — NUB170623KI3 has
-- none) appears exactly twice, never 3+ times — consistent with "the ETL ran
-- an extra time on the same UUID", not a coincidental pair of genuinely
-- distinct rows that happen to share every column.
DELETE FROM pulso.cfdi_nomina_otros_pagos a
USING pulso.cfdi_nomina_otros_pagos b
WHERE a.ctid > b.ctid
  AND a.uuid = b.uuid
  AND a.tipo_otro_pago IS NOT DISTINCT FROM b.tipo_otro_pago
  AND a.clave          IS NOT DISTINCT FROM b.clave
  AND a.concepto       IS NOT DISTINCT FROM b.concepto
  AND a.importe        IS NOT DISTINCT FROM b.importe;

ALTER TABLE pulso.cfdi_nomina_otros_pagos ADD COLUMN id BIGSERIAL PRIMARY KEY;
CREATE UNIQUE INDEX cfdi_nomina_otros_pagos_uq
  ON pulso.cfdi_nomina_otros_pagos (uuid, tipo_otro_pago, clave, concepto);
