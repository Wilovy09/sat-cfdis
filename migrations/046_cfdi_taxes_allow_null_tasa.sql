-- DAT-1 (PULSO_auditoria_y_correcciones_v2.md): insert_taxes discarded every
-- tax row without TasaOCuota — common for ISR retentions and exempt rates —
-- because `tasa` sits inside cfdi_taxes' PRIMARY KEY, which forces NOT NULL.
-- The Rust-side fix (db/cfdis.rs::insert_taxes) now only skips a row that has
-- neither importe nor base; this migration makes the schema able to accept
-- what that fix will start sending.
--
-- A PRIMARY KEY can't have a nullable column, so the natural key moves to a
-- UNIQUE INDEX (which treats NULL as distinct per row, matching how a real
-- CFDI can carry more than one no-tasa retention) and a surrogate BIGSERIAL
-- becomes the real PK — same pattern as NOM-5's fix for cfdi_nomina_otros_pagos.
ALTER TABLE pulso.cfdi_taxes DROP CONSTRAINT cfdi_taxes_pkey;
ALTER TABLE pulso.cfdi_taxes ALTER COLUMN tasa DROP NOT NULL;
ALTER TABLE pulso.cfdi_taxes ADD COLUMN id BIGSERIAL PRIMARY KEY;
CREATE UNIQUE INDEX cfdi_taxes_uq ON pulso.cfdi_taxes (uuid, impuesto, tipo_factor, tasa, is_retenido);
