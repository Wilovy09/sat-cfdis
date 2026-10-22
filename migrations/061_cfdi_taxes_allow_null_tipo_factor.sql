-- AUD-010 (PULSO_Correcciones_Lote4.md): cfdi_taxes has had zero is_retenido
-- rows and no ISR (impuesto '001') system-wide since Lote 2.
--
-- Root cause, confirmed against a real 2026 comprobante with the arithmetic
-- fingerprint (subtotal - descuento + trasladados > total): the parser
-- already parses the comprobante-level <cfdi:Retencion> correctly (it has
-- done so all along),
-- but the CFDI 4.0 XSD declares that node with ONLY Impuesto and Importe --
-- unlike cfdi:Traslado (comprobante- and concept-level) and concept-level
-- cfdi:Retencion, which all require the full TipoFactor/TasaOCuota/Base
-- breakdown. So every comprobante-level retention row is guaranteed to have
-- tipo_factor = NULL.
--
-- insert_taxes (db/cfdis.rs) batches an entire comprobante's tax rows into
-- ONE `INSERT ... SELECT * FROM UNNEST(...)` statement. tipo_factor was
-- NOT NULL, so that single row aborted the WHOLE statement with a
-- not-null-constraint error -- silently dropping the retention AND every
-- other tax row batched alongside it (only logged as a tracing::warn).
-- Verified directly: replaying that exact INSERT against the live DB raised
-- `null value in column "tipo_factor" violates not-null constraint`.
ALTER TABLE pulso.cfdi_taxes ALTER COLUMN tipo_factor DROP NOT NULL;

-- Making tipo_factor nullable reopens, on a second column, the exact
-- NULL-is-distinct duplication risk migration 046 fixed for tasa (same
-- pattern as NOM-5/NOM-6): Postgres unique indexes never treat two NULLs as
-- equal, so reprocessing a comprobante whose retention rows carry neither
-- tipo_factor nor tasa would insert a fresh duplicate row every run, and
-- `ON CONFLICT DO NOTHING` (no explicit target -- it matches any applicable
-- unique index) would never catch it.
--
-- Replace the raw-column unique index with a COALESCE-normalized expression
-- index so a comprobante-level retention's (NULL tipo_factor, NULL tasa)
-- collapses onto one sentinel pair instead of comparing distinct every time.
DROP INDEX pulso.cfdi_taxes_uq;

CREATE UNIQUE INDEX cfdi_taxes_uq ON pulso.cfdi_taxes (
    uuid,
    impuesto,
    COALESCE(tipo_factor, '~'),
    COALESCE(tasa, -1),
    is_retenido
);
