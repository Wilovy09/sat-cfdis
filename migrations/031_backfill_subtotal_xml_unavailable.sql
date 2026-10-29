-- Backfill subtotal for CFDIs permanently marked as XML-unavailable (xml_available = -1).
-- When the SAT could not serve the XML, subtotal stays NULL and the STORED GENERATED column
-- total_neto_mxn computes to 0, making analytics show $0 for those months.
--
-- Same approximation as migration 013: subtotal = total / tipo_cambio.
-- For MXN invoices (tipo_cambio = 1) this is exact.
-- For FX invoices it restores the original-currency subtotal.
-- Excludes tipo_comprobante IN ('P','N') because payments and nómina are
-- aggregated from their own child tables, not from total_neto_mxn.
UPDATE pulso.cfdis
SET
    subtotal  = total / NULLIF(COALESCE(tipo_cambio, 1.0), 0),
    descuento = 0
WHERE xml_available = -1
  AND subtotal  IS NULL
  AND descuento IS NULL
  AND total     IS NOT NULL
  AND tipo_comprobante NOT IN ('P', 'N');
