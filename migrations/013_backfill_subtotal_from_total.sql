-- Backfill subtotal for CFDIs ingested via JSON/SAT export that lacked SubTotal.
-- When subtotal IS NULL but total IS NOT NULL, use total as a proxy for subtotal.
-- This approximates (subtotal - descuento) ≈ total / tipo_cambio for MXN invoices
-- and correctly re-computes total_neto_mxn via the STORED GENERATED column.
--
-- NOTE: This sets subtotal = total / tipo_cambio, descuento = 0 where both were NULL.
-- For MXN invoices (tipo_cambio = 1), this is exact. For FX invoices it restores
-- the original-currency subtotal. Any invoice where subtotal was genuinely different
-- from total (i.e., there was IVA breakdown) will still be approximate — but it is
-- strictly better than 0, which is what the GENERATED column produces when subtotal IS NULL.
UPDATE pulso.cfdis
SET
    subtotal  = total / NULLIF(COALESCE(tipo_cambio, 1.0), 0),
    descuento = 0
WHERE subtotal  IS NULL
  AND descuento IS NULL
  AND total     IS NOT NULL
  AND tipo_comprobante NOT IN ('P', 'N');
