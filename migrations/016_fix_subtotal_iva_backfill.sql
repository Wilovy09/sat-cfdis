-- Migration 016: Fix subtotals incorrectly backfilled by migration 013.
--
-- Migration 013 set subtotal = total / tipo_cambio for CFDIs that lacked SubTotal.
-- For MXN invoices (tipo_cambio = 1) this means subtotal = total, which INCLUDES IVA.
-- The CFDI standard says SubTotal is the pre-IVA base, so this inflates total_neto_mxn
-- by ~16% for standard-rate invoices.
--
-- This migration corrects those rows by applying the standard 16% IVA divisor.
-- Rows are identified by: xml_available = -1 (no XML ever parsed) AND
-- subtotal ≈ total / tipo_cambio (fingerprint of the migration 013 backfill).
--
-- CFDIs with 0% or 8% IVA will be slightly under-corrected; the error is smaller
-- than leaving them at 100% of total. Rows with real XML (xml_available = 1) are
-- untouched — their subtotal came from the actual XML node.

UPDATE pulso.cfdis
SET subtotal = ROUND(
    (total / NULLIF(COALESCE(tipo_cambio, 1.0), 0) / 1.16)::numeric,
    6
)
WHERE xml_available = -1
  AND tipo_comprobante IN ('I', 'E', 'T')
  AND total     IS NOT NULL
  AND subtotal  IS NOT NULL
  AND ABS(subtotal - total / NULLIF(COALESCE(tipo_cambio, 1.0), 0)) < 0.01;
