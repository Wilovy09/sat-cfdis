-- Re-run of migration 016's fix. That migration corrected subtotal = total
-- (migration 013's mistake) to subtotal = total / 1.16 for xml_available = -1
-- rows, one time. But `mark_xml_unavailable_for_job` (src/db/cfdis.rs) — the
-- live code that marks a CFDI permanently XML-unavailable after repeated SAT
-- download failures — kept using the uncorrected formula the whole time, so
-- every invoice that failed download after 016 ran got the same ~16%
-- overstatement again. Confirmed against a fresh SAT reference export for
-- RFC ADC101206334 (Axented): 546 invoices inflated by exactly 1.16x, all with
-- xml_available = -1 and subtotal == total. Platform-wide this fingerprint
-- matches 3,685 rows across ~$13.1M MXN of overstated net revenue.
--
-- Same caveat as 016: CFDIs with 0% or 8% IVA get slightly under-corrected;
-- that error is smaller than leaving them at 100% of total.
UPDATE pulso.cfdis
SET subtotal = ROUND(
    (total / NULLIF(COALESCE(tipo_cambio, 1.0), 0) / 1.16)::numeric,
    6
)
WHERE xml_available = -1
  AND tipo_comprobante NOT IN ('P', 'N')
  AND total     IS NOT NULL
  AND subtotal  IS NOT NULL
  AND ABS(subtotal - total / NULLIF(COALESCE(tipo_cambio, 1.0), 0)) < 0.01;
