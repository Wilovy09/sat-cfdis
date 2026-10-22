-- Support periodic re-verification of estado_sat for invoices flagged cancelled.
-- SAT lets the receptor reject a cancellation request within ~72h, reverting the
-- CFDI back to "Vigente" — but nothing in the pipeline ever re-polls SAT after the
-- initial scrape, so a rejected cancellation stays wrongly marked cancelled forever,
-- silently underreporting revenue. estado_sat_checked_at lets a background worker
-- track which cancelled invoices still need a fresh look.
ALTER TABLE pulso.cfdis
ADD COLUMN IF NOT EXISTS estado_sat_checked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_cfdis_cancelled_recheck
    ON pulso.cfdis (estado_sat_checked_at)
    WHERE is_cancelled;
