-- Tracks re-download attempts for CFDIs permanently marked xml_available=-1
-- (real XML never obtained, subtotal/currency guessed, payroll detail
-- missing entirely). Existing ETL enrichment (find_needs_enrichment) only
-- ever looks at xml_available=0 — these rows were deliberately excluded from
-- any future retry. This column backs the new bulk re-download worker
-- (src/services/xml_redownload.rs) that specifically targets them, capped so
-- a genuinely-gone-at-SAT UUID doesn't get retried forever.
ALTER TABLE pulso.cfdis
ADD COLUMN IF NOT EXISTS xml_redownload_attempts INTEGER NOT NULL DEFAULT 0;
