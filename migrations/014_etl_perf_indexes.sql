-- Performance indexes for ETL enrichment queries.
-- The enrichment loop does:
--   JOIN cfdis c ON c.uuid = ji.uuid WHERE c.xml_available = 0
-- xml_available partial index speeds up both the enrichment scan and the
-- jobs_needing_enrichment DISTINCT query.
CREATE INDEX IF NOT EXISTS idx_cfdis_xml_available
    ON pulso.cfdis (xml_available)
    WHERE xml_available = 0;

-- Composite index for the enrichment inner loop:
--   WHERE ji.job_id = $1 AND c.xml_available = 0
-- Covers the job_id filter on job_invoices side.
CREATE INDEX IF NOT EXISTS idx_job_invoices_uuid
    ON pulso.job_invoices (uuid);
