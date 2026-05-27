-- Speeds up the enrichment inner loop when a job is nearly done.
-- find_needs_enrichment joins job_invoices(job_id=$1) with cfdis(xml_available=0).
-- Without this, Postgres scans all job_invoices rows (potentially thousands) to
-- find the few remaining unenriched ones. With this partial index on uuid, Postgres
-- can probe the small set of unenriched cfdis and join outward instead.
CREATE INDEX IF NOT EXISTS idx_cfdis_uuid_xml0
    ON pulso.cfdis (uuid)
    WHERE xml_available = 0;
