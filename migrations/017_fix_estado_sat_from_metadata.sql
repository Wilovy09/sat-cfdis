-- Migration 017: Fix estado_sat for records loaded without XML.
--
-- Bug: from_metadata() read v["estado"] but the PHP SAT scraper outputs
-- v["estadoComprobante"] (e.g. "Cancelado" / "Vigente"). As a result, every
-- metadata-only record (xml_available = -1 or 0) was stored with
-- estado_sat = 'vigente' regardless of its actual SAT status, so cancelled
-- invoices were not being filtered out by analytics queries.
--
-- Fix: for each CFDI whose UUID appears in job_invoices with a metadata row
-- that carries a non-empty estadoComprobante, overwrite estado_sat with that
-- value (lowercased). Records with xml_available = 1 (parsed from real XML)
-- already have the correct estado_sat and are left untouched.

UPDATE pulso.cfdis c
SET estado_sat = LOWER(TRIM(ji_estado))
FROM (
    SELECT DISTINCT ON (uuid)
           uuid,
           TRIM(metadata::json ->> 'estadoComprobante') AS ji_estado
    FROM   pulso.job_invoices
    WHERE  TRIM(metadata::json ->> 'estadoComprobante') IS NOT NULL
      AND  TRIM(metadata::json ->> 'estadoComprobante') <> ''
    ORDER  BY uuid, job_id DESC          -- prefer latest job's metadata
) ji
WHERE  c.uuid = ji.uuid
  AND  c.xml_available != 1             -- don't overwrite XML-parsed records
  AND  LOWER(TRIM(ji.ji_estado)) <> c.estado_sat;  -- skip if already correct
