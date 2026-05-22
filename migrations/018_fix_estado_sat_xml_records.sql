-- Migration 018: Extend 017's estado_sat fix to XML-parsed records.
--
-- Migration 017 fixed metadata-only records (xml_available != 1).
-- But extract_estado_from_meta() in etl.rs — used for XML-parsed records —
-- had the same bug: it read v["estado"] instead of v["estadoComprobante"],
-- so xml_available=1 records also ended up with estado_sat='vigente' when
-- they were actually cancelled.
--
-- This migration applies the same metadata backfill without the xml_available
-- restriction, covering all records not yet corrected.

UPDATE pulso.cfdis c
SET estado_sat = LOWER(TRIM(ji_estado))
FROM (
    SELECT DISTINCT ON (uuid)
           uuid,
           TRIM(metadata::json ->> 'estadoComprobante') AS ji_estado
    FROM   pulso.job_invoices
    WHERE  TRIM(metadata::json ->> 'estadoComprobante') IS NOT NULL
      AND  TRIM(metadata::json ->> 'estadoComprobante') <> ''
    ORDER  BY uuid, job_id DESC
) ji
WHERE  c.uuid = ji.uuid
  AND  LOWER(TRIM(ji.ji_estado)) <> c.estado_sat;
