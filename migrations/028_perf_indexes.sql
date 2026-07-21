-- Partial index for PPD outstanding balance query in hallazgos.
-- Covers: WHERE rfc_emisor=$1 AND tipo_comprobante='I' AND metodo_pago='PPD' AND NOT is_cancelled
CREATE INDEX IF NOT EXISTS idx_cfdis_ppd_emisor
    ON pulso.cfdis (rfc_emisor)
    WHERE tipo_comprobante = 'I'
      AND metodo_pago = 'PPD'
      AND NOT is_cancelled;

-- Partial index for job_invoices ETL discovery: find unprocessed rows without
-- scanning the entire table. Requires marking rows as processed after ETL.
-- NOTE: this is a prerequisite for the etl_processed column (added separately).
-- For now, speed up the LEFT JOIN anti-join with a covering index.
CREATE INDEX IF NOT EXISTS idx_job_invoices_uuid_jobid
    ON pulso.job_invoices (uuid, job_id);
