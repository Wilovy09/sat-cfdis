-- Composite indexes for common analytics query patterns
-- Covers: WHERE rfc_emisor = $1 AND (year > $2 OR (year = $2 AND month >= $3)) ...
CREATE INDEX IF NOT EXISTS idx_cfdis_emisor_ym
    ON pulso.cfdis (rfc_emisor, year, month);

CREATE INDEX IF NOT EXISTS idx_cfdis_receptor_ym
    ON pulso.cfdis (rfc_receptor, year, month);

-- ETL queries filter on job_id
CREATE INDEX IF NOT EXISTS idx_job_invoices_job_id
    ON pulso.job_invoices (job_id);

-- Normalization rules filter on owner_rfc + action
CREATE INDEX IF NOT EXISTS idx_norm_rules_owner_action
    ON pulso.normalization_rules (owner_rfc, action);
