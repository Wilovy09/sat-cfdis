-- Normalize all UUIDs to uppercase so JOIN in ETL always matches
UPDATE pulso.job_invoices SET uuid = UPPER(uuid) WHERE uuid != UPPER(uuid);
UPDATE pulso.cfdis SET uuid = UPPER(uuid) WHERE uuid != UPPER(uuid);
