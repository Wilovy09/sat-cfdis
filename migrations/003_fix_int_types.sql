-- Fix INTEGER (INT4) columns that are mapped to i64 in Rust → BIGINT (INT8)
ALTER TABLE pulso.sync_jobs ALTER COLUMN found TYPE BIGINT;
ALTER TABLE pulso.cfdis ALTER COLUMN year TYPE BIGINT;
ALTER TABLE pulso.cfdis ALTER COLUMN month TYPE BIGINT;
ALTER TABLE pulso.cfdis ALTER COLUMN xml_available TYPE BIGINT;
