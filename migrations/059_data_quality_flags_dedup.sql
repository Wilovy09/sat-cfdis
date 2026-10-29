-- Migration 059: L4-06 -- dedupe pulso.data_quality_flags, one row per detection.
--
-- The table had no uniqueness constraint, so reprocessing the same payment complement
-- (the admin endpoint and the XML-redownload worker both can) kept appending rows,
-- inflating a COUNT(*) over time. Table is empty today (no ingest has run since the
-- column/table were added), so there's nothing to dedupe first.
CREATE UNIQUE INDEX IF NOT EXISTS idx_data_quality_flags_dedup
    ON pulso.data_quality_flags (flag_type, payment_uuid);
