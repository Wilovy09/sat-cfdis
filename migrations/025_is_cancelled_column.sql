-- Stored generated boolean replacing the unindexable UPPER(COALESCE(estado_sat,'')) NOT LIKE '%CANCEL%' pattern
ALTER TABLE pulso.cfdis
ADD COLUMN IF NOT EXISTS is_cancelled BOOLEAN GENERATED ALWAYS AS (
    UPPER(COALESCE(estado_sat, '')) LIKE '%CANCEL%'
) STORED;

CREATE INDEX IF NOT EXISTS idx_cfdis_not_cancelled
    ON pulso.cfdis (is_cancelled)
    WHERE NOT is_cancelled;
