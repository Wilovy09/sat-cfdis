ALTER TABLE pulso.normalization_rules ADD COLUMN IF NOT EXISTS cfdi_uuid TEXT;
CREATE INDEX IF NOT EXISTS idx_norm_rules_cfdi_uuid ON pulso.normalization_rules(cfdi_uuid) WHERE cfdi_uuid IS NOT NULL;
