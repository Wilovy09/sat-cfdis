-- Migration 067: L6-10 C4 -- two normalization_rules rows targeting the exact same
-- cfdi_uuid (the comprobante-level exclusion rule type) were only avoided by convention
-- in the UI (it hides "Ajustar CFDI individual" once a receipt already carries a rule) --
-- nothing stopped it in the database or the server. Two such rules on the same receipt
-- double-count it wherever pulso.cfdi_exclusion is consumed (the EBITDA bridge, the
-- excluidos listing, every dashboard that nets out an exclusion).
--
-- action = 'exclude' is the only value normalization_rules.action has ever held for a
-- cfdi_uuid-level rule -- verified against live data (every row with cfdi_uuid IS NOT
-- NULL has action = 'exclude') -- and it's the same discriminant pulso.cfdi_exclusion's
-- cfdi_uuid branch already keys on (migration 062), so scoping the index to it mirrors
-- what the rest of the system already assumes instead of guessing at a new rule.
--
-- Verified against the current data (no pre-existing duplicate (owner_rfc, cfdi_uuid)
-- pair for an active exclusion rule) before writing this, so this index is safe to
-- create as-is -- it will not fail on existing rows.
CREATE UNIQUE INDEX IF NOT EXISTS idx_norm_rules_owner_cfdi_uuid_unique
    ON pulso.normalization_rules (owner_rfc, cfdi_uuid)
    WHERE cfdi_uuid IS NOT NULL AND action = 'exclude';
