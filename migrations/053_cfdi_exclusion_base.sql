-- Migration 053: L3-01 (shared comprobante-exclusion base) + L3-02 (generic-RFC name key).
--
-- Per PULSO_Correcciones_Lote3: 11 sites today each reimplement "is this comprobante
-- excluded for this owner" with a different subset of the same rules, and two of the
-- eleven are wrong (AUD-012's broken AND/OR precedence in hallazgos.rs, AUD-020/L3-17's
-- NULL-column misread in summary.rs). L3-06 adds 30 more reads across 6 files that today
-- apply no exclusion at all. One base, everyone reads it.
--
-- pulso.cfdi_exclusion: one row per (owner_rfc, uuid) that IS excluded for that owner,
-- carrying the matching rule's id so callers that need rule metadata (list_excluded_cfdis)
-- can join back to normalization_rules instead of re-deriving the match themselves.
--   - UUID rule (cfdi_uuid IS NOT NULL): case-insensitive match, no owner/direction
--     re-check beyond nr.owner_rfc itself -- a specific-document rule excludes it
--     outright for that owner, matching the existing (correct) behavior in quarterly.rs.
--   - Counterparty rule: requires the comprobante to actually involve the owner on the
--     matching side (c.rfc_emisor = nr.owner_rfc for an 'emitidos' rule, c.rfc_receptor =
--     nr.owner_rfc for 'recibidos') -- this is the check AUD-012 and L3-04 both lacked,
--     letting a rule "leak" onto comprobantes that don't belong to its owner at all
--     (L3-05's Adquiere Latam payroll leak, L3-04's whole-database leak).
--   - L3-02: for a counterparty rule on a generic SAT RFC (XAXX/XEXX), source_name_key
--     narrows the match to the one real counterparty behind that RFC. NULL (the case for
--     every rule on an ordinary RFC, and the only value ever written for one) means
--     "match by RFC alone" -- today's behavior, unchanged.
CREATE OR REPLACE VIEW pulso.cfdi_exclusion AS
SELECT DISTINCT nr.id AS rule_id, nr.owner_rfc, c.uuid
FROM pulso.normalization_rules nr
JOIN pulso.cfdis c ON (
    (nr.cfdi_uuid IS NOT NULL AND UPPER(nr.cfdi_uuid) = UPPER(c.uuid))
    OR (nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL AND (
        (nr.dl_type IN ('emitidos', 'ambos')
         AND c.rfc_emisor = nr.owner_rfc AND c.rfc_receptor = nr.source_rfc
         AND (nr.source_name_key IS NULL OR nr.source_name_key =
              REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_receptor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g')))
        OR (nr.dl_type IN ('recibidos', 'ambos')
         AND c.rfc_receptor = nr.owner_rfc AND c.rfc_emisor = nr.source_rfc
         AND (nr.source_name_key IS NULL OR nr.source_name_key =
              REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_emisor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g')))
    ))
)
WHERE nr.action = 'exclude';

-- L3-02: the composite match key. NULL for every rule on an ordinary RFC (never backfilled
-- below) and for a generic-RFC rule whose captured name normalizes to blank -- both cases
-- collapse to "match by RFC alone", same as cp_key_expr()'s row-level counterparty key in
-- summary.rs, which this mirrors exactly (REGEXP_REPLACE pair, same pattern, same order).
ALTER TABLE pulso.normalization_rules ADD COLUMN IF NOT EXISTS source_name_key TEXT;

-- Backfill ONLY the generic-RFC rules that already exist (deliberately NOT ordinary RFCs --
-- narrowing an ordinary-RFC rule by name would silently shrink coverage nobody asked to
-- shrink, and the same real counterparty can appear under more than one normalized name).
UPDATE pulso.normalization_rules
SET source_name_key = NULLIF(
    REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(source_name, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'),
    ''
)
WHERE source_rfc IN ('XAXX010101000', 'XEXX010101000')
  AND cfdi_uuid IS NULL
  AND source_name_key IS NULL;
