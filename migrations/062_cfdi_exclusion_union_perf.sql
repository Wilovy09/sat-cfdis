-- Migration 062: L4-03 / live incident -- pulso.cfdi_exclusion's OR-joined query plan.
--
-- Production incident (2026-08-28): every payroll/hallazgos query against
-- pulso.nomina_normalizada was taking 50-300+ seconds for the large RFC, exhausting
-- the connection pool and starving requests for every RFC.
--
-- Root cause found via EXPLAIN: cfdi_exclusion's single JOIN condition is a
-- disjunction across two unrelated match strategies --
--   (cfdi_uuid match) OR (rfc_emisor/rfc_receptor + dl_type + name_key + period match)
-- -- and Postgres cannot satisfy an OR spanning different columns with one index
-- lookup. The planner fell back to a nested loop with a full sequential scan of
-- ALL of pulso.cfdis (64,680 rows platform-wide, not scoped to the owner) for
-- every matching normalization_rules row, re-evaluating the regex-based name-key
-- comparison on every single one.
--
-- Fixed by splitting the OR into three UNIONed branches, each with a clean
-- AND-only join condition the planner can drive off pulso.normalization_rules
-- (few rows) using idx_cfdis_rfc_emisor/idx_cfdis_rfc_receptor, instead of a full
-- scan gated by a filter. Semantics are unchanged -- same three cases the old OR
-- covered, verified against the exact same control values L3-01/L3-02 were
-- verified against originally (66/15 comprobantes for the test RFC, the foreign-owned
-- client's 63/4,308,030).
CREATE OR REPLACE VIEW pulso.cfdi_exclusion AS
SELECT nr.id AS rule_id, nr.owner_rfc, c.uuid
FROM pulso.normalization_rules nr
JOIN pulso.cfdis c ON UPPER(nr.cfdi_uuid) = UPPER(c.uuid)
WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NOT NULL

UNION

SELECT nr.id AS rule_id, nr.owner_rfc, c.uuid
FROM pulso.normalization_rules nr
JOIN pulso.cfdis c
  ON c.rfc_emisor = nr.owner_rfc AND c.rfc_receptor = nr.source_rfc
WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL
  AND nr.dl_type IN ('emitidos', 'ambos')
  AND (nr.source_name_key IS NULL OR nr.source_name_key =
       REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_receptor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
  AND (nr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= nr.period_start)
  AND (nr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= nr.period_end)

UNION

SELECT nr.id AS rule_id, nr.owner_rfc, c.uuid
FROM pulso.normalization_rules nr
JOIN pulso.cfdis c
  ON c.rfc_receptor = nr.owner_rfc AND c.rfc_emisor = nr.source_rfc
WHERE nr.action = 'exclude' AND nr.cfdi_uuid IS NULL AND nr.source_rfc IS NOT NULL
  AND nr.dl_type IN ('recibidos', 'ambos')
  AND (nr.source_name_key IS NULL OR nr.source_name_key =
       REGEXP_REPLACE(REGEXP_REPLACE(TRIM(UPPER(COALESCE(c.nombre_emisor, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g'))
  AND (nr.period_start IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) >= nr.period_start)
  AND (nr.period_end IS NULL OR (c.year::text || '-' || LPAD(c.month::text, 2, '0')) <= nr.period_end);
