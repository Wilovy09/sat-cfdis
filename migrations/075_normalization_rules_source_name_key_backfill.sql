-- C14-07/DEC-088: a normalization rule created against a generic-RFC (XAXX/XEXX)
-- counterparty used to save the composite "GENERIC_RFC||NAME" key straight into
-- source_rfc -- pulso.cfdi_exclusion and every other consumer compare source_rfc against
-- the bare RFC column, so a composite value there never matched anything. The
-- application-level fix (normalization.rs's create_rule/update_rule) now splits it at
-- write time; this is the one-time backfill for anything saved with the old behavior
-- before this deploy. Idempotent: a source_rfc without "||" is untouched.
UPDATE pulso.normalization_rules
SET source_rfc = split_part(source_rfc, '||', 1),
    source_name_key = NULLIF(
        REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(UPPER(COALESCE(split_part(source_rfc, '||', 2), ''))), '\s+', ' ', 'g'),
            '[^A-Z0-9 &\-]', '', 'g'
        ),
        ''
    )
WHERE source_rfc LIKE '%||%';
