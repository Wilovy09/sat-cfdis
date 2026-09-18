-- V14-04: migration 076's TRUNCATE trigger only advanced version on rows that already
-- existed in pulso.rfc_data_version -- an RFC that had never been bumped before (no job,
-- no ETL enrichment, no rule write yet) has no row there, so a TRUNCATE of the rule tables
-- left its cache at the same version its cached entries were written with, i.e. not
-- invalidated at all. That's exactly the guarantee this trigger exists to close.
--
-- Fix: insert-or-bump against pulso.users (the tracked-RFC universe every other
-- invalidation path in this lote already checks via db::users::get_all_with_credentials),
-- not just the rows rfc_data_version happens to already have.
CREATE OR REPLACE FUNCTION pulso.trg_bump_all_rfc_data_versions() RETURNS trigger AS $$
BEGIN
    INSERT INTO pulso.rfc_data_version (rfc, version)
    SELECT rfc, 1 FROM pulso.users WHERE deleted_at IS NULL
    ON CONFLICT (rfc) DO UPDATE
        SET version = pulso.rfc_data_version.version + 1,
            updated_at = now();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
