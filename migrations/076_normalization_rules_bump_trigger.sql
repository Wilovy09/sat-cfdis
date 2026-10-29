-- C14-01/DEC-083/AUD-142/AUD-143: invalidation lives with the DATA, not with whichever
-- Rust code path wrote it. Six write statements over the two rule tables (three
-- counterparty, three payroll) fed pulso.cfdi_exclusion/pulso.nomina_normalizada without
-- ever advancing rfc_data_version, so a rule create/edit/delete changed real numbers
-- while every cached analytics response kept serving the pre-rule figures until the next
-- deploy. A trigger in the SAME transaction as the write is what a seventh write path
-- someone adds later can't forget, and what guarantees no reader ever sees the rule
-- change without the invalidation (or vice versa) if the transaction rolls back.
--
-- Same operation as services::response_cache::bump_version (Rust) -- an upsert that
-- advances rfc_data_version.version by one, creating the row at version 1 if it doesn't
-- exist yet. Kept as one definition per side (SQL here, Rust there), each documented in
-- the other, per DEC-084's "one definition per number".
CREATE OR REPLACE FUNCTION pulso.bump_rfc_data_version(p_rfc TEXT) RETURNS void AS $$
BEGIN
    INSERT INTO pulso.rfc_data_version (rfc, version)
    VALUES (p_rfc, 1)
    ON CONFLICT (rfc) DO UPDATE
        SET version = pulso.rfc_data_version.version + 1,
            updated_at = now();
END;
$$ LANGUAGE plpgsql;

-- Row-level: fires per INSERT/UPDATE/DELETE, bumps owner_rfc. An UPDATE that changes
-- owner_rfc (moving a rule to a different owner) bumps both the old and the new owner --
-- both sides' cached figures just changed.
CREATE OR REPLACE FUNCTION pulso.trg_bump_rfc_data_version() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pulso.bump_rfc_data_version(OLD.owner_rfc);
        RETURN OLD;
    END IF;

    PERFORM pulso.bump_rfc_data_version(NEW.owner_rfc);
    IF TG_OP = 'UPDATE' AND OLD.owner_rfc IS DISTINCT FROM NEW.owner_rfc THEN
        PERFORM pulso.bump_rfc_data_version(OLD.owner_rfc);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Statement-level: TRUNCATE fires no row-level trigger and carries no OLD/NEW to target a
-- specific RFC with, so this bumps every RFC that already has a version row instead of
-- leaving TRUNCATE as a documented gap (trap 3) -- nobody truncates these tables today,
-- but the guarantee holds even if that changes. One UPDATE, not a cost per row truncated.
CREATE OR REPLACE FUNCTION pulso.trg_bump_all_rfc_data_versions() RETURNS trigger AS $$
BEGIN
    UPDATE pulso.rfc_data_version SET version = version + 1, updated_at = now();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_normalization_rules_bump_version
    AFTER INSERT OR UPDATE OR DELETE ON pulso.normalization_rules
    FOR EACH ROW EXECUTE FUNCTION pulso.trg_bump_rfc_data_version();

CREATE TRIGGER trg_normalization_rules_truncate_bump_version
    AFTER TRUNCATE ON pulso.normalization_rules
    FOR EACH STATEMENT EXECUTE FUNCTION pulso.trg_bump_all_rfc_data_versions();

CREATE TRIGGER trg_payroll_normalization_rules_bump_version
    AFTER INSERT OR UPDATE OR DELETE ON pulso.payroll_normalization_rules
    FOR EACH ROW EXECUTE FUNCTION pulso.trg_bump_rfc_data_version();

CREATE TRIGGER trg_payroll_normalization_rules_truncate_bump_version
    AFTER TRUNCATE ON pulso.payroll_normalization_rules
    FOR EACH STATEMENT EXECUTE FUNCTION pulso.trg_bump_all_rfc_data_versions();
