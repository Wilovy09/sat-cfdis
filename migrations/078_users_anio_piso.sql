-- L15-07/AUD-159/DEC-095/DEC-096: the floor of the analysis window is a stored fact per
-- RFC, not a formula evaluated against today's date. "current_year - 3" gives the right
-- answer for every RFC that already exists today, which is exactly the trap: implemented
-- that way, the floor silently drifts forward every January 1st, and years the user
-- already relies on disappear from every report with no warning. Storing it here is what
-- keeps it frozen once set.
--
-- pulso.users carries one row per RFC globally (migration 019's users_rfc_global_unique),
-- so this column lives at the RFC's one home, not duplicated per user/session. DEC-096:
-- "el piso es de todo Pulso, no sólo de Nómina" describes this column's home, not a
-- mandate to wire every module to it in this migration -- only pulso.services.analytics.
-- payroll reads it as of this lote.

ALTER TABLE pulso.users ADD COLUMN IF NOT EXISTS anio_piso INTEGER;

UPDATE pulso.users SET anio_piso = 2023 WHERE anio_piso IS NULL;

ALTER TABLE pulso.users ALTER COLUMN anio_piso SET NOT NULL;
