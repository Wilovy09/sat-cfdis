-- Live running tally from the list-count pre-pass (day-by-day, can take
-- hours for a multi-year range). Distinct from total_expected, which stays
-- the FINAL settled estimate (only written once count-pass fully finishes) —
-- this column lets the frontend show a growing "N facturas encontradas hasta
-- ahora" instead of a static "Calculando..." spinner with no signal at all.
ALTER TABLE pulso.sync_jobs ADD COLUMN IF NOT EXISTS count_progress BIGINT;
