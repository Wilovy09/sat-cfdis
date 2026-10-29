-- Distinguishes "SAT confirmed the status" from "SAT's response didn't include
-- this UUID" for the estado_sat recheck worker. Previously both cases stamped
-- estado_sat_checked_at the same way, so an inconclusive check (empty/missing
-- response) permanently silenced an invoice once it fell outside the recheck
-- worker's recent-days window — a single no-op response could get stuck forever.
-- estado_sat_check_attempts counts consecutive misses; only once it hits the
-- worker's give-up threshold does a miss get treated like a real check.
ALTER TABLE pulso.cfdis
ADD COLUMN IF NOT EXISTS estado_sat_check_attempts INTEGER NOT NULL DEFAULT 0;

-- One-time backfill: every currently-cancelled invoice was checked at most
-- once under the old logic, where an inconclusive SAT response (UUID missing
-- from the response) was indistinguishable from a confirmed "still cancelled"
-- — so anything older than the worker's recent-days window got permanently
-- stuck regardless of which one actually happened. Reset the clock for all of
-- them so every one gets a fresh look under the new attempts-aware logic,
-- automatically, without a manual DB poke per invoice.
UPDATE pulso.cfdis SET estado_sat_checked_at = NULL WHERE is_cancelled;
