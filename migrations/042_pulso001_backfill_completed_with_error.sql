-- Retroactive application of the PULSO-001 guard now built into
-- db::jobs::complete() (a job can't stay 'completed' while carrying a
-- non-null error_code). Without this backfill, the invariant only holds for
-- jobs that complete from now on — every job that already slipped through
-- before this fix shipped, including the one that corrupted Axented
-- (bcbf58c2-93b9-46a8-8c2a-87dca870e362), would stay invisible to
-- find_failed_retryable and any 'completed' == 'trustworthy' assumption
-- elsewhere.
--
-- This is a pure status relabel — it does not touch pulso.cfdis. For a job
-- whose cursor_date already reached period_to (true for essentially all 122
-- rows this matches today), gap_detector's continuation logic will find
-- nothing left to cover and mark it superseded on its next pass — it will
-- NOT re-download anything and does NOT repair whatever data those degraded
-- sessions produced. Recovering the actual missing/corrupted CFDIs (Axented's
-- 4,176 xml_available=-1 rows chief among them) is a separate task.
UPDATE pulso.sync_jobs
SET status = 'failed',
    error_msg = COALESCE(error_msg, 'Reclassified from completed by PULSO-001 backfill: job carried an unresolved error_code'),
    updated_at = to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
WHERE status = 'completed'
  AND error_code IS NOT NULL;
