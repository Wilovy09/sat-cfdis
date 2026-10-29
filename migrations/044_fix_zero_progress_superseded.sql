-- Backfill for the gap_detector bug described in H-1/H-8 of the 2026-08-14
-- Nubarium/Axented re-audit: `requeue_failed_jobs` trusted `cursor_date` to
-- mean "completed through this day" even for jobs that failed before
-- processing a single day (found = 0), where cursor_date can still sit on
-- the job's own period_from. next_day(period_from) > period_to for a
-- single-day job reads as "fully covered", so 163 jobs platform-wide got
-- marked superseded='n/a-fully-covered' without a single day actually having
-- been retried — exactly the "163 failed jobs with no second attempt" the
-- audit found empirically, just visible here as our own bookkeeping rather
-- than a missing retry mechanism.
--
-- The code fix (src/services/gap_detector.rs) stops this from recurring by
-- ignoring cursor_date whenever found = 0. This resets the ones already
-- wrongly closed out so the next cycle re-evaluates them under the fixed
-- logic and actually requeues their (still fully uncovered) date ranges.
UPDATE pulso.sync_jobs
SET superseded_by = NULL
WHERE superseded_by = 'n/a-fully-covered'
  AND found = 0;
