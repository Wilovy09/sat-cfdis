-- Support tables/columns for the gap-detector worker (src/services/gap_detector.rs).
--
-- Context: a `sync_jobs` row that exhausts retry_transient_or_fail's backoff
-- schedule (5min..24h x8) ends up status='failed' forever — nothing ever
-- revisits the date range it was covering. Separately, a day can end up with
-- zero CFDIs even inside a job's range that's marked status='completed' (a
-- transient per-day miss that doesn't fail the overall job). Both produce the
-- same symptom: a calendar day with real business activity but zero rows in
-- pulso.cfdis, invisible from inside Pulso alone — this is exactly what the
-- Nubarium day-gap report (2023-06-28, 2023-12-29, 2024-10-25, 2024-12-02)
-- surfaced from the outside.

-- gap_retry_count: how many times a failed job's leftover range has been
-- auto-requeued as a fresh job. Caps runaway retries on a permanently broken
-- RFC (revoked credentials, etc.) instead of restarting it forever.
ALTER TABLE pulso.sync_jobs
ADD COLUMN IF NOT EXISTS gap_retry_count INTEGER NOT NULL DEFAULT 0;

-- superseded_by: set on a failed job once the gap-detector has spawned a
-- continuation job for its unfinished range (or determined none was needed),
-- so the next sweep doesn't requeue the same failure twice. Not a real FK —
-- the referenced job can itself fail and get superseded again.
ALTER TABLE pulso.sync_jobs
ADD COLUMN IF NOT EXISTS superseded_by TEXT;

CREATE INDEX IF NOT EXISTS idx_sync_jobs_failed_unsuperseded
    ON pulso.sync_jobs (rfc)
    WHERE status = 'failed' AND superseded_by IS NULL;

-- Tracks how far the day-by-day zero-activity scan has progressed per RFC,
-- so each cycle resumes instead of rescanning years of history from scratch.
CREATE TABLE IF NOT EXISTS pulso.gap_scan_progress (
    rfc               TEXT PRIMARY KEY,
    last_scanned_date TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);
