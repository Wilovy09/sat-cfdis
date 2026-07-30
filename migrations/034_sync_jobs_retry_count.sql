-- Bounded auto-retry counter for transient (non-SAT) sync failures — PHP
-- worker idle-timeout or crash before completion. Lets the worker retry
-- automatically through the existing paused_limit/resume path a few times
-- before giving up and marking the job permanently 'failed', so a genuinely
-- broken RFC still surfaces "Descarga fallida" instead of retrying forever.
ALTER TABLE pulso.sync_jobs ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
