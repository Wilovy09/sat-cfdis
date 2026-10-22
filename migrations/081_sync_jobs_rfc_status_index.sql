-- Found via adquiere-logs, live prod evidence: get_active_for_rfc / get_latest_for_rfc_direction
-- (src/db/jobs.rs) -- "WHERE rfc = $1 AND status IN (...) ORDER BY ..., created_at DESC LIMIT 1"
-- -- back the sync_status endpoint (the Dashboard's "still syncing" banner), measured at
-- 3.9s and 1.9s live. Only single-column idx_sync_jobs_rfc / idx_sync_jobs_status existed
-- (migration 001) -- no composite covering (rfc, status) together, so the planner had to
-- pick one column to index-scan and filter the other in-line.
CREATE INDEX IF NOT EXISTS idx_sync_jobs_rfc_status
    ON pulso.sync_jobs (rfc, status);
