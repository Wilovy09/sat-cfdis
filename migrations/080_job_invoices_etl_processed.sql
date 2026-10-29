-- AUD-XXX (found via adquiere-logs, live prod evidence): jobs_needing_etl's
-- `LEFT JOIN pulso.cfdis c ON c.uuid = ji.uuid WHERE c.uuid IS NULL` anti-join runs every
-- 30s in etl_worker's background loop (src/services/etl.rs), unscoped by RFC or recency --
-- it re-checks EVERY job_invoices row ever inserted against cfdis, forever. Measured live
-- at 2-11 seconds per run, returning 0 rows (nothing new to do) -- that's not occasional
-- slowness, it's a standing cost on every tick that grows with total history, and it holds
-- a DB connection the whole time, starving concurrent dashboard queries of pool capacity.
-- Migration 028's own comment already flagged this as needing exactly this column
-- ("NOTE: this is a prerequisite for the etl_processed column (added separately)") --
-- this is that follow-up.
--
-- NOTE: the backfill UPDATE below does one full anti-join pass (the same shape as the
-- slow query it's replacing) to mark already-ETL'd rows -- expect this migration itself to
-- take a while on first deploy. That's a one-time cost; every run after this is bounded by
-- the partial index below instead of a full-table scan.

ALTER TABLE pulso.job_invoices ADD COLUMN IF NOT EXISTS etl_processed BOOLEAN NOT NULL DEFAULT false;

UPDATE pulso.job_invoices ji
SET etl_processed = true
FROM pulso.cfdis c
WHERE c.uuid = ji.uuid
  AND NOT ji.etl_processed;

-- Bounded by "still pending" rows only, not all of job_invoices -- stays small and cheap
-- regardless of how much history accumulates, unlike the old full-table anti-join.
CREATE INDEX IF NOT EXISTS idx_job_invoices_etl_pending
    ON pulso.job_invoices (job_id)
    WHERE NOT etl_processed;
