-- sync_jobs: tracks CIEC/FIEL listing jobs across sessions.
--
-- Compatible with both SQLite and PostgreSQL:
--   - TEXT primary keys (UUID strings)
--   - No AUTOINCREMENT / SERIAL
--   - ISO-8601 strings for all datetimes (stored as TEXT)
--   - Standard INTEGER / TEXT types only
--
-- Status flow:
--   queued → running → completed
--                    → paused_limit  (resume_at = created_at + 24.5 h)
--                    → failed
--                    → cancelled

CREATE SCHEMA IF NOT EXISTS pulso;

CREATE TABLE IF NOT EXISTS pulso.sync_jobs (
    id TEXT PRIMARY KEY NOT NULL,
    job_type TEXT NOT NULL, -- 'list'
    rfc TEXT NOT NULL,
    auth_type TEXT NOT NULL, -- 'ciec' | 'fiel'
    auth_enc TEXT NOT NULL, -- AES-256-GCM, base64(nonce||ct||tag)
    dl_type TEXT NOT NULL, -- 'emitidos' | 'recibidos'
    period_from TEXT NOT NULL, -- '2023-01-01 00:00:00'
    period_to TEXT NOT NULL, -- '2026-03-31 23:59:59'
    cursor_date TEXT, -- last date fully processed; NULL = not started
    found INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued',
    error_msg TEXT,
    resume_at TEXT, -- ISO-8601 UTC; set when paused_limit
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON pulso.sync_jobs (status);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_rfc ON pulso.sync_jobs (rfc);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_resume_at ON pulso.sync_jobs (resume_at);

-- job_invoices: accumulates invoice metadata across resumptions.
-- Rows are INSERT OR IGNORE so duplicates from overlapping windows are safe.
CREATE TABLE IF NOT EXISTS pulso.job_invoices (
    job_id TEXT NOT NULL,
    uuid TEXT NOT NULL,
    metadata TEXT NOT NULL, -- full JSON object from SAT
    PRIMARY KEY (job_id, uuid)
);

CREATE INDEX IF NOT EXISTS idx_job_invoices_job_id ON pulso.job_invoices (job_id);
