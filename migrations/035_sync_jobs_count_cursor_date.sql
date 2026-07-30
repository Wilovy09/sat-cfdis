-- Tracks the date the list-count pre-pass is currently scanning, mirroring
-- cursor_date (which tracks the actual download/list-stream pass). Lets the
-- frontend show separate "consultando" (counting) vs "descargando"
-- (downloading) month/year indicators instead of a single blended progress.
ALTER TABLE pulso.sync_jobs ADD COLUMN IF NOT EXISTS count_cursor_date TEXT;
