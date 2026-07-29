-- Machine-readable classification of error_msg, set by the PHP scraper's
-- classifySatError(): invalid_credentials | captcha_failed | login_not_registered
-- | fiel_login_failed | sat_connection_error | rate_limited | unknown_error.
-- Lets the frontend show correct, distinct copy instead of guessing from
-- free-text error_msg (which previously caused transient SAT connection
-- errors to be misreported as "wrong CIEC password" or "daily limit reached").
ALTER TABLE pulso.sync_jobs ADD COLUMN IF NOT EXISTS error_code TEXT;
