CREATE TABLE IF NOT EXISTS pulso.rfc_fiel (
    rfc          TEXT PRIMARY KEY,
    cert_s3_key  TEXT NOT NULL,
    key_s3_key   TEXT NOT NULL,
    password_enc TEXT NOT NULL,
    uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
