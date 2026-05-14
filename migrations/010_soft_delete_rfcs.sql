-- Soft delete support for user RFCs
ALTER TABLE pulso.users ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- Replace hard unique constraint (from 009) with a partial unique index
-- that only enforces uniqueness among active (non-deleted) rows.
ALTER TABLE pulso.users DROP CONSTRAINT IF EXISTS users_user_id_rfc_unique;
CREATE UNIQUE INDEX IF NOT EXISTS users_user_id_rfc_active_unique
  ON pulso.users (user_id, rfc)
  WHERE deleted_at IS NULL;
