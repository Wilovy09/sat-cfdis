-- Add stable created_at column to replace ORDER BY ctid (ctid is unstable after VACUUM/CLUSTER)
ALTER TABLE pulso.users
ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_pulso_users_user_id_created
    ON pulso.users (user_id, created_at)
    WHERE deleted_at IS NULL;
