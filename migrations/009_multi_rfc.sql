-- Prevent duplicate RFC per user
ALTER TABLE pulso.users ADD CONSTRAINT users_user_id_rfc_unique UNIQUE (user_id, rfc);

-- Admin flag on platform users
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;
