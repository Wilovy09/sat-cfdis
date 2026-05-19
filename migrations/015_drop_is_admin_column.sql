-- Admin role now comes from public.user_roles → catalogs.roles (name = 'admin').
-- The is_admin column on public.users is no longer used.
ALTER TABLE public.users DROP COLUMN IF EXISTS is_admin;
