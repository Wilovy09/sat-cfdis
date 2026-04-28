ALTER TABLE public.users
ADD COLUMN IF NOT EXISTS pulso_complete_profile BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS pulso.users (
    id TEXT NOT NULL PRIMARY KEY,
    user_id UUID NOT NULL,
    rfc TEXT NOT NULL,
    clave TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES public.users (id)
);
