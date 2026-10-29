-- Liga para que un tercero (contador, administración) capture la CIEC o la FIEL de un RFC
-- sin entrar a Pulso. La genera el usuario que analizará la empresa; el RFC queda en SU
-- cuenta. Ver docs/Pulso_Feature_Liga_Credenciales.md.
--
-- token_hash: sha256 del token de la URL (lo único por lo que se busca); token_enc: el
-- token cifrado, para volver a mostrar la misma liga mientras siga vigente. La vigencia
-- (7 días) vive en expires_at; validated_at la cierra antes. Un rechazo del SAT no la
-- cierra: solo suma failed_attempts y, al pasar el tope, bloquea hasta locked_until.
CREATE TABLE pulso.credential_requests (
    id                uuid PRIMARY KEY,
    token_hash        text NOT NULL UNIQUE,
    token_enc         text NOT NULL,
    rfc               text NOT NULL,
    requester_user_id uuid NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    expires_at        timestamptz NOT NULL,
    validated_at      timestamptz,
    credential_type   text CHECK (credential_type IN ('ciec', 'fiel')),
    contact_role      text,
    failed_attempts   integer NOT NULL DEFAULT 0,
    locked_until      timestamptz,
    -- Credencial validada que aún no se pudo ligar a la cuenta (el RFC ya es de otra
    -- cuenta y Pulso todavía no admite dos dueños); cifrada, se usa al habilitarse.
    pending_clave_enc text
);

-- Una sola liga abierta por usuario y RFC (las vencidas se ignoran al consultar).
CREATE INDEX credential_requests_requester_rfc_idx
    ON pulso.credential_requests (requester_user_id, rfc, expires_at DESC);
