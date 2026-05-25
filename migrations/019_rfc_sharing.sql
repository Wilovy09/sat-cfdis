-- Migration 019: RFC global uniqueness + sharing
-- One owner per RFC globally; other users get read-only access via pulso.rfc_shares.
-- Existing duplicates: earliest ctid row becomes owner, others get soft-deleted + share granted.

BEGIN;

-- ── 1. shares table ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pulso.rfc_shares (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    rfc           TEXT        NOT NULL,
    owner_id      UUID        NOT NULL REFERENCES public.users(id),
    shared_with   UUID        NOT NULL REFERENCES public.users(id),
    invited_email TEXT,
    granted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at    TIMESTAMPTZ
);

-- Unique active share per (rfc, recipient)
CREATE UNIQUE INDEX IF NOT EXISTS rfc_shares_active_unique
    ON pulso.rfc_shares (rfc, shared_with)
    WHERE revoked_at IS NULL;

-- Fast lookup: all RFCs shared with a given user
CREATE INDEX IF NOT EXISTS rfc_shares_shared_with_idx
    ON pulso.rfc_shares (shared_with)
    WHERE revoked_at IS NULL;

-- Fast lookup: all shares granted by owner for an RFC
CREATE INDEX IF NOT EXISTS rfc_shares_owner_idx
    ON pulso.rfc_shares (rfc, owner_id)
    WHERE revoked_at IS NULL;

-- ── 2. Resolve existing duplicate RFC ownership ───────────────────────────────
-- For each RFC with multiple active rows elect the earliest ctid as owner.
-- Displaced users get a share grant so they retain read-only access,
-- then their pulso.users row is soft-deleted.

CREATE TEMP TABLE _rfc_owners AS
SELECT DISTINCT ON (rfc) rfc, user_id AS owner_id
FROM pulso.users
WHERE deleted_at IS NULL
ORDER BY rfc, ctid;

-- Grant shares to non-owners who currently have an active row for the same RFC
INSERT INTO pulso.rfc_shares (rfc, owner_id, shared_with, granted_at)
SELECT u.rfc, o.owner_id, u.user_id, NOW()
FROM pulso.users u
JOIN _rfc_owners o ON o.rfc = u.rfc
WHERE u.deleted_at IS NULL
  AND u.user_id <> o.owner_id
ON CONFLICT DO NOTHING;

-- Soft-delete the duplicate (non-owner) rows
UPDATE pulso.users u
SET deleted_at = NOW()
FROM _rfc_owners o
WHERE o.rfc = u.rfc
  AND u.user_id <> o.owner_id
  AND u.deleted_at IS NULL;

DROP TABLE _rfc_owners;

-- ── 3. Global unique index ────────────────────────────────────────────────────
-- Safe to create now that there are no duplicate active rows.
CREATE UNIQUE INDEX IF NOT EXISTS users_rfc_global_unique
    ON pulso.users (rfc)
    WHERE deleted_at IS NULL;

COMMIT;
