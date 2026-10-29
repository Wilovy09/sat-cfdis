-- Response cache for expensive analytics endpoints. One row per
-- (rfc, endpoint, params_key); invalidated by data_version instead of a TTL, since
-- ingestion is event-driven (a sync job finding new invoices, the cancellation-recheck
-- worker flipping an estado_sat) rather than a fixed batch schedule -- see
-- src/services/response_cache.rs.

-- Bumped whenever new data actually lands for an RFC. A cached row is only served while
-- its data_version still matches this table's current value for that RFC.
CREATE TABLE IF NOT EXISTS pulso.rfc_data_version (
    rfc        TEXT PRIMARY KEY,
    version    BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pulso.endpoint_response_cache (
    rfc          TEXT NOT NULL,
    endpoint     TEXT NOT NULL,
    params_key   TEXT NOT NULL,
    data_version BIGINT NOT NULL,
    payload      JSONB NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (rfc, endpoint, params_key)
);
