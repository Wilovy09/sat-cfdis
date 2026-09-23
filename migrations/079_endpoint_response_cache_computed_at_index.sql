-- The cleanup sweep's stale-process check (response_cache.rs's run_cleanup, "computed_at
-- < cutoff") used to share one OR-ed WHERE clause with the stale-data-version check (a
-- join against pulso.rfc_data_version). Postgres can't use an index for that combined
-- predicate -- one side depends on a joined table's current value, so the planner falls
-- back to a full Seq Scan of endpoint_response_cache regardless of what's indexed here --
-- confirmed live via EXPLAIN (ANALYZE, BUFFERS) before this change. run_cleanup now issues
-- the two checks as separate DELETEs; this index is what makes the computed_at one cheap.
--
-- Single-column, not composite: the only predicate it needs to serve is "computed_at <
-- $1" on its own -- endpoint/params_key/rfc play no part in that check, and the existing
-- PK already covers every (rfc, endpoint, params_key) lookup.
CREATE INDEX IF NOT EXISTS idx_endpoint_response_cache_computed_at
    ON pulso.endpoint_response_cache (computed_at);
