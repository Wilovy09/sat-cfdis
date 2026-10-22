-- PULSO_Plan_Mejoras_SQL.md, punto #8: db/cfdis.rs's months_with_data_direction
-- ("SELECT DISTINCT year, month FROM pulso.cfdis WHERE {rfc_emisor|rfc_receptor} = $1
-- AND NOT is_cancelled") is the query that showed 3.84s live in adquiere-logs.
-- idx_cfdis_receptor_ym/idx_cfdis_emisor_ym already carry year/month, but neither is
-- filtered on is_cancelled, so Postgres still fetches the heap row per match just to
-- apply that filter -- confirmed via EXPLAIN (ANALYZE, BUFFERS) against the RFC grande:
-- Heap Blocks: exact=2209 for the receptor direction alone.
--
-- A partial index matching the WHERE clause exactly turns this into an index-only
-- scan (zero heap fetches, immune to cold cache) -- same pattern already used by
-- idx_cfdis_ppd_receptor/idx_cfdis_ppd_emisor (migration 082).
--
-- Both directions, not just the one that showed up in the log: months_with_data_direction
-- is called for both 'emitidos' (rfc_emisor) and 'recibidos' (rfc_receptor), and
-- months_with_data (same file) ORs both columns together in one query -- migration 082's
-- own comment already flagged "receptor added, emisor counterpart missing" as the exact
-- mistake to avoid; doing only rfc_receptor here would repeat it on a different pair of
-- indexes.
--
-- Pure DDL: doesn't touch months_with_data/months_with_data_direction's SQL text or the
-- Rust that calls them, only gives the planner a cheaper path to the same result set.
CREATE INDEX idx_cfdis_receptor_ym_active
    ON pulso.cfdis (rfc_receptor, year, month)
    WHERE NOT is_cancelled;

CREATE INDEX idx_cfdis_emisor_ym_active
    ON pulso.cfdis (rfc_emisor, year, month)
    WHERE NOT is_cancelled;
