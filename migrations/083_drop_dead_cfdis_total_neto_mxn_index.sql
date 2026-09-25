-- PULSO_Plan_Mejoras_SQL.md, punto #1: idx_cfdis_total_neto_mxn has 0 scans in
-- adquiere-test and no query in src/ filters or orders by the raw total_neto_mxn
-- column -- every reference to it is inside SUM(COALESCE(total_neto_mxn_ajustado,0)),
-- a different, already-aggregated column. Confirmed dead by construction (no query
-- shape can use it as written), not just by low traffic on one environment.
--
-- Pure DDL, no calculation changes: dropping an index never changes a query's
-- result set, only how Postgres gets there.
DROP INDEX pulso.idx_cfdis_total_neto_mxn;
