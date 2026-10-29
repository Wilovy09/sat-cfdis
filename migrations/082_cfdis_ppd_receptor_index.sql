-- Found via adquiere-logs, live prod evidence + EXPLAIN ANALYZE: cashflow::get's PPD
-- outstanding-balance query (src/services/analytics/cashflow.rs) is measurably slower for
-- the receptor (egresos) direction than emisor (1.6-2.4s vs 1.2-1.6s). Migration 028 added
-- idx_cfdis_ppd_emisor for hallazgos' own PPD query but never the receptor counterpart --
-- confirmed live: without it, a receptor-side query falls back to the emisor partial
-- index (still narrower than no index at all, since it shares the same tipo_comprobante/
-- metodo_pago/is_cancelled filter), bitmap-scanning candidate rows across EVERY RFC
-- instead of just this one, then discarding the rest.
--
-- NOTE: this closes the indexing gap, but EXPLAIN showed the dominant cost (~470ms of
-- ~750ms measured) is pulso.cfdi_cobro_estado's own per-row correlated subquery (the
-- payment-status lookup), not indexing -- exactly the case migration 052's own comment
-- said to watch for ("materialize if this shows up in slow-query logs"). This index
-- narrows the candidate set the subquery runs against; it doesn't remove the subquery.
CREATE INDEX IF NOT EXISTS idx_cfdis_ppd_receptor
    ON pulso.cfdis (rfc_receptor)
    WHERE tipo_comprobante = 'I'
      AND metodo_pago = 'PPD'
      AND NOT is_cancelled;
