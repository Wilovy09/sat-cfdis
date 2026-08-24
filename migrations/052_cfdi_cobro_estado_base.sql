-- Migration 052: L2-01 — shared collection-status base, and AUD-009's as_of_cutoff fix.
--
-- Four files (payments.rs, counterparties.rs, hallazgos.rs, cashflow.rs) each reimplemented
-- "how much of this invoice has been collected" with a different subset of the same rules
-- (currency conversion, cancelled-complement exclusion, credit notes). Per
-- PULSO_Correcciones_Lote2 (L2-01), payments.rs is the closest to correct; this view
-- generalizes it and adds what payments.rs itself was missing (tipo_relacion '03').
--
-- pulso.cfdi_cobro_estado: one row per non-cancelled tipo_comprobante='I' cfdi.
--   - PUE: collected at emission (DEC-001) -> pagado_mxn = total_mxn.
--   - PPD: pagado_mxn from cfdi_payment_docs, converted via tipo_cambio_dr (divide) and
--     tipo_cambio_p (multiply) -- TC-2's direction, copied verbatim from payments.rs.
--     Cancelled payment complements excluded.
--   - acreditado_mxn: credit notes (tipo_relacion '01') AND returns ('03', AUD-008) issued
--     against the invoice, tipo E, not cancelled. '04' (sustitución) is deliberately never
--     read here -- DEC-023, no logic.
--   - saldo_mxn: GREATEST(total - pagado - acreditado, 0) -- clamped, an invoice can't show
--     a negative balance from overpayment.
--   - dias_antiguedad: measured from the last complete calendar month's last day, not from
--     CURRENT_DATE (DEC-024 / L2-05) -- reproducible across two runs on different days.
--   - NO period/month cutoff here by design: this is per-invoice state; each consumer
--     decides its own universe (L2-02 bounds it, L2-03 deliberately doesn't).
--
-- Structure decision (Lote2 risk #1): plain view with correlated scalar subqueries per
-- invoice, backed by idx_cfdi_payment_docs_invoice (invoice_uuid) for the payment lookup and
-- the cfdi_relacionados PK (source_uuid, tipo_relacion, related_uuid) for the credit-note
-- lookup -- both index-scan, not sequential. At today's volume (~19k payment_docs rows)
-- this is fast enough and the planner can push the caller's rfc_emisor/rfc_receptor predicate
-- down through the view since it's not materialized. Revisit as a materialized view (refreshed
-- on a schedule) if payment_docs grows into the millions and this shows up in slow-query logs --
-- not before, since a materialized view trades this correctness fix for staleness.
--
-- pulso.rfc_as_of_cutoff: AUD-009's fix, centralized so all four consumers agree. The old
-- as_of_cutoff (last month whose invoice count clears 15% of the median) protects against a
-- partially-synced month, but doesn't protect against the CURRENT month, which always fails
-- that density check for the wrong reason (not yet finished, not badly synced). Fixed to
-- LEAST(density_cutoff, last_complete_calendar_month).
CREATE OR REPLACE VIEW pulso.rfc_as_of_cutoff AS
WITH monthly AS (
    SELECT rfc_emisor AS owner_rfc, 'emitidos' AS direccion, year, month, COUNT(*) AS cnt
    FROM pulso.cfdis
    WHERE tipo_comprobante = 'I' AND NOT is_cancelled AND dl_type IN ('emitidos', 'ambos')
    GROUP BY 1, 3, 4
    UNION ALL
    SELECT rfc_receptor, 'recibidos', year, month, COUNT(*)
    FROM pulso.cfdis
    WHERE tipo_comprobante = 'I' AND NOT is_cancelled AND dl_type IN ('recibidos', 'ambos')
    GROUP BY 1, 3, 4
),
baseline AS (
    SELECT owner_rfc, direccion, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cnt)::float8 AS median
    FROM monthly GROUP BY owner_rfc, direccion
),
density_cutoff AS (
    SELECT m.owner_rfc, m.direccion, MAX(m.year * 100 + m.month) AS density_ym
    FROM monthly m
    JOIN baseline b ON b.owner_rfc = m.owner_rfc AND b.direccion = m.direccion
    WHERE m.cnt::float8 >= GREATEST(b.median * 0.15, 3.0)
    GROUP BY m.owner_rfc, m.direccion
),
last_complete_month AS (
    SELECT (EXTRACT(YEAR FROM date_trunc('month', CURRENT_DATE) - interval '1 month')::int * 100
          + EXTRACT(MONTH FROM date_trunc('month', CURRENT_DATE) - interval '1 month')::int) AS ym
)
SELECT dc.owner_rfc, dc.direccion,
       LEAST(dc.density_ym, lcm.ym) AS as_of_ym
FROM density_cutoff dc, last_complete_month lcm;

CREATE OR REPLACE VIEW pulso.cfdi_cobro_estado AS
SELECT
    inv.uuid,
    inv.rfc_emisor,
    inv.rfc_receptor,
    inv.dl_type,
    inv.year,
    inv.month,
    inv.fecha_emision,
    inv.metodo_pago,
    COALESCE(inv.total_mxn, 0)::float8 AS total_mxn,
    pago.pagado_mxn,
    pago.acreditado_mxn,
    GREATEST(COALESCE(inv.total_mxn, 0)::float8 - pago.pagado_mxn - pago.acreditado_mxn, 0) AS saldo_mxn,
    (
        SELECT MAX(cp.fecha_pago::date)
        FROM pulso.cfdi_payment_docs pd
        JOIN pulso.cfdi_payments cp
          ON cp.payment_uuid = pd.payment_uuid AND cp.pago_num = pd.pago_num
        JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
        WHERE pd.invoice_uuid = inv.uuid AND NOT comp.is_cancelled
          AND cp.fecha_pago IS NOT NULL AND cp.fecha_pago::date >= inv.fecha_emision::date
    ) AS ultimo_pago_fecha,
    ((date_trunc('month', CURRENT_DATE) - interval '1 day')::date - inv.fecha_emision::date) AS dias_antiguedad
FROM pulso.cfdis inv
CROSS JOIN LATERAL (
    SELECT
        CASE WHEN COALESCE(inv.metodo_pago, 'PUE') != 'PPD' THEN COALESCE(inv.total_mxn, 0)::float8
             ELSE COALESCE((
                 SELECT SUM(pd.imp_pagado::float8
                            / COALESCE(NULLIF(pd.tipo_cambio_dr::float8, 0), 1)
                            * COALESCE(NULLIF(cp.tipo_cambio_p::float8,  0), 1))
                 FROM pulso.cfdi_payment_docs pd
                 JOIN pulso.cfdi_payments cp
                   ON cp.payment_uuid = pd.payment_uuid AND cp.pago_num = pd.pago_num
                 JOIN pulso.cfdis comp ON comp.uuid = pd.payment_uuid
                 WHERE pd.invoice_uuid = inv.uuid AND NOT comp.is_cancelled
             ), 0)
        END AS pagado_mxn,
        CASE WHEN COALESCE(inv.metodo_pago, 'PUE') != 'PPD' THEN 0::float8
             ELSE COALESCE((
                 SELECT SUM(COALESCE(nc.total_mxn, 0)::float8)
                 FROM pulso.cfdi_relacionados cr
                 JOIN pulso.cfdis nc ON nc.uuid = cr.source_uuid
                 WHERE cr.related_uuid = inv.uuid AND cr.tipo_relacion IN ('01', '03')
                   AND nc.tipo_comprobante = 'E' AND NOT nc.is_cancelled
             ), 0)
        END AS acreditado_mxn
) pago
WHERE inv.tipo_comprobante = 'I' AND NOT inv.is_cancelled;
