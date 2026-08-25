-- Migration 055: TC-3 / DEC-029 -- payment complements in a foreign currency valued 1:1.
--
-- A payment complement's own tipo_cambio_p is sometimes missing or literally 1 even though
-- moneda_p says the payment isn't MXN -- a SAT-side data defect on the issuer's part, not a
-- parser gap (835 other USD payments in the same dataset carry a correct rate). Per
-- PULSO_Correcciones_Lote3 (TC-3), the fix is NOT "always apply a rate": the related
-- documents decide. Two populations exist today (7 complements total):
--   - Group A (3 complements, HTR200709GP5 -> CES100706U65): moneda_p says USD but every
--     related document is in MXN with its own rate of 1. The payment really is in pesos,
--     mislabeled -- multiplying by a real exchange rate here would manufacture tens of
--     millions of pesos that don't exist. Leave as 1:1.
--   - Group B (4 complements, ADC101206334 -> the generic foreign RFC): moneda_p says USD
--     AND the related documents are also in USD, each carrying its own invoice-level
--     tipo_cambio from issuance. Value with THAT rate instead of the complement's own
--     (missing) one.
-- Re-applied at both levels that read a payment complement's own tipo_cambio_p: per-invoice
-- (pulso.cfdi_cobro_estado, migration 052) and per-complement (cashflow.rs's pago_rows/
-- pm_rows, fixed alongside this migration). When a complement's related docs span more than
-- one document, the replacement rate is the imp_pagado-weighted average of those documents'
-- own tipo_cambio -- degrades to the single-document case for every real row today.
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
                            * (CASE
                                 WHEN cp.moneda_p IS NOT NULL AND cp.moneda_p <> 'MXN'
                                      AND COALESCE(NULLIF(cp.tipo_cambio_p::float8, 0), 1) = 1
                                      AND EXISTS (
                                          SELECT 1 FROM pulso.cfdi_payment_docs tc3d
                                          WHERE tc3d.payment_uuid = cp.payment_uuid AND tc3d.moneda_dr = cp.moneda_p
                                      )
                                 THEN COALESCE((
                                     SELECT SUM(tc3d.imp_pagado::float8 * COALESCE(NULLIF(tc3i.tipo_cambio::float8, 0), 1))
                                            / NULLIF(SUM(tc3d.imp_pagado::float8), 0)
                                     FROM pulso.cfdi_payment_docs tc3d
                                     JOIN pulso.cfdis tc3i ON tc3i.uuid = tc3d.invoice_uuid
                                     WHERE tc3d.payment_uuid = cp.payment_uuid AND tc3d.moneda_dr = cp.moneda_p
                                 ), 1)
                                 ELSE COALESCE(NULLIF(cp.tipo_cambio_p::float8, 0), 1)
                               END))
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

-- Registro al ingerir (TC-3 point 2): a dedicated log table, not a UI surface (DEC-022,
-- Calidad de datos, is deliberately deferred) -- but "moneda != MXN + tipo_cambio = 1" must
-- stop passing through silently. One row per detection per sync job.
CREATE TABLE IF NOT EXISTS pulso.data_quality_flags (
    id           BIGSERIAL PRIMARY KEY,
    job_id       TEXT,
    flag_type    TEXT NOT NULL,
    payment_uuid TEXT NOT NULL,
    rfc_emisor   TEXT,
    rfc_receptor TEXT,
    moneda       TEXT,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_data_quality_flags_type ON pulso.data_quality_flags (flag_type, detected_at);
