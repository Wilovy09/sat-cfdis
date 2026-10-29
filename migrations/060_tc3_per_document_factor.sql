-- Migration 060: L4-07 -- TC-3's currency fix (migration 055) applied per complement
-- instead of per document.
--
-- The 055 CASE triggers when *any* related document shares the complement's declared
-- foreign currency, then applies the resulting weighted-average rate to *every* document
-- of that complement -- including ones genuinely in MXN. Latent today: every one of the
-- 20 complements in the live DB with mixed-currency documents has moneda_p = 'MXN', which
-- the WHEN clause's own guard (moneda_p <> 'MXN') already excludes, so none of them
-- actually hit the buggy branch. But a complement whose moneda_p is genuinely foreign AND
-- whose documents are a genuine mix would have its peso-denominated documents multiplied
-- by an exchange rate that doesn't apply to them.
--
-- Fixed to check the CURRENT document's own moneda_dr, not "does at least one document
-- match": a document only gets the substitute rate when it itself shares the complement's
-- currency; a document in MXN keeps its 1:1 treatment (the correct, unrelated Group-A
-- behavior) regardless of what its sibling documents are in.
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
                                      AND pd.moneda_dr = cp.moneda_p
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
