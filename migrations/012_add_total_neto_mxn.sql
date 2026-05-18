-- total_neto_mxn = (subtotal - descuento) × tipo_cambio, negative for Egreso (E).
-- This is the pre-IVA net revenue figure used for P&L analytics, matching the
-- Python xml-dashboard-mvp reference implementation.
ALTER TABLE pulso.cfdis
ADD COLUMN IF NOT EXISTS total_neto_mxn REAL GENERATED ALWAYS AS (
    CASE
        WHEN tipo_comprobante = 'E'
            THEN -(COALESCE(subtotal, 0.0) - COALESCE(descuento, 0.0)) * COALESCE(tipo_cambio, 1.0)
        ELSE (COALESCE(subtotal, 0.0) - COALESCE(descuento, 0.0)) * COALESCE(tipo_cambio, 1.0)
    END
) STORED;

CREATE INDEX IF NOT EXISTS idx_cfdis_total_neto_mxn ON pulso.cfdis (total_neto_mxn);
