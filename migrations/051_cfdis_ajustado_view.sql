-- Migration 051: AUD-004 — TipoRelacion-aware net total for tipo_comprobante = 'E'.
--
-- total_neto_mxn is STORED GENERATED (always negative for 'E'), so it can't be conditioned
-- on tipo_relacion without a drop/re-add of the column. Per PULSO_Correcciones_Lote1
-- (AUD-004) and its addendum: an 'E' comprobante stops subtracting from net income only
-- when it carries a '07' (aplicación de anticipo) or '02' (nota de débito) relation — '01',
-- '03', an unrelated note, and '04' (sustitución, informational only) keep subtracting as
-- today. Decided by relación, not by comprobante: any '07'/'02' relation on the row wins,
-- so the 4 comprobantes that combine '01'+'04' still subtract (the '04' side is inert).
--
-- Exposed as a view so every analytics query that reads total_neto_mxn can switch its
-- FROM clause to pulso.cfdis_ajustado and its column to total_neto_mxn_ajustado, instead of
-- each of the 8 consuming modules growing its own copy of this CASE.
CREATE OR REPLACE VIEW pulso.cfdis_ajustado AS
SELECT
    c.*,
    CASE
        WHEN c.tipo_comprobante = 'E' AND EXISTS (
            SELECT 1 FROM pulso.cfdi_relacionados r
            WHERE r.source_uuid = c.uuid AND r.tipo_relacion IN ('02', '07')
        ) THEN 0::real
        ELSE c.total_neto_mxn
    END AS total_neto_mxn_ajustado
FROM pulso.cfdis c;
