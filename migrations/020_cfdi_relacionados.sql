-- Migration 020: Add cfdi_relacionados table to track CfdiRelacionados links.
-- Used primarily to apply credit notes (tipo_relacion='01', tipo_comprobante='E')
-- against the outstanding balance of PPD invoices in cobranza calculations.

CREATE TABLE IF NOT EXISTS pulso.cfdi_relacionados (
    source_uuid   TEXT NOT NULL,  -- UUID of the CFDI containing the CfdiRelacionados section
    tipo_relacion TEXT NOT NULL,  -- SAT key: 01=nota crédito, 04=sustitución, 07=anticipo, etc.
    related_uuid  TEXT NOT NULL,  -- UUID of the referenced document
    PRIMARY KEY (source_uuid, tipo_relacion, related_uuid)
);

CREATE INDEX IF NOT EXISTS idx_cfdi_relacionados_related
    ON pulso.cfdi_relacionados(related_uuid);
