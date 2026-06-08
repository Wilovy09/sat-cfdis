-- Sprint: Módulo de Normalización V2
-- Agrega línea contable, motivo, campos CAPEX y campos de nómina extendida.

-- normalization_rules: nuevos campos de ajuste estructurado
ALTER TABLE pulso.normalization_rules
    ADD COLUMN IF NOT EXISTS accounting_line          TEXT,
    ADD COLUMN IF NOT EXISTS motivo                   TEXT,
    ADD COLUMN IF NOT EXISTS impacts_ebitda           BOOLEAN,
    ADD COLUMN IF NOT EXISTS capex_estimate_dep       BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS capex_asset_type         TEXT,
    ADD COLUMN IF NOT EXISTS capex_useful_life_years  NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS capex_annual_dep_mxn     NUMERIC(18,2);

-- payroll_normalization_rules: soporte para adjust_to_amount_mxn y exclude_specific_cfdis
ALTER TABLE pulso.payroll_normalization_rules
    ADD COLUMN IF NOT EXISTS value_mxn             NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS excluded_cfdi_uuids   TEXT[];

-- Índices de soporte para queries del puente EBITDA
CREATE INDEX IF NOT EXISTS idx_norm_rules_owner_line
    ON pulso.normalization_rules (owner_rfc, accounting_line)
    WHERE accounting_line IS NOT NULL;
