-- Normalized CFDI invoice headers (parsed from XML)
CREATE TABLE IF NOT EXISTS pulso.cfdis (
    uuid            TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL REFERENCES pulso.sync_jobs(id),
    rfc_emisor      TEXT NOT NULL,
    nombre_emisor   TEXT,
    regimen_fiscal_emisor TEXT,
    rfc_receptor    TEXT NOT NULL,
    nombre_receptor TEXT,
    uso_cfdi        TEXT,
    domicilio_fiscal_receptor TEXT,
    regimen_fiscal_receptor   TEXT,
    fecha_emision   TEXT NOT NULL,  -- ISO-8601 datetime
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    tipo_comprobante TEXT NOT NULL, -- I=ingreso E=egreso P=pago N=nomina T=traslado
    subtotal        REAL,
    descuento       REAL DEFAULT 0,
    total           REAL,
    moneda          TEXT DEFAULT 'MXN',
    tipo_cambio     REAL DEFAULT 1.0,
    total_mxn       REAL,           -- total * tipo_cambio
    metodo_pago     TEXT,
    forma_pago      TEXT,
    lugar_expedicion TEXT,
    estado_sat      TEXT DEFAULT 'vigente',
    dl_type         TEXT NOT NULL,  -- emitidos|recibidos
    xml_available   INTEGER DEFAULT 0,  -- 1 if XML was found and parsed
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cfdis_rfc_emisor   ON pulso.cfdis(rfc_emisor);
CREATE INDEX IF NOT EXISTS idx_cfdis_rfc_receptor ON pulso.cfdis(rfc_receptor);
CREATE INDEX IF NOT EXISTS idx_cfdis_year_month   ON pulso.cfdis(year, month);
CREATE INDEX IF NOT EXISTS idx_cfdis_job_id       ON pulso.cfdis(job_id);
CREATE INDEX IF NOT EXISTS idx_cfdis_tipo         ON pulso.cfdis(tipo_comprobante);

-- Tax breakdown per invoice (both trasladados and retenidos)
CREATE TABLE IF NOT EXISTS pulso.cfdi_taxes (
    uuid        TEXT NOT NULL,
    impuesto    TEXT NOT NULL,  -- 001=ISR 002=IVA 003=IEPS
    tipo_factor TEXT,           -- Tasa Cuota Exento
    tasa        REAL,           -- 0.16 0.08 0.0 etc
    base        REAL,
    importe     REAL,
    is_retenido INTEGER DEFAULT 0,  -- 0=trasladado 1=retenido
    PRIMARY KEY (uuid, impuesto, tipo_factor, tasa, is_retenido)
);

CREATE INDEX IF NOT EXISTS idx_cfdi_taxes_uuid ON pulso.cfdi_taxes(uuid);

-- Line items / conceptos
CREATE TABLE IF NOT EXISTS pulso.cfdi_concepts (
    id              BIGSERIAL PRIMARY KEY,
    uuid            TEXT NOT NULL,
    clave_prod_serv TEXT,
    clave_unidad    TEXT,
    descripcion     TEXT,
    cantidad        REAL,
    valor_unitario  REAL,
    importe         REAL,
    descuento       REAL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_cfdi_concepts_uuid ON pulso.cfdi_concepts(uuid);

-- Payment complements (tipo_comprobante = P)
CREATE TABLE IF NOT EXISTS pulso.cfdi_payments (
    payment_uuid TEXT NOT NULL,
    pago_num     INTEGER NOT NULL,  -- index within the complement
    fecha_pago   TEXT NOT NULL,
    forma_pago   TEXT,
    moneda_p     TEXT,
    monto        REAL,
    tipo_cambio_p REAL DEFAULT 1.0,
    PRIMARY KEY (payment_uuid, pago_num)
);

-- Related documents inside a payment complement
CREATE TABLE IF NOT EXISTS pulso.cfdi_payment_docs (
    payment_uuid        TEXT NOT NULL,
    pago_num            INTEGER NOT NULL,
    invoice_uuid        TEXT NOT NULL,  -- related invoice UUID
    num_parcialidad     INTEGER,
    imp_saldo_ant       REAL,
    imp_pagado          REAL,
    imp_saldo_insoluto  REAL,
    moneda_dr           TEXT,
    tipo_cambio_dr      REAL DEFAULT 1.0,
    PRIMARY KEY (payment_uuid, pago_num, invoice_uuid)
);

CREATE INDEX IF NOT EXISTS idx_cfdi_payment_docs_invoice ON pulso.cfdi_payment_docs(invoice_uuid);

-- Payroll complement (tipo_comprobante = N)
CREATE TABLE IF NOT EXISTS pulso.cfdi_nomina (
    uuid                        TEXT PRIMARY KEY,
    tipo_nomina                 TEXT,   -- O=ordinaria E=extraordinaria
    fecha_pago                  TEXT,
    fecha_inicial_pago          TEXT,
    fecha_final_pago            TEXT,
    num_dias_pagados            REAL,
    total_percepciones          REAL,
    total_deducciones           REAL,
    total_otros_pagos           REAL,
    -- Receptor (employee)
    curp                        TEXT,
    tipo_contrato               TEXT,
    tipo_regimen                TEXT,
    num_empleado                TEXT,
    departamento                TEXT,
    puesto                      TEXT,
    tipo_jornada                TEXT,
    fecha_inicio_rel_laboral    TEXT,
    antiguedad                  TEXT,
    periodicidad_pago           TEXT,
    salario_base_cot_apor       REAL,
    salario_diario_integrado    REAL,
    -- Percepciones aggregates
    total_sueldos               REAL,
    total_gravado               REAL,
    total_exento                REAL
);

CREATE INDEX IF NOT EXISTS idx_cfdi_nomina_fecha ON pulso.cfdi_nomina(fecha_pago);

-- Payroll income items
CREATE TABLE IF NOT EXISTS pulso.cfdi_nomina_percepciones (
    id                  BIGSERIAL PRIMARY KEY,
    uuid                TEXT NOT NULL,
    tipo_percepcion     TEXT,   -- 001=sueldo 002=gratificacion etc
    clave               TEXT,
    concepto            TEXT,
    importe_gravado     REAL,
    importe_exento      REAL
);

CREATE INDEX IF NOT EXISTS idx_nomina_percepciones_uuid ON pulso.cfdi_nomina_percepciones(uuid);

-- Payroll deduction items
CREATE TABLE IF NOT EXISTS pulso.cfdi_nomina_deducciones (
    id              BIGSERIAL PRIMARY KEY,
    uuid            TEXT NOT NULL,
    tipo_deduccion  TEXT,   -- 001=seguro social 002=ISR etc
    clave           TEXT,
    concepto        TEXT,
    importe         REAL
);

CREATE INDEX IF NOT EXISTS idx_nomina_deducciones_uuid ON pulso.cfdi_nomina_deducciones(uuid);

-- Counterparty normalization rules (group / exclude)
CREATE TABLE IF NOT EXISTS pulso.normalization_rules (
    id              TEXT PRIMARY KEY,
    owner_rfc       TEXT NOT NULL,  -- the company RFC that owns these rules
    dl_type         TEXT NOT NULL,  -- emitidos|recibidos
    source_rfc      TEXT,
    source_name     TEXT,
    group_name      TEXT,           -- NULL if action=exclude
    action          TEXT NOT NULL,  -- group|exclude
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_norm_rules_owner ON pulso.normalization_rules(owner_rfc, dl_type);

-- Payroll normalization rules
CREATE TABLE IF NOT EXISTS pulso.payroll_normalization_rules (
    id              TEXT PRIMARY KEY,
    owner_rfc       TEXT NOT NULL,
    rule_family     TEXT NOT NULL,  -- exclude_employee|scale_employee_pct
    employee_rfc    TEXT,
    employee_name   TEXT,
    action          TEXT NOT NULL,
    value_pct       REAL,
    period_start    TEXT,           -- YYYY-MM
    period_end      TEXT,           -- YYYY-MM
    notes           TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_payroll_norm_owner ON pulso.payroll_normalization_rules(owner_rfc);
