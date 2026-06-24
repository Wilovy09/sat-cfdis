CREATE TABLE IF NOT EXISTS pulso.cfdi_nomina_otros_pagos (
    uuid            TEXT NOT NULL,
    tipo_otro_pago  TEXT,
    clave           TEXT,
    concepto        TEXT,
    importe         NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_nomina_otros_pagos_uuid ON pulso.cfdi_nomina_otros_pagos(uuid);
