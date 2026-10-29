-- PULSO_Lote19_Cierre_L18.md, L19-06 puntos 3 y 4.
--
-- Punto 3 -- filas hijas sin factura. Ninguna tabla hija tiene llave foranea a
-- pulso.cfdis_raw, asi que una factura borrada o reprocesada dejaba sus hijos atras. Se
-- compara contra cfdis_raw (no contra la vista pulso.cfdis) y por upper() en ambos lados,
-- igual que el indice funcional de la migracion 084, para que una diferencia de
-- mayusculas en el uuid no cuente como "sin factura". La llave del padre es el uuid de la
-- propia factura en todas, salvo en pagos (payment_uuid, el complemento de pago) y en
-- relacionados (source_uuid, la factura que declara la relacion); invoice_uuid y
-- related_uuid apuntan a facturas ajenas que pueden no estar en la base, y no cuentan.
DELETE FROM pulso.cfdi_concepts x
WHERE NOT EXISTS (SELECT 1 FROM pulso.cfdis_raw c WHERE upper(c.uuid) = upper(x.uuid));

DELETE FROM pulso.cfdi_taxes x
WHERE NOT EXISTS (SELECT 1 FROM pulso.cfdis_raw c WHERE upper(c.uuid) = upper(x.uuid));

DELETE FROM pulso.cfdi_nomina x
WHERE NOT EXISTS (SELECT 1 FROM pulso.cfdis_raw c WHERE upper(c.uuid) = upper(x.uuid));

DELETE FROM pulso.cfdi_nomina_percepciones x
WHERE NOT EXISTS (SELECT 1 FROM pulso.cfdis_raw c WHERE upper(c.uuid) = upper(x.uuid));

DELETE FROM pulso.cfdi_nomina_deducciones x
WHERE NOT EXISTS (SELECT 1 FROM pulso.cfdis_raw c WHERE upper(c.uuid) = upper(x.uuid));

DELETE FROM pulso.cfdi_nomina_otros_pagos x
WHERE NOT EXISTS (SELECT 1 FROM pulso.cfdis_raw c WHERE upper(c.uuid) = upper(x.uuid));

DELETE FROM pulso.cfdi_payments x
WHERE NOT EXISTS (SELECT 1 FROM pulso.cfdis_raw c WHERE upper(c.uuid) = upper(x.payment_uuid));

DELETE FROM pulso.cfdi_payment_docs x
WHERE NOT EXISTS (SELECT 1 FROM pulso.cfdis_raw c WHERE upper(c.uuid) = upper(x.payment_uuid));

DELETE FROM pulso.cfdi_relacionados x
WHERE NOT EXISTS (SELECT 1 FROM pulso.cfdis_raw c WHERE upper(c.uuid) = upper(x.source_uuid));

-- Punto 4 -- indice unico por factura y posicion. Parcial a proposito: las filas viejas
-- tienen position NULL hasta que corra la reconstruccion (src/bin/reconstruct_concepts.rs)
-- y quedan fuera del indice, asi que las facturas duplicadas de hoy no lo violan y el
-- indice puede ir antes de la reconstruccion. Desde ya detiene lo que importa: toda
-- insercion nueva escribe position (db::cfdis::insert_concepts), y repetir el mismo
-- listado choca contra el indice y el ON CONFLICT DO NOTHING lo descarta. Cuando la
-- reconstruccion deje 0 filas con position NULL, una migracion posterior puede volver la
-- columna NOT NULL.
CREATE UNIQUE INDEX cfdi_concepts_uuid_position_key
    ON pulso.cfdi_concepts (uuid, position)
    WHERE position IS NOT NULL;
