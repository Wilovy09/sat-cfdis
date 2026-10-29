-- Pulso_Propuestas_Integridad_Descarga_RFC.md, secciones 3.1, 3.4 y 3.5: estado de
-- completitud por RFC, con detalle por mes x direccion, derivado de evidencia (el conteo
-- del SAT contra lo que Pulso tiene) y no del status de los jobs.
--
-- Una fila por RFC x mes cerrado x direccion. La escribe y la lee
-- services::reconciliation (el "vigilante" de 3.2.2). Un mes queda verificado cuando el
-- conteo del SAT, sin dias con error, es igual -- sin margen, decision UX-5 -- al numero
-- de CFDIs que Pulso tiene para ese mes y direccion, y todos tienen su XML.
CREATE TABLE pulso.rfc_month_reconciliation (
    rfc              TEXT        NOT NULL,
    ym               TEXT        NOT NULL,  -- 'YYYY-MM', por fecha de emision
    direction        TEXT        NOT NULL CHECK (direction IN ('emitidos', 'recibidos')),
    -- Conteo del SAT (list-count): vigentes y cancelados, todos los tipos. NULL = sin contar,
    -- o el ultimo conteo tuvo dias que el SAT no contesto (count_error_days > 0).
    sat_count        INTEGER,
    count_error_days INTEGER     NOT NULL DEFAULT 0,
    sat_counted_at   TIMESTAMPTZ,
    -- Lo que Pulso tiene, recalculado en cada pasada.
    pulso_count      INTEGER,
    pulso_xml_missing INTEGER,
    verified_at      TIMESTAMPTZ,
    -- Jobs dirigidos a este mes x direccion lanzados porque Pulso tenia de menos, y conteos
    -- repetidos porque tenia de mas o el conteo vino con errores. Al llegar al tope el mes
    -- pasa a revision interna (needs_review) sin bloquear el resto del RFC.
    resync_attempts  INTEGER     NOT NULL DEFAULT 0,
    recount_attempts INTEGER     NOT NULL DEFAULT 0,
    needs_review     BOOLEAN     NOT NULL DEFAULT false,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (rfc, ym, direction)
);

-- L19-01 punto 5, extendido: todo RFC con prefijo XAXX/XEXX es generico del SAT (publico en
-- general / extranjero), no solo los dos exactos de la migracion 089. register_rfc ya
-- rechaza el prefijo; esto da de baja a los duenos que existan y revoca sus accesos.
UPDATE pulso.users
SET deleted_at = NOW(), verified_at = NULL
WHERE (rfc LIKE 'XAXX%' OR rfc LIKE 'XEXX%') AND deleted_at IS NULL;

UPDATE pulso.rfc_shares
SET revoked_at = NOW()
WHERE (rfc LIKE 'XAXX%' OR rfc LIKE 'XEXX%') AND revoked_at IS NULL;
