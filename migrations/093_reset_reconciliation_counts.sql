-- Repara pulso.rfc_month_reconciliation tras dos defectos de la entrega de la migracion 092:
--
-- 1. list-count etiquetaba cada recibido como emitido (comparaba `=== DownloadType::
--    recibidos()`, y ese enum devuelve una instancia nueva en cada llamada). El conteo del
--    SAT de "emitidos" de cada mes salia inflado con los recibidos, y los recibidos nunca
--    se contaron.
-- 2. mark_verified reiniciaba los contadores de intentos. Con (1), un mes rebotaba sin fin
--    entre verificado (cuando lo contaba el job del mes, de una sola direccion) y
--    "faltan CFDIs" (cuando lo contaba la pasada de ambas direcciones), encolando un job
--    por mes en cada ciclo sin llegar nunca al tope de revision.
--
-- Los meses ya verificados se quedan: verificar exige que Pulso cuadre exacto con un
-- conteo, y eso solo pasaba con el conteo correcto (el del job, de una direccion). Lo no
-- verificado se vuelve a contar desde cero, y todos los contadores e indicadores de
-- revision que dejo el ciclo se limpian.
UPDATE pulso.rfc_month_reconciliation
SET sat_count = NULL,
    count_error_days = 0,
    sat_counted_at = NULL
WHERE verified_at IS NULL;

UPDATE pulso.rfc_month_reconciliation
SET resync_attempts = 0,
    recount_attempts = 0,
    needs_review = false,
    updated_at = now();
