-- La conciliacion comparaba un RFC en cuanto terminaba su ultimo job, sin esperar a que el
-- ETL cargara los CFDIs de ese job. Con el ETL atrasado (27-oct en TEST: 141,980 filas
-- pendientes, de ellas solo 1,045 CFDIs nuevos), meses cuyos CFDIs ya estaban descargados
-- pero aun en cola se leyeron como "Pulso tiene de menos": gastaron sus 3 intentos y
-- quedaron en revision (17 meses). services::reconciliation ahora espera a que el ETL
-- termine con el RFC; esto devuelve esos meses, y los intentos que gastaron los no
-- verificados, para que se evaluen de nuevo con los CFDIs ya cargados.
UPDATE pulso.rfc_month_reconciliation
SET needs_review = false,
    resync_attempts = 0,
    recount_attempts = 0,
    updated_at = now()
WHERE verified_at IS NULL;
