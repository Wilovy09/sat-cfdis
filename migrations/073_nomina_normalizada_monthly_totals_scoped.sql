-- Migration 073: P-06 (Bloque de desempeño) / AUD-080 -- nomina_normalizada's monthly_totals
-- CTE stops aggregating every RFC on the platform and scopes itself to the RFCs that could
-- actually need it, while staying single-execution.
--
-- `monthly_totals` has to stay MATERIALIZED: it used to be plain `MATERIALIZED` with an
-- unscoped `GROUP BY rfc_emisor, rfc_receptor, year_devengo, month_devengo` over the whole
-- platform. That MATERIALIZED-ness is load-bearing -- migration 070 made it NOT MATERIALIZED
-- once, which let the planner push the outer per-row predicate into it and recompute it once
-- per row of `base` instead of once total (a documented regression, fixed by migration 071
-- reverting to plain MATERIALIZED). But plain MATERIALIZED is itself an optimization fence:
-- nothing outside it can push a WHERE rfc_emisor = ... into its GROUP BY, so every call paid
-- for aggregating nomina totals for every RFC on the platform, not just the one being asked
-- about -- confirmed via EXPLAIN ANALYZE against the RFC de control before this migration.
--
-- Fix, first attempt (do not repeat -- kept here as the reason the final shape below looks
-- the way it does): a `base_rfcs AS NOT MATERIALIZED (SELECT DISTINCT rfc_emisor FROM base)`
-- driving a LATERAL, on the theory that `base` being NOT MATERIALIZED would let the caller's
-- own `WHERE rfc_emisor = $1` get pushed all the way into `base_rfcs`'s scan too. It doesn't:
-- `base_rfcs` is its own independent reference to `base`, with no relation to whatever
-- predicate the final SELECT's `b` happens to carry, so it always resolved to every RFC on
-- the platform. First appeared to cost real seconds on payroll::get/get_snapshot -- but that
-- turned out to be a false alarm caused by running the perf_budget suite's 6 tests in
-- parallel (cargo test's default) against the same remote RDS instance: re-run serialized
-- (`--test-threads=1`), both this first attempt AND the unmodified migration-072 view (as a
-- control, with none of this migration's changes present at all) showed the exact same
-- payroll::get/get_snapshot/employee_catalog/monthly_series numbers, all within budget --
-- proving the parallel run's contention, not this CTE, was the cost. Replaced anyway (see
-- below) because it's the more correct design regardless of that scare.
--
-- Actual fix: source the LATERAL's driving RFCs from `pulso.payroll_normalization_rules`
-- itself (`rule_rfcs`, filtered to rule_family='adjust_to_amount_mxn') instead of from
-- `base`. `payroll_normalization_rules` is tiny, so `rule_rfcs` costs nothing to scan
-- regardless of what join strategy the planner picks for `ar`/`mt` inside the `adj` LATERAL
-- (a nested loop with `ar` driving lets the executor skip `mt` entirely when `ar` is empty; a
-- hash join builds the `mt` side eagerly instead -- which plan gets picked isn't something
-- this migration controls, so the fix doesn't rely on guessing right). And it's the exact
-- set of RFCs `monthly_totals` could ever need to answer for (every row `ar` could ever
-- match), so it's still correctly scoped -- just driven by the small table that defines
-- "needs this" instead of filtering down the big one that would have to be scoped to it.
--
-- Verified before touching the live view, in this order:
--   1. Zero rules platform-wide (the current real state): EXPLAIN ANALYZE on the proposed
--      SELECT showed the entire monthly_totals subtree as "(never executed)", same as before
--      this migration -- the zero-cost-when-unused property is preserved.
--   2. Seeded one synthetic pulso.payroll_normalization_rules row
--      (rule_family='adjust_to_amount_mxn', RFC de control, deleted immediately after
--      measuring -- no client data in this file, no client data left in the database) and
--      compared the current view's output against this migration's proposed SELECT, on every
--      exposed column, platform-wide: zero rows differed either direction (`EXCEPT` both ways
--      empty) before this was applied.
--   3. With that rule active, EXPLAIN ANALYZE confirmed all three traps this item warns
--      about are closed: (a) monthly_totals' own Bitmap Heap Scan on cfdis shows
--      `Recheck Cond: (rfc_emisor = '<rfc>'::text)`, not an unfiltered scan -- it's scoped to
--      the RFC, not the whole platform; (b) the CTE's own aggregation nodes all show
--      `loops=1` -- computed once, not once per outer row; (c) output values (factor,
--      total_percepciones, factor_rule_id) matched the pre-migration view exactly for the
--      seeded row.
--   4. Applied via CREATE OR REPLACE VIEW (column list unchanged, same-shape swap, no window
--      where the view doesn't exist for the six background workers and three consumer
--      modules -- payroll.rs, hallazgos.rs, normalization.rs -- that share it), re-confirmed
--      the live view's output for the seeded row still matched, then deleted the synthetic
--      rule and confirmed the view reverts to factor=1.0 / factor_rule_id=NULL.
--   5. Re-ran the full backend perf_budget suite serialized (`--test-threads=1`, to avoid the
--      false alarm from step "first attempt" above): all 6 tests pass, same numbers as the
--      unmodified migration-072 control run.
CREATE OR REPLACE VIEW pulso.nomina_normalizada AS
WITH base AS NOT MATERIALIZED (
    SELECT
        c.uuid, c.rfc_emisor, c.rfc_receptor, c.nombre_emisor, c.nombre_receptor,
        c.year, c.month, c.fecha_emision,
        n.tipo_nomina, n.fecha_pago, n.fecha_inicial_pago, n.fecha_final_pago, n.num_dias_pagados,
        n.curp, n.tipo_contrato, n.tipo_regimen, n.num_empleado, n.departamento, n.puesto,
        n.tipo_jornada, n.fecha_inicio_rel_laboral, n.antiguedad, n.periodicidad_pago,
        n.salario_base_cot_apor, n.salario_diario_integrado,
        n.total_percepciones, n.total_deducciones, n.total_otros_pagos,
        n.total_sueldos, n.total_gravado, n.total_exento,
        EXTRACT(YEAR FROM pulso.devengo_date(n.fecha_final_pago, c.fecha_emision))::bigint AS year_devengo,
        EXTRACT(MONTH FROM pulso.devengo_date(n.fecha_final_pago, c.fecha_emision))::bigint AS month_devengo
    FROM pulso.cfdis c
    JOIN pulso.cfdi_nomina n ON n.uuid = c.uuid
    WHERE c.tipo_comprobante = 'N' AND NOT c.is_cancelled
),
excl_emp AS (
    SELECT id AS rule_id, owner_rfc, employee_rfc, period_start, period_end, created_at
    FROM pulso.payroll_normalization_rules
    WHERE action = 'exclude' AND rule_family IN ('exclude_employee', 'exclusion')
),
rule_rfcs AS NOT MATERIALIZED (
    SELECT DISTINCT owner_rfc AS rfc_emisor
    FROM pulso.payroll_normalization_rules
    WHERE rule_family = 'adjust_to_amount_mxn' AND value_mxn IS NOT NULL
),
monthly_totals AS MATERIALIZED (
    SELECT bt.rfc_emisor, bt.rfc_receptor, bt.year_devengo, bt.month_devengo, bt.month_percepciones
    FROM rule_rfcs br,
    LATERAL (
        SELECT base.rfc_emisor, base.rfc_receptor, base.year_devengo, base.month_devengo,
               SUM(COALESCE(base.total_percepciones, 0))::float8 AS month_percepciones
        FROM base
        WHERE base.rfc_emisor = br.rfc_emisor
        GROUP BY base.rfc_emisor, base.rfc_receptor, base.year_devengo, base.month_devengo
    ) bt
),
excl_uuid AS (
    SELECT owner_rfc, uuid, MIN(rule_id) AS rule_id
    FROM pulso.cfdi_exclusion
    GROUP BY owner_rfc, uuid
)
SELECT
    b.uuid, b.rfc_emisor, b.rfc_receptor, b.nombre_emisor, b.nombre_receptor,
    b.year, b.month, b.fecha_emision,
    b.tipo_nomina, b.fecha_pago, b.fecha_inicial_pago, b.fecha_final_pago, b.num_dias_pagados,
    b.curp, b.tipo_contrato, b.tipo_regimen, b.num_empleado, b.departamento, b.puesto,
    b.tipo_jornada, b.fecha_inicio_rel_laboral, b.antiguedad, b.periodicidad_pago,
    b.salario_base_cot_apor, b.salario_diario_integrado,
    COALESCE(adj.factor, scl.factor, 1.0) AS factor,
    (
        (EXISTS (
            SELECT 1 FROM excl_emp e
            WHERE e.owner_rfc = b.rfc_emisor AND e.employee_rfc = b.rfc_receptor
              AND (e.period_start IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) >= e.period_start)
              AND (e.period_end IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) <= e.period_end)
        ))
        OR ex.rule_id IS NOT NULL
    ) AS is_excluded,
    COALESCE(b.total_percepciones, 0)::float8 * COALESCE(adj.factor, scl.factor, 1.0) AS total_percepciones,
    COALESCE(b.total_deducciones, 0)::float8  * COALESCE(adj.factor, scl.factor, 1.0) AS total_deducciones,
    COALESCE(b.total_otros_pagos, 0)::float8  * COALESCE(adj.factor, scl.factor, 1.0) AS total_otros_pagos,
    COALESCE(b.total_sueldos, 0)::float8      * COALESCE(adj.factor, scl.factor, 1.0) AS total_sueldos,
    COALESCE(b.total_gravado, 0)::float8      * COALESCE(adj.factor, scl.factor, 1.0) AS total_gravado,
    COALESCE(b.total_exento, 0)::float8       * COALESCE(adj.factor, scl.factor, 1.0) AS total_exento,
    (
        SELECT e.rule_id FROM excl_emp e
        WHERE e.owner_rfc = b.rfc_emisor AND e.employee_rfc = b.rfc_receptor
          AND (e.period_start IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) >= e.period_start)
          AND (e.period_end IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) <= e.period_end)
        ORDER BY e.created_at DESC
        LIMIT 1
    ) AS employee_rule_id,
    COALESCE(adj.rule_id, scl.rule_id) AS factor_rule_id,
    b.year_devengo,
    b.month_devengo
FROM base b
LEFT JOIN LATERAL (
    SELECT ar.id AS rule_id, ar.value_mxn::float8 / NULLIF(mt.month_percepciones, 0) AS factor
    FROM pulso.payroll_normalization_rules ar
    LEFT JOIN monthly_totals mt
      ON mt.rfc_emisor = b.rfc_emisor AND mt.rfc_receptor = b.rfc_receptor
     AND mt.year_devengo = b.year_devengo AND mt.month_devengo = b.month_devengo
    WHERE ar.owner_rfc = b.rfc_emisor AND ar.employee_rfc = b.rfc_receptor
      AND ar.rule_family = 'adjust_to_amount_mxn' AND ar.value_mxn IS NOT NULL
      AND (ar.period_start IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) >= ar.period_start)
      AND (ar.period_end IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) <= ar.period_end)
    ORDER BY ar.created_at DESC
    LIMIT 1
) adj ON true
LEFT JOIN LATERAL (
    SELECT sr.id AS rule_id, sr.value_pct::float8 / 100.0 AS factor
    FROM pulso.payroll_normalization_rules sr
    WHERE sr.owner_rfc = b.rfc_emisor AND sr.employee_rfc = b.rfc_receptor
      AND sr.rule_family = 'scale_employee_pct' AND sr.value_pct IS NOT NULL
      AND (sr.period_start IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) >= sr.period_start)
      AND (sr.period_end IS NULL OR (b.year_devengo::text || '-' || LPAD(b.month_devengo::text, 2, '0')) <= sr.period_end)
    ORDER BY sr.created_at DESC
    LIMIT 1
) scl ON true
LEFT JOIN excl_uuid ex ON ex.owner_rfc = b.rfc_emisor AND ex.uuid = b.uuid;
