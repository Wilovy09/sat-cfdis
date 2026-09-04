//! L6-03: consistency invariants (items 1 and 2).
//!
//! **Design note, added after these were first written**: the original version of this file
//! checked a DATA precondition (does any nomina row have emision != devengo? do
//! total_otros_pagos/total_deducciones net to nonzero?) rather than the actual CODE. That
//! meant they'd stay red forever regardless of whether L6-06/07/08 got fixed -- new
//! late-arriving receipts and nonzero otros_pagos/deducciones are a permanent property of
//! real payroll data, not evidence of a bug. Confirmed directly: after L6-06/07/08 landed
//! (verified correct by reading the diffs -- hallazgos.rs now sums pure total_percepciones,
//! payroll.rs's indem_rows and normalization.rs's three bridge sources now use
//! year_devengo/month_devengo in both WHERE and GROUP BY/SELECT), these tests still failed
//! with the exact same numbers as before the fix. Rewritten below to check the actual
//! source text of the fixed queries instead -- the same technique `static_invariants.rs`
//! already uses for items 3/4, extended here to items 1/2. No DB connection needed anymore.
use std::fs;

fn read_src(rel_path: &str) -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    fs::read_to_string(format!("{manifest_dir}/{rel_path}"))
        .unwrap_or_else(|e| panic!("failed to read {rel_path}: {e}"))
}

/// The `nom_rows` query block in hallazgos.rs's H3 computation, isolated by its own
/// preceding comment marker so a match is anchored to the right query even if the file
/// grows more SUM(total_percepciones...) sites elsewhere.
fn h3_nom_rows_block(content: &str) -> &str {
    let marker = "let nom_rows = sqlx::query(";
    let start = content
        .find(marker)
        .unwrap_or_else(|| panic!("hallazgos.rs: couldn't find `{marker}` -- has H3's query been renamed or restructured?"));
    let end = content[start..]
        .find(".bind(rfc)")
        .map(|rel| start + rel)
        .unwrap_or(content.len());
    &content[start..end]
}

/// Item 1: una sola definicion de costo de nomina -- detects L6-06.
///
/// hallazgos.rs's H3 (evolucion del flujo visible) must sum pure `total_percepciones`, the
/// single definition every other consumer (payroll.rs's summary/by_month/by_year/
/// by_month_ordinaria, list_ebitda_bridge_adjustments's three nomina sources,
/// list_excluded_cfdis's payroll branch) uses since Lote 5. Checks the query text directly:
/// the old bug was `SUM(n.total_percepciones + n.total_otros_pagos - n.total_deducciones)`.
#[test]
fn costo_nomina_una_sola_definicion_detecta_l6_06() {
    let content = read_src("src/services/analytics/hallazgos.rs");
    let block = h3_nom_rows_block(&content);

    assert!(
        block.contains("SUM(n.total_percepciones)"),
        "L6-06: hallazgos.rs's H3 nom_rows query no longer contains the expected \
         `SUM(n.total_percepciones)` -- has it regressed to a different formula? Block:\n{block}"
    );
    assert!(
        !block.contains("total_otros_pagos") && !block.contains("total_deducciones"),
        "L6-06: hallazgos.rs's H3 nom_rows query still references total_otros_pagos/\
         total_deducciones -- the single correct definition is total_percepciones alone. \
         Block:\n{block}"
    );
}

/// Isolates the `indem_rows` query in payroll.rs by its NRS03 comment marker.
fn indem_rows_block(content: &str) -> &str {
    let marker = "let indem_rows = sqlx::query(";
    let start = content
        .find(marker)
        .unwrap_or_else(|| panic!("payroll.rs: couldn't find `{marker}` -- has indem_rows been renamed or restructured?"));
    let end = content[start..]
        .find("ORDER BY year, month, total_perc DESC")
        .map(|rel| start + rel)
        .unwrap_or(content.len());
    &content[start..end]
}

/// Item 2a: una sola definicion de mes de nomina, en el WHERE y en la proyeccion --
/// detects L6-07. payroll.rs's `indem_rows` was the one query left computing its "month"
/// via an inline devengo expression while filtering by emision -- checks it now uses the
/// view's own `year_devengo`/`month_devengo` in both places, with no inline EXTRACT left.
#[test]
fn mes_devengo_where_vs_select_detecta_l6_07() {
    let content = read_src("src/services/analytics/payroll.rs");
    let block = indem_rows_block(&content);

    assert!(
        block.contains("n.year_devengo") && block.contains("n.month_devengo"),
        "L6-07: payroll.rs's indem_rows no longer references n.year_devengo/n.month_devengo \
         -- has it regressed to a different month definition? Block:\n{block}"
    );
    assert!(
        !block.contains("EXTRACT(YEAR FROM COALESCE") && !block.contains("EXTRACT(MONTH FROM COALESCE"),
        "L6-07: payroll.rs's indem_rows still computes devengo inline (EXTRACT(...FROM \
         COALESCE...)) instead of reading year_devengo/month_devengo from the view. \
         Block:\n{block}"
    );
    assert!(
        block.contains("n.year_devengo > $2") || block.contains("n.year_devengo= $2") || block.contains("n.year_devengo = $2"),
        "L6-07: payroll.rs's indem_rows WHERE window doesn't appear to filter by \
         n.year_devengo -- it must filter by the same definition it projects. Block:\n{block}"
    );
}

/// Isolates one of `list_ebitda_bridge_adjustments`'s three nomina source query blocks by
/// its `let <name>_rows = sqlx::query(` marker.
fn bridge_source_block<'a>(content: &'a str, fn_name: &str) -> &'a str {
    let marker = format!("let {fn_name} = sqlx::query(");
    let start = content.find(&marker).unwrap_or_else(|| {
        panic!("normalization.rs: couldn't find `{marker}` -- has this bridge source been renamed?")
    });
    // Each source's query ends at its own `.bind(owner_rfc)` call, which starts the
    // parameter-binding chain -- consistent across all three sources in this file.
    let end = content[start..]
        .find(".bind(owner_rfc)")
        .map(|rel| start + rel)
        .unwrap_or(content.len());
    &content[start..end]
}

/// Item 2b: una sola definicion de mes de nomina, en el WHERE y en el GROUP BY -- detects
/// L6-08. All three of `list_ebitda_bridge_adjustments`'s nomina sources must filter AND
/// group by `year_devengo`/`month_devengo`, matching payroll.rs's by_year/by_month --
/// changing only the window and leaving the GROUP BY on emision would let this pass without
/// the real problem (the bridge's yearly breakdown landing on a different year than the
/// P&L's) being fixed, so both are checked explicitly.
#[test]
fn mes_devengo_where_vs_groupby_detecta_l6_08() {
    let content = read_src("src/services/analytics/normalization.rs");

    for fn_name in ["employee_excl_rows", "receipt_excl_rows", "factor_diff_rows"] {
        let block = bridge_source_block(&content, fn_name);

        assert!(
            block.contains("year_devengo"),
            "L6-08: normalization.rs's {fn_name} no longer references year_devengo -- has \
             it regressed to emision (year)? Block:\n{block}"
        );
        assert!(
            block.contains("GROUP BY") && {
                let group_by_idx = block.find("GROUP BY").unwrap();
                block[group_by_idx..].contains("year_devengo")
            },
            "L6-08: normalization.rs's {fn_name} filters by year_devengo but its GROUP BY \
             doesn't reference year_devengo -- changing only the WHERE window without also \
             fixing the GROUP BY leaves the bridge's per-year breakdown on a different \
             population than the P&L's. Block:\n{block}"
        );
        assert!(
            !block.contains("c.year") && !block.contains("nn.year,") && !block.contains("nn.year "),
            "L6-08: normalization.rs's {fn_name} still appears to reference emision-based \
             year (c.year / nn.year) somewhere in its WHERE or GROUP BY, alongside \
             year_devengo -- both should be fully switched, not partially. Block:\n{block}"
        );
    }
}
