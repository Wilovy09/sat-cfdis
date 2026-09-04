//! L6-03: consistency invariants (items 3 and 4, the two static/text-search ones -- no DB
//! needed). Plain `#[test]`s that grep the SQL embedded in `src/**/*.rs`.
//!
//! Both are heuristics, not a SQL parser, per the Lote 6 doc's own "basta una busqueda de
//! texto" -- each doc comment below states precisely what the heuristic does and does not
//! catch.
use std::fs;
use std::path::{Path, PathBuf};

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn src_files() -> Vec<PathBuf> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let mut files = Vec::new();
    collect_rs_files(&Path::new(manifest_dir).join("src"), &mut files);
    files.sort();
    files
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

// ---------------------------------------------------------------------------
// Item 3: ninguna columna sin calificar en una subconsulta correlacionada.
// ---------------------------------------------------------------------------
//
// This is the L4-01 bug class: a bare (unqualified) column reference inside a
// `(SELECT ...)` block resolves against whichever table in scope declares that column
// name first -- which, for a subquery whose own FROM/JOIN table happens to share a column
// name with the outer query's table, can silently be the subquery's OWN table instead of
// the outer one the author meant to correlate against.
//
// Heuristic (not a SQL parser):
// 1. Find every `(SELECT ... )` block (balanced-paren scan) in each SQL string literal.
// 2. Within a block, collect every `alias.column` reference and every alias the block's
//    own FROM/JOIN clauses introduce.
// 3. A block only counts as "correlated" (touches an outer table) if it contains at least
//    one `alias.column` reference whose alias is NOT one the block itself introduces --
//    i.e. it must be reaching for something declared outside it.
// 4. Only for a block classified as correlated: flag any bare (no `.` prefix) identifier
//    used in a comparison (`= x`, `x =`, `<`, `>`, `<=`, `>=`, `<>`, `!=`) that isn't a
//    keyword, a bind placeholder ($1, $2, ...), a numeric literal, or immediately after AS.
//
// What this catches: exactly the shape L4-01 was -- a correlated subquery (proven
// correlated by an existing qualified outer-table reference elsewhere in the same
// subquery) that ALSO leaves some other column bare, risking it silently binding to the
// subquery's own table instead.
//
// What this does NOT catch: a subquery that is correlated ONLY through a bare column (no
// qualified reference anywhere to prove correlation) -- there's no textual signal to key
// off in that case without an actual catalog of table columns. It also doesn't understand
// CTEs' visibility rules precisely; a `FROM cte_name` inside a block is treated as
// introducing an alias like any other table, which is a reasonable approximation for this
// codebase's actual query shapes (checked by hand against every current match, see below).
#[test]
fn no_unqualified_column_in_correlated_subquery() {
    let keywords: &[&str] = &[
        "SELECT",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
        "EXISTS",
        "IN",
        "LIMIT",
        "AS",
        "ON",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "FULL",
        "CROSS",
        "LATERAL",
        "GROUP",
        "BY",
        "ORDER",
        "DESC",
        "ASC",
        "IS",
        "LIKE",
        "BETWEEN",
        "COALESCE",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "DISTINCT",
        "COUNT",
        "SUM",
        "AVG",
        "MAX",
        "MIN",
        "OVER",
        "PARTITION",
        "FILTER",
        "WITH",
        "UNION",
        "ALL",
        "EXTRACT",
        "YEAR",
        "MONTH",
        "DAY",
    ];
    let is_keyword = |w: &str| keywords.iter().any(|k| k.eq_ignore_ascii_case(w));

    let mut findings: Vec<String> = Vec::new();

    for path in src_files() {
        let content = fs::read_to_string(&path).unwrap();
        let bytes = content.as_bytes();

        let mut idx = 0usize;
        while let Some(rel) = content[idx..].find("(SELECT") {
            let open = idx + rel;
            let Some(close) = find_matching_close(bytes, open) else {
                break;
            };
            let block = &content[open..=close];

            // "pulso" is this codebase's one schema prefix, never a real table alias --
            // `pulso.some_table` matches the same `word.word` shape as `alias.column` but
            // isn't a correlated reference, so it's excluded on both sides of this check.
            let refs: Vec<(String, String)> = qualified_refs(block)
                .into_iter()
                .filter(|(alias, _)| !alias.eq_ignore_ascii_case("pulso"))
                .collect();
            let own_aliases = subquery_own_aliases(block);
            let is_correlated = refs.iter().any(|(alias, _)| !own_aliases.contains(alias));

            if is_correlated {
                for bare in bare_comparison_identifiers(block, &is_keyword) {
                    findings.push(format!(
                        "{}: correlated subquery references bare `{bare}` (not qualified \
                         with a table alias) -- {}",
                        path.display(),
                        block.lines().next().unwrap_or(block).trim()
                    ));
                }
            }

            idx = close + 1;
        }
    }

    assert!(
        findings.is_empty(),
        "possible unqualified column(s) in a correlated subquery (L4-01-class bug):\n{}",
        findings.join("\n")
    );
}

fn find_matching_close(bytes: &[u8], open_idx: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (i, &b) in bytes.iter().enumerate().skip(open_idx) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Every `alias.column` pair in `text` (alias must start with a letter/underscore, so a
/// decimal literal like `12.5` is never mistaken for one).
fn qualified_refs(text: &str) -> Vec<(String, String)> {
    let bytes = text.as_bytes();
    let mut refs = Vec::new();
    for (i, &b) in bytes.iter().enumerate() {
        if b != b'.' {
            continue;
        }
        let mut a_start = i;
        while a_start > 0 && is_word_byte(bytes[a_start - 1]) {
            a_start -= 1;
        }
        if a_start == i {
            continue; // nothing before the dot
        }
        let mut c_end = i + 1;
        while c_end < bytes.len() && is_word_byte(bytes[c_end]) {
            c_end += 1;
        }
        if c_end == i + 1 {
            continue; // nothing after the dot
        }
        let alias = &text[a_start..i];
        if alias
            .chars()
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_')
        {
            refs.push((alias.to_string(), text[i + 1..c_end].to_string()));
        }
    }
    refs
}

/// Aliases the block's OWN `FROM`/`JOIN` clauses introduce: the last bare word right after
/// a table path (possibly `schema.table`) and before the next keyword/comma/paren.
fn subquery_own_aliases(block: &str) -> std::collections::HashSet<String> {
    let mut aliases = std::collections::HashSet::new();
    let words = tokenize(block);
    let mut i = 0;
    while i < words.len() {
        let w = &words[i];
        if w.eq_ignore_ascii_case("FROM") || w.eq_ignore_ascii_case("JOIN") {
            let mut j = i + 1;
            // table path: word, optionally ".word"
            if j < words.len() {
                j += 1;
                if j + 1 < words.len() && words[j] == "." {
                    j += 2;
                }
            }
            // optional alias: a bare word that isn't a keyword/punctuation
            if j < words.len() {
                let candidate = &words[j];
                let is_punct = candidate
                    .chars()
                    .next()
                    .is_some_and(|c| !c.is_alphanumeric() && c != '_');
                let stop_words = [
                    "WHERE", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "FULL", "CROSS", "GROUP",
                    "ORDER", "LIMIT", "AS",
                ];
                if !is_punct && !stop_words.iter().any(|s| s.eq_ignore_ascii_case(candidate)) {
                    aliases.insert(candidate.clone());
                }
            }
        }
        i += 1;
    }
    aliases
}

/// Splits SQL text into a stream of word-tokens and single-character punctuation tokens
/// (whitespace dropped), preserving order -- enough to walk "FROM x y" shaped sequences
/// without a real SQL grammar.
fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
        } else if is_word_byte(b) {
            let start = i;
            while i < bytes.len() && is_word_byte(bytes[i]) {
                i += 1;
            }
            tokens.push(text[start..i].to_string());
        } else {
            tokens.push((b as char).to_string());
            i += 1;
        }
    }
    tokens
}

/// Bare (unqualified) identifiers used as a comparison operand inside `block`, skipping
/// keywords, bind placeholders, numeric literals, and anything immediately preceded by a
/// `.` (already qualified) or immediately following `AS`.
fn bare_comparison_identifiers(block: &str, is_keyword: &impl Fn(&str) -> bool) -> Vec<String> {
    let tokens = tokenize(block);
    let ops = ["=", "<", ">", "<>", "!="];
    let mut found = Vec::new();

    for (i, tok) in tokens.iter().enumerate() {
        let is_ident = tok
            .chars()
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_');
        if !is_ident || is_keyword(tok) {
            continue;
        }
        // Skip if qualified: previous token is "."
        if i > 0 && tokens[i - 1] == "." {
            continue;
        }
        // Skip if it's the column right after AS (an output alias, not a reference).
        if i > 0 && tokens[i - 1].eq_ignore_ascii_case("AS") {
            continue;
        }
        // Skip if what follows is "." (it's the alias half of alias.column, not a bare column).
        if tokens.get(i + 1).is_some_and(|t| t == ".") {
            continue;
        }
        let next_is_op = tokens.get(i + 1).is_some_and(|t| ops.contains(&t.as_str()));
        let prev_is_op = i > 0 && ops.contains(&tokens[i - 1].as_str());
        if next_is_op || prev_is_op {
            found.push(tok.clone());
        }
    }
    found
}

// ---------------------------------------------------------------------------
// Item 4: ninguna lectura de columna NUMERIC/REAL decodificada como f64 sin conversion.
// ---------------------------------------------------------------------------
//
// L5-01 added `get_f64`/`get_f64_opt` (services/analytics/summary.rs) as the one place a
// NUMERIC/REAL column is meant to be decoded into `f64` -- sqlx's `f64` only decodes
// FLOAT8, so a bare NUMERIC/REAL read silently fails and `.unwrap_or(0.0)`/`.ok()` used to
// turn that into a wrong-but-plausible zero.
//
// Heuristic: find every `.try_get(...)` call site whose result is unambiguously typed as
// `f64`/`Option<f64>` -- either an explicit turbofish (`try_get::<f64, _>`/
// `try_get::<Option<f64>, _>`) or the `let x: f64 = ....try_get("col").unwrap_or(0.0)`
// shape this codebase actually uses -- that is NOT itself part of `get_f64`/`get_f64_opt`'s
// own body (those call `try_get::<f64, _>(col)` with a variable, not a string-literal
// argument, so they never match the literal-argument patterns below). For each match,
// require that the SAME file casts that exact column to `::float8` (immediately, allowing
// intervening closing parens, followed by `AS <column>`) -- i.e. the call site is safe
// EVEN WITHOUT going through the helper, because the driver already receives a FLOAT8.
//
// What this catches: a new call site added later that decodes a raw (uncast) NUMERIC/REAL
// column directly as f64 without the helper AND without a `::float8` cast -- the exact
// L5-01 bug shape.
//
// What this does NOT catch: `get_f64`/`get_f64_opt` failing to warn (that's the helper's
// own job, not this test's); a cast under a DIFFERENT alias than the column name passed to
// `try_get` (this codebase always aliases `::float8 AS <same name>`, so it hasn't come up);
// or a non-literal / dynamically-built column name (none exist today for f64 reads).
#[test]
fn f64_decode_without_cast_or_helper() {
    let mut findings: Vec<String> = Vec::new();

    for path in src_files() {
        let content = fs::read_to_string(&path).unwrap();
        // Collapse (not strip) whitespace: a cast's `AS <column>` is followed by a real SQL
        // keyword (FROM/WHERE/...) with only whitespace between them, so removing
        // whitespace entirely would erase that word boundary (`AS avg_daysFROM...` would
        // wrongly look like the identifier continues into "FROM").
        let normalized = collapse_whitespace(&content);

        for (col, snippet) in explicit_f64_turbofish_columns(&content)
            .into_iter()
            .chain(let_bound_f64_columns(&content))
        {
            if !column_has_float8_cast(&normalized, &col) {
                findings.push(format!(
                    "{}: `.try_get(\"{col}\")` decoded as f64/Option<f64> without going \
                     through get_f64/get_f64_opt, and no `::float8 AS {col}` cast found in \
                     this file -- {snippet}",
                    path.display()
                ));
            }
        }
    }

    assert!(
        findings.is_empty(),
        "possible unconverted NUMERIC/REAL column decoded as f64 (L5-01-class bug):\n{}",
        findings.join("\n")
    );
}

/// `.try_get::<f64, _>("col")` / `.try_get::<Option<f64>, _>("col")` -- only matches a
/// string-literal argument, which is how every real call site in this codebase writes it;
/// `get_f64_opt`'s own internal call passes a `&str` variable, so it never matches here.
fn explicit_f64_turbofish_columns(content: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for marker in ["try_get::<f64", "try_get::<Option<f64>"] {
        let mut idx = 0;
        while let Some(rel) = content[idx..].find(marker) {
            let start = idx + rel;
            if let Some(col) = extract_literal_arg_after(content, start) {
                out.push((col, line_containing(content, start)));
            }
            idx = start + marker.len();
        }
    }
    out
}

/// `let x: f64 = <expr>.try_get("col")...` / `let x: Option<f64> = <expr>.try_get("col")...`
/// on a single line -- the shape every current non-helper f64 read in this codebase uses.
fn let_bound_f64_columns(content: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (line_no, line) in content.lines().enumerate() {
        let is_f64_binding = line.contains(": f64 =") || line.contains(": Option<f64> =");
        if !is_f64_binding || !line.contains(".try_get(") {
            continue;
        }
        if let Some(rel) = line.find(".try_get(")
            && let Some(col) = extract_literal_arg_after(line, rel)
        {
            out.push((col, format!("line {}: {}", line_no + 1, line.trim())));
        }
    }
    out
}

/// Given the byte offset of a `try_get` call's start, finds the first `"..."` string
/// literal inside its argument list (returns `None` if the argument isn't a literal, e.g.
/// a bare variable like `get_f64_opt`'s own `col`).
fn extract_literal_arg_after(content: &str, from: usize) -> Option<String> {
    let bytes = content.as_bytes();
    let open_paren = content[from..].find('(')? + from;
    // The argument must start with a quote within a few characters (allowing the turbofish
    // `::<f64, _>(` to have already been consumed, so this is right after '(').
    let mut i = open_paren + 1;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if bytes.get(i) != Some(&b'"') {
        return None; // not a literal argument (e.g. a bare variable) -- not our concern here.
    }
    let start = i + 1;
    let end = content[start..].find('"')? + start;
    Some(content[start..end].to_string())
}

fn line_containing(content: &str, byte_offset: usize) -> String {
    content[..byte_offset]
        .rfind('\n')
        .map_or(content, |nl| &content[nl + 1..])
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
}

/// Collapses every run of whitespace (including newlines, so a wrapped multi-line SQL
/// literal reads as one line) into a single space. Unlike stripping whitespace entirely,
/// this preserves the word boundary between an identifier and the next bare keyword
/// (`AS avg_days FROM ...`) -- stripping it would glue them into `avg_daysFROM`.
fn collapse_whitespace(content: &str) -> String {
    let mut out = String::with_capacity(content.len());
    let mut last_was_space = false;
    for c in content.chars() {
        if c.is_whitespace() {
            if !last_was_space {
                out.push(' ');
            }
            last_was_space = true;
        } else {
            out.push(c);
            last_was_space = false;
        }
    }
    out
}

/// Whether whitespace-collapsed `normalized` contains `::float8` followed -- after skipping
/// any mix of closing parens and single spaces (e.g. `AVG(...)::float8) AS col`) -- by
/// `AS <column>` at a word boundary.
fn column_has_float8_cast(normalized: &str, column: &str) -> bool {
    let marker = "::float8";
    let bytes = normalized.as_bytes();
    let mut idx = 0usize;
    while let Some(rel) = normalized[idx..].find(marker) {
        let marker_end = idx + rel + marker.len();
        let mut pos = marker_end;
        while matches!(bytes.get(pos), Some(b' ' | b')')) {
            pos += 1;
        }
        if let Some(after_as) = normalized[pos..].strip_prefix("AS ")
            && let Some(rest) = after_as.strip_prefix(column)
        {
            let boundary_ok = rest
                .chars()
                .next()
                .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
            if boundary_ok {
                return true;
            }
        }
        idx = marker_end;
    }
    false
}
