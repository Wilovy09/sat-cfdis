//! PULSO_Lote18_Seguridad.md, L18-12 punto 4: reconstruye pulso.cfdi_concepts desde el XML
//! propio de cada factura, asignando `position` (migración 088) -- corrige las ~15,812
//! facturas duplicadas como efecto colateral, porque reemplaza en vez de sumar.
//!
//! Manual, no automático: `cargo run --bin reconstruct_concepts -- --dry-run` primero
//! (reporta qué haría, no escribe nada), luego `cargo run --bin reconstruct_concepts` para
//! aplicar de verdad. Cada factura se procesa en su propia transacción -- un error en una
//! no aborta el resto.
//!
//! Dos caminos:
//! 1. XML disponible (xml_available = 1): se vuelve a bajar (misma ruta de storage que usa
//!    el servidor), se re-parsea, y se reemplaza TODO lo que había para ese uuid --
//!    borrado + inserción en la misma transacción, con `position` en el orden real del
//!    documento. Esto es lo que de verdad arregla los duplicados: el XML real nunca tuvo
//!    la línea repetida, solo la tabla.
//! 2. XML no disponible (xml_available != 1): no hay de dónde reconstruir. L19-06: sólo se
//!    quitan copias cuando la factura está inflada de verdad -- sus conceptos suman más que
//!    su subtotal (más de 1 peso y más de 0.1 %) Y cada línea distinta aparece un múltiplo
//!    del mismo número de veces (el patrón de "todo al doble"). Entonces se conserva la
//!    fracción que corresponde de cada línea (no una sola: dos líneas idénticas legítimas
//!    en el XML siguen siendo dos). Si no, nada se borra y sólo se numera. El uuid se
//!    reporta en cada fallo para que alguien decida si vale la pena perseguir el XML.
//!
//! Al terminar (sin --dry-run) refresca la vista de nómina e invalida el caché de
//! respuestas (L19-06 punto 5). El índice único (uuid, position) es parcial y ya existe
//! (migración 090): las filas viejas, con position NULL, no lo violan.

use futures_util::stream::{self, StreamExt};
use pulso_backend::config::Config;
use pulso_backend::services::{nomina_refresh, storage, xml_parser};
use sqlx::Row;

/// How many invoices are in flight at once. Bounded well under the pool's default 15
/// connections (`POSTGRES_POOL_SIZE`) because each task only holds a connection for its
/// own short transaction -- the rest of its time is spent awaiting the S3 GET, which
/// doesn't touch the pool at all. Sequential (concurrency 1) measured ~6 rows/s against
/// real S3, dominated by per-request network latency, not CPU -- so this is I/O-bound and
/// scales with concurrency, not core count.
const CONCURRENCY: usize = 24;

#[derive(sqlx::FromRow, Clone)]
struct Target {
    uuid: String,
    rfc_emisor: String,
    rfc_receptor: String,
    fecha_emision: String,
    xml_available: i64,
    subtotal: Option<f64>,
}

enum Outcome {
    Xml,
    Dedup,
    NumberedOnly,
    /// No XML, and still summing above its subtotal -- nothing safe to remove.
    StillInflated(String),
    Failed(String),
}

#[tokio::main]
async fn main() {
    let dry_run = std::env::args().any(|a| a == "--dry-run");

    dotenvy::dotenv().ok();
    let cfg = Config::from_env();
    let pool = pulso_backend::db::init_pool(&cfg)
        .await
        .expect("connect to Postgres");
    let aws_cfg = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&aws_cfg);
    let bucket = cfg.s3_bucket.clone().unwrap_or_default();

    // cfdis_raw, the table itself: data integrity has to cover every invoice.
    let targets: Vec<Target> = sqlx::query_as(
        r#"SELECT DISTINCT c.uuid, c.rfc_emisor, c.rfc_receptor, c.fecha_emision,
                  c.xml_available, c.subtotal::float8 AS subtotal
           FROM pulso.cfdis_raw c
           JOIN pulso.cfdi_concepts cc ON cc.uuid = c.uuid
           ORDER BY c.uuid"#,
    )
    .fetch_all(&pool)
    .await
    .expect("query targets");

    let total = targets.len();
    println!(
        "{total} facturas con conceptos a reconstruir{} (concurrencia: {CONCURRENCY})",
        if dry_run {
            " (--dry-run, sin escribir)"
        } else {
            ""
        }
    );

    let mut ok_from_xml = 0usize;
    let mut ok_dedup_only = 0usize;
    let mut ok_numbered_only = 0usize;
    let mut failed: Vec<String> = Vec::new();
    let mut still_inflated: Vec<String> = Vec::new();
    let mut completed = 0usize;

    let mut results = stream::iter(targets)
        .map(|t| {
            let pool = pool.clone();
            let s3 = s3.clone();
            let bucket = bucket.clone();
            async move { process_one(t, &pool, &s3, &bucket, dry_run).await }
        })
        .buffer_unordered(CONCURRENCY);

    while let Some(outcome) = results.next().await {
        match outcome {
            Outcome::Xml => ok_from_xml += 1,
            Outcome::Dedup => ok_dedup_only += 1,
            Outcome::NumberedOnly => ok_numbered_only += 1,
            Outcome::StillInflated(uuid) => still_inflated.push(uuid),
            Outcome::Failed(uuid) => failed.push(uuid),
        }
        completed += 1;
        if completed.is_multiple_of(500) {
            println!(
                "... {completed}/{total} (xml: {ok_from_xml}, dedup: {ok_dedup_only}, fallidas: {})",
                failed.len()
            );
        }
    }

    println!();
    println!("Reconstruidas desde XML real: {ok_from_xml}");
    println!("Sin XML, copias quitadas: {ok_dedup_only}");
    println!("Sin XML, sin copias (sólo numeradas): {ok_numbered_only}");
    println!(
        "Fallaron (no se pudo leer o parsear el XML): {}",
        failed.len()
    );
    for u in &failed {
        println!("  {u}");
    }

    println!(
        "Sin XML y aún por encima de su subtotal (sin copias regulares que quitar): {}",
        still_inflated.len()
    );
    for u in &still_inflated {
        println!("  {u}");
    }

    if !dry_run {
        let state = nomina_refresh::NominaRefreshState::new();
        match nomina_refresh::refresh_guarded(&pool, &state).await {
            Ok(_) => println!("Vista de nómina refrescada y caché invalidado."),
            Err(e) => eprintln!("No se pudo refrescar la vista de nómina: {e}"),
        }
    }
}

async fn process_one(
    t: Target,
    pool: &sqlx::PgPool,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    dry_run: bool,
) -> Outcome {
    // The XML is stored under its emission date (routes::invoices::extract_cfdi_path_info),
    // not under cfdis.year/month -- for nómina those hold the devengo month.
    let xml = match (t.xml_available, emission_ymd(&t.fecha_emision)) {
        (1, Some((year, month, day))) => {
            storage::get(
                s3,
                bucket,
                &t.rfc_emisor,
                &t.rfc_receptor,
                year,
                month,
                day,
                &t.uuid.to_lowercase(),
            )
            .await
        }
        _ => None,
    };

    match xml {
        Some(bytes) => match xml_parser::parse(&bytes, "reconstruct_concepts", "ambos", "") {
            Some(parsed) => {
                if !dry_run && let Err(e) = replace_from_xml(pool, &t.uuid, &parsed.concepts).await
                {
                    eprintln!("{}: fallo al reemplazar desde XML: {e}", t.uuid);
                    return Outcome::Failed(t.uuid);
                }
                Outcome::Xml
            }
            None => {
                eprintln!("{}: XML descargado pero no se pudo parsear", t.uuid);
                Outcome::Failed(t.uuid)
            }
        },
        None => match dedup_in_place(pool, &t.uuid, t.subtotal, dry_run).await {
            Ok(DedupResult::CopiesRemoved) => Outcome::Dedup,
            Ok(DedupResult::NumberedOnly) => Outcome::NumberedOnly,
            Ok(DedupResult::StillInflated) => Outcome::StillInflated(t.uuid),
            Err(e) => {
                eprintln!("{}: fallo al de-duplicar: {e}", t.uuid);
                Outcome::Failed(t.uuid)
            }
        },
    }
}

/// (year, month, day) of a `fecha_emision` like `2024-08-31T12:00:00`.
fn emission_ymd(fecha_emision: &str) -> Option<(u32, u32, u32)> {
    let year = fecha_emision.get(0..4)?.parse().ok()?;
    let month = fecha_emision.get(5..7)?.parse().ok()?;
    let day = fecha_emision.get(8..10)?.parse().ok()?;
    Some((year, month, day))
}

async fn replace_from_xml(
    pool: &sqlx::PgPool,
    uuid: &str,
    concepts: &[xml_parser::ParsedConcept],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM pulso.cfdi_concepts WHERE uuid = $1")
        .bind(uuid)
        .execute(&mut *tx)
        .await?;
    for (pos, c) in concepts.iter().enumerate() {
        sqlx::query(
            r#"INSERT INTO pulso.cfdi_concepts
                (uuid, clave_prod_serv, clave_unidad, descripcion, cantidad, valor_unitario,
                 importe, descuento, position)
               VALUES ($1, $2, $3, $4, $5::real, $6::real, $7::real, $8::real, $9)"#,
        )
        .bind(uuid)
        .bind(&c.clave_prod_serv)
        .bind(&c.clave_unidad)
        .bind(&c.descripcion)
        .bind(c.cantidad)
        .bind(c.valor_unitario)
        .bind(c.importe)
        .bind(c.descuento)
        .bind(pos as i32)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}

/// One stored concept row: its id, the columns that make two lines "the same", and its
/// importe.
struct StoredConcept {
    id: i64,
    key: String,
    importe: f64,
}

/// L19-06: sum of the concepts exceeds the invoice's subtotal by more than 1 peso and more
/// than 0.1 % -- the verification threshold of the item.
fn exceeds_subtotal(concepts_sum: f64, subtotal: Option<f64>) -> bool {
    let Some(subtotal) = subtotal else {
        return false;
    };
    let excess = concepts_sum - subtotal;
    excess > 1.0 && excess > subtotal.abs() * 0.001
}

fn gcd(a: usize, b: usize) -> usize {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// Which rows to keep (in id order) and which to drop. Copies go only when the invoice is
/// inflated (`exceeds_subtotal`) and every distinct line repeats a multiple of the same
/// factor; then each line keeps `count / factor` rows, its lowest ids. Otherwise nothing is
/// dropped -- identical lines can be legitimate.
fn plan_dedup(rows: &[StoredConcept], subtotal: Option<f64>) -> (Vec<i64>, Vec<i64>) {
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for r in rows {
        *counts.entry(r.key.as_str()).or_default() += 1;
    }
    let factor = counts.values().copied().fold(0, gcd);
    let concepts_sum: f64 = rows.iter().map(|r| r.importe).sum();
    if factor < 2 || !exceeds_subtotal(concepts_sum, subtotal) {
        return (rows.iter().map(|r| r.id).collect(), Vec::new());
    }

    let mut kept_per_key: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    let (mut keep, mut drop) = (Vec::new(), Vec::new());
    for r in rows {
        let kept = kept_per_key.entry(r.key.as_str()).or_default();
        if *kept < counts[r.key.as_str()] / factor {
            *kept += 1;
            keep.push(r.id);
        } else {
            drop.push(r.id);
        }
    }
    (keep, drop)
}

enum DedupResult {
    CopiesRemoved,
    NumberedOnly,
    StillInflated,
}

/// No hay XML de donde reconstruir -- quita copias sólo si `plan_dedup` lo justifica y
/// numera lo que queda en orden de `id`.
async fn dedup_in_place(
    pool: &sqlx::PgPool,
    uuid: &str,
    subtotal: Option<f64>,
    dry_run: bool,
) -> Result<DedupResult, sqlx::Error> {
    let rows = sqlx::query(
        r#"SELECT id, clave_prod_serv, clave_unidad, descripcion, cantidad, valor_unitario,
                  importe, descuento
           FROM pulso.cfdi_concepts WHERE uuid = $1 ORDER BY id"#,
    )
    .bind(uuid)
    .fetch_all(pool)
    .await?;

    let mut stored = Vec::with_capacity(rows.len());
    for row in &rows {
        let importe = row.try_get::<Option<f32>, _>("importe")?;
        stored.push(StoredConcept {
            id: row.try_get("id")?,
            key: format!(
                "{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}",
                row.try_get::<Option<String>, _>("clave_prod_serv")?,
                row.try_get::<Option<String>, _>("clave_unidad")?,
                row.try_get::<Option<String>, _>("descripcion")?,
                row.try_get::<Option<f32>, _>("cantidad")?,
                row.try_get::<Option<f32>, _>("valor_unitario")?,
                importe,
                row.try_get::<Option<f32>, _>("descuento")?,
            ),
            importe: f64::from(importe.unwrap_or(0.0)),
        });
    }
    let (keep_ids_in_order, drop_ids) = plan_dedup(&stored, subtotal);
    let result = if !drop_ids.is_empty() {
        DedupResult::CopiesRemoved
    } else if exceeds_subtotal(stored.iter().map(|r| r.importe).sum(), subtotal) {
        DedupResult::StillInflated
    } else {
        DedupResult::NumberedOnly
    };
    if dry_run {
        return Ok(result);
    }

    let mut tx = pool.begin().await?;
    if !drop_ids.is_empty() {
        sqlx::query("DELETE FROM pulso.cfdi_concepts WHERE id = ANY($1)")
            .bind(&drop_ids)
            .execute(&mut *tx)
            .await?;
    }
    // Cleared first so a re-run can't collide with the (uuid, position) unique index while
    // renumbering.
    sqlx::query("UPDATE pulso.cfdi_concepts SET position = NULL WHERE uuid = $1")
        .bind(uuid)
        .execute(&mut *tx)
        .await?;
    for (pos, id) in (0_i32..).zip(&keep_ids_in_order) {
        sqlx::query("UPDATE pulso.cfdi_concepts SET position = $1 WHERE id = $2")
            .bind(pos)
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i64, key: &str, importe: f64) -> StoredConcept {
        StoredConcept {
            id,
            key: key.to_string(),
            importe,
        }
    }

    #[test]
    fn halves_an_invoice_stored_twice() {
        let rows = [
            row(1, "a", 100.0),
            row(2, "b", 50.0),
            row(3, "a", 100.0),
            row(4, "b", 50.0),
        ];
        assert_eq!(plan_dedup(&rows, Some(150.0)), (vec![1, 2], vec![3, 4]));
    }

    #[test]
    fn keeps_legitimate_identical_lines_when_the_sum_matches() {
        let rows = [row(1, "a", 100.0), row(2, "a", 100.0)];
        assert_eq!(plan_dedup(&rows, Some(200.0)), (vec![1, 2], vec![]));
    }

    #[test]
    fn keeps_a_fraction_per_line_not_a_single_row() {
        // Real invoice: line "a" twice, line "b" once -- stored twice over.
        let rows = [
            row(1, "a", 10.0),
            row(2, "a", 10.0),
            row(3, "b", 5.0),
            row(4, "a", 10.0),
            row(5, "a", 10.0),
            row(6, "b", 5.0),
        ];
        assert_eq!(
            plan_dedup(&rows, Some(25.0)),
            (vec![1, 2, 3], vec![4, 5, 6])
        );
    }

    #[test]
    fn leaves_rows_alone_when_counts_share_no_factor() {
        let rows = [row(1, "a", 100.0), row(2, "a", 100.0), row(3, "b", 100.0)];
        assert_eq!(plan_dedup(&rows, Some(100.0)), (vec![1, 2, 3], vec![]));
    }

    #[test]
    fn subtotal_threshold_needs_both_one_peso_and_a_tenth_of_a_percent() {
        assert!(!exceeds_subtotal(1_000_000.5, Some(1_000_000.0)));
        assert!(!exceeds_subtotal(1_000_900.0, Some(1_000_000.0)));
        assert!(exceeds_subtotal(1_001_001.0, Some(1_000_000.0)));
        assert!(!exceeds_subtotal(100.0, None));
    }

    #[test]
    fn emission_ymd_reads_the_emission_date() {
        assert_eq!(emission_ymd("2024-08-31T12:00:00"), Some((2024, 8, 31)));
        assert_eq!(emission_ymd("bad"), None);
    }
}
