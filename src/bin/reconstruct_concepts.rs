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
//! 2. XML no disponible (xml_available != 1): no hay de dónde reconstruir. En vez de dejar
//!    la factura intacta (con sus posibles copias), se de-duplican las filas que ya
//!    existen -- mismas columnas exactas, se queda la de `id` más chico -- y se numera lo
//!    que sobrevive en ese mismo orden. El uuid se reporta en ambos casos de fallo para que
//!    alguien decida si vale la pena perseguir el XML por separado.
//!
//! Punto 5 del ítem (el índice único en uuid+position) va en una migración aparte, DESPUÉS
//! de correr esto -- hoy las duplicadas lo violarían de inmediato.

use futures_util::stream::{self, StreamExt};
use pulso_backend::config::Config;
use pulso_backend::services::{storage, xml_parser};
use sqlx::Row;
use std::collections::HashSet;

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
    year: i64,
    month: i64,
    fecha_emision: String,
    xml_available: i64,
}

enum Outcome {
    Xml,
    Dedup,
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

    // cfdis_raw, not the L18-06 access-filtered `cfdis` view: data integrity has to cover
    // every invoice regardless of whether its RFC has cleared verified-owner access yet.
    let targets: Vec<Target> = sqlx::query_as(
        r#"SELECT DISTINCT c.uuid, c.rfc_emisor, c.rfc_receptor, c.year, c.month,
                  c.fecha_emision, c.xml_available
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
    let mut failed: Vec<String> = Vec::new();
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
            Outcome::Failed(uuid) => failed.push(uuid),
        }
        completed += 1;
        if completed % 500 == 0 {
            println!(
                "... {completed}/{total} (xml: {ok_from_xml}, dedup: {ok_dedup_only}, fallidas: {})",
                failed.len()
            );
        }
    }

    println!();
    println!("Reconstruidas desde XML real: {ok_from_xml}");
    println!("Solo de-duplicadas (XML no disponible): {ok_dedup_only}");
    println!(
        "Fallaron (no se pudo leer o parsear el XML): {}",
        failed.len()
    );
    for u in &failed {
        println!("  {u}");
    }
}

async fn process_one(
    t: Target,
    pool: &sqlx::PgPool,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    dry_run: bool,
) -> Outcome {
    let day: u32 = t
        .fecha_emision
        .get(8..10)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let xml = if t.xml_available == 1 {
        storage::get(
            s3,
            bucket,
            &t.rfc_emisor,
            &t.rfc_receptor,
            t.year as u32,
            t.month as u32,
            day,
            &t.uuid.to_lowercase(),
        )
        .await
    } else {
        None
    };

    match xml {
        Some(bytes) => match xml_parser::parse(&bytes, "reconstruct_concepts", "ambos", "") {
            Some(parsed) => {
                if !dry_run
                    && let Err(e) = replace_from_xml(pool, &t.uuid, &parsed.concepts).await
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
        None => {
            if !dry_run
                && let Err(e) = dedup_in_place(pool, &t.uuid).await
            {
                eprintln!("{}: fallo al de-duplicar: {e}", t.uuid);
                return Outcome::Failed(t.uuid);
            }
            Outcome::Dedup
        }
    }
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

/// No hay XML de donde reconstruir -- de-duplica lo que ya existe (mismas columnas,
/// conserva el `id` más chico de cada grupo) y numera lo que sobrevive en ese orden.
async fn dedup_in_place(pool: &sqlx::PgPool, uuid: &str) -> Result<(), sqlx::Error> {
    let rows = sqlx::query(
        r#"SELECT id, clave_prod_serv, clave_unidad, descripcion, cantidad, valor_unitario,
                  importe, descuento
           FROM pulso.cfdi_concepts WHERE uuid = $1 ORDER BY id"#,
    )
    .bind(uuid)
    .fetch_all(pool)
    .await?;

    let mut seen: HashSet<String> = HashSet::new();
    let mut keep_ids_in_order: Vec<i64> = Vec::new();
    let mut drop_ids: Vec<i64> = Vec::new();

    for row in &rows {
        let id: i64 = row.try_get("id")?;
        let key = format!(
            "{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}",
            row.try_get::<Option<String>, _>("clave_prod_serv")?,
            row.try_get::<Option<String>, _>("clave_unidad")?,
            row.try_get::<Option<String>, _>("descripcion")?,
            row.try_get::<Option<f32>, _>("cantidad")?,
            row.try_get::<Option<f32>, _>("valor_unitario")?,
            row.try_get::<Option<f32>, _>("importe")?,
            row.try_get::<Option<f32>, _>("descuento")?,
        );
        if seen.insert(key) {
            keep_ids_in_order.push(id);
        } else {
            drop_ids.push(id);
        }
    }

    let mut tx = pool.begin().await?;
    if !drop_ids.is_empty() {
        sqlx::query("DELETE FROM pulso.cfdi_concepts WHERE id = ANY($1)")
            .bind(&drop_ids)
            .execute(&mut *tx)
            .await?;
    }
    for (pos, id) in keep_ids_in_order.iter().enumerate() {
        sqlx::query("UPDATE pulso.cfdi_concepts SET position = $1 WHERE id = $2")
            .bind(pos as i32)
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}
