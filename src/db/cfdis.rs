use crate::services::xml_parser::{
    ParsedCfdi, ParsedConcept, ParsedNomina, ParsedNominaDeduccion, ParsedNominaOtroPago,
    ParsedNominaPercepcion,
    ParsedPayment, ParsedPaymentDoc, ParsedRelacionado, ParsedTax,
};
use sqlx::PgPool;

// ---------------------------------------------------------------------------
// Insert helpers
// ---------------------------------------------------------------------------

pub async fn upsert_cfdi(pool: &PgPool, c: &ParsedCfdi) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO pulso.cfdis (
            uuid, job_id, rfc_emisor, nombre_emisor, regimen_fiscal_emisor,
            rfc_receptor, nombre_receptor, uso_cfdi,
            domicilio_fiscal_receptor, regimen_fiscal_receptor,
            fecha_emision, year, month, tipo_comprobante,
            subtotal, descuento, total, moneda, tipo_cambio, total_mxn,
            metodo_pago, forma_pago, lugar_expedicion,
            estado_sat, dl_type, xml_available, created_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
            $15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27
        )
        ON CONFLICT(uuid) DO UPDATE SET
            nombre_emisor             = excluded.nombre_emisor,
            regimen_fiscal_emisor     = excluded.regimen_fiscal_emisor,
            nombre_receptor           = excluded.nombre_receptor,
            uso_cfdi                  = excluded.uso_cfdi,
            domicilio_fiscal_receptor = excluded.domicilio_fiscal_receptor,
            regimen_fiscal_receptor   = excluded.regimen_fiscal_receptor,
            tipo_comprobante          = excluded.tipo_comprobante,
            subtotal                  = excluded.subtotal,
            descuento                 = excluded.descuento,
            total                     = excluded.total,
            moneda                    = excluded.moneda,
            tipo_cambio               = excluded.tipo_cambio,
            total_mxn                 = excluded.total_mxn,
            metodo_pago               = excluded.metodo_pago,
            forma_pago                = excluded.forma_pago,
            lugar_expedicion          = excluded.lugar_expedicion,
            estado_sat                = excluded.estado_sat,
            xml_available             = excluded.xml_available
        "#,
    )
    .bind(&c.uuid)
    .bind(&c.job_id)
    .bind(&c.rfc_emisor)
    .bind(&c.nombre_emisor)
    .bind(&c.regimen_fiscal_emisor)
    .bind(&c.rfc_receptor)
    .bind(&c.nombre_receptor)
    .bind(&c.uso_cfdi)
    .bind(&c.domicilio_fiscal_receptor)
    .bind(&c.regimen_fiscal_receptor)
    .bind(&c.fecha_emision)
    .bind(c.year)
    .bind(c.month)
    .bind(&c.tipo_comprobante)
    .bind(c.subtotal)
    .bind(c.descuento)
    .bind(c.total)
    .bind(&c.moneda)
    .bind(c.tipo_cambio)
    .bind(c.total_mxn)
    .bind(&c.metodo_pago)
    .bind(&c.forma_pago)
    .bind(&c.lugar_expedicion)
    .bind(&c.estado_sat)
    .bind(&c.dl_type)
    .bind(c.xml_available)
    .bind(&c.created_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_taxes(
    pool: &PgPool,
    uuid: &str,
    taxes: &[ParsedTax],
) -> Result<(), sqlx::Error> {
    let mut uuids: Vec<&str> = Vec::new();
    let mut impuestos: Vec<Option<String>> = Vec::new();
    let mut tipo_factores: Vec<Option<String>> = Vec::new();
    let mut tasas: Vec<Option<f64>> = Vec::new();
    let mut bases: Vec<Option<f64>> = Vec::new();
    let mut importes: Vec<Option<f64>> = Vec::new();
    let mut is_retenidos: Vec<i64> = Vec::new();

    for t in taxes {
        if t.tasa.is_none() {
            continue;
        }
        uuids.push(uuid);
        impuestos.push(t.impuesto.clone());
        tipo_factores.push(t.tipo_factor.clone());
        tasas.push(t.tasa);
        bases.push(t.base);
        importes.push(t.importe);
        is_retenidos.push(t.is_retenido);
    }

    if uuids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_taxes
            (uuid, impuesto, tipo_factor, tasa, base, importe, is_retenido)
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::float8[], $5::float8[], $6::float8[], $7::int8[])
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(&uuids)
    .bind(&impuestos)
    .bind(&tipo_factores)
    .bind(&tasas)
    .bind(&bases)
    .bind(&importes)
    .bind(&is_retenidos)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_concepts(
    pool: &PgPool,
    uuid: &str,
    concepts: &[ParsedConcept],
) -> Result<(), sqlx::Error> {
    if concepts.is_empty() {
        return Ok(());
    }

    let mut uuids: Vec<&str> = Vec::with_capacity(concepts.len());
    let mut clave_prod_servs: Vec<Option<String>> = Vec::with_capacity(concepts.len());
    let mut clave_unidades: Vec<Option<String>> = Vec::with_capacity(concepts.len());
    let mut descripciones: Vec<Option<String>> = Vec::with_capacity(concepts.len());
    let mut cantidades: Vec<Option<f64>> = Vec::with_capacity(concepts.len());
    let mut valor_unitarios: Vec<Option<f64>> = Vec::with_capacity(concepts.len());
    let mut importes: Vec<Option<f64>> = Vec::with_capacity(concepts.len());
    let mut descuentos: Vec<Option<f64>> = Vec::with_capacity(concepts.len());

    for c in concepts {
        uuids.push(uuid);
        clave_prod_servs.push(c.clave_prod_serv.clone());
        clave_unidades.push(c.clave_unidad.clone());
        descripciones.push(c.descripcion.clone());
        cantidades.push(c.cantidad);
        valor_unitarios.push(c.valor_unitario);
        importes.push(c.importe);
        descuentos.push(c.descuento);
    }

    sqlx::query(r#"
        INSERT INTO pulso.cfdi_concepts
            (uuid, clave_prod_serv, clave_unidad, descripcion, cantidad, valor_unitario, importe, descuento)
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::float8[], $6::float8[], $7::float8[], $8::float8[])
        ON CONFLICT DO NOTHING
        "#)
    .bind(&uuids)
    .bind(&clave_prod_servs)
    .bind(&clave_unidades)
    .bind(&descripciones)
    .bind(&cantidades)
    .bind(&valor_unitarios)
    .bind(&importes)
    .bind(&descuentos)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_payments(
    pool: &PgPool,
    payment_uuid: &str,
    payments: &[ParsedPayment],
) -> Result<(), sqlx::Error> {
    if payments.is_empty() {
        return Ok(());
    }

    let mut payment_uuids: Vec<&str> = Vec::with_capacity(payments.len());
    let mut pago_nums: Vec<i64> = Vec::with_capacity(payments.len());
    let mut fecha_pagos: Vec<Option<String>> = Vec::with_capacity(payments.len());
    let mut forma_pagos: Vec<Option<String>> = Vec::with_capacity(payments.len());
    let mut moneda_ps: Vec<Option<String>> = Vec::with_capacity(payments.len());
    let mut montos: Vec<Option<f64>> = Vec::with_capacity(payments.len());
    let mut tipo_cambio_ps: Vec<Option<f64>> = Vec::with_capacity(payments.len());

    for (i, p) in payments.iter().enumerate() {
        let idx = i as i64;
        payment_uuids.push(payment_uuid);
        pago_nums.push(idx);
        fecha_pagos.push(p.fecha_pago.clone());
        forma_pagos.push(p.forma_pago.clone());
        moneda_ps.push(p.moneda_p.clone());
        montos.push(p.monto);
        tipo_cambio_ps.push(p.tipo_cambio_p);
    }

    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_payments
            (payment_uuid, pago_num, fecha_pago, forma_pago, moneda_p, monto, tipo_cambio_p)
        SELECT * FROM UNNEST($1::text[], $2::int8[], $3::text[], $4::text[], $5::text[], $6::float8[], $7::float8[])
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(&payment_uuids)
    .bind(&pago_nums)
    .bind(&fecha_pagos)
    .bind(&forma_pagos)
    .bind(&moneda_ps)
    .bind(&montos)
    .bind(&tipo_cambio_ps)
    .execute(pool)
    .await?;

    // insert_payment_doc is left as-is per task instructions (nested docs are complex)
    for (i, p) in payments.iter().enumerate() {
        let idx = i as i64;
        for doc in &p.docs {
            insert_payment_doc(pool, payment_uuid, idx, doc).await?;
        }
    }
    Ok(())
}

async fn insert_payment_doc(
    pool: &PgPool,
    payment_uuid: &str,
    pago_num: i64,
    d: &ParsedPaymentDoc,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_payment_docs
            (payment_uuid, pago_num, invoice_uuid, num_parcialidad,
             imp_saldo_ant, imp_pagado, imp_saldo_insoluto, moneda_dr, tipo_cambio_dr)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(payment_uuid)
    .bind(pago_num)
    .bind(&d.invoice_uuid)
    .bind(d.num_parcialidad)
    .bind(d.imp_saldo_ant)
    .bind(d.imp_pagado)
    .bind(d.imp_saldo_insoluto)
    .bind(&d.moneda_dr)
    .bind(d.tipo_cambio_dr)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_relacionados(
    pool: &PgPool,
    source_uuid: &str,
    relacionados: &[ParsedRelacionado],
) -> Result<(), sqlx::Error> {
    if relacionados.is_empty() {
        return Ok(());
    }

    let mut source_uuids: Vec<&str> = Vec::with_capacity(relacionados.len());
    let mut tipo_relaciones: Vec<&str> = Vec::with_capacity(relacionados.len());
    let mut related_uuids: Vec<&str> = Vec::with_capacity(relacionados.len());

    for r in relacionados {
        source_uuids.push(source_uuid);
        tipo_relaciones.push(&r.tipo_relacion);
        related_uuids.push(&r.related_uuid);
    }

    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_relacionados (source_uuid, tipo_relacion, related_uuid)
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[])
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(&source_uuids)
    .bind(&tipo_relaciones)
    .bind(&related_uuids)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_nomina(pool: &PgPool, uuid: &str, n: &ParsedNomina) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_nomina (
            uuid, tipo_nomina, fecha_pago, fecha_inicial_pago, fecha_final_pago,
            num_dias_pagados, total_percepciones, total_deducciones, total_otros_pagos,
            curp, tipo_contrato, tipo_regimen, num_empleado, departamento, puesto,
            tipo_jornada, fecha_inicio_rel_laboral, antiguedad, periodicidad_pago,
            salario_base_cot_apor, salario_diario_integrado,
            total_sueldos, total_gravado, total_exento
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24
        )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(uuid)
    .bind(&n.tipo_nomina)
    .bind(&n.fecha_pago)
    .bind(&n.fecha_inicial_pago)
    .bind(&n.fecha_final_pago)
    .bind(n.num_dias_pagados)
    .bind(n.total_percepciones)
    .bind(n.total_deducciones)
    .bind(n.total_otros_pagos)
    .bind(&n.curp)
    .bind(&n.tipo_contrato)
    .bind(&n.tipo_regimen)
    .bind(&n.num_empleado)
    .bind(&n.departamento)
    .bind(&n.puesto)
    .bind(&n.tipo_jornada)
    .bind(&n.fecha_inicio_rel_laboral)
    .bind(&n.antiguedad)
    .bind(&n.periodicidad_pago)
    .bind(n.salario_base_cot_apor)
    .bind(n.salario_diario_integrado)
    .bind(n.total_sueldos)
    .bind(n.total_gravado)
    .bind(n.total_exento)
    .execute(pool)
    .await?;

    insert_nomina_percepcion(pool, uuid, &n.percepciones).await?;
    insert_nomina_deduccion(pool, uuid, &n.deducciones).await?;
    insert_nomina_otro_pago(pool, uuid, &n.otros_pagos).await?;
    Ok(())
}

async fn insert_nomina_percepcion(
    pool: &PgPool,
    uuid: &str,
    percepciones: &[ParsedNominaPercepcion],
) -> Result<(), sqlx::Error> {
    if percepciones.is_empty() {
        return Ok(());
    }

    let mut uuids: Vec<&str> = Vec::with_capacity(percepciones.len());
    let mut tipo_percepciones: Vec<Option<String>> = Vec::with_capacity(percepciones.len());
    let mut claves: Vec<Option<String>> = Vec::with_capacity(percepciones.len());
    let mut conceptos: Vec<Option<String>> = Vec::with_capacity(percepciones.len());
    let mut importe_gravados: Vec<Option<f64>> = Vec::with_capacity(percepciones.len());
    let mut importe_exentos: Vec<Option<f64>> = Vec::with_capacity(percepciones.len());

    for p in percepciones {
        uuids.push(uuid);
        tipo_percepciones.push(p.tipo_percepcion.clone());
        claves.push(p.clave.clone());
        conceptos.push(p.concepto.clone());
        importe_gravados.push(p.importe_gravado);
        importe_exentos.push(p.importe_exento);
    }

    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_nomina_percepciones
            (uuid, tipo_percepcion, clave, concepto, importe_gravado, importe_exento)
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::float8[], $6::float8[])
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(&uuids)
    .bind(&tipo_percepciones)
    .bind(&claves)
    .bind(&conceptos)
    .bind(&importe_gravados)
    .bind(&importe_exentos)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_nomina_deduccion(
    pool: &PgPool,
    uuid: &str,
    deducciones: &[ParsedNominaDeduccion],
) -> Result<(), sqlx::Error> {
    if deducciones.is_empty() {
        return Ok(());
    }

    let mut uuids: Vec<&str> = Vec::with_capacity(deducciones.len());
    let mut tipo_deducciones: Vec<Option<String>> = Vec::with_capacity(deducciones.len());
    let mut claves: Vec<Option<String>> = Vec::with_capacity(deducciones.len());
    let mut conceptos: Vec<Option<String>> = Vec::with_capacity(deducciones.len());
    let mut importes: Vec<Option<f64>> = Vec::with_capacity(deducciones.len());

    for d in deducciones {
        uuids.push(uuid);
        tipo_deducciones.push(d.tipo_deduccion.clone());
        claves.push(d.clave.clone());
        conceptos.push(d.concepto.clone());
        importes.push(d.importe);
    }

    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_nomina_deducciones
            (uuid, tipo_deduccion, clave, concepto, importe)
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::float8[])
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(&uuids)
    .bind(&tipo_deducciones)
    .bind(&claves)
    .bind(&conceptos)
    .bind(&importes)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_nomina_otro_pago(
    pool: &PgPool,
    uuid: &str,
    otros_pagos: &[ParsedNominaOtroPago],
) -> Result<(), sqlx::Error> {
    if otros_pagos.is_empty() {
        return Ok(());
    }

    let mut uuids: Vec<&str> = Vec::with_capacity(otros_pagos.len());
    let mut tipo_otros_pagos: Vec<Option<String>> = Vec::with_capacity(otros_pagos.len());
    let mut claves: Vec<Option<String>> = Vec::with_capacity(otros_pagos.len());
    let mut conceptos: Vec<Option<String>> = Vec::with_capacity(otros_pagos.len());
    let mut importes: Vec<Option<f64>> = Vec::with_capacity(otros_pagos.len());

    for op in otros_pagos {
        uuids.push(uuid);
        tipo_otros_pagos.push(op.tipo_otro_pago.clone());
        claves.push(op.clave.clone());
        conceptos.push(op.concepto.clone());
        importes.push(op.importe);
    }

    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_nomina_otros_pagos
            (uuid, tipo_otro_pago, clave, concepto, importe)
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::float8[])
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(&uuids)
    .bind(&tipo_otros_pagos)
    .bind(&claves)
    .bind(&conceptos)
    .bind(&importes)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Query helpers (used by ETL)
// ---------------------------------------------------------------------------

/// Pairs of (uuid, metadata_json) from job_invoices not yet in cfdis.
pub async fn find_pending_etl(
    pool: &PgPool,
    job_id: &str,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    use sqlx::Row;
    let rows = sqlx::query(
        r#"
        SELECT ji.uuid, ji.metadata
        FROM pulso.job_invoices ji
        LEFT JOIN pulso.cfdis c ON c.uuid = ji.uuid
        WHERE ji.job_id = $1 AND c.uuid IS NULL
        "#,
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let uuid: String = r.try_get("uuid").unwrap_or_default();
            let meta: String = r.try_get("metadata").unwrap_or_default();
            (uuid, meta)
        })
        .collect())
}

/// Job IDs that have invoice rows not yet ETL'd into cfdis.
pub async fn jobs_needing_etl(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    use sqlx::Row;
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ji.job_id
        FROM pulso.job_invoices ji
        LEFT JOIN pulso.cfdis c ON c.uuid = ji.uuid
        WHERE c.uuid IS NULL
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.try_get("job_id").unwrap_or_default())
        .collect())
}

/// Job IDs that have cfdis parsed from metadata only (xml_available=0),
/// which may now have XMLs available in storage.
pub async fn jobs_needing_enrichment(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    use sqlx::Row;
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ji.job_id
        FROM pulso.job_invoices ji
        JOIN pulso.cfdis c ON c.uuid = ji.uuid
        WHERE c.xml_available = 0
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.try_get("job_id").unwrap_or_default())
        .collect())
}

/// Invoices for a job that are in cfdis but were parsed from metadata only.
/// Limited batch to avoid overwhelming storage on each ETL cycle.
pub async fn find_needs_enrichment(
    pool: &PgPool,
    job_id: &str,
    limit: i64,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    use sqlx::Row;
    let rows = sqlx::query(
        r#"
        SELECT ji.uuid, ji.metadata
        FROM pulso.job_invoices ji
        JOIN pulso.cfdis c ON c.uuid = ji.uuid
        WHERE ji.job_id = $1 AND c.xml_available = 0
        ORDER BY ji.uuid
        LIMIT $2
        "#,
    )
    .bind(job_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let uuid: String = r.try_get("uuid").unwrap_or_default();
            let meta: String = r.try_get("metadata").unwrap_or_default();
            (uuid, meta)
        })
        .collect())
}

/// Returns true if concepts already exist for this UUID (prevents duplicate inserts).
pub async fn concepts_exist(pool: &PgPool, uuid: &str) -> bool {
    sqlx::query("SELECT 1 FROM pulso.cfdi_concepts WHERE uuid = $1 LIMIT 1")
        .bind(uuid)
        .fetch_optional(pool)
        .await
        .map(|r| r.is_some())
        .unwrap_or(false)
}

/// Reset CFDIs so they are re-enriched from storage on the next ETL cycle.
/// Clears all parsed data (taxes, concepts, payments, nomina, relacionados) and sets
/// xml_available=0 so the enrichment worker picks them up again.
/// Returns the number of CFDIs queued for reprocessing.
pub async fn reset_for_reprocessing(
    pool: &PgPool,
    rfc: &str,
    dl_type: &str,       // "emitidos" | "recibidos" | "ambos"
    from_year: Option<i32>,
    from_month: Option<i32>,
    to_year: Option<i32>,
    to_month: Option<i32>,
) -> Result<u64, sqlx::Error> {
    let emit = dl_type != "recibidos";
    let recv = dl_type != "emitidos";

    let period_clause = match (from_year, to_year) {
        (Some(fy), Some(ty)) => format!(
            "AND (c.year > {fy} OR (c.year = {fy} AND c.month >= {fm})) \
             AND (c.year < {ty} OR (c.year = {ty} AND c.month <= {tm}))",
            fy = fy, fm = from_month.unwrap_or(1),
            ty = ty, tm = to_month.unwrap_or(12)
        ),
        _ => String::new(),
    };

    let owner_clause = match (emit, recv) {
        (true, true) => "(c.rfc_emisor = $1 OR c.rfc_receptor = $1)".to_string(),
        (true, false) => "c.rfc_emisor = $1".to_string(),
        (false, true) => "c.rfc_receptor = $1".to_string(),
        _ => return Ok(0),
    };

    let sql = format!(
        r#"
        WITH targets AS (
            SELECT c.uuid FROM pulso.cfdis c
            WHERE {owner_clause}
              AND c.xml_available IN (1, -1)
              {period_clause}
        ),
        del_taxes   AS (DELETE FROM pulso.cfdi_taxes               WHERE uuid         IN (SELECT uuid FROM targets)),
        del_conc    AS (DELETE FROM pulso.cfdi_concepts             WHERE uuid         IN (SELECT uuid FROM targets)),
        del_pdocs   AS (DELETE FROM pulso.cfdi_payment_docs         WHERE payment_uuid IN (SELECT uuid FROM targets)),
        del_pmts    AS (DELETE FROM pulso.cfdi_payments             WHERE payment_uuid IN (SELECT uuid FROM targets)),
        del_rel     AS (DELETE FROM pulso.cfdi_relacionados         WHERE source_uuid  IN (SELECT uuid FROM targets)),
        del_nomperc AS (DELETE FROM pulso.cfdi_nomina_percepciones  WHERE uuid         IN (SELECT uuid FROM targets)),
        del_nomded  AS (DELETE FROM pulso.cfdi_nomina_deducciones   WHERE uuid         IN (SELECT uuid FROM targets)),
        del_nomop   AS (DELETE FROM pulso.cfdi_nomina_otros_pagos   WHERE uuid         IN (SELECT uuid FROM targets)),
        del_nom     AS (DELETE FROM pulso.cfdi_nomina               WHERE uuid         IN (SELECT uuid FROM targets))
        UPDATE pulso.cfdis SET xml_available = 0
        WHERE uuid IN (SELECT uuid FROM targets)
        "#,
    );

    let result = sqlx::query(&sql).bind(rfc).execute(pool).await?;
    Ok(result.rows_affected())
}

/// Mark all xml_available=0 CFDIs in a job as permanently unavailable (xml_available=-1).
/// Called after repeated failed attempts to fetch the XML from both storage and SAT.
pub async fn mark_xml_unavailable_for_job(
    pool: &PgPool,
    job_id: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"UPDATE pulso.cfdis c
           SET xml_available = -1
           FROM pulso.job_invoices ji
           WHERE c.uuid = ji.uuid AND ji.job_id = $1 AND c.xml_available = 0"#,
    )
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}
