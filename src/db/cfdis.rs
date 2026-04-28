use crate::services::xml_parser::{
    ParsedCfdi, ParsedConcept, ParsedNomina, ParsedNominaDeduccion, ParsedNominaPercepcion,
    ParsedPayment, ParsedPaymentDoc, ParsedTax,
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
    for t in taxes {
        if t.tasa.is_none() {
            continue;
        }
        sqlx::query(
            r#"
            INSERT INTO pulso.cfdi_taxes
                (uuid, impuesto, tipo_factor, tasa, base, importe, is_retenido)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(uuid)
        .bind(&t.impuesto)
        .bind(&t.tipo_factor)
        .bind(t.tasa)
        .bind(t.base)
        .bind(t.importe)
        .bind(t.is_retenido)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn insert_concepts(
    pool: &PgPool,
    uuid: &str,
    concepts: &[ParsedConcept],
) -> Result<(), sqlx::Error> {
    for c in concepts {
        sqlx::query(r#"
            INSERT INTO pulso.cfdi_concepts
                (uuid, clave_prod_serv, clave_unidad, descripcion, cantidad, valor_unitario, importe, descuento)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            "#)
        .bind(uuid)
        .bind(&c.clave_prod_serv)
        .bind(&c.clave_unidad)
        .bind(&c.descripcion)
        .bind(c.cantidad)
        .bind(c.valor_unitario)
        .bind(c.importe)
        .bind(c.descuento)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn insert_payments(
    pool: &PgPool,
    payment_uuid: &str,
    payments: &[ParsedPayment],
) -> Result<(), sqlx::Error> {
    for (i, p) in payments.iter().enumerate() {
        let idx = i as i64;
        sqlx::query(
            r#"
            INSERT INTO pulso.cfdi_payments
                (payment_uuid, pago_num, fecha_pago, forma_pago, moneda_p, monto, tipo_cambio_p)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(payment_uuid)
        .bind(idx)
        .bind(&p.fecha_pago)
        .bind(&p.forma_pago)
        .bind(&p.moneda_p)
        .bind(p.monto)
        .bind(p.tipo_cambio_p)
        .execute(pool)
        .await?;

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

pub async fn insert_nomina(
    pool: &PgPool,
    uuid: &str,
    n: &ParsedNomina,
) -> Result<(), sqlx::Error> {
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

    for p in &n.percepciones {
        insert_nomina_percepcion(pool, uuid, p).await?;
    }
    for d in &n.deducciones {
        insert_nomina_deduccion(pool, uuid, d).await?;
    }
    Ok(())
}

async fn insert_nomina_percepcion(
    pool: &PgPool,
    uuid: &str,
    p: &ParsedNominaPercepcion,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_nomina_percepciones
            (uuid, tipo_percepcion, clave, concepto, importe_gravado, importe_exento)
        VALUES ($1,$2,$3,$4,$5,$6)
        "#,
    )
    .bind(uuid)
    .bind(&p.tipo_percepcion)
    .bind(&p.clave)
    .bind(&p.concepto)
    .bind(p.importe_gravado)
    .bind(p.importe_exento)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_nomina_deduccion(
    pool: &PgPool,
    uuid: &str,
    d: &ParsedNominaDeduccion,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO pulso.cfdi_nomina_deducciones
            (uuid, tipo_deduccion, clave, concepto, importe)
        VALUES ($1,$2,$3,$4,$5)
        "#,
    )
    .bind(uuid)
    .bind(&d.tipo_deduccion)
    .bind(&d.clave)
    .bind(&d.concepto)
    .bind(d.importe)
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
        LEFT JOIN pulso.cfdis c ON UPPER(c.uuid) = UPPER(ji.uuid)
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
        LEFT JOIN pulso.cfdis c ON UPPER(c.uuid) = UPPER(ji.uuid)
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
