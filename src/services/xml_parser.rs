use quick_xml::Reader;
/// CFDI 3.3 / 4.0 XML parser.
/// Handles: Ingreso (I), Egreso (E), Traslado (T), Pago (P), Nómina (N).
use quick_xml::events::Event;

// ---------------------------------------------------------------------------
// Output structs
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct ParsedCfdi {
    pub uuid: String,
    pub job_id: String,
    pub rfc_emisor: String,
    pub nombre_emisor: Option<String>,
    pub regimen_fiscal_emisor: Option<String>,
    pub rfc_receptor: String,
    pub nombre_receptor: Option<String>,
    pub uso_cfdi: Option<String>,
    pub domicilio_fiscal_receptor: Option<String>,
    pub regimen_fiscal_receptor: Option<String>,
    pub fecha_emision: String,
    pub year: i64,
    pub month: i64,
    pub tipo_comprobante: String,
    pub subtotal: Option<f64>,
    pub descuento: Option<f64>,
    pub total: Option<f64>,
    pub moneda: Option<String>,
    pub tipo_cambio: Option<f64>,
    pub total_mxn: Option<f64>,
    pub metodo_pago: Option<String>,
    pub forma_pago: Option<String>,
    pub lugar_expedicion: Option<String>,
    pub estado_sat: String,
    pub dl_type: String,
    pub xml_available: i64,
    pub created_at: String,
    pub taxes: Vec<ParsedTax>,
    pub concepts: Vec<ParsedConcept>,
    pub payments: Vec<ParsedPayment>,
    pub nomina: Option<ParsedNomina>,
}

#[derive(Debug, Default, Clone)]
pub struct ParsedTax {
    pub impuesto: Option<String>,
    pub tipo_factor: Option<String>,
    pub tasa: Option<f64>,
    pub base: Option<f64>,
    pub importe: Option<f64>,
    pub is_retenido: i64,
}

#[derive(Debug, Default, Clone)]
pub struct ParsedConcept {
    pub clave_prod_serv: Option<String>,
    pub clave_unidad: Option<String>,
    pub descripcion: Option<String>,
    pub cantidad: Option<f64>,
    pub valor_unitario: Option<f64>,
    pub importe: Option<f64>,
    pub descuento: Option<f64>,
}

#[derive(Debug, Default, Clone)]
pub struct ParsedPayment {
    pub fecha_pago: Option<String>,
    pub forma_pago: Option<String>,
    pub moneda_p: Option<String>,
    pub monto: Option<f64>,
    pub tipo_cambio_p: Option<f64>,
    pub docs: Vec<ParsedPaymentDoc>,
}

#[derive(Debug, Default, Clone)]
pub struct ParsedPaymentDoc {
    pub invoice_uuid: String,
    pub num_parcialidad: Option<i64>,
    pub imp_saldo_ant: Option<f64>,
    pub imp_pagado: Option<f64>,
    pub imp_saldo_insoluto: Option<f64>,
    pub moneda_dr: Option<String>,
    pub tipo_cambio_dr: Option<f64>,
}

#[derive(Debug, Default, Clone)]
pub struct ParsedNomina {
    pub tipo_nomina: Option<String>,
    pub fecha_pago: Option<String>,
    pub fecha_inicial_pago: Option<String>,
    pub fecha_final_pago: Option<String>,
    pub num_dias_pagados: Option<f64>,
    pub total_percepciones: Option<f64>,
    pub total_deducciones: Option<f64>,
    pub total_otros_pagos: Option<f64>,
    // Receptor
    pub curp: Option<String>,
    pub tipo_contrato: Option<String>,
    pub tipo_regimen: Option<String>,
    pub num_empleado: Option<String>,
    pub departamento: Option<String>,
    pub puesto: Option<String>,
    pub tipo_jornada: Option<String>,
    pub fecha_inicio_rel_laboral: Option<String>,
    pub antiguedad: Option<String>,
    pub periodicidad_pago: Option<String>,
    pub salario_base_cot_apor: Option<f64>,
    pub salario_diario_integrado: Option<f64>,
    // Percepciones aggregates
    pub total_sueldos: Option<f64>,
    pub total_gravado: Option<f64>,
    pub total_exento: Option<f64>,
    pub percepciones: Vec<ParsedNominaPercepcion>,
    pub deducciones: Vec<ParsedNominaDeduccion>,
}

#[derive(Debug, Default, Clone)]
pub struct ParsedNominaPercepcion {
    pub tipo_percepcion: Option<String>,
    pub clave: Option<String>,
    pub concepto: Option<String>,
    pub importe_gravado: Option<f64>,
    pub importe_exento: Option<f64>,
}

#[derive(Debug, Default, Clone)]
pub struct ParsedNominaDeduccion {
    pub tipo_deduccion: Option<String>,
    pub clave: Option<String>,
    pub concepto: Option<String>,
    pub importe: Option<f64>,
}

// ---------------------------------------------------------------------------
// Parser state
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Clone)]
enum Ctx {
    Root,
    Comprobante,
    Conceptos,
    Concepto,
    ImpuestosGlobal,
    Traslados,
    Retenciones,
    Complemento,
    Pagos,
    Pago,
    PagoTraslados,
    Nomina,
    NominaReceptor,
    NominaPercepciones,
    NomPercepciones,
    NomPercepcion,
    NomDeducciones,
    NomDeduccion,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn parse(
    xml_bytes: &[u8],
    job_id: &str,
    dl_type: &str,
    estado_sat: &str,
) -> Option<ParsedCfdi> {
    let mut reader = Reader::from_reader(xml_bytes);
    reader.config_mut().trim_text(true);

    let mut cfdi = ParsedCfdi::default();
    cfdi.job_id = job_id.to_string();
    cfdi.dl_type = dl_type.to_string();
    cfdi.estado_sat = estado_sat.to_string();
    cfdi.xml_available = 1;
    cfdi.created_at = utc_now();

    let mut ctx_stack: Vec<Ctx> = vec![Ctx::Root];
    let mut current_concept = ParsedConcept::default();
    let mut current_payment = ParsedPayment::default();
    let mut current_nomina = ParsedNomina::default();
    let mut has_nomina = false;
    let mut in_tax_retencion = false;
    let mut buf = Vec::new();

    loop {
        let event = reader.read_event_into(&mut buf);
        match event {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let is_empty = matches!(event, Ok(Event::Empty(_)));
                let local = local_name(e.name().as_ref());
                let ctx = ctx_stack.last().cloned().unwrap_or(Ctx::Root);

                match (local.as_str(), &ctx) {
                    ("Comprobante", _) => {
                        parse_comprobante_attrs(e, &mut cfdi);
                        if !is_empty {
                            ctx_stack.push(Ctx::Comprobante);
                        }
                    }
                    ("Emisor", Ctx::Comprobante) => {
                        parse_emisor_attrs(e, &mut cfdi);
                    }
                    ("Receptor", Ctx::Comprobante) => {
                        parse_receptor_attrs(e, &mut cfdi);
                    }
                    ("Conceptos", Ctx::Comprobante) => {
                        if !is_empty {
                            ctx_stack.push(Ctx::Conceptos);
                        }
                    }
                    ("Concepto", Ctx::Conceptos) => {
                        current_concept = ParsedConcept::default();
                        parse_concept_attrs(e, &mut current_concept);
                        if !is_empty {
                            ctx_stack.push(Ctx::Concepto);
                        } else {
                            cfdi.concepts.push(current_concept.clone());
                        }
                    }
                    ("Impuestos", Ctx::Comprobante) => {
                        if !is_empty {
                            ctx_stack.push(Ctx::ImpuestosGlobal);
                        }
                    }
                    ("Traslados", Ctx::ImpuestosGlobal) => {
                        in_tax_retencion = false;
                        if !is_empty {
                            ctx_stack.push(Ctx::Traslados);
                        }
                    }
                    ("Retenciones", Ctx::ImpuestosGlobal) => {
                        in_tax_retencion = true;
                        if !is_empty {
                            ctx_stack.push(Ctx::Retenciones);
                        }
                    }
                    ("Traslado", Ctx::Traslados) | ("Retencion", Ctx::Retenciones) => {
                        let mut current_tax = ParsedTax::default();
                        parse_tax_attrs(e, &mut current_tax, in_tax_retencion);
                        cfdi.taxes.push(current_tax);
                    }
                    ("Complemento", Ctx::Comprobante) => {
                        if !is_empty {
                            ctx_stack.push(Ctx::Complemento);
                        }
                    }
                    ("TimbreFiscalDigital", Ctx::Complemento) => {
                        if let Some(uuid) = attr(e, b"UUID") {
                            cfdi.uuid = uuid.to_uppercase();
                        }
                    }
                    // Payment complement (pago10 or pago20 namespace)
                    ("Pagos", Ctx::Complemento) => {
                        if !is_empty {
                            ctx_stack.push(Ctx::Pagos);
                        }
                    }
                    ("Pago", Ctx::Pagos) => {
                        current_payment = ParsedPayment::default();
                        parse_pago_attrs(e, &mut current_payment);
                        if !is_empty {
                            ctx_stack.push(Ctx::Pago);
                        } else {
                            cfdi.payments.push(current_payment.clone());
                        }
                    }
                    ("DoctoRelacionado", Ctx::Pago) => {
                        let doc = parse_docto_relacionado(e);
                        current_payment.docs.push(doc);
                    }
                    // Nomina complement
                    ("Nomina", Ctx::Complemento) => {
                        current_nomina = ParsedNomina::default();
                        parse_nomina_attrs(e, &mut current_nomina);
                        has_nomina = true;
                        if !is_empty {
                            ctx_stack.push(Ctx::Nomina);
                        }
                    }
                    ("Receptor", Ctx::Nomina) => {
                        parse_nomina_receptor_attrs(e, &mut current_nomina);
                        if !is_empty {
                            ctx_stack.push(Ctx::NominaReceptor);
                        }
                    }
                    ("Percepciones", Ctx::Nomina) => {
                        parse_nomina_percepciones_totals(e, &mut current_nomina);
                        if !is_empty {
                            ctx_stack.push(Ctx::NomPercepciones);
                        }
                    }
                    ("Percepcion", Ctx::NomPercepciones) => {
                        let p = parse_nomina_percepcion(e);
                        current_nomina.percepciones.push(p);
                    }
                    ("Deducciones", Ctx::Nomina) => {
                        if !is_empty {
                            ctx_stack.push(Ctx::NomDeducciones);
                        }
                    }
                    ("Deduccion", Ctx::NomDeducciones) => {
                        let d = parse_nomina_deduccion(e);
                        current_nomina.deducciones.push(d);
                    }
                    _ => {}
                }
            }

            Ok(Event::End(ref e)) => {
                let local = local_name(e.name().as_ref());
                match local.as_str() {
                    "Concepto" => {
                        cfdi.concepts.push(current_concept.clone());
                        ctx_stack.pop();
                    }
                    "Pago" => {
                        cfdi.payments.push(current_payment.clone());
                        ctx_stack.pop();
                    }
                    "Nomina" => {
                        cfdi.nomina = Some(current_nomina.clone());
                        ctx_stack.pop();
                    }
                    _ => {
                        // pop matching ctx
                        let cur = ctx_stack.last().cloned();
                        let should_pop = matches!(
                            (local.as_str(), cur),
                            ("Comprobante", Some(Ctx::Comprobante))
                                | ("Conceptos", Some(Ctx::Conceptos))
                                | ("Impuestos", Some(Ctx::ImpuestosGlobal))
                                | ("Traslados", Some(Ctx::Traslados))
                                | ("Retenciones", Some(Ctx::Retenciones))
                                | ("Complemento", Some(Ctx::Complemento))
                                | ("Pagos", Some(Ctx::Pagos))
                                | ("Percepciones", Some(Ctx::NomPercepciones))
                                | ("Deducciones", Some(Ctx::NomDeducciones))
                                | ("Receptor", Some(Ctx::NominaReceptor))
                        );
                        if should_pop {
                            ctx_stack.pop();
                        }
                    }
                }
            }

            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    if has_nomina && cfdi.nomina.is_none() {
        cfdi.nomina = Some(current_nomina);
    }

    // Require at minimum an RFC emisor
    if cfdi.rfc_emisor.is_empty() {
        return None;
    }

    // Compute total_mxn
    if let Some(total) = cfdi.total {
        let tc = cfdi.tipo_cambio.unwrap_or(1.0);
        cfdi.total_mxn = Some(total * tc);
    }

    Some(cfdi)
}

/// Parse from job_invoices metadata JSON (no XML available).
/// Extracts what's available from the SAT listing metadata.
pub fn from_metadata(meta_json: &str, job_id: &str, dl_type: &str) -> Option<ParsedCfdi> {
    let v: serde_json::Value = serde_json::from_str(meta_json).ok()?;

    let uuid = v["uuid"]
        .as_str()
        .or_else(|| v["Uuid"].as_str())
        .or_else(|| v["UUID"].as_str())
        .unwrap_or_default()
        .to_uppercase();

    if uuid.is_empty() {
        return None;
    }

    let fecha = v["fecha"]
        .as_str()
        .or_else(|| v["Fecha"].as_str())
        .or_else(|| v["fechaEmision"].as_str())
        .unwrap_or_default();

    let (year, month) = parse_year_month(fecha);

    let rfc_emisor = v["rfcEmisor"]
        .as_str()
        .or_else(|| v["RfcEmisor"].as_str())
        .or_else(|| v["rfc_emisor"].as_str())
        .unwrap_or("UNKNOWN")
        .to_uppercase();

    let rfc_receptor = v["rfcReceptor"]
        .as_str()
        .or_else(|| v["RfcReceptor"].as_str())
        .or_else(|| v["rfc_receptor"].as_str())
        .unwrap_or("UNKNOWN")
        .to_uppercase();

    let nombre_emisor = v["nombreEmisor"]
        .as_str()
        .or_else(|| v["NombreEmisor"].as_str())
        .map(str::to_string);

    let nombre_receptor = v["nombreReceptor"]
        .as_str()
        .or_else(|| v["NombreReceptor"].as_str())
        .map(str::to_string);

    let total = parse_f64_val(&v, &["total", "Total", "monto"]);
    let moneda = v["moneda"]
        .as_str()
        .or_else(|| v["Moneda"].as_str())
        .map(str::to_string);
    let tipo_cambio = parse_f64_val(&v, &["tipoCambio", "TipoCambio", "tipo_cambio"]).or(Some(1.0));

    let tipo_comprobante = v["efectoComprobante"]
        .as_str()
        .or_else(|| v["EfectoComprobante"].as_str())
        .or_else(|| v["tipoComprobante"].as_str())
        .unwrap_or("I")
        .to_uppercase();

    // Map efectoComprobante (Emitido/Recibido) to tipo
    let tipo_comprobante = match tipo_comprobante.as_str() {
        "INGRESO" | "I" => "I",
        "EGRESO" | "E" => "E",
        "TRASLADO" | "T" => "T",
        "PAGO" | "P" => "P",
        "NOMINA" | "N" => "N",
        _ => "I",
    }
    .to_string();

    let estado_sat = v["estado"]
        .as_str()
        .or_else(|| v["Estado"].as_str())
        .unwrap_or("vigente")
        .to_lowercase();

    let total_mxn = total.map(|t| t * tipo_cambio.unwrap_or(1.0));

    Some(ParsedCfdi {
        uuid,
        job_id: job_id.to_string(),
        rfc_emisor,
        nombre_emisor,
        rfc_receptor,
        nombre_receptor,
        fecha_emision: fecha.to_string(),
        year,
        month,
        tipo_comprobante,
        total,
        moneda,
        tipo_cambio,
        total_mxn,
        estado_sat,
        dl_type: dl_type.to_string(),
        xml_available: 0,
        created_at: utc_now(),
        ..Default::default()
    })
}

// ---------------------------------------------------------------------------
// Attribute parsers
// ---------------------------------------------------------------------------

fn parse_comprobante_attrs(e: &quick_xml::events::BytesStart<'_>, c: &mut ParsedCfdi) {
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "Fecha" => {
                let v = val();
                let (y, m) = parse_year_month(&v);
                c.fecha_emision = v;
                c.year = y;
                c.month = m;
            }
            "TipoDeComprobante" => c.tipo_comprobante = val().to_uppercase(),
            "SubTotal" => c.subtotal = val().parse().ok(),
            "Descuento" => c.descuento = val().parse().ok(),
            "Total" => c.total = val().parse().ok(),
            "Moneda" => c.moneda = Some(val()),
            "TipoCambio" => c.tipo_cambio = val().parse().ok(),
            "MetodoPago" => c.metodo_pago = Some(val()),
            "FormaPago" => c.forma_pago = Some(val()),
            "LugarExpedicion" => c.lugar_expedicion = Some(val()),
            _ => {}
        }
    }
}

fn parse_emisor_attrs(e: &quick_xml::events::BytesStart<'_>, c: &mut ParsedCfdi) {
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = String::from_utf8_lossy(&attr.value).to_uppercase();
        match key.as_str() {
            "Rfc" => c.rfc_emisor = val.to_string(),
            "Nombre" => c.nombre_emisor = Some(String::from_utf8_lossy(&attr.value).to_string()),
            "RegimenFiscal" => c.regimen_fiscal_emisor = Some(val.to_string()),
            _ => {}
        }
    }
}

fn parse_receptor_attrs(e: &quick_xml::events::BytesStart<'_>, c: &mut ParsedCfdi) {
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "Rfc" => c.rfc_receptor = val().to_uppercase(),
            "Nombre" => c.nombre_receptor = Some(val()),
            "UsoCFDI" => c.uso_cfdi = Some(val()),
            "DomicilioFiscalReceptor" => c.domicilio_fiscal_receptor = Some(val()),
            "RegimenFiscalReceptor" => c.regimen_fiscal_receptor = Some(val()),
            _ => {}
        }
    }
}

fn parse_concept_attrs(e: &quick_xml::events::BytesStart<'_>, c: &mut ParsedConcept) {
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "ClaveProdServ" => c.clave_prod_serv = Some(val()),
            "ClaveUnidad" => c.clave_unidad = Some(val()),
            "Descripcion" => c.descripcion = Some(val()),
            "Cantidad" => c.cantidad = val().parse().ok(),
            "ValorUnitario" => c.valor_unitario = val().parse().ok(),
            "Importe" => c.importe = val().parse().ok(),
            "Descuento" => c.descuento = val().parse().ok(),
            _ => {}
        }
    }
}

fn parse_tax_attrs(e: &quick_xml::events::BytesStart<'_>, t: &mut ParsedTax, is_ret: bool) {
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "Impuesto" => t.impuesto = Some(val()),
            "TipoFactor" => t.tipo_factor = Some(val()),
            "TasaOCuota" => t.tasa = val().parse().ok(),
            "Base" => t.base = val().parse().ok(),
            "Importe" => t.importe = val().parse().ok(),
            _ => {}
        }
    }
    t.is_retenido = if is_ret { 1 } else { 0 };
}

fn parse_pago_attrs(e: &quick_xml::events::BytesStart<'_>, p: &mut ParsedPayment) {
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "FechaPago" => p.fecha_pago = Some(val()),
            "FormaDePagoP" => p.forma_pago = Some(val()),
            "MonedaP" => p.moneda_p = Some(val()),
            "Monto" => p.monto = val().parse().ok(),
            "TipoCambioP" => p.tipo_cambio_p = val().parse().ok(),
            _ => {}
        }
    }
}

fn parse_docto_relacionado(e: &quick_xml::events::BytesStart<'_>) -> ParsedPaymentDoc {
    let mut d = ParsedPaymentDoc::default();
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "IdDocumento" => d.invoice_uuid = val().to_uppercase(),
            "NumParcialidad" => d.num_parcialidad = val().parse().ok(),
            "ImpSaldoAnt" => d.imp_saldo_ant = val().parse().ok(),
            "ImpPagado" => d.imp_pagado = val().parse().ok(),
            "ImpSaldoInsoluto" => d.imp_saldo_insoluto = val().parse().ok(),
            "MonedaDR" => d.moneda_dr = Some(val()),
            "TipoCambioDR" => d.tipo_cambio_dr = val().parse().ok(),
            _ => {}
        }
    }
    d
}

fn parse_nomina_attrs(e: &quick_xml::events::BytesStart<'_>, n: &mut ParsedNomina) {
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "TipoNomina" => n.tipo_nomina = Some(val()),
            "FechaPago" => n.fecha_pago = Some(val()),
            "FechaInicialPago" => n.fecha_inicial_pago = Some(val()),
            "FechaFinalPago" => n.fecha_final_pago = Some(val()),
            "NumDiasPagados" => n.num_dias_pagados = val().parse().ok(),
            "TotalPercepciones" => n.total_percepciones = val().parse().ok(),
            "TotalDeducciones" => n.total_deducciones = val().parse().ok(),
            "TotalOtrosPagos" => n.total_otros_pagos = val().parse().ok(),
            _ => {}
        }
    }
}

fn parse_nomina_receptor_attrs(e: &quick_xml::events::BytesStart<'_>, n: &mut ParsedNomina) {
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "Curp" => n.curp = Some(val()),
            "TipoContrato" => n.tipo_contrato = Some(val()),
            "TipoRegimen" => n.tipo_regimen = Some(val()),
            "NumEmpleado" => n.num_empleado = Some(val()),
            "Departamento" => n.departamento = Some(val()),
            "Puesto" => n.puesto = Some(val()),
            "TipoJornada" => n.tipo_jornada = Some(val()),
            "FechaInicioRelLaboral" => n.fecha_inicio_rel_laboral = Some(val()),
            "Antiguedad" => n.antiguedad = Some(val()),
            "PeriodicidadPago" => n.periodicidad_pago = Some(val()),
            "SalarioBaseCotApor" => n.salario_base_cot_apor = val().parse().ok(),
            "SalarioDiarioIntegrado" => n.salario_diario_integrado = val().parse().ok(),
            _ => {}
        }
    }
}

fn parse_nomina_percepciones_totals(e: &quick_xml::events::BytesStart<'_>, n: &mut ParsedNomina) {
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "TotalSueldos" => n.total_sueldos = val().parse().ok(),
            "TotalGravado" => n.total_gravado = val().parse().ok(),
            "TotalExento" => n.total_exento = val().parse().ok(),
            _ => {}
        }
    }
}

fn parse_nomina_percepcion(e: &quick_xml::events::BytesStart<'_>) -> ParsedNominaPercepcion {
    let mut p = ParsedNominaPercepcion::default();
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "TipoPercepcion" => p.tipo_percepcion = Some(val()),
            "Clave" => p.clave = Some(val()),
            "Concepto" => p.concepto = Some(val()),
            "ImporteGravado" => p.importe_gravado = val().parse().ok(),
            "ImporteExento" => p.importe_exento = val().parse().ok(),
            _ => {}
        }
    }
    p
}

fn parse_nomina_deduccion(e: &quick_xml::events::BytesStart<'_>) -> ParsedNominaDeduccion {
    let mut d = ParsedNominaDeduccion::default();
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = || String::from_utf8_lossy(&attr.value).to_string();
        match key.as_str() {
            "TipoDeduccion" => d.tipo_deduccion = Some(val()),
            "Clave" => d.clave = Some(val()),
            "Concepto" => d.concepto = Some(val()),
            "Importe" => d.importe = val().parse().ok(),
            _ => {}
        }
    }
    d
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

/// Strip namespace prefix: `cfdi:Comprobante` → `Comprobante`
fn local_name(name: &[u8]) -> String {
    let s = std::str::from_utf8(name).unwrap_or("");
    if let Some(pos) = s.rfind(':') {
        s[pos + 1..].to_string()
    } else {
        s.to_string()
    }
}

fn attr(e: &quick_xml::events::BytesStart<'_>, key: &[u8]) -> Option<String> {
    for a in e.attributes().flatten() {
        let k = local_name(a.key.as_ref());
        if k.as_bytes() == key {
            return Some(String::from_utf8_lossy(&a.value).to_string());
        }
    }
    None
}

fn parse_year_month(fecha: &str) -> (i64, i64) {
    let parts: Vec<&str> = fecha.splitn(3, '-').collect();
    if parts.len() < 2 {
        return (0, 0);
    }
    let y = parts[0].parse().unwrap_or(0);
    let m = parts[1].parse().unwrap_or(0);
    (y, m)
}

fn parse_f64_val(v: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    for k in keys {
        if let Some(n) = v[k].as_f64() {
            return Some(n);
        }
        if let Some(s) = v[k].as_str() {
            if let Ok(f) = s.parse::<f64>() {
                return Some(f);
            }
        }
    }
    None
}

fn utc_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let s = secs % 86400;
    let days = secs / 86400;
    let (y, mo, d) = days_to_ymd(days);
    let h = s / 3600;
    let mi = (s % 3600) / 60;
    let sec = s % 60;
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{sec:02}Z")
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let mut y = 1970u64;
    let mut rem = days;
    loop {
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let dy = if leap { 366 } else { 365 };
        if rem < dy {
            break;
        }
        rem -= dy;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let months = [
        31u64,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut mo = 1u64;
    for &dm in &months {
        if rem < dm {
            break;
        }
        rem -= dm;
        mo += 1;
    }
    (y, mo, rem + 1)
}
