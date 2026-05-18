# Documentación Completa: XML Dashboard MVP

> **Propósito de este documento**: Contexto exhaustivo para LLMs y desarrolladores. Cada afirmación incluye la referencia exacta al archivo fuente y línea de donde proviene.

---

## 1. Descripción General del Proyecto

El **XML Dashboard MVP** es una aplicación Streamlit de análisis financiero para Excel de exportaciones SAT (CFDI). Diseñada para **due diligence, M&A y FP&A**, procesa dos universos principales:

1. **Facturas (Emitidas/Recibidas)**: Análisis de ingresos y egresos con desglose por cliente, cobranza y recurrencia
2. **Nómina (CFDI Complement)**: Análisis de estructura salarial, composición de percepciones/deducciones, auditoría fiscal, pasivo laboral estimado y gestión de altas/bajas

La app está construida con **Python 3.11**, **Streamlit 1.53+**, **Pandas**, **Plotly** y **python-pptx**, integrando un sistema de tokens visuales centralizado, normalización de datos robusta y exportaciones editables (Excel, PowerPoint).

---

## 2. Diagrama de Arquitectura (ASCII)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Streamlit Frontend (app.py, ui_catalog.py)              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┬──────────────────┬─────────────────────────────────┐   │
│  │   UI Renderers  │  Chart Builders  │  HTML Builders & Tokens         │   │
│  │  (renderers.py) │ (chart_builders) │  (html_builders, tokens, css)   │   │
│  └─────────────────┴──────────────────┴─────────────────────────────────┘   │
│                          ↓                                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │              Session State Management (session_state.py)             │   │
│  │         Almacena: df_std, df_clean, rules, warnings, etc.           │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                          ↓                                                   │
└──────────────────────────────────────────┬──────────────────────────────────┘
                                           │
┌──────────────────────────────────────────▼──────────────────────────────────┐
│                  Core Data Processing Pipeline (src/)                       │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─ INGEST ────────────────────────────────────────────────────────────┐    │
│  │ ingest.py: read_excel_smart() → IngestResult                       │    │
│  └───────────────────┬───────────────────────────────────────────────┘    │
│                      ↓                                                      │
│  ┌─ MAPPING ───────────────────────────────────────────────────────────┐    │
│  │ mapping.py: apply_column_mapping() con column_mapping.yml          │    │
│  │            detect_dataset_type(), detect_company_identity()        │    │
│  └───────────────────┬───────────────────────────────────────────────┘    │
│                      ↓                                                      │
│  ┌─ CLEANING ──────────────────────────────────────────────────────────┐    │
│  │ clean.py: clean_invoices() para facturas                           │    │
│  │ analysis_payroll.py: clean_payroll() para nómina                   │    │
│  │ payments_analysis.py: clean_payment_complements() para compl.pago  │    │
│  └───────────────────┬───────────────────────────────────────────────┘    │
│                      ↓                                                      │
│  ┌─ STANDARD VIEW ─────────────────────────────────────────────────────┐    │
│  │ analysis.py: build_standard_view() → [month, year, total_neto_mxn] │    │
│  │ analysis_payroll.py: build_standard_view_payroll()                 │    │
│  └───────────────────┬───────────────────────────────────────────────┘    │
│                      ↓                                                      │
│  ┌─ ANALYSIS ──────────────────────────────────────────────────────────┐    │
│  │ analysis.py: monthly_net, ltm_summary, top_counterparties, etc.    │    │
│  │ analysis_payroll.py: dept concentration, employee profiles         │    │
│  │ payments_analysis.py: collections (Emitidas), payables (Recibidas) │    │
│  │ payroll_audit.py: comparación real vs teórico, desglose SAT        │    │
│  │ quarterly.py: agregación trimestral                                │    │
│  │ payroll_normalization.py: ajustes de normalización de empleados    │    │
│  │ normalization_actions.py: agrupación de contrapartes               │    │
│  └───────────────────┬───────────────────────────────────────────────┘    │
│                      ↓                                                      │
│  ┌─ CASH FLOW ─────────────────────────────────────────────────────────┐    │
│  │ cashflow.py: build_cash_flow() con supuestos de cobranza/pagos     │    │
│  │ cashflow_tax.py: estimación de IVA/ISR/patronales mensuales        │    │
│  │ cashflow_presentation.py: bundles para UI y export                 │    │
│  │ cashflow_snapshot_runtime.py: snapshot desde invoice/payroll data  │    │
│  └───────────────────┬───────────────────────────────────────────────┘    │
│                      ↓                                                      │
│  ┌─ NARRATIVE & FINDINGS ──────────────────────────────────────────────┐    │
│  │ invoice_narratives.py: builders para RES01 findings                │    │
│  └───────────────────┬───────────────────────────────────────────────┘    │
│                      ↓                                                      │
│  ┌─ EXPORT ────────────────────────────────────────────────────────────┐    │
│  │ export_pptx.py: PowerPoint editable para facturas                  │    │
│  │ export_cashflow_pptx.py: PowerPoint para cash flow analysis        │    │
│  │ export_availability.py: chequeo de disponibilidad de exports       │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Estructura de Directorios

```
xml-dashboard-mvp/
│
├── app.py                          # Aplicación Streamlit principal
├── ui_catalog.py                   # Catálogo interno de UI para referencias de equipo
├── requirements.txt                # Dependencias (pandas, streamlit, plotly, pptx, etc.)
├── column_mapping.yml              # Mapeo de columnas para facturas
├── column_mapping_payroll.yml      # Mapeo de columnas para nómina
├── README.md                       # Guía de instalación y uso
│
├── src/
│   ├── ingest.py                   # Lectura de Excel (smart sheet selection)
│   ├── mapping.py                  # Mapeo de columnas + detección de tipo de dataset
│   ├── clean.py                    # Limpieza de facturas: montos, fechas, tipo_comprobante
│   ├── analysis.py                 # Análisis financiero: monthly_net, LTM, top clientes, YoY
│   ├── analysis_payroll.py         # Análisis de nómina: clean_payroll, KPIs salariales
│   ├── payments_analysis.py        # Análisis de cobranza/CxP con complementos de pago
│   ├── payroll_intake.py           # Selección inteligente de hoja de nómina en Excel
│   ├── payroll_normalization.py    # Reglas de normalización de empleados (excluir/escalar)
│   ├── payroll_audit.py            # Auditoría fiscal: ISR real vs teórico, IMSSable
│   ├── quarterly.py                # Agregación mensual → trimestral
│   ├── cashflow.py                 # Proyección de flujo de caja con supuestos
│   ├── cashflow_tax.py             # Estimación mensual de IVA, ISR, cuotas patronales
│   ├── cashflow_presentation.py    # Bundles de datos para UI de cashflow
│   ├── cashflow_snapshot_runtime.py# Snapshot de cashflow desde datos de sesión
│   ├── normalization_actions.py    # Agrupación/renombrado de contrapartes
│   ├── invoice_dates.py            # Helpers de fecha para facturas
│   ├── invoice_narratives.py       # Texto narrativo automático (findings RES01)
│   ├── ltm_display.py              # Control de renderizado de bloque LTM
│   ├── pre_dashboard.py            # Validaciones pre-dashboard (warning health)
│   ├── session_state.py            # Estado de sesión Streamlit
│   ├── warning_health.py           # Sistema de health checks con warnings
│   └── export_availability.py      # Disponibilidad de exportaciones
│
├── ui/
│   ├── renderers.py                # Renderizadores de secciones (NRS01–NRS12, etc.)
│   ├── chart_builders.py           # Constructores de gráficas Plotly
│   ├── html_builders.py            # HTML cards, tablas, badges, scorecards
│   ├── tokens.py                   # Design tokens: colores, tamaños, estilos
│   ├── css_loader.py               # Carga de CSS para la app
│   ├── plotly_theme.py             # Tema global de Plotly
│   └── section_registry.py         # Registro de secciones activas en el dashboard
│
├── tests/                          # Pruebas unitarias y de contrato
└── data/                           # Datos de ejemplo para pruebas
```

---

## 4. Pipeline de Procesamiento: Facturas

### 4.1 Ingesta — `src/ingest.py`

**Función principal**: `read_excel_smart(uploaded_file) → IngestResult`
**Fuente**: [`src/ingest.py:17-43`](src/ingest.py)

Lógica de selección de hoja:

```python
# src/ingest.py:35
sheet_used = "XML" if "XML" in sheets else sheets[0]
```

- Si existe una hoja llamada exactamente `"XML"`, la usa.
- Si no, usa la **primera hoja**.
- Lee **todo como `dtype=str`** para evitar coerciones automáticas de tipos.
- Elimina filas/columnas completamente vacías (`src/ingest.py:39-40`).

**Retorna** `IngestResult` (dataclass):
- `df_raw`: DataFrame crudo
- `sheet_used`: nombre de hoja elegida
- `available_sheets`: lista de todas las hojas

> **Por qué `dtype=str`**: Los montos SAT pueden venir con `$`, `,` o espacios. La limpieza explícita en `clean.py` los convierte a numérico de forma controlada.

---

### 4.2 Mapeo de Columnas — `src/mapping.py`

**Función principal**: `apply_column_mapping(df_raw, mapping) → MappingResult`
**Fuente**: [`src/mapping.py:252-334`](src/mapping.py)

#### 4.2.1 Carga del YAML de mapeo

```python
# src/mapping.py:113-118
def load_mapping_yaml(path: str = "column_mapping.yml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    if yaml is not None:
        return yaml.safe_load(raw)
    return _load_simple_yaml(raw)  # fallback sin PyYAML
```

El fallback `_load_simple_yaml` ([`src/mapping.py:46-102`](src/mapping.py)) parsea YAML manualmente para ambientes sin `pyyaml`. Soporta: comentarios con `#`, strings con comillas dobles, booleans, integers, listas con `- `, dicts anidados.

#### 4.2.2 Estructura del YAML de facturas

**Fuente**: [`column_mapping.yml:1-197`](column_mapping.yml)

El YAML tiene tres secciones:

| Sección | Propósito |
|---------|-----------|
| `required` | Campos obligatorios; su ausencia puede bloquear el pipeline |
| `optional` | Campos opcionales que enriquecen el análisis |
| `derived` | Flags de construcción automática |

**Campos requeridos** (con ejemplos de aliases) — [`column_mapping.yml:5-92`](column_mapping.yml):

| Campo interno | Aliases YAML de ejemplo |
|---------------|------------------------|
| `fecha_emision` | `"FechaEmisionXML"`, `"FechaTimbradoXML"`, `"Fecha de emisión"` |
| `tipo_comprobante` | `"Tipo"`, `"TipoComprobante"`, `"Tipo de Comprobante"` |
| `rfc_emisor` | `"RFC Emisor"`, `"RFCEmisor"`, `"RFC del Emisor"` |
| `rfc_receptor` | `"RFC Receptor"`, `"RFCReceptor"` |
| `subtotal` | `"SubTotal"`, `"Subtotal"`, `"Sub Total"` |
| `descuento` | `"Descuento"`, `"DESCUENTO"` |
| `total` | `"Total"`, `"TOTAL"` |
| `moneda` | `"Moneda"`, `"Currency"`, `"Divisa"` |
| `tipo_cambio` | `"TipoCambio"`, `"TC"`, `"exchange_rate"` |

**Campos opcionales** (con ejemplos) — [`column_mapping.yml:95-191`](column_mapping.yml):

| Campo interno | Uso |
|---------------|-----|
| `uuid` | Deduplicación exacta de facturas |
| `estado_sat` | Vigencia/cancelación SAT |
| `metodo_pago` | `PPD` (parcialidades) vs `PUE` (pago único) |
| `forma_pago` | Transferencia, cheque, efectivo, etc. |
| `iva_16_importe`, `iva_8_importe` | Desglose fiscal |
| `isr_retenido`, `iva_retenido` | Retenciones aplicadas |

#### 4.2.3 Búsqueda de columna exacta

```python
# src/mapping.py:120-125
def _find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None
```

**Importante**: Coincidencia exacta de string (case-sensitive). Los aliases en el YAML cubren las variantes de mayúsculas más comunes. No hay fuzzy matching.

#### 4.2.4 Reglas de validación flexibles

```python
# src/mapping.py:293-325
for internal_name in required.keys():
    # Emitidas: no requiere datos de emisor (es la propia empresa)
    if dataset_type == "Emitidas" and internal_name in ["rfc_emisor", "nombre_emisor"]:
        continue
    # Recibidas: no requiere datos de receptor
    if dataset_type == "Recibidas" and internal_name in ["rfc_receptor", "nombre_receptor"]:
        continue
    # Si ya hay fecha completa, año/mes no son obligatorios
    if internal_name in ["anio_emision", "mes_emision"] and "fecha_emision" in df_mapped.columns:
        continue
    # fecha_emision puede construirse con año+mes
    if internal_name == "fecha_emision":
        has_anio_mes = ("anio_emision" in df_mapped.columns) and ("mes_emision" in df_mapped.columns)
        if ("fecha_emision" not in df_mapped.columns) and has_anio_mes:
            warnings.append("No existe 'fecha_emision'. Se construirá usando...")
            continue
    # tipo_comprobante nunca bloquea (solo emite warning)
    if internal_name == "tipo_comprobante":
        if "tipo_comprobante" not in df_mapped.columns:
            warnings.append("No existe 'tipo_comprobante'. Se asumirá positivo...")
            continue
```

**Fuente**: [`src/mapping.py:293-325`](src/mapping.py)

**Regla crítica**: `tipo_comprobante` **nunca bloquea** el pipeline aunque falte; simplemente todos los montos quedan positivos.

---

### 4.3 Detección Automática de Tipo de Dataset — `src/mapping.py`

**Función**: `detect_dataset_type(df_mapped) → DatasetDetection`
**Fuente**: [`src/mapping.py:135-215`](src/mapping.py)

Esta función resuelve si el archivo contiene facturas **emitidas** (ingresos de la empresa) o **recibidas** (gastos de la empresa), basándose en cuál de los dos RFC (emisor o receptor) es constante:

```python
# src/mapping.py:180-195
if n_emisor == 1 and n_receptor > 1:
    return DatasetDetection(
        dataset_type="Emitidas",
        note="Emisor constante y receptor variable: facturas emitidas (Ingresos)."
    )
if n_receptor == 1 and n_emisor > 1:
    return DatasetDetection(
        dataset_type="Recibidas",
        note="Receptor constante y emisor variable: facturas recibidas (Egresos)."
    )
```

**Casos adicionales** ([`src/mapping.py:143-214`](src/mapping.py)):
- Sin columna `rfc_emisor` → asume **Emitidas**
- Sin columna `rfc_receptor` → asume **Recibidas**
- Sin ninguna de las dos → `"Mixto"` con warning
- Ambos RFC varían → `"Mixto"` con warning (raro, indica error en archivo)

**Resultado**: `DatasetDetection` dataclass ([`src/mapping.py:128-133`](src/mapping.py)):
- `dataset_type`: `"Emitidas"` | `"Recibidas"` | `"Mixto"`
- `n_unique_emisor`, `n_unique_receptor`: cantidad de RFC únicos
- `warning_mixed_both_vary`: booleano
- `note`: descripción textual de la clasificación

---

### 4.4 Diferenciación de Tipos de Comprobante — `src/clean.py`

Esta es la lógica central para clasificar cada CFDI dentro del pipeline de facturas.

#### 4.4.1 El campo `tipo_comprobante` y el SAT

El SAT define el atributo `TipoDeComprobante` en cada CFDI. Los valores estándar son:

| Código SAT | Significado |
|------------|-------------|
| `I` | **Ingreso** — factura normal de venta |
| `E` | **Egreso** — nota de crédito, devolución |
| `T` | **Traslado** — carta porte, sin importe fiscal |
| `P` | **Pago** — complemento de pago (recibo electrónico) |
| `N` | **Nómina** — recibo de nómina (pipeline separado) |

Cuando se exporta desde el Portal SAT o herramientas como Contpaqi/Aspel/CONTPAQi Comercial, el valor puede venir como:
- Código corto: `"I"`, `"E"`, `"P"`, `"N"`, `"T"`
- Texto completo: `"I - INGRESO"`, `"E - EGRESO"`, `"Egreso"`
- Código descriptivo: `"NC"` (nota de crédito)
- Texto parcial: `"NOTA DE CREDITO"`

#### 4.4.2 Función `_is_negative_comprobante`

**Fuente**: [`src/clean.py:57-72`](src/clean.py)

```python
def _is_negative_comprobante(tipo_series: pd.Series) -> pd.Series:
    s = tipo_series.astype(str).str.upper().fillna("")
    s = s.str.strip()

    cond_egreso = s.str.contains("EGRESO")                              # L.68
    cond_nc = s.str.contains(r"\bNC\b", regex=True)                     # L.69
    cond_nota_credito = s.str.contains("NOTA") & (                      # L.70
        s.str.contains("CRED") | s.str.contains("CRÉD")
    )

    return cond_egreso | cond_nc | cond_nota_credito
```

**Regla**: Si cualquiera de las condiciones es `True` → el comprobante es negativo.

| Condición | Matchea con |
|-----------|------------|
| `contains("EGRESO")` | `"E - EGRESO"`, `"EGRESO"`, `"Egreso"` |
| `contains(r"\bNC\b")` | `"NC"` como token completo (no `"INCOMPLETO"`) |
| `contains("NOTA") & contains("CRED")` | `"NOTA DE CRÉDITO"`, `"NOTA CREDITO"` |

**Por qué texto y no código**: Los exportadores SAT y de terceros producen formatos inconsistentes. Buscar `"EGRESO"` como substring es más robusto que `== "E"`, que fallaría para `"E - EGRESO"`. Fuente de diseño: [`src/clean.py:57-72`](src/clean.py).

#### 4.4.3 Aplicación del signo negativo en `clean_invoices`

**Fuente**: [`src/clean.py:164-179`](src/clean.py)

```python
# src/clean.py:164-179
if "subtotal" not in df.columns or "descuento" not in df.columns:
    base = pd.NA
else:
    base = (df["subtotal"].fillna(0) - df["descuento"].fillna(0))  # L.169

if "tipo_comprobante" in df.columns:
    neg = _is_negative_comprobante(df["tipo_comprobante"])          # L.172
else:
    neg = pd.Series([False] * len(df))                              # L.174 — sin tipo_comprobante todo es positivo

df["total_neto_mxn"] = pd.to_numeric(base, errors="coerce") * pd.to_numeric(df["tipo_cambio"], errors="coerce")  # L.178
df.loc[neg, "total_neto_mxn"] = df.loc[neg, "total_neto_mxn"] * -1  # L.179 — inversión del signo
```

**Resultado**: La columna `total_neto_mxn` = `(subtotal - descuento) × tipo_cambio`, negativo para egresos/NC.

> **Diferencia entre `total_mxn` y `total_neto_mxn`**:
> - `total_mxn` ([`src/clean.py:152-162`](src/clean.py)): basado en `Total` (incluyendo IVA). Para cobranza y CxP.
> - `total_neto_mxn` ([`src/clean.py:164-179`](src/clean.py)): basado en `Subtotal - Descuento`, con signo según `tipo_comprobante`. Para análisis financiero/P&L.

#### 4.4.4 Separación Facturas vs Nómina

Las nóminas (`TipoDeComprobante = N`) se procesan en un **pipeline completamente independiente**:

```
Facturas (I, E, T, P)                    Nómina (N)
─────────────────────                    ──────────
column_mapping.yml                       column_mapping_payroll.yml
src/ingest.py → src/mapping.py           src/payroll_intake.py (selección de hoja)
src/clean.py                             src/analysis_payroll.py:clean_payroll()
src/analysis.py                          src/analysis_payroll.py:build_standard_view_payroll()
payments_analysis.py (CxC/CxP)          payroll_audit.py, payroll_normalization.py
```

Los archivos de nómina **nunca mezclan** con el DataFrame de facturas. La detección de que un archivo es de nómina ocurre en `src/payroll_intake.py` mediante la presencia de columnas fuertes como `rfc_receptor`, `fecha_final_pago`, `total_percepciones`, `total_deducciones` (definidas en [`src/payroll_intake.py:15-20`](src/payroll_intake.py)).

---

### 4.5 Limpieza de Facturas — `src/clean.py`

**Función principal**: `clean_invoices(df_mapped) → CleanResult`
**Fuente**: [`src/clean.py:101-205`](src/clean.py)

Pasos en orden de ejecución:

| Paso | Línea | Descripción |
|------|-------|-------------|
| 1 | L.106-117 | Normalizar RFC a mayúsculas con `_upper_clean()`; crear `moneda = "MXN"` si falta |
| 2 | L.118-126 | Construir `fecha_emision` desde `anio_emision + mes_emision` si no existe |
| 3 | L.120-126 | Parsear `fecha_emision` como datetime; extraer `anio`, `mes` |
| 4 | L.129-139 | Convertir `subtotal`, `descuento`, `total`, `tipo_cambio` a numérico con `_to_number()` |
| 3b | L.134-139 | Columnas de IVA/IEPS/ISR a numérico (valores en moneda original) |
| 5 | L.141-150 | Forzar `tipo_cambio = 1.0` donde `moneda = MXN` y TC es NaN o ≤ 0 |
| 6 | L.152-162 | Calcular `total_mxn` = `total × tipo_cambio` (para cobranza) |
| 7 | L.164-179 | Calcular `total_neto_mxn` = `(subtotal - descuento) × TC × signo_comprobante` |
| 8 | L.181-184 | Crear `estado_sat = "NO_REPORTADO"` si no existe |
| 9 | L.186-193 | Normalizar nombres por RFC: tomar nombre del CFDI más reciente por RFC |
| 10 | L.195-203 | Deduplicar por `uuid` si existe |

**Función `_canonical_name_by_rfc`** ([`src/clean.py:75-98`](src/clean.py)): Para cada RFC, toma el nombre de la fila más reciente (por `fecha_emision`) y lo asigna a todas las filas con ese RFC. Evita que el mismo RFC aparezca con múltiples nombres según el export.

**Función `_to_number`** ([`src/clean.py:16-21`](src/clean.py)):
```python
def _to_number(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "nan": None, "None": None})
    s = s.str.replace("$", "", regex=False)
    s = s.str.replace(",", "", regex=False)
    return pd.to_numeric(s, errors="coerce")
```
Elimina `$` y `,` antes de parsear. Maneja `"nan"`, `"None"` como nulos.

---

### 4.6 Vista Estándar — `src/analysis.py`

**Función principal**: `build_standard_view(df_clean, dataset_type) → StandardViewResult`
**Fuente**: [`src/analysis.py:86-204`](src/analysis.py)

Transforma el DataFrame limpio en una vista analítica uniforme con columnas:
- `month` (Period[M]), `year` (int), `month_num` (int)
- `total_neto_mxn` (float)
- `contraparte_rfc`, `contraparte_nombre`, `contraparte_key`, `contraparte_label`, `contraparte_tipo`

#### Asignación de contraparte según tipo de dataset

```python
# src/analysis.py:149-161
if dataset_type == "Emitidas":
    rfc_col = "rfc_receptor"       # La empresa emite → clientes son receptores
    name_col = "nombre_receptor"
    role = "cliente"
elif dataset_type == "Recibidas":
    rfc_col = "rfc_emisor"         # La empresa recibe → proveedores son emisores
    name_col = "nombre_emisor"
    role = "proveedor"
```

#### Resolución robusta de fecha

**Función `_find_best_date_column`** ([`src/analysis.py:33-83`](src/analysis.py)): Si no existe `fecha_emision`, busca en orden:
1. `fecha_emision` (explícita)
2. Candidatos exactos: `FechaTimbradoXML`, `fecha_timbrado`, `Fecha`, etc.
3. Heurística por substring: primero `"timbr"`, luego `"emisi"`, luego `"fecha"`

Si todo falla, intenta construir desde `anio_emision + mes_emision` ([`src/analysis.py:125-132`](src/analysis.py)).

#### `contraparte_key` vs `contraparte_label`

```python
# src/analysis.py:178-187
df["contraparte_key"] = df["contraparte_rfc"]
mask_key_empty = df["contraparte_key"].eq("") | df["contraparte_key"].isna()
df.loc[mask_key_empty, "contraparte_key"] = df.loc[mask_key_empty, "contraparte_nombre"]  # fallback a nombre

df["contraparte_label"] = df["contraparte_nombre"]
mask_label_empty = df["contraparte_label"].eq("") | df["contraparte_label"].isna()
df.loc[mask_label_empty, "contraparte_label"] = df.loc[mask_label_empty, "contraparte_key"]
```

- `contraparte_key`: RFC si existe, nombre si no → usado para **agrupar** (estable)
- `contraparte_label`: nombre si existe, key si no → usado para **mostrar** al usuario

---

## 5. Análisis Financiero — `src/analysis.py`

### 5.1 Ingresos Netos Mensuales

**Función**: `monthly_net(df_std) → pd.DataFrame`
**Fuente**: [`src/analysis.py:207-214`](src/analysis.py)

```python
def monthly_net(df_std: pd.DataFrame) -> pd.DataFrame:
    g = (
        df_std.dropna(subset=["month"])
        .groupby(["year", "month_num", "month"], as_index=False)["total_neto_mxn"]
        .sum()
        .sort_values(["year", "month_num"])
    )
    return g
```

Suma `total_neto_mxn` por mes. Como los egresos/NC ya son negativos (por `_is_negative_comprobante`), el resultado es el ingreso neto real.

### 5.2 Crecimiento Año contra Año (YoY)

**Función**: `yoy_growth_from_monthly(monthly_df) → pd.DataFrame`
**Fuente**: [`src/analysis.py:217-225`](src/analysis.py)

```python
annual["prev"] = annual["total_neto_mxn"].shift(1)
annual["yoy_pct"] = (annual["total_neto_mxn"] / annual["prev"] - 1.0) * 100.0
```

Suma anual, luego calcula `(año_actual / año_anterior - 1) × 100`. El primer año siempre tiene `yoy_pct = NaN`.

### 5.3 Top Contrapartes por Año

**Función**: `top_counterparties_table(df_std, top_n) → pd.DataFrame`
**Fuente**: [`src/analysis.py:228-296`](src/analysis.py)

Para cada año produce filas con grupo `"Top"`, `"Otros"` y `"Total"`. Las contrapartes fuera del top N se agregan en una sola fila `"OTROS"` ([`src/analysis.py:260-274`](src/analysis.py)).

Columnas resultado:
- `year`, `contraparte_key`, `contraparte_label`
- `total_neto_mxn`, `num_facturas`, `ticket_promedio`
- `total_year`, `pct_year` (share del total anual)
- `grupo` (`"Top"` | `"Otros"` | `"Total"`)

### 5.4 Recurrencia de Clientes

**Función**: `recurrence_months_table(df_std) → pd.DataFrame`
**Fuente**: [`src/analysis.py:299-323`](src/analysis.py)

Calcula cuántos clientes (contrapartes únicas por RFC) compraron en X meses distintos. Resultado: distribución de `meses_activos` → `clientes` → `total_neto_mxn` → `pct_total`.

Útil para detectar la concentración en clientes esporádicos vs recurrentes.

### 5.5 Retención de Clientes

**Función**: `retention_summary(df_std) → pd.DataFrame`
**Fuente**: [`src/analysis.py:326-381`](src/analysis.py)

Para cada año calcula:
- `clientes_nuevos`: presentes este año, no en el anterior
- `clientes_perdidos`: presentes el año anterior, no en este
- `clientes_retenidos`: presentes en ambos años
- `ventas_nuevos`, `ventas_retenidos`, `pct_ventas_nuevos`, `pct_ventas_retenidos`

El primer año en el dataset tiene `clientes_perdidos = 0` y `clientes_retenidos = 0` (no hay referencia anterior).

---

## 6. Análisis LTM (Last Twelve Months) — `src/analysis.py`

### 6.1 ¿Qué es LTM?

LTM ("Last Twelve Months") es un indicador estándar de M&A/due diligence que normaliza el análisis de empresas con datos parciales o que no cierran en diciembre. Representa los últimos 12 meses de actividad a partir de un mes de corte.

### 6.2 Función principal: `ltm_summary`

**Fuente**: [`src/analysis.py:427-538`](src/analysis.py)

**Parámetros**:
- `df_std`: vista estándar de facturas
- `cutoff_month`: `"YYYY-MM"` o `pd.Period`. Si es `None`, usa el último mes con datos.

**Lógica de ventanas** ([`src/analysis.py:454-459`](src/analysis.py)):
```python
cutoff = monthly["_period"].max()   # o el parámetro cutoff_month
ltm_start     = cutoff - 11         # 12 meses hacia atrás (inclusive)
prior_start   = cutoff - 23         # LTM previo: -24 a -12
prior_end     = cutoff - 12
```

**Campos retornados** ([`src/analysis.py:516-538`](src/analysis.py)):

| Campo | Descripción |
|-------|-------------|
| `ltm_value` | Suma de `total_neto_mxn` en los últimos 12 meses |
| `ltm_months` | Meses con datos en la ventana LTM (puede ser < 12 si el dataset es parcial) |
| `ltm_avg_monthly` | Promedio mensual en el LTM |
| `prior_ltm_value` | Suma del LTM anterior (meses -24 a -12) |
| `vs_prior_ltm_pct` | Crecimiento LTM vs LTM previo (solo si prior tiene ≥ 6 meses) |
| `last_full_year` | Último año calendario con 12 meses completos en el dataset |
| `vs_last_full_year_pct` | Crecimiento LTM vs último año completo |
| `is_partial_ltm` | `True` si la ventana tiene < 12 meses |

**Cómo encuentra el último año completo** ([`src/analysis.py:491-513`](src/analysis.py)):
```python
# Filtra años con exactamente 12 meses de datos, que sean anteriores al cutoff
# (o el propio año del cutoff si es diciembre)
fy_cands = by_year[
    (by_year["n_months"] == 12)
    & (
        (by_year["_year"] < _cutoff_yr)
        | ((by_year["_year"] == _cutoff_yr) & _cutoff_year_end)
    )
]
```

### 6.3 Gate de visualización: `ltm_display_allowed`

**Fuente**: [`src/analysis.py:725-759`](src/analysis.py)

Suprime el bloque LTM cuando el `cutoff_month` cae en **enero o febrero** del año siguiente al último año completo. Esos dos meses tienen tan poco dato que la comparación LTM vs FY confunde más que informa.

```python
# src/analysis.py:756-759
return not (
    cutoff_p.year == int(ltm_dict["last_full_year"]) + 1
    and cutoff_p.month <= 2
)
```

### 6.4 LTM por contraparte: `ltm_by_counterparty`

**Fuente**: [`src/analysis.py:541-679`](src/analysis.py)

Mismo concepto pero por contraparte individual. Detecta:
- `"Nueva en LTM"`: activa en LTM actual, no en LTM previo
- `"Perdida vs LTM previo"`: activa en LTM previo, no en el actual
- `"Retenida"`: activa en ambos periodos

Incluye contrapartes que desaparecieron en el LTM actual (presentes solo en LTM previo) con `ltm_actual_mxn = 0.0` ([`src/analysis.py:622-634`](src/analysis.py)).

---

## 7. Análisis de Cobranza/CxP — `src/payments_analysis.py`

### 7.1 Propósito

Cruza las facturas (Emitidas o Recibidas) con los **complementos de pago** (XMLs de tipo `P`) para calcular:
- Qué facturas están cobradas / pendientes
- Días promedio de cobro (PPD)
- Montos en riesgo de incobrabilidad

### 7.2 Detección de archivo de complementos de pago

**Función**: `is_payments_related_df(df_raw) → bool`
**Fuente**: [`src/payments_analysis.py:147-159`](src/payments_analysis.py)

```python
required_signals = {
    _norm_col("IdDocumento"),   # UUID de la factura referenciada
    _norm_col("FechaPago"),     # Fecha del pago
    _norm_col("ImpPagado"),     # Monto pagado
}
secondary_signals = {
    _norm_col("ImpSaldoInsoluto"),
    _norm_col("FormaDePagoP"),
    _norm_col("Monto"),
}
return required_signals.issubset(norm_cols) and len(norm_cols & secondary_signals) >= 2
```

Requiere los 3 campos obligatorios + al menos 2 de los secundarios. Normaliza nombres con `_norm_col()` (elimina tildes, dobles espacios, convierte a lowercase).

### 7.3 Constantes de riesgo

**Fuente**: [`src/payments_analysis.py:42-44`](src/payments_analysis.py)

```python
MATERIAL_RISK_DAYS     = 180       # Facturas > 180 días sin cobrar = riesgo material
MATERIAL_RISK_MIN_MXN  = 1000.0   # Solo si saldo pendiente > $1,000 MXN
MATERIAL_RISK_MIN_PCT  = 3.0      # Solo si saldo pendiente > 3% del total de la factura
```

Una factura entra a "riesgo material" solo si cumple **las tres condiciones** simultáneamente ([`src/payments_analysis.py:757-760`](src/payments_analysis.py)).

### 7.4 Mapeo de aliases de columnas DR

**Fuente**: [`src/payments_analysis.py:12-40`](src/payments_analysis.py)

El dict `DR_COLUMN_ALIASES` mapea nombre interno → lista de aliases de columna. Ejemplo:
```python
"imp_pagado":  ["ImpPagado"],
"invoice_uuid": ["IdDocumento"],
"fecha_pago":  ["FechaPago", "Fecha Pago"],
"forma_pago_p": ["FormaDePagoP", "Forma de Pago P"],
```

### 7.5 Lógica PPD vs PUE

**Fuente**: [`src/payments_analysis.py:715-719`](src/payments_analysis.py)

```python
# PPD (Pago en Parcialidades Diferidas): el pago viene en complemento DR
# PUE (Pago en Una Exhibición): se considera cobrada el día de emisión
invoice_view["cobrado_aplicado_mxn"] = np.where(
    invoice_view["metodo_pago_norm"].eq("PUE"),
    invoice_view["total_factura_mxn"],       # PUE: asume cobrado el total
    invoice_view["cobrado_ppd_mxn"].fillna(0.0),  # PPD: suma de ImpPagado en DRs
)
```

- **PUE**: Se asume cobrado al 100% en la fecha de emisión, sin esperar complemento.
- **PPD**: Solo se considera cobrado lo que aparece en complementos de pago vinculados.

### 7.6 Fecha de corte de antigüedad (as_of_date)

**Función**: `_resolve_collection_as_of_date`
**Fuente**: [`src/payments_analysis.py:308-381`](src/payments_analysis.py)

Determina la "fecha de hoy" lógica para calcular antigüedad de facturas. **No usa `pd.Timestamp.today()`** directamente; en cambio:

1. Toma el mes máximo de facturas emitidas
2. Si ese mes final tiene muy pocas facturas (< 15% del baseline de 12 meses previos) y está separado ≥ 2 meses del cierre anterior → lo ignora como **outlier** y usa el mes anterior ([`src/payments_analysis.py:344-357`](src/payments_analysis.py))
3. Si la fecha máxima de facturas está más de 31 días en el futuro → también la descarta

Esto evita inflar la antigüedad de CxC cuando el Export incluye facturas de un mes incompleto.

### 7.7 Flujo de análisis de cobranza

**Función interna**: `_analyze_invoice_payment_perspective`
**Fuente**: [`src/payments_analysis.py:384-1254`](src/payments_analysis.py)

Pasos principales:
1. Limpia y normaliza el DataFrame de facturas (RFC, moneda, fecha)
2. Excluye facturas con fecha futura respecto a `today_norm`
3. Si hay DRs: filtra cancelados SAT, excluye pagos futuros, vincula DR → factura por `invoice_uuid = uuid`
4. Agrega por factura: `cobrado_ppd_mxn`, `saldo_ultimo_mxn`, `dias_ultimo_pago`
5. Calcula `saldo_pendiente_mxn`, `pct_cobrado`, flags de riesgo
6. Genera tablas: `invoice_collection_df`, `monthly_collections_df`, `counterparty_collection_df`, `payment_methods_df`, `warnings_df`

**Puntos de entrada públicos**:
- `analyze_emitidas_collections(df_invoices, df_dr_clean, ...)` → [`src/payments_analysis.py:1257-1274`](src/payments_analysis.py)
- `analyze_recibidas_payables(df_invoices, df_dr_clean, ...)` → [`src/payments_analysis.py:1277-1294`](src/payments_analysis.py)

Los labels (español) cambian según el tipo: "Cobrado" para Emitidas, "Pagado" para Recibidas. Controlado por `_analysis_labels()` ([`src/payments_analysis.py:271-305`](src/payments_analysis.py)).

---

## 8. Pipeline de Nómina

### 8.1 Selección inteligente de hoja — `src/payroll_intake.py`

**Función principal**: `select_best_payroll_sheet_from_workbook(...) → dict`
**Fuente**: [`src/payroll_intake.py:297-395`](src/payroll_intake.py)

El Excel de nómina puede tener múltiples hojas; esta función evalúa todas para elegir la mejor.

#### Columnas señal de nómina

```python
# src/payroll_intake.py:15-27
PAYROLL_STRONG_COLUMNS = [    # Peso 3 cada una
    "rfc_receptor",
    "fecha_final_pago",
    "total_percepciones",
    "total_deducciones",
]
PAYROLL_MEDIUM_COLUMNS = [    # Peso 1 cada una
    "uuid",
    "nombre_receptor",
    "tipo_nomina",
    "total",
]
```

Score de una hoja = `(3 × strong_hits) + medium_hits`. La hoja es elegible si `strong_hits >= 2` ó `strong_hits >= 1 AND medium_hits >= 2` ([`src/payroll_intake.py:239`](src/payroll_intake.py)).

#### Re-encabezado automático

**Función**: `reheader_payroll_if_needed(df) → (df, bool)`
**Fuente**: [`src/payroll_intake.py:159-187`](src/payroll_intake.py)

Si los encabezados reales no están en la fila 1 (caso común en exports SAT con metadatos arriba), busca entre las primeras 60 filas la que tenga más coincidencias con `{rfcreceptor, fechafinalpago, totalpercepciones, totaldeducciones, tiponomina, uuid}`. Si esa fila tiene score ≥ 2, la convierte en encabezado.

#### Criterio de selección final

**Fuente**: [`src/payroll_intake.py:82-93`](src/payroll_intake.py)

Prioridad de mayor a menor:
1. Hojas con RFC válidos (no genéricos como `XAXX010101000`)
2. Mayor cantidad de filas con RFC válido
3. Mayor ratio de RFC válidos
4. No confundida con facturas (`invoice_like_signal = False`)
5. Mayor score de columnas

### 8.2 Columnas del YAML de nómina

**Fuente**: [`column_mapping_payroll.yml`](column_mapping_payroll.yml)

Mapeo dedicado con aliases para:
- `rfc_receptor` (RFC del empleado)
- `nombre_receptor` (nombre del empleado)
- `fecha_final_pago` (fecha de pago del recibo)
- `total_percepciones` (ingresos brutos)
- `total_deducciones` (descuentos: ISR, IMSS)
- `total_otros_pagos` (subsidio al empleo, etc.)
- `tipo_nomina` (`"Ordinaria"`, `"Extraordinaria"`)
- `clave_tipo_deduccion`, `clave_tipo_percepcion` (claves SAT)

### 8.3 Normalización de empleados — `src/payroll_normalization.py`

**Fuente**: [`src/payroll_normalization.py:12-64`](src/payroll_normalization.py)

Sistema de reglas para ajustar salarios antes del análisis:

**Familias de reglas** ([`src/payroll_normalization.py:27`](src/payroll_normalization.py)):
- `"exclude_employee"`: Excluir completamente a un empleado del análisis (p.ej. socios dueños con sueldos distorsionados)
- `"scale_employee_pct"`: Escalar el sueldo de un empleado por un porcentaje (p.ej. ajustar a tiempo completo)

**Intenciones** ([`src/payroll_normalization.py:28-31`](src/payroll_normalization.py)):
- `"empleado_no_operativo"`: Socios, dueños, directivos no operativos
- `"situacion_extraordinaria"`: Bono de liquidación, pago de adeudo, evento único
- `"sueldo_ajustado"`: Sueldo que no refleja el mercado (muy alto o muy bajo)

Las reglas se aplican por periodo (`period_start_month` → `period_end_month`) y por `employee_key` (RFC del empleado).

---

## 9. Agregación Trimestral — `src/quarterly.py`

### 9.1 Diseño

**Fuente**: [`src/quarterly.py:1-14`](src/quarterly.py)

Restricción de diseño documentada: **nunca imputa meses faltantes**. Un trimestre es completo solo si los 3 meses calendario están presentes en el dataset.

### 9.2 Mapeo mes → trimestre

```python
# src/quarterly.py:22-28
_MONTH_TO_QUARTER: dict[int, int] = {
    1: 1, 2: 1, 3: 1,   # Q1: enero, febrero, marzo
    4: 2, 5: 2, 6: 2,   # Q2: abril, mayo, junio
    7: 3, 8: 3, 9: 3,   # Q3: julio, agosto, septiembre
    10: 4, 11: 4, 12: 4, # Q4: octubre, noviembre, diciembre
}
```

### 9.3 Función principal: `monthly_to_quarterly`

**Fuente**: [`src/quarterly.py:40-60`](src/quarterly.py)

Retorna DataFrame con columnas:
- `year`, `quarter_num`, `quarter_label` (ej. `"Q1 2024"`)
- `<value_col>` (suma del trimestre)
- `months_present` (cuántos meses hay con datos, 1-3)
- `is_complete` (True solo si `months_present == 3`)

---

## 10. Sistema de Warnings — `src/warning_health.py`

Centraliza los avisos no-bloqueantes del pipeline. Los warnings se acumulan en listas y se muestran al usuario en el dashboard.

Tipos de warning generados por módulo:

| Módulo | Fuente | Ejemplos |
|--------|--------|---------|
| `clean.py` | L.114-125, L.148, L.162-163, L.183 | "No existe columna 'moneda'. Se asume MXN.", "Se removieron N duplicados por UUID." |
| `mapping.py` | L.311-313, L.318-319 | "No existe 'tipo_comprobante'. Se asumirá positivo.", "No existe 'fecha_emision'. Se construirá desde año/mes." |
| `analysis.py` | L.113-118, L.133 | "No existe 'fecha_emision'. Usaremos columna X.", "No hay fecha ni anio/mes." |
| `payments_analysis.py` | L.1000-1044 | "Fecha de corte de antigüedad ajustada. Se ignoró mes final atípico." |

---

## 11. Cashflow — `src/cashflow.py`

Proyección de flujo de caja prospectivo a partir de:
- Supuestos de días de cobro (CxC) y días de pago (CxP)
- Patrones históricos de ingresos/egresos del dataset de facturas
- Obligaciones de nómina del dataset de nómina (si disponible)
- Estimación de obligaciones fiscales de `cashflow_tax.py`

Los supuestos son parametrizables por el usuario desde la UI. El resultado es un DataFrame mes-a-mes con:
- `ingresos_proyectados`, `egresos_proyectados`
- `nomina_mensual`, `impuestos_mensuales`
- `flujo_neto`, `saldo_acumulado`

---

## 12. Sistema de Exportación

### 12.1 Export de facturas: `src/export_pptx.py`

Genera un PowerPoint con diapositivas editables. Secciones:
- KPIs financieros (LTM, YoY)
- Top clientes/proveedores
- Gráfica mensual
- Tabla de retención

### 12.2 Export de cashflow: `src/export_cashflow_pptx.py`

PowerPoint dedicado para la proyección de flujo de caja.

### 12.3 Disponibilidad de exports: `src/export_availability.py`

Valida que los datos necesarios para cada export estén presentes en sesión antes de mostrar el botón de descarga al usuario.

---

## 13. UI — `ui/`

### 13.1 Design Tokens — `ui/tokens.py`

Define colores, tamaños y estilos como constantes centralizadas. Todos los componentes UI importan de aquí. Cambiar un token impacta toda la app.

### 13.2 Renderizadores de secciones — `ui/renderers.py`

Cada sección del dashboard tiene una función renderizadora nombrada `render_NRS{N}_{descripcion}()`. El prefijo `NRS` significa "Named Report Section".

Ejemplos:
- `render_NRS01_resumen()` — KPIs globales
- `render_NRS05_top_counterparties()` — Top clientes/proveedores
- `render_NRS07_recurrencia()` — Análisis de recurrencia
- `render_NRS09_cobranza()` — Análisis de cobranza/CxP

### 13.3 Chart Builders — `ui/chart_builders.py`

Funciones que retornan objetos `plotly.graph_objects.Figure` listos para renderizar. Separados de los renderers para facilitar testing unitario.

### 13.4 HTML Builders — `ui/html_builders.py`

Genera HTML para cards de KPI, scorecards, badges de estado, tablas con formato. Retornan strings HTML que Streamlit renderiza con `st.markdown(..., unsafe_allow_html=True)`.

---

## 14. Estado de Sesión — `src/session_state.py`

Gestiona el estado de la sesión Streamlit. Persiste entre re-renders:
- `df_raw`, `df_clean`, `df_std` (DataFrames de facturas)
- `df_payroll_clean`, `df_payroll_std` (DataFrames de nómina)
- `df_dr_clean` (complementos de pago)
- `normalization_rules` (reglas de normalización de empleados)
- `warnings` (lista de warnings del pipeline)
- `column_mapping_config` (config YAML cargado)

---

## 15. Resumen del Flujo Completo (Facturas)

```
1. Usuario carga Excel
        │
        ▼
2. ingest.py:read_excel_smart()
   → Elige hoja "XML" o primera
   → Lee todo como dtype=str
        │
        ▼
3. mapping.py:apply_column_mapping()
   → Lee column_mapping.yml
   → Renombra columnas a nombres internos
   → detect_dataset_type(): Emitidas / Recibidas / Mixto
   → Valida campos requeridos (tipo_comprobante nunca bloquea)
        │
        ▼
4. clean.py:clean_invoices()
   → RFC a mayúsculas
   → Montos a float (elimina $, ,)
   → tipo_cambio = 1.0 para MXN
   → total_mxn = total × tipo_cambio
   → _is_negative_comprobante() detecta EGRESO / NC / NOTA CRED
   → total_neto_mxn = (subtotal - descuento) × TC × signo
   → Normaliza nombres por RFC (más reciente)
   → Deduplica por UUID
        │
        ▼
5. analysis.py:build_standard_view()
   → Asigna contraparte (receptor si Emitidas, emisor si Recibidas)
   → Construye month/year/month_num
   → Retorna df_std con total_neto_mxn, contraparte_*
        │
        ▼
6. Análisis (en paralelo):
   ├── monthly_net() → ingresos por mes
   ├── yoy_growth_from_monthly() → crecimiento YoY
   ├── top_counterparties_table() → concentración de clientes
   ├── recurrence_months_table() → recurrencia
   ├── retention_summary() → retención/churn
   ├── ltm_summary() → LTM para M&A
   └── payments_analysis → CxC/CxP si hay complementos
        │
        ▼
7. ui/renderers.py → renderiza en Streamlit
8. export_pptx.py → genera PowerPoint si el usuario lo solicita
```

---

## 16. Conceptos de Dominio SAT/México

| Término | Significado |
|---------|------------|
| **CFDI** | Comprobante Fiscal Digital por Internet — la factura electrónica oficial de México |
| **RFC** | Registro Federal de Contribuyentes — identificador único de persona física/moral ante el SAT |
| **SAT** | Servicio de Administración Tributaria — autoridad fiscal mexicana |
| **UUID** | Folio Fiscal — identificador único e irrepetible de cada CFDI |
| **TipoDeComprobante** | Atributo del CFDI: I (Ingreso), E (Egreso), T (Traslado), P (Pago), N (Nómina) |
| **PPD** | Pago en Parcialidades o Diferido — el pago se registra con complemento posterior |
| **PUE** | Pago en Una Exhibición — se paga en el momento de la factura |
| **Complemento de Pago** | CFDI tipo P que vincula un pago a una factura PPD via `IdDocumento` |
| **ImpPagado** | Monto efectivamente pagado en el complemento de pago |
| **ImpSaldoInsoluto** | Saldo pendiente por pagar en el complemento de pago |
| **CxC** | Cuentas por Cobrar (Emitidas pendientes) |
| **CxP** | Cuentas por Pagar (Recibidas pendientes) |
| **IVA** | Impuesto al Valor Agregado — actualmente 16% general, 8% en zona fronteriza |
| **ISR** | Impuesto Sobre la Renta |
| **Nómina** | Recibos de salario (CFDI tipo N) — procesados en pipeline separado |
| **Percepciones** | Ingresos del empleado (sueldo, bonos, horas extra) |
| **Deducciones** | Descuentos al empleado (ISR retenido, cuotas IMSS) |
| **UMA** | Unidad de Medida y Actualización — índice para calcular límites de IMSS/INFONAVIT |
| **LTM** | Last Twelve Months — métrica M&A de los últimos 12 meses a fecha de corte |
| **FY** | Fiscal Year — año fiscal completo (generalmente enero-diciembre en México) |
| **Due Diligence** | Auditoría financiera previa a una transacción de M&A |
