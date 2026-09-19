# Pulso — Cómo funciona el proyecto

## Resumen ejecutivo

Pulso es una plataforma de analítica financiera para PyMEs mexicanas construida sobre CFDI. El punto de entrada es un scraper que descarga facturas directamente del portal del SAT sin intervención manual — el usuario da sus credenciales (FIEL o CIEC), y el sistema descarga, normaliza y guarda sus CFDI de forma continua. Pero descargar la factura es solo el primer paso: la mayoría del valor está en lo que viene después — cruzar esos datos contra reglas de negocio y convertirlos en reportes de ingresos, egresos, cartera, nómina y hallazgos que antes se armaban a mano en Excel.

---

## El problema que resuelve

El portal del SAT no tiene una API pública oficial. Para descargar facturas hay que entrar al sitio web, iniciar sesión, navegar menús y hacer clic en botones. Hacer eso para cientos o miles de facturas —y repetirlo todos los días, para decenas de empresas— es inviable manualmente. Y una vez descargadas, un CFDI crudo no es un reporte: hay que timbrar por devengo, no por emisión; excluir traslados entre RFC del mismo dueño; separar nómina ordinaria de extraordinaria; saber qué contraparte detrás de un RFC genérico es cuál.

**Solución:** automatizar la descarga con un scraper (programa que simula ser un navegador), guardar todo en un modelo de datos normalizado, y exponer los reportes ya calculados como una API HTTP moderna que un dashboard consume.

---

## Arquitectura general

```
┌───────────────────────────────────────────────────────────────────┐
│                    FRONTEND — Vue 3 + TypeScript                  │
│              (repo separado: pulso-adquiere, Vite + Pinia)        │
└───────────────────────────────┬─────────────────────────────────┘
                                │ HTTP (JSON, SSE)
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│                    API EN RUST (Actix-web + sqlx)                 │
│  • Autenticación (JWT / Google OAuth), multi-tenencia por RFC     │
│  • Rutas de analítica (ingresos, egresos, nómina, cartera, …)     │
│  • Caché de respuestas invalidado por evento, no por TTL          │
│  • Workers en segundo plano: sync, ETL, gap-detection, limpieza   │
└──────────────┬───────────────────────────────┬────────────────────┘
               │                               │ stdin/stdout (JSON)
               ▼                               │ (proceso hijo)
    ┌─────────────────────┐                    ▼
    │  POSTGRES (RDS)     │        ┌───────────────────────────┐
    │  pulso.cfdis, users,│        │   CLI EN PHP (cfdi-scraper) │
    │  nomina, reglas,    │        │  Login FIEL/CIEC, navega   │
    │  caché de analítica │        │  el portal, extrae CFDI    │
    └─────────────────────┘        └──────────────┬──────────────┘
                                                   │ HTTPS
                                                   ▼
                                       ┌───────────────────────┐
                                       │    Portal del SAT     │
                                       │   (cfdi.sat.gob.mx)   │
                                       └───────────────────────┘
```

| Capa | Tecnología | Rol |
|---|---|---|
| Cliente | Vue 3 + TypeScript (repo `pulso-adquiere`) | Dashboard: Ingresos, Egresos, Nómina, Cartera, Normalización |
| API | Rust + Actix-web + sqlx | Coordinador, autenticación, analítica, caché |
| Base de datos | PostgreSQL (RDS) | Única fuente de verdad — CFDI normalizados, reglas, caché |
| Scraper | PHP + phpcfdi | Interacción con el portal del SAT |

No hay frontend servido por este repo — Pulso dejó de usar plantillas Tera; el cliente es una SPA separada que solo habla HTTP con esta API.

---

## La base de datos

Pulso guarda todo en PostgreSQL vía `sqlx`, con **78 migraciones** registradas (`sqlx::migrate!`, aplicadas automáticamente al arrancar — nunca a mano contra la base compartida). Las tablas principales, todas bajo el esquema `pulso`:

| Tabla / vista | Para qué |
|---|---|
| `users` | Una fila por RFC, dueño global (nunca dos usuarios activos con el mismo RFC) |
| `rfc_shares` | Acceso de solo lectura a un RFC ajeno, revocable |
| `sync_jobs` / `job_invoices` | Cola de descargas y su resultado crudo (metadata sin normalizar) |
| `cfdis` + hijas (`cfdi_taxes`, `cfdi_concepts`, `cfdi_payments`, `cfdi_relacionados`) | Facturas normalizadas |
| `cfdi_nomina` + hijas (`_percepciones`, `_deducciones`, `_otros_pagos`) | Complemento de nómina, desglosado |
| `normalization_rules` / `payroll_normalization_rules` | Reglas de agrupación/exclusión por contraparte o por empleado |
| `rfc_data_version` / `endpoint_response_cache` | Caché de respuestas de analítica (ver más abajo) |
| `cfdis_ajustado` (vista) | `cfdis` con el factor de escala y exclusiones ya aplicados |
| `nomina_normalizada` (vista) | Nómina con exclusiones, factor de escala y devengo ya resueltos |

---

## Multi-tenencia: un RFC, un dueño

Un RFC solo puede tener **un dueño activo** en toda la plataforma (índice único global sobre `pulso.users.rfc`) — es el dueño quien guarda la credencial cifrada y quien dispara las sincronizaciones. Cualquier otro usuario que necesite ver ese RFC recibe un acceso de **solo lectura** vía `pulso.rfc_shares` (revocable, sin duplicar la credencial ni la cola de sync). Esto evita que dos personas terminen sincronizando el mismo RFC por separado, con dos historiales que no cuadran entre sí.

---

## Los workers en segundo plano

Al arrancar, la API separa un pool de conexiones a Postgres para peticiones de usuario y otro más chico para trabajo en segundo plano (para que una sincronización larga no le quite conexiones al dashboard), y lanza siete workers:

| Worker | Cada cuánto | Qué hace |
|---|---|---|
| `resume_worker` | 30s | Reanuda jobs `queued`/`paused_limit`, corre el scraper PHP |
| `etl_worker` | 30s | Convierte resultados crudos de un job en filas normalizadas de `cfdis` |
| `daily_sync_worker` | 1h | Encola la sincronización diaria automática de cada RFC registrado |
| `recheck_cancelled` | periódico | Revalida `estado_sat` contra el SAT (cancelaciones tardías o revertidas) |
| `gap_detector` | periódico | Encuentra días del calendario que faltan en `cfdis` y los reencola |
| `xml_redownload` | cada 6h | Re-descarga el XML real de CFDI marcados sin XML disponible |
| `response_cache::cleanup_worker` | cada 6h | Barre filas de caché de analítica que ya nadie puede servir |

---

## La capa PHP — el scraper

### ¿Por qué PHP?

El portal del SAT ya cuenta con una biblioteca open source de alta calidad en PHP: [`phpcfdi/cfdi-sat-scraper`](https://github.com/phpcfdi/cfdi-sat-scraper). Esta biblioteca lleva años siendo mantenida por la comunidad mexicana de desarrollo fiscal y cubre todos los casos de uso del SAT (FIEL, CIEC, captchas, reintentos, paginación, etc.).

Reescribirla en otro lenguaje hubiera tomado meses y producido algo menos confiable. **Reutilizamos lo que ya funciona.**

### ¿Qué hace el CLI, y quién lo llama hoy?

El archivo `php-cli/bin/cfdi-scraper` es un programa de línea de comandos que lee una instrucción JSON por stdin, autentica al usuario en el portal del SAT (FIEL o CIEC), ejecuta el comando solicitado (`list`, `list-stream`, `list-count` o `download`), y devuelve el resultado por stdout, también en JSON.

```
Rust → [ JSON de instrucción ] → stdin del proceso PHP
Rust ← [ JSON de resultado   ] ← stdout del proceso PHP
```

La mayoría de las descargas hoy **no** las dispara una petición HTTP en vivo: las dispara `resume_worker` sobre jobs que ya están en la cola (`pulso.sync_jobs`), en segundo plano. La ruta con streaming en vivo (`/api/v1/invoices/list/stream` + captcha) sigue existiendo para consultas manuales puntuales — ver la sección de SSE más abajo.

### Autenticación FIEL

La FIEL usa un certificado (`.cer`) y una llave privada (`.key`) en formato DER. El portal del SAT espera los archivos en formato PEM. La conversión la hace Rust antes de llamar a PHP, usando el comando `openssl` del sistema, y le pasa las rutas de los archivos PEM temporales al CLI.

### Autenticación CIEC

La CIEC usa RFC + contraseña, pero el SAT exige resolver un captcha antes de dar acceso. El CLI maneja dos estrategias:

| Estrategia | Cuándo se usa | Descripción |
|---|---|---|
| **BoxFactura AI** | Si `BOXFACTURA_CONFIG_PATH` está configurado | Modelo local de IA (ONNX) que resuelve el captcha automáticamente, sin intervención humana |
| **Captcha manual** | Si no hay IA configurada | El CLI envía la imagen del captcha a Rust, Rust la reenvía al navegador, el usuario la resuelve y la respuesta viaja de regreso al CLI |

### SSL y el SAT

El SAT usa configuraciones TLS obsoletas (clave Diffie-Hellman de 1024 bits). Las versiones modernas de OpenSSL rechazan esto por defecto. Para solucionarlo, el CLI configura Guzzle con:

```php
CURLOPT_SSL_CIPHER_LIST => 'DEFAULT@SECLEVEL=1'
```

Esto baja el nivel de seguridad mínimo de OpenSSL solo para las conexiones al SAT, sin afectar nada más.

---

## La capa de analítica

Cada módulo bajo `src/services/analytics/` calcula un reporte, y casi siempre tiene una ruta gemela bajo `/api/v1/analytics/{rfc}/...`:

| Módulo | Qué calcula |
|---|---|
| `summary` | Total facturado, conteo de facturas — y helpers compartidos por casi todo lo demás |
| `cashflow` | Facturado vs. cobrado en el tiempo, posición de caja acumulada, PUE/PPD |
| `concepts` | Top productos/servicios por clave SAT y descripción |
| `counterparties` | Concentración de contrapartes, top-10 %, rollup por contraparte |
| `data_quality` | CFDI sin XML real, meses del rango sincronizado sin ningún registro |
| `fiscal` | IVA/ISR/IEPS, tasa efectiva, desglose por moneda |
| `geography` | Desglose por código postal / estado de expedición |
| `hallazgos` / `hallazgos_egresos` | Hallazgos con nombre propio (H1–H9) sobre ingresos y egresos |
| `normalization` | CRUD de reglas de agrupación/exclusión, de contraparte y de nómina |
| `payments` | Cobranza y cuentas por pagar: antigüedad, exposición |
| `payroll` | Nómina por empleado, plantilla actual, altas y bajas, rotación |
| `period_comparison` / `quarterly` | Comparativos año contra año, por mes y por trimestre fiscal |
| `recurrence` / `retention` | Recurrencia de contrapartes, retención/pérdida año contra año |
| `xml_breakdown` / `xml_count` | Estado del XML (disponible, pendiente, no disponible) |

### Rutas principales

| Grupo | Ejemplos |
|---|---|
| Auth | `/api/v1/auth/register`, `/login`, `/google` |
| Usuarios | `/api/v1/users/rfcs`, `/rfcs/{rfc}/shares`, `/rfcs/{rfc}/fiel` |
| Facturas (manual) | `/api/v1/invoices/list/stream`, `/captcha/solve`, `/download` |
| Cola | `/api/v1/queue`, `/queue/{id}` |
| Analítica | `/api/v1/analytics/{rfc}/summary`, `/payroll`, `/cashflow`, `/hallazgos`, … |
| Admin | `/api/v1/admin/reprocess`, `/admin/rfcs`, `/admin/users` |

---

## El caché de respuestas

Las consultas de analítica pueden tardar segundos sobre el RFC con más historial, y se piden una y otra vez sin que los datos cambien entre una petición y otra. `pulso.endpoint_response_cache` guarda la respuesta ya calculada por `(rfc, endpoint, params)`, invalidada **por evento, no por tiempo**: cada vez que algo mueve las cifras de un RFC (un job encuentra facturas nuevas, el ETL enriquece una, se crea o edita una regla de normalización) se avanza un contador (`pulso.rfc_data_version`) y la siguiente lectura recalcula sola. Es puramente aditivo — un miss o un error de escritura del caché nunca convierte en error la petición, solo la vuelve más lenta — y un despliegue invalida todo el caché sin ningún paso extra, porque toda fila escrita por un proceso anterior deja de contar como vigente.

---

## La capa Rust — la API

### ¿Por qué Rust?

- **Rendimiento y concurrencia:** Actix-web maneja miles de conexiones simultáneas con muy poca memoria. Esto importa cuando varias empresas hacen consultas de analítica pesadas al mismo tiempo.
- **Seguridad de memoria:** Rust garantiza en tiempo de compilación que no hay fugas de memoria ni condiciones de carrera. Las credenciales de los usuarios viven cifradas en Postgres, nunca en texto plano en memoria más tiempo del necesario.
- **Despliegue simple:** el binario de Rust es un ejecutable único. En EC2 solo se necesita instalar PHP y tener acceso a Postgres; Rust ya va empaquetado.

### Streaming en tiempo real (SSE) para consultas manuales

Para una consulta puntual (no la sincronización automática de fondo), en lugar de esperar a que PHP termine de revisar semanas o meses de facturas, se usa **Server-Sent Events**: PHP escribe una línea JSON por factura conforme la va encontrando, y Rust la reenvía de inmediato al navegador.

### El flujo del captcha (CIEC sin IA)

Cuando el SAT presenta un captcha, PHP no puede resolverlo solo:

```
PHP       →  imagen del captcha (stdout)  →  Rust
Rust      →  evento SSE "__captcha__"     →  Navegador
Navegador →  muestra modal con imagen
Usuario   →  escribe la respuesta
Navegador →  POST /captcha/solve          →  Rust
Rust      →  respuesta (stdin)            →  PHP
PHP       →  continúa el login            →  SAT
```

En Rust, la coordinación entre el endpoint `/captcha/solve` (que recibe la respuesta del navegador) y el stream SSE (que necesita escribirla en stdin de PHP) se hace con un **canal oneshot de Tokio**: un mensaje de ida única que desbloquea el stream cuando llega la respuesta.

---

## El frontend

La interfaz web es una **SPA en Vue 3 + TypeScript** (Vite, Pinia, Tailwind, Chart.js), en el repo separado `pulso-adquiere`. Este backend no sirve HTML ni archivos estáticos del cliente — solo expone la API que la SPA consume. El dashboard cubre Ingresos, Egresos, Nómina, Cartera, Comparativos, Hallazgos y Normalización, además del flujo de sincronización (FIEL/CIEC, captcha manual, progreso en vivo).

---

## Decisiones de arquitectura clave

### "¿Por qué no todo en un solo lenguaje?"

La alternativa hubiera sido reescribir el scraper del SAT en Rust desde cero. Esto implicaría meses de trabajo para replicar lo que `phpcfdi` ya hace, mantenerlo actualizado cada vez que el SAT cambia su portal, y asumir el riesgo de errores en casos borde (reintentos, paginación, estados de comprobante, etc.). Con la arquitectura actual, si el SAT cambia algo, la comunidad de `phpcfdi` lo actualiza y nosotros solo actualizamos la dependencia de PHP.

### "¿Por qué la sincronización es una cola en la base de datos, y no una petición HTTP que espera?"

Sincronizar meses de historial de un RFC puede tardar horas y el SAT limita cuántas descargas se pueden hacer por día. Una petición HTTP que espera eso se cae por timeout, y no sobrevive un reinicio del servidor. Un job en `pulso.sync_jobs` sí: se retoma solo, respeta el límite diario del SAT (`paused_limit` + `resume_at`), y un usuario puede cerrar el navegador sin perder el progreso.

### "¿Por qué el caché se invalida por evento y no por TTL?"

La ingesta es dirigida por eventos (un job encuentra facturas, el ETL las enriquece, se sincroniza o edita una regla), no por un lote fijo cada N minutos. Un TTL fijo o sirve datos viejos entre refrescos arbitrarios, o descarta entradas de caché perfectamente válidas sin ninguna razón. Contar versiones es barato (un upsert) y exacto.

### "¿Por qué un RFC solo puede tener un dueño?"

Si dos usuarios sincronizaran el mismo RFC por separado, cada uno tendría su propia cola de jobs, su propio historial de ETL, y las cifras de analítica podrían no cuadrar entre las dos vistas del mismo negocio. Un solo dueño por RFC, con acceso de lectura compartido hacia los demás, garantiza que solo existe una versión de la verdad para cada RFC.

### "¿Por qué archivos temporales para FIEL?"

PHP necesita los archivos PEM como rutas en disco (así lo requiere la biblioteca `phpcfdi/credentials`). Rust crea un directorio temporal al inicio de cada petición y lo elimina automáticamente al terminar, gracias al tipo `TempDir` que en Rust garantiza limpieza aunque haya errores.

### "¿Por qué SSE y no WebSockets, para las consultas manuales?"

SSE es unidireccional (servidor → cliente), más simple que WebSockets y suficiente para este caso. Los navegadores lo soportan nativamente, no se necesita ninguna biblioteca cliente, y funciona bien detrás de proxies y balanceadores de carga. Para el captcha, la comunicación del cliente al servidor usa una petición HTTP normal separada, lo cual es más robusto.

---

## Despliegue en EC2 (Ubuntu 24.04)

### Requisitos

| Componente | Versión mínima |
|---|---|
| PHP CLI | 8.3 |
| Extensiones PHP | `curl`, `gd`, `mbstring`, `zip`, `xml`, `ffi` |
| openssl CLI | cualquiera |
| Rust | 1.75+ |
| PostgreSQL | accesible por red (RDS u otro), con el certificado CA si se exige SSL `verify-full` |

### Script de preparación

El repositorio incluye `prepare_ec2.sh` que instala todo lo necesario de una vez:

```bash
bash prepare_ec2.sh                        # usa /arena/sat-cfdis por defecto
bash prepare_ec2.sh /ruta/al/proyecto      # o pasa tu propia ruta
```

El script:
1. Instala PHP 8.3 con todas las extensiones necesarias y openssl
2. Habilita FFI en php.ini (requerido por el modelo ONNX de captchas)
3. Descarga la librería nativa de ONNX Runtime para Linux
4. Instala Rust si no está instalado
5. Compila el binario de Rust en modo release

### Variables de entorno (`.env`)

```env
HOST=0.0.0.0
PORT=8080

# Base de datos
POSTGRES_HOST=...
POSTGRES_PORT=5432
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_DATABASE=...
POSTGRES_CERT_PATH=/ruta/al/rds-ca-bundle.pem   # requerido en release (SSL verify-full)
POSTGRES_POOL_SIZE=15            # pool de peticiones de usuario
POSTGRES_WORKER_POOL_SIZE=5      # pool separado para los workers en segundo plano
POSTGRES_ACQUIRE_TIMEOUT_SECS=10

# Scraper PHP
PHP_BIN=php
PHP_CLI_PATH=/arena/sat-cfdis/php-cli/bin/cfdi-scraper
BOXFACTURA_CONFIG_PATH=/arena/sat-cfdis/libs/sat-captcha-ai-model/model/configs.yaml
HTTPS_PROXY=...                  # proxy residencial para las llamadas al SAT

# Almacenamiento de XML
S3_BUCKET=...

# Auth
JWT_SECRET=...
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_REDIRECT_URI=...
APP_BASE_URL=...

# Correo transaccional (fin de sync, fin de mes, sync fallido)
SENDGRID_API_KEY=...
SENDGRID_FROM=...

# CORS
ALLOWED_ORIGINS=...
ALLOWED_METHODS=...
```

> `BOXFACTURA_CONFIG_PATH` activa la resolución automática de captchas con el modelo ONNX local. Si no se configura, el captcha se mostrará al usuario para resolverlo manualmente.

### Arrancar el servidor

```bash
./target/release/pulso-backend
```

O con nohup para que sobreviva al cierre de sesión:

```bash
nohup ./target/release/pulso-backend &
```

Al arrancar, el binario corre las migraciones pendientes contra Postgres automáticamente (`sqlx::migrate!`) antes de aceptar tráfico.

---

## Resumen del flujo completo (consulta manual, CIEC con captcha)

Este es el flujo de una consulta puntual en vivo — la sincronización automática de fondo (la ruta más común día a día) no tiene captcha ni SSE: simplemente corre como job en `pulso.sync_jobs`, retomado por `resume_worker`.

```
1. Usuario llena el formulario y hace clic en "Consultar"
2. Navegador hace POST /api/v1/invoices/list/stream con RFC, contraseña y período
3. Rust valida la petición y lanza el proceso PHP con el comando "list-stream"
4. PHP intenta hacer login en el SAT con RFC + contraseña
5. El SAT responde con una imagen captcha
6. PHP la codifica en base64 y la escribe en stdout como JSON
7. Rust lee esa línea, genera un ID de sesión único y lo registra
8. Rust envía un evento SSE al navegador con la imagen y el ID
9. El navegador muestra un modal con la imagen del captcha
10. El usuario escribe la respuesta y hace clic en "Enviar"
11. El navegador hace POST /api/v1/invoices/captcha/solve con la respuesta e ID
12. Rust envía la respuesta al stream SSE mediante un canal interno
13. El stream escribe la respuesta en el stdin de PHP
14. PHP recibe la respuesta, completa el login y empieza a buscar facturas
15. Por cada factura encontrada, PHP escribe una línea en stdout
16. Rust la lee y la envía como evento SSE al navegador
17. El navegador agrega la fila a la tabla en tiempo real
18. Al terminar, PHP escribe {"__done__": true, "total": N}
19. El navegador marca la consulta como completada
```
