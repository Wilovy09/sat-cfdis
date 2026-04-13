# Pulso SAT — Cómo funciona el proyecto

## Resumen ejecutivo

Pulso SAT es una herramienta interna que permite descargar facturas CFDI directamente del portal del SAT, sin intervención manual. El usuario proporciona sus credenciales (FIEL o CIEC), elige un período, y el sistema descarga automáticamente los XML o PDF de sus facturas.

---

## El problema que resuelve

El portal del SAT no tiene una API pública oficial. Para descargar facturas hay que entrar al sitio web, iniciar sesión, navegar menús y hacer clic en botones. Hacer eso para cientos o miles de facturas es inviable manualmente.

**Solución:** automatizar ese proceso mediante un scraper (programa que simula ser un navegador) y exponerlo como una API HTTP moderna.

---

## Arquitectura general

```
┌─────────────────────────────────────────────────────────────────┐
│                         NAVEGADOR / CLIENTE                     │
│              (Frontend HTML o consumidor de la API)             │
└───────────────────────────────┬─────────────────────────────────┘
                                │ HTTP (JSON / SSE)
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      API EN RUST (Actix-web)                    │
│  • Valida y enruta las peticiones                               │
│  • Convierte certificados FIEL de DER a PEM                     │
│  • Gestiona la sesión del captcha                               │
│  • Devuelve resultados al cliente (JSON, XML, PDF, ZIP)         │
└───────────────────────────────┬─────────────────────────────────┘
                                │ stdin / stdout (JSON)
                                │ (proceso hijo)
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   CLI EN PHP (cfdi-scraper)                     │
│  • Se conecta al portal del SAT usando Guzzle (HTTP)            │
│  • Autentica con FIEL o CIEC                                    │
│  • Navega el portal y extrae los metadatos de las facturas      │
│  • Descarga los archivos XML / PDF                              │
│  • Habla con Rust por stdin/stdout (JSON)                       │
└───────────────────────────────┬─────────────────────────────────┘
                                │ HTTPS
                                ▼
                    ┌───────────────────────┐
                    │    Portal del SAT     │
                    │   (cfdi.sat.gob.mx)   │
                    └───────────────────────┘
```

El sistema tiene tres capas:

| Capa | Tecnología | Rol |
|---|---|---|
| Cliente | HTML + JS (Tera) | Interfaz de usuario |
| API | Rust + Actix-web | Coordinador y servidor HTTP |
| Scraper | PHP + phpcfdi | Interacción con el portal del SAT |

---

## La capa PHP — el scraper

### ¿Por qué PHP?

El portal del SAT ya cuenta con una biblioteca open source de alta calidad en PHP: [`phpcfdi/cfdi-sat-scraper`](https://github.com/phpcfdi/cfdi-sat-scraper). Esta biblioteca lleva años siendo mantenida por la comunidad mexicana de desarrollo fiscal y cubre todos los casos de uso del SAT (FIEL, CIEC, captchas, reintentos, paginación, etc.).

Reescribirla en otro lenguaje hubiera tomado meses y producido algo menos confiable. **Reutilizamos lo que ya funciona.**

### ¿Qué hace el CLI?

El archivo `php-cli/bin/cfdi-scraper` es un programa de línea de comandos que:

1. **Lee una instrucción** de su entrada estándar (stdin) en formato JSON.
2. **Autentica al usuario** en el portal del SAT (con FIEL o CIEC).
3. **Ejecuta el comando** solicitado (`list`, `list-stream` o `download`).
4. **Devuelve los resultados** por su salida estándar (stdout), también en JSON.

```
Rust → [ JSON de instrucción ] → stdin del proceso PHP
Rust ← [ JSON de resultado   ] ← stdout del proceso PHP
```

### Comandos disponibles

| Comando | Descripción | Salida |
|---|---|---|
| `list` | Lista facturas por período o UUID | Un JSON con el arreglo completo al final |
| `list-stream` | Lista facturas día a día | Una línea JSON por factura, en tiempo real |
| `download` | Descarga XML o PDF de UUIDs dados | Archivos en disco + JSON con las rutas |

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

## La capa Rust — la API

### ¿Por qué Rust?

- **Rendimiento y concurrencia:** Rust con Actix-web maneja miles de conexiones simultáneas con muy poca memoria. Esto importa cuando múltiples usuarios hacen consultas largas al mismo tiempo.
- **Seguridad de memoria:** Rust garantiza en tiempo de compilación que no hay fugas de memoria ni condiciones de carrera. Las credenciales de los usuarios (que solo viven en memoria mientras dura la petición) nunca se filtran.
- **Despliegue simple:** el binario de Rust es un ejecutable único sin dependencias. En EC2 solo se necesita instalar PHP; Rust ya va empaquetado.

### Rutas de la API

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/health` | Verificación de estado del servidor |
| `POST` | `/api/v1/invoices/list` | Lista facturas (respuesta completa al terminar) |
| `POST` | `/api/v1/invoices/list/stream` | Lista facturas en tiempo real (SSE) |
| `POST` | `/api/v1/invoices/download` | Descarga XML o PDF (devuelve el archivo) |
| `POST` | `/api/v1/invoices/captcha/solve` | Entrega la respuesta del captcha al proceso PHP |

### ¿Cómo se comunica Rust con PHP?

Rust lanza PHP como un proceso hijo y se comunica con él mediante **tuberías (pipes)**:

```
Rust escribe en stdin  →  PHP lee de STDIN
PHP escribe en stdout  →  Rust lee de stdout
```

Para consultas normales (`list`, `download`), el flujo es simple:
1. Rust escribe el JSON de la instrucción en el stdin de PHP.
2. PHP procesa y escribe el resultado en stdout.
3. Rust lee el resultado y lo devuelve al cliente HTTP.

Para el streaming (`list-stream`), PHP escribe una línea JSON por factura según las va encontrando, y Rust las reenvía al navegador inmediatamente.

### Streaming en tiempo real (SSE)

En lugar de esperar a que PHP termine de consultar semanas o meses de facturas, usamos **Server-Sent Events (SSE)**. El flujo es:

```
PHP encuentra una factura → escribe JSON en stdout
Rust lee esa línea       → la envía como evento SSE al navegador
Navegador recibe evento  → agrega la factura a la tabla en tiempo real
```

El usuario ve cómo llegan las facturas una por una, sin esperar a que termine toda la consulta.

### El flujo del captcha (CIEC sin IA)

Este es el caso más complejo. Cuando el SAT presenta un captcha, PHP no puede resolverlo solo. La solución usa un canal de comunicación bidireccional:

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

La interfaz web está generada con **Tera**, un motor de plantillas de Rust similar a Jinja2. No hay framework de JavaScript; el frontend es HTML + CSS + JS vanilla.

Características:
- **Autenticación FIEL:** carga de archivos `.cer` y `.key` con drag & drop.
- **Autenticación CIEC:** formulario de RFC + contraseña.
- **Streaming en vivo:** tabla que se llena en tiempo real con animaciones.
- **Búsqueda y filtrado:** en los resultados sin recargar la página.
- **Descarga individual o masiva:** XML o PDF, con agrupación en ZIP automática si son varios archivos.
- **Modal de captcha:** aparece automáticamente cuando el SAT lo exige.

---

## Decisiones de arquitectura clave

### "¿Por qué no todo en un solo lenguaje?"

La alternativa hubiera sido reescribir el scraper del SAT en Rust desde cero. Esto implicaría:
- Meses de trabajo para replicar lo que `phpcfdi` ya hace.
- Mantener actualizado el código cada vez que el SAT cambia su portal.
- Asumir el riesgo de errores en casos borde (reintentos, paginación, estados de comprobante, etc.).

Con la arquitectura actual, si el SAT cambia algo, la comunidad de `phpcfdi` lo actualiza y nosotros solo actualizamos la dependencia de PHP.

### "¿Por qué las credenciales van en el cuerpo de cada petición?"

Las credenciales **nunca se almacenan** en el servidor. Llegan con la petición, se usan para autenticar con el SAT y se descartan cuando la petición termina. Esto elimina el riesgo de una filtración de base de datos porque sencillamente no hay base de datos de credenciales.

### "¿Por qué archivos temporales para FIEL?"

PHP necesita los archivos PEM como rutas en disco (así lo requiere la biblioteca `phpcfdi/credentials`). Rust crea un directorio temporal al inicio de cada petición y lo elimina automáticamente al terminar, gracias al tipo `TempDir` que en Rust garantiza limpieza aunque haya errores.

### "¿Por qué SSE y no WebSockets?"

SSE es unidireccional (servidor → cliente), más simple que WebSockets y suficiente para este caso. Los navegadores lo soportan nativamente, no se necesita ninguna biblioteca cliente, y funciona bien detrás de proxies y balanceadores de carga. Para el captcha, la comunicación del cliente al servidor usa una petición HTTP normal separada, lo cual es más robusto.

---

## Resumen del flujo completo (CIEC con captcha)

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
