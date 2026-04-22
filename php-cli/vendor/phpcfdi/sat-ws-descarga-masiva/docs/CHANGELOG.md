# CHANGELOG

## Acerca de los números de versiones

Respetamos el estándar [Versionado Semántico 2.0.0](https://semver.org/lang/es/).

En resumen, [SemVer](https://semver.org/) es un sistema de versiones de tres componentes `X.Y.Z`
que nombraremos así: ` Breaking . Feature . Fix `, donde:

- `Breaking`: Rompe la compatibilidad de código con versiones anteriores.
- `Feature`: Agrega una nueva característica que es compatible con lo anterior.
- `Fix`: Incluye algún cambio (generalmente correcciones) que no agregan nueva funcionalidad.

**Importante:** Las reglas de SEMVER no aplican si estás usando una rama (por ejemplo `main-dev`)
o estás usando una versión cero (por ejemplo `0.18.4`).

## Versión 1.1.3 2026-04-07

Esta es una actualización de mantenimiento que tiene cambios mínimos en el código y compatibilidad con PHP 8.5.
Esta actualización corrige el *build* en GitHub.

- Se actualiza el año de licencia.
- Se modifican los tipos de `Request#jsonSerialize` y `Response#jsonSerialize`.
- Se modifica `InteractsXmlTrait#findAttributes` para hacerla más entendible.

Adicionalmente, se incluyen estos cambios hechos por mantenimiento:

- Se actualiza la configuración de PHPUnit para que falle y muestre todos los detalles en cualquier incidencia.
- En los flujos de trabajo de GitHub:
  - Se actualiza `sonarqube-scan-action` a la versión 7.
  - Se actualizan las acciones de GitHub a sus últimas versiones.
  - Se incluye PHP 8.5 a la matriz de pruebas.
  - Se actualizan los trabajos para ejecutarse en PHP 8.5.
  - Se renombra el trabajo `tests` a `phpunit`.
- Se actualizan herramientas de desarrollo.

## Versión 1.1.2 2025-09-25

- Se detecta que en PHP 8.4, para la llamada al método `SplFileObject::setCsvControl()` se debe establecer el parámetro
  `$escape` dado que cambiará en futuras versiones.
- Se hacen ajustes al entorno de desarrollo para sustituir las anotaciones por atributos.
- Se actualizan los grupos de reglas de `php-cs-fixer` pues cambiaron de nombre.
- Se actualizan las herramientas de desarrollo.

Adicionalmente, se incluyen estos cambios hechos por mantenimiento:

- Se actualizan las herramientas `phpcs` y `phpcbf` a la versión 4.0.0.
- Se moderniza la integración con SonarQube-Cloud.
- Se actualiza `sonarqube-scan-action` a la versión 6.
- Las pruebas se ejecutan configurando la zona de tiempo a `America/Mexico_City`.

## Versión 1.1.1 2025-06-02

Se mejora la documentación y las validaciones previas a enviar una consulta al SAT.

- Se revisa que no se exceda el límite inferior de la consulta.
- Se revisa que al solicitar XML de tipo recibidos entonces se soliciten solo los vigentes. 

## Versión 1.1.0 2025-05-30

El SAT liberó el 2025-05-30 una nueva versión del *Servicio Web de Descarga Masiva de CFDI y Retenciones*.
Esta versión compatibiliza la versión actual con el nuevo servicio.

Adicionalmente, se agrega un método de prevalidación para una consulta a través del método `QueryParameters::validate()`
que devuelve un arreglo con una lista de mensajes con los errores encontrados.

Se regresa la capacidad de cambiar el *token* en el *servicio.
Es importante para poder restablecer el *token* desde una capa de persistencia.

Gracias a todos los miembros de PhpCfdi que han colaborado con la elaboración de esta versión.
En especial a `@blacktrue` y `@TheSpectroMx` por su atención y trabajo.

## Versión 1.0.0 2025-04-12

La versión `1.0.0` es una actualización al código para poderlo mantener más fácilmente.
Se usa la numeración de versión mayor 1.0.
Ahora la versión mínima de PHP es 8.1.
Lea las notas para [Actualizar de `0.5.x` a `1.0.x`](UPGRADE_0.5_1.0.md).

## Versión 0.5.4 2024-04-17

- Se corrige un bug donde en algunas ocasiones falla al procesar la respuesta de una descarga de paquetes.

El método `DOMDocument::loadXML()` con `LibXML >= 1.11.0` trunca a 10,000,000 bytes el contenido de un
nodo de tipo texto. Esto lleva a que el contenido de un paquete se trunque y el archivo ZIP descargado
se encuentre truncado y, por lo tanto, corrupto. Se ha corregido usando la opción `LIBXML_PARSEHUGE`.
Al usar `LIBXML_PARSEHUGE` se está quitando una protección natural que impide una denegación de servicio.
Sin embargo, podemos considerar segura esta acción dado que solo ocurre en el contexto de respuestas
recibidas del servicio de descarga masiva del SAT.

- Se actualiza el archivo de licencia a 2024.

## Mantenimiento 2024-04-17

- Se mejoran las pruebas del rasgo `ComplementoTrait`.
- Se actualizaron los flujos de trabajo:
  - Se agregó PHP 8.3 a la matriz de pruebas.
  - Los trabajos ahora corren en PHP 8.3.
  - Se actualizan las acciones de GitHub a la versión 4.
  - Se cambia la variable `build/tests/matrix/php-versions` a singular.
- Se actualizaron las herramientas de desarrollo.

## Mantenimiento 2023-10-30

- El proceso de integración continua falló al momento de verificar el estilo de código con `php-cs-fixer:3.37.1`.
  Se actualiza el código con los ajustes necesarios.
- Se actualizaron las herramientas de desarrollo.

## Mantenimiento 2023-07-10

- El proceso de integración continua falló en PHP 8.1 y PHP 8.2 al momento de verificar la firma de un mensaje SOAP firmado.
  En algunas ocasiones se consulta el espacio de nombres, pero no se encuentra el prefijo.
  Esto se ha solucionado consultando el mensaje original `Envelope`, en lugar del mensaje sin envoltura SOAP.
- Se actualizaron las herramientas de desarrollo.
- En el archivo de configuración de `php-cs-fixer`, se cambió la regla obsoleta `function_typehint_space` por `type_declaration_spaces`.

## Mantenimiento 2023-06-19

- Se actualiza la FIEL del RFC `EKU9003173C9` que estaba vencida.
  Este cambio requiere varias modificaciones en las pruebas.
- Se actualiza el CSD del RFC `EKU9003173C9` que estaba vencido.
  Este cambio provocó cambiar la prueba `FielTest::testIsNotValidUsingCsd`.
- Se actualizan las herramientas de desarrollo.
- Los flujos de trabajo se pueden ejecutar a voluntad.
- Se corrigen el problema encontrado por SonarCloud donde se usaba paréntesis en el retorno de una función:
  `return ($a === $b);`.
- Se remueven las rutas fijas de las herramientas `phpcs` y `phpcbf`.

## Versión 0.5.3 2023-06-07

- Se verifica la compatibilidad con PHP 8.2.
- Se eliminan las llaves en la interpolación de variables (deprecado en PHP 8.2).
- Se actualiza el año de la licencia.
- Se modifican las pruebas para futura compatibilidad con PHPUnit 10.
- Se corrige la exclusión de archivos en `sonar-project.properties`.
- Se corrige el archivo `CONTRIBUTING.md` usando el ancla correcta del proyecto.
- Se corrige la insignia de construcción `badge-build`.
- En los flujos de trabajo
  - Se agrega PHP 8.2 a la matriz de pruebas.
  - Se ejecutan los trabajos en PHP 8.2.
  - Se sustituye la directiva de GitHub `::set-output` por `$GITHUB_OUTPUT`.
- Se actualizan las herramientas de desarrollo.

## Versión 0.5.2 2022-09-30

#### Consulta por UUID

Gracias a la solicitud de cambios en [`luisiturrios1/python-cfdiclient#42`](https://github.com/luisiturrios1/python-cfdiclient/pull/42)
por `@alan196`, hemos podido verificar que la documentación del servicio con respecto a la consulta por UUID está incorrecta.

- El campo no se llama `UUID`, se llama `Folio`.
- El campo `RfcSolicitante` no se debe omitir.
- El campo `TipoSolicitud` no se debe omitir.
- Los demás campos no deben existir.

Por lo tanto, se han hecho las correcciones necesarias para hacer la consulta por `UUID`.

## Versión 0.5.1 2022-09-28

### Se corrigen XML mal formados

Se hace una revisión del constructor de mensages firmados XML (`FielRequestBuilder`) y se corrigen
aquellos lugares donde es posible inyectar caracteres no escapados XML.

El caso más normal es cuando el RFC solicitante contiene un ámpersand `&`, sin embargo se corrigió:

- Verificación: `@IdSolicitud` y `@RfcSolicitante`.
- Descarga: `@IdPaquete` y `@RfcSolicitante`.
- General: `X509IssuerName`.

Gracias comunidad por reportar el problema en el [canal de discord](https://discord.gg/aFGYXvX).

## Versión 0.5.0 2022-09-24

Esta actualización contiene cambios significativos en la forma de usar la librería,
específicamente en la creación de objetos de consulta `QueryParameters`.
Lea las notas para [Actualizar de `0.4.x` a `0.5.x`](UPGRADE_0.4_0.5.md),
y la documentación principal en el [README.md](../README.md).

A partir de septiembre 2022 el SAT incluye un archivo de nombre `<UUID>_tercero.txt`.
Este archivo contiene la información de terceros y se vincula con la información de `Metadata` por el UUID.

### Cambios incompatibles

```text
$ roave-backward-compatibility-check --from v0.4.7
[BC] REMOVED: Method PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters#__construct() was removed
[BC] CHANGED: Method __construct() of class PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters visibility reduced from public to private
[BC] CHANGED: The number of required arguments for PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters#__construct() increased from 4 to 9
[BC] CHANGED: The parameter $rfcMatch of PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters#__construct() changed from string to a non-contravariant PhpCfdi\SatWsDescargaMasiva\Shared\DocumentType
[BC] CHANGED: The return type of PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters#getRfcMatch() changed from string to the non-covariant PhpCfdi\SatWsDescargaMasiva\Shared\RfcMatch
[BC] CHANGED: The number of required arguments for PhpCfdi\SatWsDescargaMasiva\Shared\ServiceEndpoints#__construct() increased from 4 to 5
[BC] REMOVED: Constant PhpCfdi\SatWsDescargaMasiva\RequestBuilder\FielRequestBuilder\FielRequestBuilder::USE_SIGNER was removed
[BC] CHANGED: The parameter $created of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\FielRequestBuilder\FielRequestBuilder#authorization() changed from string to a non-contravariant PhpCfdi\SatWsDescargaMasiva\Shared\DateTime
[BC] CHANGED: The parameter $expires of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\FielRequestBuilder\FielRequestBuilder#authorization() changed from string to a non-contravariant PhpCfdi\SatWsDescargaMasiva\Shared\DateTime
[BC] CHANGED: The parameter $start of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\FielRequestBuilder\FielRequestBuilder#query() changed from string to a non-contravariant PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters
[BC] REMOVED: Class PhpCfdi\SatWsDescargaMasiva\RequestBuilder\Exceptions\RfcIssuerAndReceiverAreEmptyException has been deleted
[BC] REMOVED: Class PhpCfdi\SatWsDescargaMasiva\RequestBuilder\Exceptions\PeriodEndInvalidDateFormatException has been deleted
[BC] REMOVED: Class PhpCfdi\SatWsDescargaMasiva\RequestBuilder\Exceptions\RfcIsNotIssuerOrReceiverException has been deleted
[BC] REMOVED: Class PhpCfdi\SatWsDescargaMasiva\RequestBuilder\Exceptions\PeriodStartInvalidDateFormatException has been deleted
[BC] REMOVED: Class PhpCfdi\SatWsDescargaMasiva\RequestBuilder\Exceptions\RequestTypeInvalidException has been deleted
[BC] REMOVED: Class PhpCfdi\SatWsDescargaMasiva\RequestBuilder\Exceptions\PeriodStartGreaterThanEndException has been deleted
[BC] REMOVED: Constant PhpCfdi\SatWsDescargaMasiva\RequestBuilder\RequestBuilderInterface::USE_SIGNER was removed
[BC] CHANGED: The parameter $created of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\RequestBuilderInterface#authorization() changed from string to a non-contravariant PhpCfdi\SatWsDescargaMasiva\Shared\DateTime
[BC] CHANGED: The parameter $expires of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\RequestBuilderInterface#authorization() changed from string to a non-contravariant PhpCfdi\SatWsDescargaMasiva\Shared\DateTime
[BC] CHANGED: The parameter $created of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\RequestBuilderInterface#authorization() changed from string to PhpCfdi\SatWsDescargaMasiva\Shared\DateTime
[BC] CHANGED: The parameter $expires of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\RequestBuilderInterface#authorization() changed from string to PhpCfdi\SatWsDescargaMasiva\Shared\DateTime
[BC] CHANGED: The parameter $start of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\RequestBuilderInterface#query() changed from string to a non-contravariant PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters
[BC] CHANGED: The parameter $start of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\RequestBuilderInterface#query() changed from string to PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters
[BC] CHANGED: Parameter 0 of PhpCfdi\SatWsDescargaMasiva\RequestBuilder\RequestBuilderInterface#query() changed name from start to queryParameters
24 backwards-incompatible changes detected
```

## Versión 0.4.7 2022-08-10

No se estaba haciendo la codificación correcta de `RfcReceptor`, que provocaba un fallo cuando se solicitaba
una consulta donde el RFC recibido tuviera un ampersand `&`.

### Cambios previos 2022-07-25

La clase `MicroCatalog` requiere la definición de datos extendidos, no estaban definidos y entonces
el proceso de integración continua falló. Se agregaron para hacer esta corrección.

## Versión 0.4.6 2022-04-03

La propiedad `DOMElement::localName` puede ser `null`.
Esta actualización hace la conversión a cadena de caracteres.

## Versión 0.4.5 2022-03-22

Se compatibilizó la colocación de una consulta con el servicio de solicitud de descargas masivas
*para CFDI de Retenciones e Información de Pagos*, anteriormente, al solicitar XML el valor del
atributo `TipoSolicitud` debía ser `CFDI` y ahora debe ser `Retencion`.

Este cambio altera la API pública, pero no se considera un cambio que rompa la compatibilidad
porque el cambio ocurrió sobre la clase `QueryTranslator` marcada como `@internal`.

## Versión 0.4.4 2022-03-12

Se actualizó el servicio de solicitud de descargas masivas (consulta) a la versión 1.2 del SAT.
Esta actualización por el momento solo está sobre CFDI regulares, no sobre Retenciones e información de pagos.
En este último el servicio se encuentra caído.

Al parecer la actualización no se ha completado en el SAT, y ha estado inestable desde 2022-03-14.
Sin embargo, con esta actualización se compatibliza el servicio con el funcionamiento esperado.

### Cambios en la solicitud

Se elimina el atributo `RfcReceptor` y se agrega el elemento `RfcReceptores/RfcReceptor` para especificar
el RFC del receptor en la consulta.

### CodEstatus 5006

Se agrega a la documentación de `CodEstatus` (clase `StatusCode`) el código `5006 - Error interno en el proceso`
que se supone sustituye al código `404 - Error no Controlado` para el servicio de consulta.

### Correcciones

Se agrega el método mágico `MetadataItem::__isset(string $name): bool` que no estaba contemplado.

### Entorno de desarrollo

- En las pruebas de integración, se hacen dos pruebas de solicitud consulta, una para emitidos y otra para recibidos.
- Se actualizan los archivos de muestra en las comprobaciones unitarias.
- Se agrega como dependencia la extensión de PHP `mbstring`.
- Se refactoriza la clase interna `Helpers::nospaces()` para insertar un *Line feed (LF)*.
  después de la especificación de XML.
- En las pruebas de integración, se agrega el método `ConsumeServiceTestCase::createWebClient()`
  que devuelve un objeto `GuzzleHttp\Client` configurado correctamente con *timeouts*.
- Se actualizan las herramientas del entorno de desarrollo.
- CI: Se usan las rutas establecidas en el archivo de configuración de `phpcs`.

## Versión 0.4.3 2022-02-18

- Se elimina método innecesario `FielRequestBuilder::nospaces()` y se usa en su lugar el método `Helper::nospaces()`.
- Se actualizaron las herramientas de desarrollo y se utiliza `phive` para administrarlas.
- Se actualizaron los archivos de configuración de `php-cs-fixer` acorde a la última versión.
- Se solventaron los issues de tipos encontrados por `phpstan`.
- Se migró el proceso de integración continua de *Travis CI* a *GitHub Workflows*. Gracias *Travis CI*.
- Se actualizó el archivo de licencia del proyecto. Feliz 2022.
- Se cambia la rama principal de *master* a *main*.
- Add SonarCloud integration.
- Se elimina Scrutinizer CI. Gracias Scrutinizer.

## Versión 0.4.2 2020-11-25

- Se corrige el extractor de UUID de un CFDI, no estaba funcionando correctamente y en algunas
  ocasiones provocaba que se leyera el valor de `CfdiRelacionado@UUID` en lugar del valor correcto
  de `TimbreFiscalDigital@UUID`. Esto solo ocurría cuando en el nodo principal `<Comprobante>` se
  definía el espacio de nombres o la ubicación del esquema de `TimbreFiscalDigital`.

## Versión 0.4.1 2020-11-25

- PHPStan reporta error de tipo *"Access to an undefined property"* en la clase `MetadataItem`.
  Sin embargo, la clase implementa el método mágico `__get` por lo que la propiedad no necesariamente
  se debe considerar indefinida. Se corrigió anotando la línea para que fuera ignorada.
- Se corrigen las pruebas porque ahora PHPStan entiende el control de flujo de PHPUnit y eso rompía
  la integración contínua con Travis-CI.
- Se mejora el flujo de la prueba `ServiceConsumerTest::testRunRequestWithWebClientException`.
- Se corrige en las pruebas el uso de `current()` pues puede devolver `false` y se espera `string`.

## Versión 0.4.0 2020-10-14

- Guía de actualización de la versión 0.3.2 a la versión 0.4.0: [UPGRADE_0.3_0.4](UPGRADE_0.3_0.4.md)
- Se agregan [excepciones específicas en la librería](Excepciones.md). Además, cuando se detecta una respuesta
  que contiene un *SOAP Fault* se genera una excepción.
- Se rompe la dependencia directa de `Service` a `Fiel`, ahora depende de `RequestBuilderInterface`.
- Se crea la implementación `FielRequestBuilder` para seguir trabajando con la `Fiel`.
- Se mueve `Fiel` adentro del namespace `PhpCfdi\SatWsDescargaMasiva\RequestBuilder\FielRequestBuilder`.
- Se modifican los servicios de autenticación, consulta, descarga y verificación para que,
  en lugar de que ellos mismos construyan las peticiones XML firmadas, ahora las deleguen a `RequestBuilderInterface`.
- Ahora se puede especificar un RFC específico en la consulta:
    - Si consultamos los emitidos podríamos filtrar por el RFC receptor.
    - Si consultamos los recibidos podríamos filtrar por el RFC emisor.
- Ahora se puede consumir el servicio para los CFDI de retenciones e información de pagos.
- Se agrega la interfaz `PackageReaderInterface` que contiene el contrato esperado por un lector de paquetes.
- Se crea la clase interna `FilteredPackageReader` que implementa `PackageReaderInterface`, también se agregan
  las clases `MetadataFileFilter` y `CfdiFileFilter` que permiten el filtrado de los archivos correctos dentro
  de los paquetes del SAT.
- Se restructura `MetadataPackageReader` para cumplir con la interfaz `PackageReaderInterface`,
  ahora se comporta como una fachada de un `FilteredPackageReader`.
- Se restructura `CfdiPackageReader` para cumplir con la interfaz `PackageReaderInterface`,
  ahora se comporta como una fachada de un `FilteredPackageReader`.
- Se agrega el método generador `CfdiPackageReader::cfdis()` que contiene en su llave el UUID del CFDI
  y en el valor el contenido del CFDI.
- Se agregan los constructores estáticos `::create()` de los objetos usados en `QueryParameters` y en la propia clase.
- Se convierten varias clases en finales: `StatusCode`, `DateTime`, `DateTimePeriod`, `DownloadType`, `Fiel`,
  `RequestType`, `Token`, `QueryParameters`, `QueryResult`, `VerifyResult`, `DownloadResult`.
- Se mueven y crean diferentes clases que solo deben ser utilizadas internamente al namespace "interno"
  `PhpCfdi\SatWsDescargaMasiva\Internal`: `Helpers`, `InteractsXmlTrait`, `ServiceConsumer`, `SoapFaultInfoExtractor`.
- Se marcan como clases internas los traductores usados dentro de los servicios.
- Se mueve lógica repetida en los servicios de autenticación, consulta, verificación y descarga hacia dentro
  del método `InteractsXmlTrait::createSignature`.
- Se implementa `JsonSerializable` en todos los DTO, en los lectores de paquetes y en las excepciones específicas.
- Se agregan muchas pruebas unitarias para comprobar el funcionamiento esperado y la cobertura de código.
- Se actualizan las dependencias:
    - `guzzlehttp/guzzle` de `6.3` a `7.2`
    - `robrichards/xmlseclibs` de `3.0` a `3.1`
    - `phpunit/phpunit` de `9.1` a `9.3`
- Documentación general:
    - Se agregan bloques de documentación a clases y métodos en toda la librería.
    - Se separan los bloques de ejemplos de uso en cada caso en lugar de usar solo un bloque.
    - Los códigos de servicios cambian de `Services-StatusCode.md` a `CodigosDeServicios`.

## Versión 0.3.2 2020-07-28

- Se corrige el problema de cambio de formato al definir el nombre de los archivos contenidos en
  un paquete de Metadata, el formato anterior era `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_01.txt` y
  el nuevo es `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee-0001.txt`. La corrección se relajó para que
  admita cualquier nombre de archivo con extensión `.txt` y que esté en la raíz. Esta es la
  misma estrategia utilizada en el lector de paquetes de CFDI (issue #23).
- Se corrige el problema en que dentro de un archivo de Metadata donde puede contener caracteres
  extraños en los campos de *nombre emisor* y *nombre receptor*. La corrección se consideró tomando
  en cuenta que estos campos pueden contener *comillas* `"`, para ello se considera el pipe `|` como
  delimitador de cadenas. La segunda corrección identifica si el fin de línea `EOL` es `<CR><LF>`
  y en ese caso elimina cualquier `<LF>` intermedio (issue #23).
- PHPStan estaba dando un falso positivo al detectar que `DOMElement::$attributes` puede contener `null`.
  Esto es solo cierto para cualquier `DOMNode` pero no para `DOMElement`.
- Se corrigieron las ligas a Travis-CI.
- Se agrega a Travis-CI la versión `php: nightly`, pero se le permite fallar.

## Versión 0.3.1 2020-06-04

- Se corrige el problema de que recientemente los archivos ZIP de consultas de CFDI vienen con doble extensión,
  por ejemplo `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.xml.xml`.

## Versión 0.3.0 2020-05-01

- Se actualizan las dependencias `php: >=7.3` y `phpunit: ^9.1`.
- Se actualiza `php-cs-fixer` para usar `@PHP73Migration`.

## Versión 0.2.6 2020-04-11

- Se actualizan los tests para que usen el RFC `EKU9003173C9`.
- Se agrega un test para probar qué ocurre al usar un `CSD` en lugar de una `FIEL`.
- Se actualiza el proyecto para satisfacer `phpstan: ^0.12`.
- En Scrutinizer-CI se eliminan las dependencias de desarrollo que no son útiles para la generación del *code coverage*.
- Se utiliza `eclipxe/micro-catalog` en lugar de la clase interna `OpenEnum`.
- Se renombra `Helpers::createUuid` a `Helpers::createXmlSecurityTokenId`.

## Versión 0.2.5 2020-01-07

- Se actualiza el año de licencia a 2020.
- Se remueve método privado `FielData::readContents(): string` porque ya no está en uso.
- Se corrige la construcción con PHP 7.4 en Travis.
- Se cambia la dependencia de `phpstan-shim` a `phpstan`.


## Versión 0.2.4 2019-12-06

- Se agrega la clase `PhpCfdi\SatWsDescargaMasiva\WebClient\GuzzleWebClient` que estaba en testing
  al código distribuible, aunque no se agrega la dependencia `guzzlehttp/guzzle`.
- Se documenta el uso de `GuzzleWebClient`.
- Forzar la dependencia de `phpcfdi/credentials` a `^1.1` para leer llaves privadas en formato DER.
- Forzar la dependencia de `robrichards/xmlseclibs` a `^3.0.4` por reporte de seguridad `CVE-2019-3465`.
- Agregar ejemplo en la documentación para crear y verificar un objeto `Fiel`.
- Corrección en la documentación al crear una fiel, tenía los parámetros invertidos.
- Integración continua (Travis CI):
    - Se remueve la configuración `sudo: false`.
    - No se permite el fallo del build en PHP `7.4snapshot`.
- Integración continua (Scrutinizer):
    - Se instala la extensión `zip` con `pecl`.
    - Se elimina la información de la versión fija.
    - Se modifica el archivo de configuración para que actualice `composer`.


## Version 0.2.3 2019-09-23

- Improve usage of `ResponseInterface->getBody(): StreamInterface` using `__toString()` to retrieve contents at once.
- Include `docs/` in package, exclude development file `.phplint.yml`.
- Add PHP 7.4snapshot (allow fail) to Travis CI build matrix.
- Other minor documentation typos
 

## Version 0.2.2 2019-08-20

- Make sure when constructing a `DateTime` that it fails with an exception.
- Improve code coverage.
 

## Version 0.2.1 2019-08-20

- Make `PackageReader\MetadataContent` tolerant to non-strict CSV contents:
    - Ignore lead/inner/trail blank lines
    - Include as `#extra-01` any extra value (not listed in headers)
    - Prefill with empty strings if values are less than headers


## Version 0.2.0 2019-08-13

Breaking changes:

- `CodeRequest::isNotFound` is replaced by `CodeRequest::isEmptyResult`
- `Fiel` has been rewritten with other dependences.
  To create a Fiel object use any of this:
    - `FielData::createFiel()`
    - `Fiel::create($certificateContents, $privateKeyContents, $passPhrase)`
- XML SEC Signature now follow RFC 4514 on `X509IssuerName` node.
- Removed dependence to `eclipxe/cfdiutils`, it depends now on `phpcfdi/credentials`.

Other changes:

- Fix & improve composer/phpunit/travis/scrutinizer calls.
- Fix documentation typos.


## Version 0.1.0 2019-08-09

- Initial working release
