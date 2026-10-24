# RutaSur: propuesta de contratos de eventos

Este documento propone sobres JSON para los seis topics del enunciado. Los campos descritos como **PDF** están especificados por el caso; los marcados **Propuesta** completan detalles no definidos para poder validar y procesar mensajes. La propuesta debe contrastarse con la cátedra antes de congelar contratos.

## Convenciones comunes

El PDF declara como comunes a todo evento `eventId`, `shipmentId`, `tipo`, `ocurridoEn`, `secuencia`, `hubId`, `zonaId`, `repartidorId` y `version`. La tipificación, formato y nulabilidad de abajo son decisiones propuestas para JSON Schema.

| Campo | Origen | Uso y tipo propuesto |
|---|---|---|
| `eventId` | PDF | Identificador único del evento; string no vacío. |
| `shipmentId` | PDF | Identificador del envío y clave Kafka; string. |
| `tipo` | PDF | Estado o clase del evento; string enumerado por topic. |
| `ocurridoEn` | PDF | Tiempo de negocio, ISO 8601 con zona (`date-time`); no sustituir por timestamp de Kafka. |
| `secuencia` | PDF | Entero no negativo asociado al orden del envío. En eventos derivados se copia la secuencia del evento causal; no ordena entre productores independientes. |
| `hubId`, `zonaId`, `repartidorId` | PDF | Dimensiones analíticas; string o `null` si aún no se conoce. |
| `version` | PDF | Versión entera positiva del esquema. |
| `causadoPor` | Propuesta | Referencia `{ eventId, secuencia }` al evento que motivó un evento derivado. |
| `origen` | Propuesta | Servicio que produjo el evento derivado; ayuda a diagnóstico. |
| `franja` | Propuesta para `envios.estados` | Franja horaria simulada (`MANIANA` o `TARDE`), necesaria para reintentos y analítica posteriores. |
| `intento` | Propuesta para estados de reparto/fallo | Número de intento, empezando por 1; no representa una decisión de reprogramación. |

La clave de partición es siempre `shipmentId`, según el PDF. Kafka agrega su timestamp de recepción, distinto de `ocurridoEn`. El productor debe serializar fechas UTC con `Z`; el reloj acelerado se representa en `ocurridoEn` y no cambiando la fecha de recepción de Kafka.

## Catálogo

| Topic | Productor | Consumidores / grupo | Clave | Retención indicada |
|---|---|---|---|---|
| `envios.estados` | Simulador | Proyector (`proyector`), monitor (`monitor-sla`), notificaciones (`notificaciones`), reprogramación (`reprogramacion`), analítica (`analitica`) | `shipmentId` | 30 días |
| `envios.promesas` | Simulador | Monitor (`monitor-sla`); también proyector (`proyector`) y analítica (`analitica`) para cruzar la promesa | `shipmentId` | Compactado |
| `envios.reprogramaciones` | Servicio de reprogramación | Proyector (`proyector`) | `shipmentId` | 30 días |
| `alertas.sla` | Monitor de SLA | Proyector (`proyector`), notificaciones (`notificaciones`) | `shipmentId` | 7 días |
| `notificaciones.enviadas` | Servicio de notificaciones | Proyector (`proyector`) | `shipmentId` | 7 días |
| `envios.estados.dlq` | Cualquier consumidor que detecte evento inválido | Revisión manual; ningún consumidor de negocio | `shipmentId` original o sintético estable si falta | 30 días |

La cantidad de particiones está especificada como tres solo para `envios.estados`. Elección para esta etapa: tres particiones en todos los topics excepto `envios.estados.dlq`, que tendrá una por ser de revisión manual y no requerir paralelismo de consumidores. La configuración exacta de compactación/retención para `envios.promesas` debe respetar `cleanup.policy=compact`; el PDF no fija un plazo temporal.

## Campos específicos y ejemplos

Los ejemplos muestran campos comunes completos; `hubId`, `zonaId` y `repartidorId` pueden ser `null` antes de conocerse. Todo campo específico no listado como exigido por el PDF es una **propuesta**.

### `envios.estados`

- **PDF:** estados `CREADO`, `INGRESADO_HUB`, `CLASIFICADO`, `EN_REPARTO`, `ENTREGADO`, `ENTREGA_FALLIDA`; motivo de fallo entre `AUSENTE`, `DIRECCION_INCORRECTA`, `RECHAZADO`, `ZONA_INACCESIBLE`.
- **Propuesta:** exigir `motivo` solo para `ENTREGA_FALLIDA`; agregar `intento` para distinguir los intentos.
- Cada estado incluye `franja`; `EN_REPARTO` y `ENTREGA_FALLIDA` incluyen `intento`. El evento de fallo no contiene una fecha de próximo intento: esa decisión pertenece al consumidor de reprogramación.
- **Productor/consumidores/clave/retención:** ver catálogo.

```json
{
  "eventId": "evt-000042",
  "shipmentId": "RS-1001",
  "tipo": "ENTREGA_FALLIDA",
  "ocurridoEn": "2026-10-18T15:20:00Z",
  "secuencia": 5,
  "hubId": "HUB-01",
  "zonaId": "ZONA-SUR",
  "repartidorId": "REP-07",
  "version": 1,
  "franja": "TARDE",
  "motivo": "AUSENTE",
  "intento": 1
}
```

### `envios.promesas`

- **PDF:** el simulador produce promesas; el monitor las consume; la clave es `shipmentId`; topic compactado.
- **Propuesta:** `tipo` `PROMESA_CREADA` o `PROMESA_ACTUALIZADA`; `venceEn` como fecha ISO 8601; `versionPromesa` para versionar revisiones. El proyector también consume el topic.

```json
{
  "eventId": "evt-promise-1001-v1",
  "shipmentId": "RS-1001",
  "tipo": "PROMESA_CREADA",
  "ocurridoEn": "2026-10-18T08:00:00Z",
  "secuencia": 1,
  "hubId": "HUB-01",
  "zonaId": "ZONA-SUR",
  "repartidorId": null,
  "version": 1,
  "venceEn": "2026-10-18T20:00:00Z",
  "versionPromesa": 1
}
```

### `envios.reprogramaciones`

- **PDF:** lo produce reprogramación y lo consume el proyector; la decisión depende del motivo y número de intentos.
- **Propuesta:** `accion` (`PROGRAMAR`, `RETENER`, `DEVOLVER`), `motivo`, `intento`, `programadaPara` nullable, `franja` nullable y `causadoPor`. El estado `PENDIENTE_INTERVENCION` para `ZONA_INACCESIBLE` queda como propuesta hasta aclaración.

```json
{
  "eventId": "evt-reschedule-1001-1",
  "shipmentId": "RS-1001",
  "tipo": "REPROGRAMACION_DEFINIDA",
  "ocurridoEn": "2026-10-18T15:20:01Z",
  "secuencia": 5,
  "hubId": "HUB-01",
  "zonaId": "ZONA-SUR",
  "repartidorId": "REP-07",
  "version": 1,
  "accion": "PROGRAMAR",
  "motivo": "AUSENTE",
  "intento": 2,
  "programadaPara": "2026-10-19T09:00:00Z",
  "franja": "MANIANA",
  "causadoPor": { "eventId": "evt-000042", "secuencia": 5 },
  "origen": "reprogramacion"
}
```

### `alertas.sla`

- **PDF:** la produce el monitor; la consumen proyector y notificaciones; cuatro reglas y prioridades; una alerta no se repite por envío/regla y se cierra al entregar.
- **Propuesta:** `tipo` `ALERTA_SLA_ABIERTA` o `ALERTA_SLA_CERRADA`; `alertaId`, `regla`, `prioridad`, `causa` y referencia causal. La clave lógica de no repetición es `(shipmentId, regla)` durante toda la vida del envío; el cierre referencia el mismo `alertaId`.

```json
{
  "eventId": "evt-alert-1001-sin-salida",
  "shipmentId": "RS-1001",
  "tipo": "ALERTA_SLA_ABIERTA",
  "ocurridoEn": "2026-10-18T16:00:00Z",
  "secuencia": 5,
  "hubId": "HUB-01",
  "zonaId": "ZONA-SUR",
  "repartidorId": "REP-07",
  "version": 1,
  "alertaId": "al-1001-sin-salida",
  "regla": "SIN_SALIDA",
  "prioridad": "ALTA",
  "causa": "Faltan menos de 4 horas y no está en reparto",
  "causadoPor": { "eventId": "evt-000042", "secuencia": 5 },
  "origen": "monitor-sla"
}
```

Valores de `regla` propuestos: `SIN_INGRESO` / `MEDIA`, `SIN_SALIDA` / `ALTA`, `FALLO_DIA_LIMITE` / `ALTA`, `VENCIDO` / `CRITICA`.

### `notificaciones.enviadas`

- **PDF:** la produce notificaciones y la consume el proyector; se registra el aviso, sin enviar correo ni SMS reales.
- **Propuesta:** `notificacionId`, `eventoOrigenId`, `canal` fijo `SIMULADO`, `estado` `REGISTRADA`, `categoria` (`FALLO` o `ALERTA_SLA`) y, para el aviso por ausencia, opciones de reprogramación. La clave de idempotencia funcional es `(eventoOrigenId, categoria)`.

```json
{
  "eventId": "evt-notif-1001-fallo",
  "shipmentId": "RS-1001",
  "tipo": "NOTIFICACION_REGISTRADA",
  "ocurridoEn": "2026-10-18T15:20:02Z",
  "secuencia": 5,
  "hubId": "HUB-01",
  "zonaId": "ZONA-SUR",
  "repartidorId": "REP-07",
  "version": 1,
  "notificacionId": "nt-1001-fallo-1",
  "eventoOrigenId": "evt-000042",
  "categoria": "FALLO",
  "canal": "SIMULADO",
  "estado": "REGISTRADA",
  "opcionesReprogramacion": ["MANIANA", "TARDE"],
  "causadoPor": { "eventId": "evt-000042", "secuencia": 5 },
  "origen": "notificaciones"
}
```

### `envios.estados.dlq`

- **PDF:** cualquier consumidor puede producir; revisión manual; conservar 30 días; registrar el motivo y continuar el consumo.
- **Propuesta:** envolver el contenido no validable en vez de exigirle el esquema de evento común; agregar topic, partición, offset, grupo, código de error, mensaje, timestamp de fallo y payload original crudo. `falloId` identifica el registro DLQ. Si falta `shipmentId`, usar `sin-shipmentId:<hash-del-payload>` como clave sintética y dejarlo indicado en el envoltorio.

```json
{
  "falloId": "dlq-8f19a2",
  "shipmentId": "RS-1001",
  "eventIdOriginal": "evt-000043",
  "topicOriginal": "envios.estados",
  "particionOriginal": 1,
  "offsetOriginal": "287",
  "grupoConsumidor": "proyector",
  "codigoError": "SCHEMA_INVALIDO",
  "motivo": "ENTREGA_FALLIDA requiere motivo",
  "falloEn": "2026-10-18T15:21:00Z",
  "payloadOriginal": "{\"eventId\":\"evt-000043\",...}",
  "version": 1
}
```

El registro DLQ es un sobre de error y no un evento de dominio; por eso los metadatos de transporte y `payloadOriginal` son **propuestas**, no campos comunes exigidos por el PDF. El registro original no se modifica.

## Esquemas JSON Schema propuestos

Borrador Draft 2020-12. `BaseEvent` formaliza los campos comunes; los campos específicos se agregan en cada definición. En las composiciones se permite la extensión del objeto base para admitir dichos campos. Los nombres de eventos y campos agregados son propuestas donde el PDF no fija una forma exacta.

| Topic | Definición en `$defs` |
|---|---|
| `envios.estados` | `EnvioEstado` |
| `envios.promesas` | `Promesa` |
| `envios.reprogramaciones` | `Reprogramacion` |
| `alertas.sla` | `AlertaSla` |
| `notificaciones.enviadas` | `Notificacion` |
| `envios.estados.dlq` | `DeadLetter` |

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://rutasur.local/contracts/events.schema.json",
  "$defs": {
    "BaseEvent": {
      "type": "object",
      "properties": {
        "eventId": { "type": "string", "minLength": 1 },
        "shipmentId": { "type": "string", "minLength": 1 },
        "tipo": { "type": "string", "minLength": 1 },
        "ocurridoEn": { "type": "string", "format": "date-time" },
        "secuencia": { "type": "integer", "minimum": 0 },
        "hubId": { "type": ["string", "null"] },
        "zonaId": { "type": ["string", "null"] },
        "repartidorId": { "type": ["string", "null"] },
        "version": { "type": "integer", "minimum": 1 }
      },
      "required": [
        "eventId", "shipmentId", "tipo", "ocurridoEn", "secuencia",
        "hubId", "zonaId", "repartidorId", "version"
      ]
    },
    "EnvioEstado": {
      "allOf": [
        { "$ref": "#/$defs/BaseEvent" },
        {
          "type": "object",
          "properties": {
            "tipo": {
              "enum": ["CREADO", "INGRESADO_HUB", "CLASIFICADO", "EN_REPARTO", "ENTREGADO", "ENTREGA_FALLIDA"]
            },
            "motivo": {
              "enum": ["AUSENTE", "DIRECCION_INCORRECTA", "RECHAZADO", "ZONA_INACCESIBLE"]
            },
            "intento": { "type": "integer", "minimum": 1 },
            "franja": { "enum": ["MANIANA", "TARDE"] }
          },
          "required": ["franja"],
          "allOf": [
            {
              "if": { "properties": { "tipo": { "const": "ENTREGA_FALLIDA" } }, "required": ["tipo"] },
              "then": {
                "properties": {
                  "motivo": { "enum": ["AUSENTE", "DIRECCION_INCORRECTA", "RECHAZADO", "ZONA_INACCESIBLE"] },
                  "intento": { "type": "integer", "minimum": 1 }
                },
                "required": ["motivo", "intento"]
              }
            },
            {
              "if": { "properties": { "tipo": { "const": "EN_REPARTO" } }, "required": ["tipo"] },
              "then": {
                "properties": { "intento": { "type": "integer", "minimum": 1 } },
                "required": ["intento"]
              }
            }
          ]
        }
      ]
    },
    "Promesa": {
      "allOf": [
        { "$ref": "#/$defs/BaseEvent" },
        {
          "type": "object",
          "properties": {
            "tipo": { "enum": ["PROMESA_CREADA", "PROMESA_ACTUALIZADA"] },
            "venceEn": { "type": "string", "format": "date-time" },
            "versionPromesa": { "type": "integer", "minimum": 1 }
          },
          "required": ["venceEn", "versionPromesa"]
        }
      ]
    },
    "Reprogramacion": {
      "allOf": [
        { "$ref": "#/$defs/BaseEvent" },
        {
          "type": "object",
          "properties": {
            "tipo": { "const": "REPROGRAMACION_DEFINIDA" },
            "accion": { "enum": ["PROGRAMAR", "RETENER", "DEVOLVER"] },
            "motivo": { "enum": ["AUSENTE", "DIRECCION_INCORRECTA", "RECHAZADO", "ZONA_INACCESIBLE"] },
            "intento": { "type": "integer", "minimum": 1 },
            "programadaPara": { "type": ["string", "null"], "format": "date-time" },
            "franja": { "enum": ["MANIANA", "TARDE", null] },
            "causadoPor": { "$ref": "#/$defs/CausadoPor" },
            "origen": { "const": "reprogramacion" }
          },
          "required": ["accion", "motivo", "intento", "programadaPara", "franja", "causadoPor", "origen"]
        }
      ]
    },
    "AlertaSla": {
      "allOf": [
        { "$ref": "#/$defs/BaseEvent" },
        {
          "type": "object",
          "properties": {
            "tipo": { "enum": ["ALERTA_SLA_ABIERTA", "ALERTA_SLA_CERRADA"] },
            "alertaId": { "type": "string", "minLength": 1 },
            "regla": { "enum": ["SIN_INGRESO", "SIN_SALIDA", "FALLO_DIA_LIMITE", "VENCIDO"] },
            "prioridad": { "enum": ["MEDIA", "ALTA", "CRITICA"] },
            "causa": { "type": "string", "minLength": 1 },
            "causadoPor": { "$ref": "#/$defs/CausadoPor" },
            "origen": { "const": "monitor-sla" }
          },
          "required": ["alertaId", "regla", "prioridad", "causa", "origen"]
        }
      ]
    },
    "Notificacion": {
      "allOf": [
        { "$ref": "#/$defs/BaseEvent" },
        {
          "type": "object",
          "properties": {
            "tipo": { "const": "NOTIFICACION_REGISTRADA" },
            "notificacionId": { "type": "string", "minLength": 1 },
            "eventoOrigenId": { "type": "string", "minLength": 1 },
            "categoria": { "enum": ["FALLO", "ALERTA_SLA"] },
            "canal": { "const": "SIMULADO" },
            "estado": { "const": "REGISTRADA" },
            "opcionesReprogramacion": { "type": "array", "items": { "enum": ["MANIANA", "TARDE"] } },
            "causadoPor": { "$ref": "#/$defs/CausadoPor" },
            "origen": { "const": "notificaciones" }
          },
          "required": ["notificacionId", "eventoOrigenId", "categoria", "canal", "estado", "origen"]
        }
      ]
    },
    "CausadoPor": {
      "type": "object",
      "properties": {
        "eventId": { "type": "string", "minLength": 1 },
        "secuencia": { "type": "integer", "minimum": 0 }
      },
      "required": ["eventId", "secuencia"],
      "additionalProperties": false
    },
    "DeadLetter": {
      "type": "object",
      "properties": {
        "falloId": { "type": "string", "minLength": 1 },
        "shipmentId": { "type": "string", "minLength": 1 },
        "eventIdOriginal": { "type": ["string", "null"] },
        "topicOriginal": { "type": "string", "minLength": 1 },
        "particionOriginal": { "type": "integer", "minimum": 0 },
        "offsetOriginal": { "type": "string", "minLength": 1 },
        "grupoConsumidor": { "type": "string", "minLength": 1 },
        "codigoError": { "type": "string", "minLength": 1 },
        "motivo": { "type": "string", "minLength": 1 },
        "falloEn": { "type": "string", "format": "date-time" },
        "payloadOriginal": { "type": "string" },
        "version": { "type": "integer", "minimum": 1 }
      },
      "required": [
        "falloId", "shipmentId", "topicOriginal", "particionOriginal",
        "offsetOriginal", "grupoConsumidor", "codigoError", "motivo",
        "falloEn", "payloadOriginal", "version"
      ]
    }
  }
}
```

## Reglas de procesamiento asociadas

- Validar cada mensaje en el consumidor; un inválido se envuelve en DLQ y el consumidor continúa. No reenviar errores de DLQ recursivamente.
- Aplicar `eventId` único por consumidor. Para estados, descartar también `secuencia <= ultimaSecuenciaAplicada`; un evento derivado conserva la secuencia causal pero se deduplica por su propio `eventId`.
- Confirmar offsets manualmente después de la persistencia idempotente. Usar transacción Mongo para efecto y marcador; para efecto Kafka, outbox transaccional y deduplicación en el destino.
- `envios.promesas` es compactado por `shipmentId`; publicar revisiones con clave estable y una versión creciente de promesa.
- El replay debe usar grupo y generación de proyección independientes o limpiar solo offsets/marcadores de la vista objetivo; no reiniciar los grupos activos de los otros servicios.

## Generación y reloj simulado

- En la etapa del simulador, el único productor implementado publica `envios.promesas` y `envios.estados`. `envios.reprogramaciones`, `alertas.sla`, `notificaciones.enviadas` y `envios.estados.dlq` siguen reservados a servicios posteriores; el simulador no toma decisiones ni escribe directamente en la DLQ.
- El duplicado deliberado vuelve a publicar exactamente los mismos bytes JSON, con el mismo `eventId` y `shipmentId`; no genera un nuevo evento lógico.
- El caso fuera de orden conserva los `eventId` y `secuencia` originales, pero cambia el orden de publicación. Los logs lo identifican como inyección deliberada.
- El caso malformado es JSON parseable que viola el esquema (falta `motivo` en un fallo), se publica en `envios.estados` y queda para que el futuro consumidor lo derive a DLQ.
- `SimulatedClock`, exportado desde `packages/contracts/src/index.js`, avanza por tiempo monotónico multiplicado por `acceleration`, aunque no haya eventos. Su snapshot conserva `simulatedAt` y un ancla civil para estimar inactividad entre reinicios; el reloj civil solo mide esa pausa y nunca reemplaza `ocurridoEn`.
- Un consumidor futuro debe llamar `observeEvent(ocurridoEn, timestampKafka)` y persistir periódicamente `snapshot()`. La actualización siempre aplica el máximo entre reloj actual proyectado, avance desde timestamp Kafka y `ocurridoEn`; por lo tanto, un evento atrasado no hace retroceder el reloj. La recuperación de una caída estima el intervalo con reloj civil y queda sujeta a ajustes del reloj del host.
- No se agregan topics ni campos de reloj a los sobres de eventos. El timestamp Kafka existente más la utilidad compartida y el estado persistido son suficientes para la etapa posterior.

## Verificación Kafka

`npm run verify:kafka` publica escenarios en los topics reales y consume con un grupo aleatorio `rutasur-verification-*` que parte desde el inicio. No modifica offsets de los futuros grupos, pero los registros de verificación quedan retenidos como datos de demostración; no se borran topics ni volúmenes.

### Resultado de etapa 3

La validación JSON corre antes de publicar cada evento de origen. Kafka confirmó claves iguales a `shipmentId`, lectura de promesa y estados, bytes idénticos en copias duplicadas, secuencias crecientes salvo la inyección marcada y 500 IDs distintos en la ráfaga. Los eventos malformados se publican en `envios.estados`; que el futuro proyector los derive a DLQ queda pendiente.

`ausente` confirma únicamente la publicación de `ENTREGA_FALLIDA/AUSENTE`; la fecha y franja de próximo intento dependen del futuro servicio de reprogramación. Alertas SLA, notificaciones, proyecciones Mongo y analítica tampoco se producen en esta etapa.

## Ambigüedades que afectan los contratos

1. **Cantidad de grupos:** el catálogo detalla cinco consumidores, aunque el alcance menciona cuatro. Se modelan cinco; confirmar el conteo.
2. **Consumidor de promesas:** se agrega el proyector además del monitor para poblar `envios.promesas`; confirmar si ambos deben consumir.
3. **`ZONA_INACCESIBLE`:** se propone `RETENER` y solicitar intervención, sin reprogramación automática.
4. **Reloj inactivo:** los eventos llevan `ocurridoEn`, pero no existe topic de ticks entre los seis declarados. Se propone persistir anclas de reloj simulado en el monitor y avanzar el reloj lógico en su evaluación periódica usando el factor de aceleración. Un reloj distribuido solo por eventos requeriría ampliar catálogo.
5. **Secuencias de varios productores:** un contador global por envío es ambiguo cuando monitor y servicios derivados publican en paralelo. Se propone que un evento derivado copie la secuencia causal y agregue `causadoPor`; `secuencia` no se usa para ordenar eventos derivados entre sí.
6. **Replay e interrupciones:** deduplicación y offset se coordinan con transacciones Mongo; para republicaciones se usa outbox. Replay con generación nueva evita que los marcadores viejos bloqueen reconstrucción.
7. **Franja horaria:** se proponen `MANIANA` y `TARDE`; límites, zona horaria y calendario hábil quedan configurables y pendientes de confirmación.
8. **Control del simulador:** los escenarios requieren un comando, pero Next.js se define como lector de Mongo. Se propone una ruta de control HTTP limitada al simulador; el flujo entre servicios permanece por Kafka.

## Implementación de etapa 5

Notificaciones y reprogramación ya publican sus eventos derivados con IDs deterministas por servicio, envío y evento causal. Los tres consumidores de negocio derivan mensajes inválidos a DLQ mediante outbox transaccional. La analítica también consume `envios.promesas`, con su propio grupo `analitica`, para calcular cumplimiento de SLA sin consultar otro servicio ni leer proyecciones ajenas.

La decisión de reprogramación conserva `ocurridoEn` y `secuencia` del fallo causal. `intento` indica el siguiente intento solo si `accion=PROGRAMAR`; en retención/devolución conserva el intento fallido. Ausencia programa a las 09:00 o 15:00 locales del siguiente día hábil, en franja opuesta. El calendario usa un offset UTC fijo configurable y feriados explícitos. Zona inaccesible produce `RETENER` y no inventa un estado nuevo en `envios.estados`.

El índice único de métricas incluye `hubId` además de fecha/zona/franja para que hubs diferentes no se mezclen. Las tasas se calculan sobre sumas de numeradores y denominadores; no se promedian porcentajes precalculados. La definición operativa de SLA de esta etapa usa entregas con promesa conocida. El cuarto indicador histórico se incorpora en la etapa 8.


## Implementación de etapa 6

El monitor ya consume estados y promesas y publica en `alertas.sla`. `alertaId` es determinista por envío/regla; los IDs de apertura y cierre son distintos y estables. La secuencia y `causadoPor` referencian el último estado conocido, o la promesa cuando aún no se recibió un estado. `ocurridoEn` es el momento de evaluación del reloj simulado, también cuando la alerta aparece por ausencia de nuevos eventos.

La regla de fallo en día límite compara la fecha local del fallo con la del vencimiento, conservando tiempo de evento ante mensajes atrasados. Las reglas preventivas de 10 y 4 horas dejan paso a `VENCIDO` tras el plazo. Una entrega cierra todas las reglas abiertas; revisiones de promesa no vuelven a emitir una regla ya registrada. No se agregan topics ni campos nuevos a los contratos.


## Interfaz y control de etapa 7

La interfaz no agrega topics ni modifica sobres de eventos. Sus endpoints de lectura consultan las vistas MongoDB. El tracking devuelve una lista explícita de campos públicos; el SSE solo anuncia cambios y el cliente vuelve a consultar el render del servidor.

El comando HTTP del panel al simulador es una excepción acotada para la demo: `{scenario, seed?}`. Solo el simulador genera IDs, tiempos y mensajes y los publica mediante los contratos existentes. Los consumidores de negocio no reciben llamadas HTTP ni dependen de la interfaz. El control devuelve `{scenario, seed, startAt, events, shipmentIds}` para identificar la corrida y permitir su reproducción por CLI.


## Cuarto indicador y generaciones de etapa 8

El grupo adicional `analitica-hubs` consume `envios.estados`; no agrega topics ni cambia los sobres. Cada envío aporta como máximo una pareja inicial ingreso-salida. Para este indicador de pares, las fases se ordenan por secuencia causal, permitiendo completar una fase atrasada sin proyectar ni hacer retroceder un estado de envío.

`metricas_hubs` guarda suma de duraciones y cantidad por fecha de salida/hub/zona/franja. Generaciones independientes usan sufijos en estado, métricas y outbox, y una dimensión `generation` en marcadores. El modo finito captura offsets exclusivos y termina; el modo vivo activa la vista al alcanzar el histórico y continúa. `vistas_activas` es metadato de lectura escrito por el propio proyector analítico, no un canal de eventos entre consumidores.
