 RutaSur simula el seguimiento de envíos mediante eventos:

 - Un simulador genera estados: en camino, entregado, fallido, etc.
 - Kafka distribuye esos eventos a servicios independientes.
 - Los servicios actualizan MongoDB, generan notificaciones, deciden reprogramaciones y detectan demoras.
 - Una interfaz web muestra envíos, alertas e indicadores en tiempo real.

 La idea central es que un mismo evento puede activar varias acciones sin acoplar los servicios entre sí. Todo se ejecuta localmente
 con Docker.

# RutaSur: seguimiento de envíos orientado a eventos

RutaSur es un operador ficticio de paquetería que necesita conocer el estado de sus envíos e intervenir antes de que fallen las entregas. Este TP implementa la historia del [enunciado de TP7](TP7_RutaSur_especificacion_del_caso_practico.pdf): seguimiento en vivo, reacciones independientes a fallos, alertas preventivas y analítica basada en eventos.

## Inicio rápido

Desde la carpeta `rutasur-tp7`, con Docker Engine y Docker Compose v2 o posterior:

```bash
docker compose --profile projector --profile business --profile sla --profile web --profile simulator --profile hubs up -d --build
```

1. Abrir **http://localhost:3000**: la torre de control muestra los envíos del simulador continuo.
2. Entrar a **`/demo`** para disparar escenarios y abrir el detalle o el tracking público del envío generado.
3. Consultar **`/analitica`** y actualizar los indicadores para ver los resultados agregados.

Este comando inicia todos los componentes, incluido el cuarto indicador. **Para la defensa, seguir [docs/DEMO.md](docs/DEMO.md)**: allí se incorpora ese indicador después de generar datos, para demostrar la lectura del histórico.

## Cómo funciona

```text
Simulador → Kafka → consumidores independientes → MongoDB → Next.js
                ↑              │                     │
                └──────────────┘                     └─ change streams → SSE → navegador
                    eventos derivados
```

- **Simulador:** reemplaza al depósito y a la aplicación del repartidor. Publica promesas y cambios de estado; no escribe directamente las pantallas.
- **Kafka:** conserva los eventos y la posición de lectura de cada grupo. Un mismo evento puede activar varias reacciones sin llamadas directas entre consumidores.
- **Consumidores Node.js:** el proyector construye las vistas; notificaciones, reprogramación y monitor de SLA publican eventos derivados; analítica mantiene agregados.
- **MongoDB:** guarda las vistas de consulta y el estado auxiliar de los consumidores. Las vistas se pueden reconstruir a partir del histórico retenido en Kafka.
- **Next.js:** consulta MongoDB, no Kafka. Para las pantallas en vivo, los change streams generan avisos SSE que provocan la actualización del navegador. La analítica se actualiza al consultar.

El panel `/demo` es un control auxiliar: solicita al simulador que publique escenarios. No reemplaza la comunicación por Kafka entre los consumidores ni escribe las vistas.

## Los cuatro casos de uso del PDF

| Caso | Qué ocurre | Dónde comprobarlo |
|---|---|---|
| **CU1 · Ciclo de vida** | El proyector aplica los estados y mantiene un historial sin duplicados ni retrocesos por secuencias antiguas | Torre de control, detalle y tracking público |
| **CU2 · Entrega fallida** | Un `ENTREGA_FALLIDA` dispara, de forma independiente, un aviso simulado, una decisión de reprogramación y el agregado del fallo | Detalle y analítica |
| **CU3 · Riesgo de SLA** | El monitor cruza estados y promesas y evalúa también el paso del tiempo para alertar antes del vencimiento | Cola de alertas y detalle |
| **CU4 · Analítica y reprocesamiento** | Tres indicadores explican entregas y fallos; un grupo adicional calcula el tiempo medio en el hub leyendo el histórico y luego continúa en vivo | Analítica y paso 8 de la demo |

El recorrido habitual es `CREADO → INGRESADO_HUB → CLASIFICADO → EN_REPARTO → ENTREGADO` o `ENTREGA_FALLIDA`. Una decisión de reprogramación no cambia por sí sola el estado: los eventos del siguiente intento corresponden al sistema de origen, representado por el simulador.

### Por qué los servicios están separados

Los cinco consumidores base son **proyector**, **notificaciones**, **reprogramación**, **monitor de SLA** y **analítica**, cada uno con su grupo. Detener notificaciones no detiene el tracking ni las otras reacciones; al reiniciarla, recupera sus mensajes pendientes. El sexto consumidor, **analítica de hubs**, se agrega para demostrar el reprocesamiento sin alterar los offsets de los cinco anteriores.

La entrega es **al menos una vez**, con efectos idempotentes: Kafka puede volver a entregar un mensaje, pero los consumidores evitan aplicar de nuevo su efecto. No se promete una única publicación física entre Kafka y MongoDB. La clave `shipmentId` mantiene juntos los eventos de un envío dentro de cada topic; la secuencia y la versión de promesa permiten tratar duplicados, eventos antiguos y cruces entre topics.

### Alcance y criterios de aceptación

Se ejecuta localmente con Docker Compose. No incluye GPS, optimización de rutas, autenticación, correo/SMS real, alta disponibilidad ni pruebas formales de carga. Los puertos publicados se limitan a `127.0.0.1`.

La aceptación del PDF exige tracking correcto, duplicados sin efecto, eventos inválidos en DLQ, reglas de fallos y SLA, recuperación tras una caída, indicadores contrastados con una semilla fija y reconstrucción de vistas. También exige arranque con un comando y menos de dos segundos entre evento y pantalla en el flujo continuo: las mediciones locales y sus límites están en la evidencia, no constituyen una garantía de producción.

| Necesidad | Documento |
|---|---|
| Presentar los cuatro casos y los fallos provocados en unos 15 minutos | [Guion de demo](docs/DEMO.md) |
| Consultar esquemas, topics y decisiones ante ambigüedades del PDF | [Contratos](docs/CONTRATOS.md) |
| Revisar resultados registrados y límites de las pruebas | [Evidencias](docs/EVIDENCIAS.md) |
| Consultar el historial de implementación por etapas | [Plan](docs/PLAN.md) |

## Configuración local

El arranque sin perfiles (`docker compose up -d`) levanta **solo la infraestructura**: Kafka en modo KRaft y MongoDB como replica set de un nodo, más sus inicializadores. No requiere ZooKeeper ni credenciales. Para el sistema completo, usar el comando de inicio rápido.

### Requisitos previos

- Docker Engine con Docker Compose v2 o posterior.
- Puertos locales `19092` (Kafka) y `27028` (MongoDB) libres, o valores alternativos en `.env`.
- Acceso al registro de imágenes la primera vez que se descarguen las imágenes.

### Iniciar solo la infraestructura

Para iniciar solo la infraestructura, desde la raíz de `rutasur-tp7`:

```bash
docker compose up -d
```

Compose inicia Kafka y MongoDB, espera sus healthchecks y ejecuta automáticamente `kafka-init` y `mongo-init`. Los inicializadores son repetibles: validan la configuración existente y no recrean ni limpian datos. Si la configuración de un topic o del replica set no coincide, terminan con un error descriptivo.

Para usar otros puertos, copiar `.env.example` a `.env` y cambiar `KAFKA_EXTERNAL_PORT` o `MONGODB_EXTERNAL_PORT`. No agregar secretos a ese archivo.

### Conexiones

| Cliente | Dirección |
|---|---|
| Kafka desde la máquina anfitriona | `localhost:19092` |
| Kafka desde otro contenedor de esta red Compose | `kafka:9092` |
| MongoDB desde la máquina anfitriona | `mongodb://localhost:27028/rutasur?directConnection=true` |
| MongoDB desde otro contenedor de esta red Compose | `mongodb://mongo:27017/rutasur?replicaSet=rs0` |

El cliente anfitrión de MongoDB usa `directConnection=true` porque la dirección anunciada por el replica set (`mongo:27017`) solo resuelve dentro de la red Docker. Desde contenedores se usa la URI con `replicaSet=rs0`, que permite change streams y transacciones.

### Detener conservando datos

```bash
docker compose --profile projector --profile business --profile sla --profile web --profile simulator --profile hubs stop
```

Los datos permanecen en los volúmenes `rutasur-kafka-data` y `rutasur-mongo-data` del proyecto predeterminado. No ejecutar `docker compose down -v` salvo que se quiera borrar explícitamente esos datos.

<details>
<summary>Referencia técnica: operación, servicios, reglas y verificaciones</summary>

Las cifras de verificación conservadas a continuación son resultados registrados durante el desarrollo, no ejecuciones nuevas de esta guía. Consultar [EVIDENCIAS.md](docs/EVIDENCIAS.md) para sus condiciones y límites.

## Salud, logs y topics

Ver estado de servicios e inicializadores:

```bash
docker compose ps --all
```

Consultar logs:

```bash
docker compose logs -f kafka mongo kafka-init mongo-init
```

Verificar el replica set:

```bash
docker compose exec mongo mongosh "mongodb://mongo:27017/admin?directConnection=true" --quiet --eval 'rs.status().members.map(member => ({ name: member.name, state: member.stateStr }))'
```

Listar y describir los topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe
```

Reejecutar de forma segura los inicializadores:

```bash
docker compose run --rm kafka-init
docker compose run --rm mongo-init
```

Los topics tienen factor de replicación 1. Se usan tres particiones para los cinco topics de negocio; `envios.estados.dlq` usa una partición porque es de revisión manual. Las retenciones y `cleanup.policy` se inicializan según [docs/CONTRATOS.md](docs/CONTRATOS.md).

## Simulador y escenarios reproducibles

El simulador usa Node.js 22 y separa la generación determinista (`services/simulator/src/generator.js`) de KafkaJS (`services/simulator/src/kafka.js`). Los contratos JSON y el reloj reusable están en `packages/contracts`. Solo publica eventos de origen: promesas y estados. Los servicios de negocio y el monitor producen los eventos derivados. Cada consumidor valida sus entradas y deriva los mensajes inválidos a la DLQ.

Instalar y probar la lógica sin Kafka:

```bash
npm ci
npm test
```

Las opciones se configuran en `.env` (partiendo de `.env.example`) o por argumentos. Con igual seed, fecha inicial y configuración se producen los mismos IDs, contenidos y tiempos. La aceleración por defecto es `120`: una hora simulada transcurre en 30 segundos de reloj real.

### Escenarios locales

```bash
npm run simulate:continuous
npm run simulate:happy -- --seed=demo
npm run simulate:absent -- --seed=demo
npm run simulate:delayed -- --seed=demo
npm run simulate:third-failure -- --seed=demo
npm run simulate:burst -- --seed=demo
npm run simulate:duplicate -- --seed=demo
npm run simulate:out-of-order -- --seed=demo
npm run simulate:malformed -- --seed=demo
```

El escenario `absent` solo publica el fallo por ausencia; no decide ni publica una reprogramación. `malformed` publica JSON válido que incumple el esquema en `envios.estados`, no en la DLQ. Los logs incluyen `eventId`, `shipmentId`, topic, partición y offset. `Ctrl+C` cierra KafkaJS ordenadamente durante el flujo continuo.

### Simulador en Docker

El simulador usa un perfil explícito y no se inicia con `docker compose up -d`; esto mantiene aislados los escenarios puntuales. Construir la imagen y ejecutar un feliz:

```bash
docker compose --profile simulator build simulator
docker compose --profile simulator run --rm simulator happy --seed=demo
```

Para el flujo continuo, ejecutarlo aparte:

```bash
docker compose --profile simulator up -d simulator
docker compose --profile simulator stop simulator
```

Detener el flujo antes de correr escenarios aislados para que sus eventos no se intercalen. El simulador se conecta a `kafka:9092` dentro de Compose; no publica puertos nuevos.

### Variables de configuración

| Variable | Predeterminado | Significado |
|---|---:|---|
| `SIMULATOR_SHIPMENTS_PER_MINUTE` | `20` | Ritmo del flujo continuo |
| `SIMULATOR_SEED` | `2026` | Semilla reproducible |
| `SIMULATOR_START_AT` | `2026-10-18T08:00:00.000Z` | Inicio del reloj simulado |
| `SIMULATOR_ACCELERATION` | `120` | Milisegundos simulados por milisegundo real |
| `SIMULATOR_ZONE_FAILURE_RATES` | Norte `0.05`, Centro `0.15`, Sur `0.30` | Probabilidad de fallo condicionada a zona |
| `SIMULATOR_FAILURE_REASONS` | Ausente `0.60`, dirección `0.20`, rechazo `0.15`, zona `0.05` | Distribución de motivos |
| `SIMULATOR_HUB_DELAY_PROBABILITY` | `0.10` | Probabilidad de detenerse en el hub |
| `SIMULATOR_DUPLICATE_PROBABILITY` | `0.02` | Inyección en flujo continuo |
| `SIMULATOR_OUT_OF_ORDER_PROBABILITY` | `0.02` | Inyección en flujo continuo |
| `SIMULATOR_MALFORMED_PROBABILITY` | `0.01` | Inyección en flujo continuo |

### Verificación de Kafka real

```bash
npm run verify:kafka
```

Publica feliz, ausente, demorado, tercer fallo, ráfaga de 500 envíos, duplicado, fuera de orden y malformado; luego lee promesas/estados con un grupo de verificación aleatorio y comprueba clave, bytes, secuencia y duplicados. No toca offsets de los futuros consumidores. La prueba deja sus eventos retenidos en Kafka como datos de demo, no borra datos ni volúmenes.

### Resultado verificado

- `npm test`: 11 pruebas pasan.
- `npm run verify:kafka`: 2.920 mensajes leídos, 500 IDs de ráfaga y grupo aislado sin commits.
- `docker compose config --quiet`, build de imagen y escenario feliz one-shot en Docker: pasaron.
- El flujo continuo cerró KafkaJS correctamente con SIGINT.

Estos resultados corresponden a la verificación de generación y transporte. Para la evidencia integral, consultar [docs/EVIDENCIAS.md](docs/EVIDENCIAS.md).

## Proyector: vistas del envío (CU1)

El proyector es un proceso Node.js independiente, con grupo Kafka `proyector`. Consume estados, promesas, reprogramaciones, alertas y notificaciones; mantiene las colecciones `envios`, `alertas` y `notificaciones`. No decide la reprogramación ni detecta riesgos de SLA: refleja los resultados que publican los servicios responsables.

Para ejecutarlo desde la máquina anfitriona, con Kafka y MongoDB activos:

```bash
npm run start:projector
```

Para ejecutarlo con Docker:

```bash
docker compose --profile projector up -d --build projector
```

El perfil es explícito: el comando sin perfiles inicia solo la infraestructura. El proyector confirma offsets después de persistir y guarda el efecto y su marcador de deduplicación en una transacción MongoDB. Descarta secuencias de estado menores o iguales a la última aplicada. Las promesas se ordenan por su propia versión porque llegan por un topic independiente. Un estado inválido va a `envios.estados.dlq` con payload original y motivo; luego continúa el consumo. Un fallo de persistencia detiene el proceso sin confirmar el mensaje fallido.

Los índices incluyen identificadores únicos, búsquedas operativas y TTL de siete días para los marcadores. El replica set habilita los change streams que la interfaz transforma en avisos SSE.

### Verificación de integración

```bash
npm test
npm run verify:projector
```

El verificador usa Kafka y MongoDB reales. Comprueba ciclo feliz, duplicados, secuencias antiguas, promesas antes y después de estados, proyección de eventos derivados, DLQ y continuidad, caída después de la transacción MongoDB y antes del commit Kafka, fallo de escritura sin avance de offset y dos reconstrucciones aisladas que coinciden con la vista de negocio original. También verifica que cerrar una alerta preserve su fecha de apertura.

Crea grupos y generaciones temporales; elimina sus colecciones y marcadores al terminar. Conserva los eventos publicados en Kafka y no modifica los offsets del grupo de producción `proyector`. La publicación DLQ y su marcador MongoDB no son atómicos: una caída entre ambos puede repetir la publicación con el mismo `falloId`.

### Reconstrucción aislada

```bash
npm run projector:replay -- --generation=reconstruccion-1 --cutoffs='{"envios.estados":[{"partition":0,"nextOffset":"100"}]}'
```

El JSON anterior es solo un ejemplo de formato. Para una reconstrucción completa deben incluirse los cinco topics consumidos y todas sus particiones con datos, usando como `nextOffset` el extremo superior obtenido del broker antes de comenzar. El límite es exclusivo. El replay lee desde el inicio con un grupo nuevo y escribe en colecciones con sufijo de generación, sin sustituir las colecciones activas. No reutilizar la generación ni el grupo de una reconstrucción anterior. Este replay no realiza el cambio de vista activa.

## Reacciones independientes a fallos (CU2)

Tres procesos Node.js, cada uno con su grupo Kafka y su estado MongoDB:

| Servicio | Grupo | Entradas | Resultado |
|---|---|---|---|
| Notificaciones | `notificaciones` | Estados y alertas SLA | Publica un aviso simulado por fallo y por apertura de alerta en `notificaciones.enviadas` |
| Reprogramación | `reprogramacion` | Estados | Publica la decisión en `envios.reprogramaciones` |
| Analítica | `analitica` | Estados y promesas | Actualiza `metricas_diarias` con incrementos atómicos |

Arrancar infraestructura, proyector y los tres servicios:

```bash
docker compose --profile projector --profile business up -d --build
```

El simulador continúa siendo un perfil separado. Para probar un fallo:

```bash
npm run simulate:absent -- --seed=etapa5
```

El proyector refleja la decisión y el aviso en las vistas. La reprogramación registra el próximo intento; no genera por sí misma otro evento de reparto en el simulador. Las decisiones `RETENER` y `DEVOLVER` aparecen en `reprogramacionActual.accion`; el último estado de origen permanece en `estado`.

También pueden ejecutarse los servicios con `npm run start:notifications`, `npm run start:rescheduling` y `npm run start:analytics`, cada uno en su terminal.

### Políticas de reprogramación

- Ausente: siguiente día hábil y franja opuesta; horarios locales de referencia 09:00 para mañana y 15:00 para tarde.
- Dirección incorrecta: retener, sin intento automático.
- Rechazado: devolver.
- Tercer intento fallido o posterior: devolver, cualquiera sea el motivo.
- Zona inaccesible: retener para intervención, según la propuesta documentada ante la ambigüedad del enunciado.

El calendario usa lunes a viernes, `BUSINESS_UTC_OFFSET_MINUTES=-180` y una lista opcional `BUSINESS_HOLIDAYS=2026-12-25,2027-01-01`. No descarga feriados ni ajusta horario de verano automáticamente. La misma configuración debe mantenerse entre reinicios y consumidores para reproducir resultados.

### Persistencia y recuperación

La lógica compartida de `services/business/` conserva tres procesos separados y grupos independientes. Cada servicio valida el esquema, descarta duplicados y secuencias antiguas del stream de estados y confirma el offset después de persistir y publicar sus salidas pendientes. Las alertas se deduplican por evento y no se comparan con la secuencia del stream de estados.

Una transacción MongoDB guarda el estado, el marcador y la outbox (o los incrementos analíticos). Las outboxes `outbox_notificaciones`, `outbox_reprogramacion` y `outbox_analitica` se publican antes de confirmar el offset y al arrancar el proceso. La última solo genera salidas DLQ. Si el proceso cae después de publicar pero antes de marcar la salida, puede volver a publicar el mismo evento con ID estable; el proyector lo deduplica. No se promete una única publicación física entre Kafka y MongoDB.

Los estados auxiliares son `estado_notificaciones`, `estado_reprogramacion` y `estado_analitica`. Los marcadores tienen TTL de siete días; las outboxes y el estado se conservan. Los logs incluyen servicio, evento, envío, topic, partición y offset. Si una escritura o publicación falla, el proceso termina sin confirmar el mensaje y se puede reiniciar con el mismo grupo.

### Indicadores disponibles

`metricas_diarias` usa dimensiones `fecha`, `zonaId`, `hubId` y `franja` con índice único. La fecha es la fecha local del resultado del intento.

| Indicador | Cálculo sobre sumas de contadores |
|---|---|
| Entrega en primer intento | `entregasPrimerIntento / primerosIntentos`; el denominador incluye primeros intentos con resultado, exitosos o fallidos |
| Motivos de fallo | `motivos.AUSENTE`, `motivos.DIRECCION_INCORRECTA`, `motivos.RECHAZADO`, `motivos.ZONA_INACCESIBLE`; cuenta cada intento fallido |
| Cumplimiento de SLA | `entregasEnPlazo / entregasConPromesa`; considera envíos entregados con promesa conocida, comparando `ocurridoEn <= venceEn` |

Ante denominador cero, la interfaz muestra sin datos. El estado por envío permite cruzar una promesa llegada después de la entrega y corregir por delta un cambio de versión de promesa. Las versiones antiguas se ignoran. Para una entrega sin `intento`, se usa el intento del último `EN_REPARTO`, o 1 si no hay dato previo. El cuarto indicador histórico se explica en la sección de analítica de hubs y replay.

### Verificar las reacciones a fallos

```bash
npm test
npm run verify:business
```

La integración usa grupos nuevos y una base `rutasur_verify_business_<id>` exclusiva que elimina al terminar. Publica diez fallos, verifica las cuatro políticas y el tercer intento, IDs duplicados y secuencia antigua, un ciclo entregado, promesas tardías y versionadas, aviso ante apertura de alerta, DLQ y continuidad. Detiene notificaciones, comprueba que reprogramación y analítica siguen, mide el atraso del grupo y verifica su recuperación. Provoca una caída tras la transacción MongoDB y antes de publicar la outbox; al reiniciar se proyecta un solo aviso.

Resultado verificado el 23 de octubre de 2026: 10 fallos, 10 decisiones de reprogramación, 11 avisos (10 fallos y 1 alerta), 7 primeros intentos con 1 entrega exitosa y 1 entrega fuera de plazo según la última promesa. También pasaron las pruebas locales y la construcción de las tres imágenes Docker. Los eventos quedan retenidos en Kafka; los grupos activos y la base `rutasur` no se modifican.

## Monitor de riesgo de SLA (CU3)

El monitor es un proceso Node.js separado con grupo `monitor-sla`. Consume `envios.estados` y `envios.promesas`, mantiene estado propio por envío y publica aperturas/cierres en `alertas.sla`. El proyector actualiza `alertas` y notificaciones registra un aviso por apertura.

Para ejecutar la infraestructura y los cinco consumidores:

```bash
docker compose --profile projector --profile business --profile sla up -d --build
```

El simulador se ejecuta aparte (`npm run simulate:delayed -- --seed=sla-demo` o perfil `simulator`). Desde el anfitrión, el monitor también puede arrancarse con `npm run start:sla`.

### Reglas y prioridades

| Regla | Condición | Prioridad |
|---|---|---|
| `SIN_INGRESO` | Faltan menos de 10 horas y no hay evidencia de ingreso al hub | Media |
| `SIN_SALIDA` | Faltan menos de 4 horas y el último estado no es `EN_REPARTO` | Alta |
| `FALLO_DIA_LIMITE` | Un fallo ocurrió en la misma fecha local que el vencimiento de la promesa | Alta |
| `VENCIDO` | Pasó el vencimiento sin entrega | Crítica |

Las dos primeras reglas aplican antes del vencimiento, con umbrales estrictos; al estar vencido corresponde la regla crítica. El fallo en día límite conserva el tiempo de evento, aunque llegue tarde. La fecha local usa `BUSINESS_UTC_OFFSET_MINUTES`, igual que la analítica. Puede haber varias reglas abiertas para un envío; cada regla se publica una sola vez durante su vida. Un cambio de promesa no reabre una regla ya emitida.

`ENTREGADO` cierra todas las alertas abiertas del envío. Las reglas no se reabren tras la entrega, ni por promesas posteriores o estados antiguos. Si la entrega llegó antes que la promesa, no se genera una alerta por ese cruce tardío. Las secuencias antiguas y versiones de promesa no vigentes no sustituyen el estado aceptado.

### Reloj, estado y recuperación

El reloj se inicializa con el primer evento válido y comparte `SimulatedClock` con el simulador. No se inicializa la fecha de negocio con la hora actual de la máquina. Se evalúa al procesar cada mensaje y cada minuto simulado: con aceleración 120 el intervalo real nominal es 500 ms. `SLA_EVALUATION_INTERVAL_SIMULATED_MS` configura ese intervalo; `SIMULATOR_ACCELERATION` debe ser igual al del simulador.

El reloj sigue avanzando cuando no llegan eventos. Su snapshot guarda el instante simulado, el ancla civil y la aceleración en `reloj_monitor_sla`. Al reiniciar, estima el tiempo transcurrido durante la caída y lo multiplica por la aceleración. La hora civil sirve para medir esa pausa; no reemplaza las fechas de negocio. La estimación conserva la limitación documentada ante cambios del reloj del anfitrión. Cambiar la aceleración con un reloj persistido produce un error de configuración, para evitar reinterpretar el histórico.

El proceso restaura a memoria `estado_monitor_sla`, que conserva último estado, secuencia, promesa vigente y reglas emitidas/cerradas. Las operaciones de mensajes y del temporizador se serializan. Una transacción MongoDB guarda el estado, el marcador, la outbox y el snapshot asociado. Las evaluaciones periódicas solo escriben envíos cuando hay nuevas alertas; el reloj se guarda aunque no haya salidas.

`outbox_monitor_sla` conserva las salidas pendientes y publicadas. Se publica antes de confirmar offsets y al iniciar el proceso. Una caída después de publicar y antes de marcar puede repetir los mismos bytes con el mismo ID; el proyector deduplica. Una caída después de persistir y antes de publicar deja una salida que se recupera al arrancar. Los registros de reglas en el estado no tienen TTL y evitan reabrirlas aunque expire el marcador de siete días.

Los mensajes inválidos de estados o promesas van a DLQ con su motivo; el monitor continúa. Las colecciones del monitor son propias: no consulta `envios`, `alertas` ni el estado de otros consumidores para decidir. La configuración local ejecuta una instancia del monitor.

### Verificación

```bash
npm test
npm run verify:sla
```

Resultado verificado el 23 de octubre de 2026: pruebas locales, integración completa e imagen Docker del monitor correctas.

El verificador usa una base temporal `rutasur_verify_sla_<id>`, grupos aislados y tres procesos reales: monitor, proyector y notificaciones. Verifica las cuatro reglas/prioridades, duplicados, secuencias antiguas, cruces entre topics, promesa versionada, evaluación sin eventos, recuperación del reloj y caída después de la transacción antes de publicar. Comprueba seis aperturas, seis cierres y seis avisos de alerta; las fechas de apertura se conservan al cerrar. También prueba DLQ y consumo posterior. Limpia la base y los procesos temporales, conservando los eventos retenidos en Kafka.

La interfaz consulta las alertas proyectadas y las actualiza mediante SSE, como se explica a continuación.


## Interfaz: torre de control, tracking y analítica

Interfaz en `apps/web`, con Next.js App Router, React y el cliente MongoDB. Renderiza los datos en el servidor y aplica actualizaciones en vivo desde un componente cliente. No importa Kafka ni bibliotecas de los consumidores.

### Levantar la aplicación completa

```bash
docker compose --profile projector --profile business --profile sla --profile web up -d --build
```

Abrir **http://localhost:3000**. `WEB_PORT` cambia el puerto publicado; siempre se limita al anfitrión local. El perfil `web` incluye la interfaz y el control HTTP del simulador. Para iniciar también el flujo continuo, sumar `--profile simulator` al mismo comando. Se conservan los volúmenes existentes.

| Ruta | Pantalla |
|---|---|
| `/` | Torre de control: contadores, filtros por estado/zona/código, envíos paginados, alertas priorizadas y movimientos recientes |
| `/envios/<shipmentId>` | Detalle: historial, intentos, promesa, decisión de reprogramación, alertas y avisos |
| `/tracking` | Búsqueda pública por código |
| `/tracking/<shipmentId>` | Estado, fecha estimada e historial público, con actualización en vivo |
| `/analitica` | Tres indicadores base y tiempo en el centro al activar hubs; filtros de zona, centro y fechas |
| `/demo` | Ocho escenarios del simulador, con enlaces a los envíos publicados |

Las pantallas usan datos reales de las proyecciones y presentan estados vacíos o errores recuperables cuando no hay datos o MongoDB no está disponible. La vista es adaptable a escritorio y móvil. Las capturas de QA se generan con datos sintéticos en una base temporal, nunca insertados en la base de operación.

### Desarrollo sin Docker para Next.js

Con la infraestructura y los consumidores activos:

```bash
npm ci
npm --prefix apps/web ci
npm run start:simulator-control
# En otra terminal:
npm run web:dev
```

Para verificar y ejecutar la compilación de producción: `npm run web:build` y `npm run web:start`. Los scripts de la raíz leen `.env`. La aplicación usa `MONGODB_URI` y `MONGODB_DATABASE`; el control usa `SIMULATOR_CONTROL_PORT` y `SIMULATOR_CONTROL_HOST`. Next.js consulta el control en `SIMULATOR_CONTROL_URL`, por defecto `http://127.0.0.1:3001` fuera de Docker.

### Flujo de datos y privacidad

`GET /api/events` abre un change stream sobre `envios`, `alertas` y `notificaciones`. La suscripción del tracking público observa solo el envío solicitado. El SSE envía únicamente avisos `ready` y `changed`, nunca documentos, tokens de MongoDB ni payloads internos. Al quedar listo, el cliente refresca los datos para cubrir la ventana entre render inicial y suscripción. Los cambios posteriores se agrupan en intervalos cortos con espera acotada para que una ráfaga no posponga indefinidamente el refresco.

Hay heartbeat, reconexión automática, detección de navegador sin conexión y cierre del cursor al cancelar o salir de la página. La analítica se actualiza al consultar, según el PDF; no abre un stream.

El tracking público usa un DTO explícito: `shipmentId`, `estado`, `fechaEstimada` e historial de `tipo`/`ocurridoEn`. No incluye hub, zona, repartidor, intentos, secuencias, eventos causales, alertas ni notificaciones internas. Las rutas operativas son locales y no tienen autenticación, que queda fuera del alcance del TP.

Las consultas de analítica agregan **todo** el período filtrado sobre `metricas_diarias`; el límite visual de 200 filas solo afecta al detalle. Se suman numeradores y denominadores antes de calcular tasas, sin promediar porcentajes.

### Control de escenarios

El panel hace un `POST /api/demo` que valida el escenario y deriva el comando al control del simulador. Ese proceso usa los mismos generadores y productor del simulador y publica en Kafka. La interfaz no escribe proyecciones ni publica eventos directamente. Esta ruta de control implementa la excepción de demo documentada en el plan; los cinco consumidores siguen comunicándose por Kafka.

La semilla es opcional; se genera una nueva si está vacía. El control toma como inicio el instante de su reloj simulado y devuelve `seed` y `startAt` junto a los IDs. Para reproducir exactamente esa corrida por CLI, usar ambos valores con `--seed` y `--start-at`. Se permiten feliz, ausente, demorado, tercer fallo, ráfaga, duplicado, fuera de orden y malformado. El flujo continuo se ejecuta con el comando/perfil del simulador.

### Verificación reproducible

```bash
npm test
npm run web:build
npm run verify:web
```

La integración levanta Next.js de producción, proyector y control del simulador en puertos libres, con una base `rutasur_verify_web_<id>` y un grupo Kafka exclusivos. Verifica SSR, filtros, búsqueda literal, orden de alertas, privacidad HTML/JSON, agregación de más filas que las visibles, SSE por envío y escenario feliz a través del proxy y el simulador. Limpia procesos y base temporal; conserva eventos Kafka. Sin navegador, mide el trayecto Kafka → proyección → SSE/API.

Opcionalmente se puede ejecutar la misma integración con Chromium para probar actualización visible, reconexión, móvil y botones:

```bash
npm install --prefix /tmp/rutasur-browser playwright
PLAYWRIGHT_BROWSERS_PATH=/tmp/rutasur-browser/cache /tmp/rutasur-browser/node_modules/.bin/playwright install chromium
PLAYWRIGHT_BROWSERS_PATH=/tmp/rutasur-browser/cache WEB_VERIFY_BROWSER_MODULE=/tmp/rutasur-browser/node_modules/playwright/index.mjs npm run verify:web
```

Resultado verificado el 23 de octubre de 2026: 211 ms de Kafka a SSE/API y 640 ms hasta el cambio visible en tracking. En una muestra de tres envíos a 20/min, la torre de control actualizó en 775, 500 y 593 ms. Pasaron la desconexión/reconexión, la vista móvil sin desborde horizontal, el botón de envío feliz y la validación de privacidad. Son mediciones locales de una muestra controlada; no son una prueba formal de carga.

También pasaron las pruebas locales, la compilación Next.js, la construcción de las imágenes y una prueba real de la imagen standalone con MongoDB, HTML, CSS y validación del panel.

Playwright y Chromium son herramientas de QA temporales; no son dependencias de ejecución del proyecto. El verificador imprime si se usó navegador y los tiempos medidos. Las capturas se guardan en `/tmp/rutasur-control-desktop.png` y `/tmp/rutasur-control-mobile.png`.

El cuarto indicador demuestra la lectura del histórico con un grupo independiente, como se explica a continuación.


## Analítica de hubs, replay y aceptación (CU4)

El consumidor adicional `services/hub-analytics` usa un **grupo nuevo**, `analitica-hubs`, sobre `envios.estados`. No reinicia ni modifica los grupos existentes. Se activa después de generar datos para demostrar que Kafka conserva el histórico:

```bash
docker compose --profile hubs up -d --no-deps --build hub-analytics
```

Lee desde el inicio retenido, muestra `Histórico de hubs completo.` y sigue incorporando eventos nuevos. En `/analitica`, pulsar **Actualizar indicadores** para ver **Tiempo en el centro**. También puede ejecutarse con `npm run start:hub-analytics` desde el anfitrión.

### Definición del cuarto indicador

Una muestra es el primer `INGRESADO_HUB` y el primer `EN_REPARTO` con secuencia posterior del mismo envío. Se usa la diferencia entre sus `ocurridoEn`, no el tiempo de recepción ni el reloj de la máquina. Las salidas de reintentos no agregan otra muestra de la misma permanencia inicial.

Se guardan `duracionTotalMs` y `enviosConSalida` en `metricas_hubs`, por fecha local de salida, hub de ingreso, zona y franja. El promedio por hub y el total se calculan dividiendo sumas, sin promediar promedios. Los filtros incluyen también hubs/zonas que solo tengan datos de este indicador.

El consumidor conserva fases por secuencia para completar una pareja si llega invertida y corregir por delta una fase anterior tardía. No proyecta el estado actual del envío ni lo hace retroceder. Descarta secuencias repetidas de las fases; no cuenta intervalos negativos ni envíos sin ambas fases. Efecto, estado y marcador se guardan en una transacción; los inválidos se publican mediante outbox a la DLQ. Los offsets se confirman después.

### Reconstrucción aislada

```bash
npm run replay:hubs -- --generation=replay-demo-1
```

El comando captura el extremo superior de cada partición, lee desde el inicio y termina al procesar esos límites exclusivos. Escribe `metricas_hubs__replay-demo-1` y `estado_hubs__replay-demo-1`, con marcadores de generación propios. No sustituye la vista activa ni toca offsets de otros grupos. Usar otra generación para una reconstrucción independiente; mantener generación y grupo para retomar una corrida interrumpida.

Para reconstruir y después continuar en vivo con una nueva vista:

```bash
npm run start:hub-analytics -- --generation=hubs-v2
```

Detener previamente solo la instancia anterior de **hubs**. La nueva generación se activa en `vistas_activas` al alcanzar el histórico capturado y sigue consumiendo; la interfaz consulta esa vista. Los cinco consumidores originales continúan activos. En Docker, configurar `HUB_GENERATION=hubs-v2` antes de recrear únicamente `hub-analytics`.

`reprocesamientos_hubs` conserva grupo, límites y estado de cada generación. El replay compara offsets efectivamente procesados, no el extremo anunciado de un lote todavía incompleto. Se pausan las particiones que alcanzaron su límite en modo finito.

### Verificación integral y demo

```bash
npm run verify:all
npm run verify:compose
```

La primera orden ejecuta pruebas locales, compilación Next.js y las integraciones de proyector, reacciones, SLA, interfaz y aceptación. `verify:acceptance` también puede ejecutarse sola tras `npm run web:build`. Con `WEB_VERIFY_BROWSER_MODULE` y `PLAYWRIGHT_BROWSERS_PATH` configurados como en la sección de verificación de la interfaz, incluye Chromium.

La aceptación publica siete escenarios con semilla `tp7-aceptacion-v1` e inicio `2026-10-23T08:00:00.000Z`. Antes de las fases adicionales, el recuento manual esperado es:

| Resultado | Valor |
|---|---:|
| Primeros intentos con resultado | 5 |
| Entregas en primer intento | 3 (60%) |
| Intentos fallidos | 4, todos por ausencia |
| Entregas con promesa / en plazo | 3 / 3 (100%) |
| Parejas ingreso-salida históricas | 6 |
| Tiempo acumulado / medio | 360.000 ms / 60.000 ms |

La demora no tiene resultado final; el fallo malformado no suma un fallo válido. Ambos explican por qué siete envíos no producen siete primeros intentos con resultado. El malformado sí conserva fases válidas de ingreso y salida, por lo que aporta al cuarto indicador. Un nuevo feliz agrega la séptima muestra, manteniendo el promedio de un minuto.

La aceptación detiene/reinicia notificaciones, verifica lag y recuperación, inicia el grupo histórico con los otros consumidores activos, compara dos replays de hubs y borra/reconstruye `envios` en su base temporal. La vista de negocio reconstruida coincide con la original. La captura del panel histórico queda en `/tmp/rutasur-analitica-final.png` cuando se usa Chromium.

`verify:compose` usa un nombre `rutasur-verify-<id>`, puertos libres y volúmenes nuevos cuyo nombre comprueba antes de iniciar. Verifica flujo continuo, pantalla/API, reacciones al fallo, SLA y activación del indicador sin reiniciar los cinco consumidores. Luego elimina exclusivamente ese proyecto temporal. Comprueba un arranque sin datos previos en el mismo host; no aprovisiona otra máquina ni sustituye una prueba formal de carga.

Resultado de cierre: suite completa secuencial aprobada, aceptación con Chromium aprobada y arranque Compose con volúmenes nuevos aprobado. Esta última corrida registró cinco envíos y cinco muestras de hubs, sin reiniciar los cinco consumidores base.

El control del simulador avanza su reloj hasta el último instante publicado por un escenario, de modo que el siguiente escenario no empiece antes de los eventos que acaba de generar.

El recorrido de defensa está en [docs/DEMO.md](docs/DEMO.md), y las pruebas y sus límites en [docs/EVIDENCIAS.md](docs/EVIDENCIAS.md).

</details>
