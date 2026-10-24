# RutaSur: relevamiento y plan de implementación

## Objetivo

Construir una plataforma local de seguimiento de envíos orientada a eventos. El simulador publica eventos en Apache Kafka; cinco servicios Node.js independientes los procesan y mantienen proyecciones en MongoDB; Next.js consulta esas proyecciones para mostrar tracking, operación y analítica. Kafka es el registro de eventos y MongoDB es un modelo de lectura reconstruible.

El trabajo se limita al caso práctico: sin integraciones reales, GPS, optimización de rutas, autenticación, envíos reales de mensajes, nube ni pruebas formales de carga. La solución debe arrancar localmente con Docker Compose. Este documento contiene el plan y los resultados de las etapas implementadas.

## Casos de uso

1. **CU1, ciclo de vida y tracking:** proyectar los estados del envío, preservar su historial y permitir consultar el estado actual. Descartar duplicados y eventos con secuencia ya aplicada o anterior; enviar eventos inválidos a la DLQ.
2. **CU2, entrega fallida:** ante `ENTREGA_FALLIDA`, notificaciones, reprogramación y analítica reaccionan de forma independiente. La política depende del motivo y del número de intentos.
3. **CU3, riesgo de SLA:** cruzar estados y promesas, evaluar también el paso del tiempo y publicar alertas con prioridad; cerrar la alerta al entregar.
4. **CU4, analítica y replay:** mantener tres indicadores incrementales y, posteriormente, agregar el tiempo medio entre `INGRESADO_HUB` y `EN_REPARTO` por hub mediante un grupo nuevo que relee el histórico.

### Reglas de reprogramación del enunciado

- `AUSENTE`: nuevo intento el siguiente día hábil, en la franja opuesta a la del fallo.
- `DIRECCION_INCORRECTA`: retener hasta que se corrija la dirección; no reprogramar automáticamente.
- `RECHAZADO`: marcar para devolución al comercio.
- Tercer intento fallido, cualquiera sea el motivo: devolución al comercio.
- `ZONA_INACCESIBLE`: el enunciado no define la reacción; la propuesta de retener y pedir intervención está en Ambigüedades y propuestas.

## Componentes

| Componente | Responsabilidad | Grupo Kafka |
|---|---|---|
| Simulador | Generar envíos, promesas, reloj simulado, flujo continuo y escenarios reproducibles; publicar eventos de origen. | No consume |
| Proyector | Mantener `envios`, `alertas` y `notificaciones` en MongoDB; aplicar orden e idempotencia. | `proyector` |
| Notificaciones | Registrar avisos simulados por fallos y alertas; publicar el resultado. | `notificaciones` |
| Reprogramación | Aplicar la política por motivo e intento; publicar la decisión del próximo paso. | `reprogramacion` |
| Monitor de SLA | Mantener estado por envío, evaluar vencimientos y emitir aperturas/cierres de alertas. | `monitor-sla` |
| Analítica | Actualizar los tres indicadores diarios. | `analitica` |
| Analítica de hubs (etapa 8) | Completar el cuarto indicador desde el histórico y continuar en vivo. | `analitica-hubs` y grupos por generación |
| Next.js | Torre de control, detalle, tracking público, analítica y controles de demo; leer proyecciones de MongoDB y exponer cambios por SSE. | No consume |
| Kafka | Log de eventos, distribución desacoplada y offsets por grupo. | Infraestructura |
| MongoDB | Vistas derivadas, deduplicación/estado de consumidores y change streams; ejecutar como replica set de un nodo. | Infraestructura |

**Conteo que se adopta para planificar:** cinco consumidores independientes: proyector, notificaciones, reprogramación, monitor de SLA y analítica. El simulador produce y Next.js consulta MongoDB; ninguno cuenta como consumidor Kafka. La diferencia con «cuatro» en el alcance se registra abajo para confirmar con la cátedra.

## Estructura propuesta

Estructura inicial para un repositorio nuevo en la raíz del workspace. Los servicios deben poder arrancarse y detenerse individualmente. No se propone NestJS ni otro framework de backend; usar Node.js y las bibliotecas cliente necesarias. Next.js se conserva para la interfaz porque lo exige el enunciado.

```text
.
├── docker-compose.yml
├── package.json
├── .env.example
├── docs/
│   ├── PLAN.md
│   └── CONTRATOS.md
├── infra/
│   ├── kafka/
│   └── mongo/
├── packages/
│   └── contracts/              # esquemas, nombres de topics y validadores compartidos
├── services/
│   ├── simulator/
│   ├── projector/
│   ├── notifications/
│   ├── rescheduling/
│   ├── sla-monitor/
│   └── analytics/
├── apps/
│   └── web/                    # Next.js: vistas, route handlers y SSE
└── tests/
    ├── contracts/
    └── integration/
```

Los cinco servicios son procesos Node.js separados, con comandos y configuración individuales. La raíz puede coordinar scripts y Docker Compose, pero no debe convertirlos en un único proceso. La estructura es una propuesta: primero se verificará el tooling disponible y luego se decidirá si conviene un workspace npm o paquetes independientes.

## Etapas y verificaciones

| Etapa | Trabajo | Depende de | Verificaciones para esa etapa |
|---|---|---|---|
| 1. Relevamiento y planificación | Acordar componentes, estructura, contratos iniciales y ambigüedades. | Ninguna | Revisar requisitos, propuesta de topics y criterios de aceptación. |
| 2. Infraestructura local | Compose con Kafka KRaft y MongoDB replica set de un nodo; inicialización idempotente de topics y documentación operativa. | Etapa 1 | Compose válido, healthchecks, PRIMARY, change streams/transacciones, seis topics y segundo arranque sin pérdida. |
| 3. Contratos y simulador | Contratos JSON, generación determinista, reloj compartido, productor y escenarios de demo/fallo. | Etapa 2 | Validación correcta/incorrecta; reproducibilidad por seed y configuración; claves Kafka; secuencias, duplicados y 500 envíos de ráfaga; escenarios leídos desde Kafka. |
| 4. Proyector | Consumir estados y eventos derivados; construir vistas; guardar historial, última secuencia e idempotencia; DLQ; índices y change streams. | Etapas 2 y 3 | Probar duplicado, fuera de orden, evento inválido, reinicio y reconstrucción desde el log; la vista reconstruida coincide con la original. |
| 5. Reacciones a fallos | Implementar por separado notificaciones, reprogramación y analítica incremental. Cada servicio tiene su propio grupo, proceso y configuración. | Etapas 2 a 4 | Un fallo dispara los efectos previstos; verificar todos los motivos, exactamente una proyección por efecto, fallo/reinicio aislado y avance del lag del grupo detenido. |
| 6. Monitor de SLA | Cruzar `envios.estados` y `envios.promesas`, persistir estado, evaluar en cada evento y periódicamente con reloj simulado; emitir y cerrar alertas. | Etapas 2 a 4 | Probar las cuatro reglas/prioridades, no repetición, cierre al entregar y reinicio sin perder ni repetir alertas. |
| 7. Interfaz | Vistas Next.js de torre de control, detalle, tracking público, analítica y panel de demo; consultas MongoDB, change streams y SSE. | Etapas 4 a 6 | Probar vistas con datos reales de una corrida; comprobar SSE y latencia evento-pantalla menor a 2 segundos; confirmar que la interfaz no consume Kafka. |
| 8. Replay y aceptación integral | Agregar cuarto indicador con un grupo nuevo desde el inicio; documentar operación y ejecutar el guion de demo y aceptación. | Etapas 2 a 7 | Comparación manual con semilla fija; replay sin reiniciar los demás grupos; borrar/reconstruir vistas; recorrido de los ocho pasos de demo. |

La etapa 3 separa generación pura y transporte Kafka para hacer reproducible el simulador. El reloj común vive en `packages/contracts`; el simulador y el futuro monitor pueden compartirlo. El verificador de Kafka usa un grupo temporal único. La implementación de simulador no inicia consumidores de negocio.

### Resultado de etapa 3

- [x] `npm test`: 11 pruebas pasan, incluyendo reproducibilidad, contratos válidos/inválidos, duplicados, secuencias, 500 envíos y reloj compartido/reanudable.
- [x] `npm run verify:kafka`: promesas y estados leídos desde Kafka con grupo aleatorio y sin commits; escenarios e inyecciones verificados. La corrida final leyó 2.920 mensajes y confirmó 500 `shipmentId` en la ráfaga.
- [x] Compose válido; imagen del simulador construida; escenario feliz one-shot publicado desde Docker.
- [x] El flujo continuo publicó envíos y cerró KafkaJS ordenadamente al recibir SIGINT.
- [x] Se conservaron infraestructura, puertos y volúmenes existentes; no se detuvieron servicios de otros proyectos.

**Estado al cerrar la etapa 3:** el simulador solo genera eventos de origen. La etapa 4, documentada abajo, agrega proyecciones MongoDB y DLQ; las decisiones de negocio, métricas e interfaz siguen pendientes.

### Resultado de etapa 4 — 23 de octubre de 2026

- [x] Proyector independiente con perfil Docker y comandos de ejecución y replay.
- [x] Transacciones MongoDB para efecto y marcador; commits Kafka posteriores a persistencia.
- [x] Historial, deduplicación, secuencias antiguas y promesas recibidas en distinto orden entre topics.
- [x] Índices operativos, únicos y TTL de siete días.
- [x] Estado inválido a DLQ con motivo y payload original; el mensaje válido posterior se procesa.
- [x] Proyección de reprogramación, notificación y apertura/cierre de alerta; al cerrar se conserva la fecha de apertura.
- [x] Caída después de transacción y antes del commit: reinicio sin repetir el efecto.
- [x] Fallo de escritura: el offset fallido no se confirma.
- [x] Dos replays aislados producen la misma vista de negocio para siete envíos de prueba.
- [x] Regresión local: el replay no termina al ver el extremo del log si todavía faltan lotes por procesar.
- [x] Imagen Docker del proyector construida y comando de ayuda ejecutado desde Compose.

Verificación reproducible: `npm test` y `npm run verify:projector`. El verificador limpia sus proyecciones temporales; conserva eventos Kafka. Los eventos derivados se inyectan como datos de prueba: sus productores de negocio aún no existen. MongoDB permite change streams, pero la integración con SSE se implementará con la interfaz. El replay crea colecciones aisladas; todavía no reemplaza automáticamente la vista activa. La DLQ puede repetir una publicación si el proceso cae antes de guardar su marcador; conserva un `falloId` estable.

La etapa 5 se documenta abajo. Los criterios integrales de interfaz y arranque completo siguen abiertos.

### Resultado de etapa 5 — 23 de octubre de 2026

Implementados notificaciones, reprogramación y analítica como procesos y grupos separados. El runtime compartido mantiene transacciones de efecto/marcador/outbox, validación por consumidor, DLQ, secuencia de estados y offsets manuales. Cada proceso puede detenerse y reiniciarse por separado.

- Reprogramación cubre ausencia (siguiente día hábil/franja opuesta), dirección incorrecta, rechazo y tercer fallo. Zona inaccesible adopta retención para intervención como propuesta. Calendario UTC-3 y feriados configurables.
- Notificaciones publica avisos simulados por fallos y apertura de alertas; los cierres no generan avisos.
- Analítica mantiene primer intento, motivos y SLA mediante deltas incrementales y estado por envío. Consume promesas para cruzar entregas y corregir versiones tardías.
- Perfil Compose `business`, scripts individuales y verificador `npm run verify:business` con base y grupos aislados.
- Verificados `npm test`, las seis pruebas de negocio ejecutadas directamente, integración real completa y construcción de las tres imágenes Docker.

La verificación reproduce diez fallos, once avisos, siete primeros intentos y una entrega fuera de plazo; incluye aislamiento, lag, recuperación de outbox, duplicados, secuencias antiguas y DLQ. Las fórmulas y limitaciones se detallan en el README. No se genera automáticamente un nuevo estado de reparto tras una decisión de reprogramación. El monitor de SLA y el frontend siguen pendientes; las alertas de esta prueba son eventos inyectados con contrato válido.

La etapa 6 agrega el monitor con estado persistido y reloj simulado.

### Resultado de etapa 6 — 23 de octubre de 2026

Monitor de SLA implementado como proceso y grupo independientes (`monitor-sla`), con estado propio en memoria restaurado desde MongoDB. Cruza estados/promesas, evalúa por evento y por minuto simulado y publica aperturas/cierres por outbox transaccional.

- Cuatro reglas y prioridades del PDF; no repetición por envío/regla durante toda su vida y cierre al entregar.
- Reloj compartido, snapshot persistido y avance durante la caída; aceleración consistente con el simulador.
- Transacción de estado, marcador, outbox y reloj; offsets posteriores a persistir/publicar; DLQ y continuidad.
- Perfil Compose `sla`, comando `npm run start:sla`, pruebas unitarias de límites y verificador real `npm run verify:sla`.
- Verificados `npm test`, las cinco pruebas SLA ejecutadas directamente, integración completa, configuración Compose, construcción Docker y carga de módulos dentro de la imagen.

El verificador comprueba seis aperturas, seis cierres y seis avisos de alerta, con temporizador sin nuevos eventos, cruce de topics, duplicados, estados antiguos, reinicio y recuperación de una salida pendiente tras caída controlada. La interfaz y los criterios de latencia evento-pantalla siguen pendientes.

La etapa 7 agrega Next.js y SSE; luego siguen cuarto indicador histórico y aceptación integral.

### Resultado de etapa 7 — 23 de octubre de 2026

Next.js implementado en `apps/web`: torre de control, detalle, tracking público, analítica y panel de demo. Las páginas se renderizan en el servidor leyendo MongoDB; un componente cliente aplica cambios mediante SSE y refresco de los datos.

- Change streams sobre vistas de envíos, alertas y notificaciones; ámbito por envío para tracking. Mensajes SSE sin documentos internos, heartbeat y cierre al cancelar.
- Tracking con DTO público explícito y pruebas de ausencia de datos internos en HTML/JSON.
- Filtros, paginación, prioridad de alertas, estados vacíos y errores recuperables. Analítica con sumas completas independientes del límite del detalle visual.
- Panel con ocho escenarios mediante un control HTTP acotado al simulador; Next.js no produce ni consume Kafka y no escribe proyecciones.
- Perfil `web` para interfaz y control, Docker standalone y scripts locales. `npm run verify:web` usa puertos, base y grupo aislados.
- Pruebas unitarias, compilación de producción, integración HTTP/SSE y navegador Chromium. Se verifica actualización sin recargar, móvil y escenario feliz desde la interfaz.

La prueba inicial midió 212 ms de Kafka a SSE/API y 532 ms hasta el cambio visible en el tracking. Son mediciones locales de una corrida controlada, no una prueba formal de carga. La corrida final midió 211 ms a SSE/API y 640 ms al cambio visible; tres envíos del flujo a 20/min se vieron en 775, 500 y 593 ms. También pasó la desconexión/reconexión del navegador. Las imágenes Docker se construyeron y la imagen standalone respondió con HTML/CSS y conexión MongoDB.

La etapa 8 agrega el cuarto indicador y la aceptación integral.

### Resultado de etapa 8 — 23 de octubre de 2026

Implementado el grupo adicional `analitica-hubs` sobre el log de estados, con transacción de contribución/marcador, outbox DLQ, límites finitos por offset y generaciones aisladas. El modo vivo activa su vista al completar el histórico y continúa consumiendo. La interfaz muestra el promedio por hub y aplica los filtros existentes.

La aceptación individual con Chromium verificó siete escenarios de semilla fija: cinco primeros intentos con tres éxitos, cuatro fallos y tres entregas en plazo. El grupo nuevo agregó seis muestras históricas de un minuto y una séptima posterior, sin reiniciar los consumidores existentes. Dos generaciones finitas coincidieron con la vista activa. Borrar y reconstruir diez `envios` de la base temporal dejó la misma vista de negocio.

Se incorporaron `verify:all`, `verify:compose`, [DEMO.md](DEMO.md) y [EVIDENCIAS.md](EVIDENCIAS.md). La prueba de Compose usa otro proyecto y volúmenes propios, y verifica el arranque completo con un comando. Los nombres de los volúmenes ahora dependen de `COMPOSE_PROJECT_NAME`; con el valor predeterminado conservan exactamente los nombres anteriores.

El conteo inicial de cinco consumidores se mantiene para la plataforma base; el grupo de hubs es la incorporación explícita del cuarto indicador durante la demo. No se agregan topics ni integraciones externas.

## Criterios de aceptación del enunciado

### Ciclo de vida

- [x] Un envío simulado aparece en la torre de control y en el tracking público con su estado correcto.
- [x] Un evento duplicado no agrega una entrada repetida al historial.
- [x] Un evento fuera de orden no hace retroceder el estado del envío.
- [x] Un evento malformado termina en la DLQ con su motivo.

### Entrega fallida

- [x] Un `ENTREGA_FALLIDA` genera exactamente una notificación y una reprogramación.
- [x] Cada motivo aplica su regla de reprogramación.
- [x] El tercer fallo marca el envío para devolución.
- [x] Detener un servicio no afecta a los otros dos.

### Riesgo de SLA

- [x] Cada una de las cuatro reglas genera su alerta con la prioridad definida.
- [x] Una misma regla no alerta dos veces sobre el mismo envío.
- [x] La alerta se cierra cuando el envío se entrega.
- [x] Tras reiniciar el monitor, no se pierden ni se repiten alertas.

### Analítica y reprocesamiento

- [x] Los tres indicadores coinciden con el recuento manual de la corrida `tp7-aceptacion-v1`.
- [x] El cuarto indicador se completa con datos históricos sin reiniciar los demás servicios.
- [x] Borrar envíos y reprocesar deja la misma vista de negocio, verificado en la base temporal de aceptación.

### Entorno

- [x] Arranque completo con un solo comando y volúmenes nuevos, verificado en un proyecto Docker aislado del mismo host. No se probó una segunda máquina.
- [x] La latencia entre evento y pantalla es menor a 2 segundos con el flujo continuo, verificada en una muestra local de tres envíos a 20/min (máximo 775 ms).

## Ambigüedades y propuestas

Las propuestas siguientes hacen implementable el caso, pero no sustituyen una aclaración de la cátedra. Las decisiones quedan reflejadas también en [CONTRATOS.md](CONTRATOS.md).

1. **Cuatro consumidores frente a cinco.** El alcance menciona cuatro; la arquitectura y los grupos enumeran cinco servicios consumidores. Planificar cinco: `proyector`, `notificaciones`, `reprogramacion`, `monitor-sla` y `analitica`. Es lo único que cubre las responsabilidades y grupos nombrados; pedir confirmación sobre si el número «cuatro» excluía al proyector.
2. **`ZONA_INACCESIBLE`.** No tiene regla de reprogramación. Propuesta: dejar el envío retenido en estado `PENDIENTE_INTERVENCION`, registrar notificación y no crear un reintento automático. Evita asumir que la zona será accesible al siguiente intento; se podrá liberar manualmente desde el control de demo. Confirmar la política con la cátedra.
3. **Promesas y proyector.** El proyector debe guardar la promesa junto al envío, pero el catálogo asigna `envios.promesas` solo al monitor. Agregar el proyector como consumidor independiente de ese topic, con su propio grupo, y procesar la promesa con idempotencia. No hay llamada directa entre servicios.
4. **Reloj cuando no hay eventos de estado.** `ocurridoEn` da tiempo de simulación, pero no avanza si el simulador deja de publicar. Propuesta: el simulador mantiene un reloj lógico y publica ese instante en cada evento; el monitor persiste en Mongo el último instante simulado y la hora civil en que lo observó. Durante la ejecución puede medir intervalos con reloj monotónico; tras reiniciar usa la hora civil persistida solo para estimar el tiempo transcurrido y avanzar el instante simulado por el factor de aceleración. La hora civil nunca reemplaza `ocurridoEn` como fecha de negocio. Queda el riesgo de ajuste del reloj del host durante una caída; si se exige propagar cada avance solo por Kafka, hará falta un topic/control event adicional, incompatible con los seis topics declarados.
5. **Replay frente a deduplicación.** No reutilizar los marcadores del procesamiento normal al reconstruir. Propuesta: detener solo el grupo objetivo, usar un `generationId` nuevo para sus marcadores y proyección temporal, leer desde el inicio y cambiar la vista activa al finalizar; mantener intactos los offsets y marcadores de los otros grupos. Para borrado total se reconstruyen proyecciones y marcadores por consumidor, no el log de Kafka.
6. **Interrupción entre efecto y commit del offset.** El commit manual posterior a Mongo deja una ventana de repetición. Propuesta: dentro de una transacción Mongo en replica set, guardar efecto y marcador `(consumer, generationId, eventId)`; confirmar el offset después de la transacción. Si el proceso cae después del commit Mongo, el replay encuentra el marcador y no repite el efecto. Para publicar a Kafka desde un consumidor, escribir primero un outbox en esa transacción y publicar con `eventId` estable; los consumidores destino también deduplican. No se promete exactly-once entre Kafka y Mongo.
7. **Franja y calendario hábil.** El enunciado no define límites horarios, feriados ni zona horaria. Propuesta inicial: franjas `MANIANA` y `TARDE`, día hábil de lunes a viernes y horario local del hub; dejar límites y feriados configurables y explícitos en la demo.
8. **Control de escenarios frente a interfaz de solo lectura.** El panel necesita disparar escenarios, aunque el frontend se describe como lector de MongoDB y Kafka como único contacto entre servicios. Propuesta: permitir una ruta de control acotada `POST` del panel al simulador; no se usa para comunicar servicios ni para el flujo de eventos, que sigue pasando exclusivamente por Kafka. Confirmar si esa excepción es aceptable o si se autoriza un canal de comandos.
9. **Esquema de DLQ y payload irrecuperable.** Un evento inválido puede no incluir `shipmentId` o `eventId`, pese a que el topic usa `shipmentId` como clave. Propuesta: el envoltorio DLQ genera su propio identificador, conserva el payload crudo y usa una clave sintética estable cuando falta `shipmentId` original.

## Restricciones técnicas adoptadas

- Kafka es el único canal de eventos entre servicios. Los servicios no hacen llamadas directas entre sí.
- Cinco consumidores Node.js en la plataforma base y un grupo adicional al incorporar el cuarto indicador; procesos separables, al menos una vez más idempotencia y commits manuales después de persistir.
- MongoDB es una proyección, no la fuente de verdad; replica set de un nodo para transacciones y change streams.
- Next.js consulta MongoDB y recibe actualizaciones por SSE; no consume Kafka.
- Mantener los seis topics del enunciado. Cualquier topic adicional para el reloj requiere aprobación.
- Sin NestJS ni framework de backend adicional: el enunciado solo exige Node.js. La interfaz sí usa Next.js.
