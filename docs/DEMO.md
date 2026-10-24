# RutaSur: guion de defensa y demo

Este guion sigue los ocho pasos de la sección «Guion de la demo» del [PDF de TP7](../TP7_RutaSur_especificacion_del_caso_practico.pdf). Duración orientativa: 15 minutos, sin contar la preparación. El objetivo no es recorrer código, sino mostrar cómo los eventos permiten actuar durante la operación.

## La historia que se va a demostrar

RutaSur tiene tracking poco confiable, entregas fallidas e incumplimientos que se descubren tarde. La solución convierte cada cambio de estado en un evento y permite que varios servicios reaccionen sin depender entre sí.

| Pasos | Caso del PDF | Resultado que se debe mostrar |
|---|---|---|
| 1–2 | CU1 · Ciclo de vida | El mismo envío aparece en la operación y en el tracking público |
| 3 | CU2 · Entrega fallida | Un fallo genera aviso, decisión de reprogramación y agregado analítico |
| 4 | CU3 · Riesgo de SLA | Una alerta permite intervenir antes del vencimiento |
| 5–6 | Recuperación y validación | La caída de un consumidor y un evento inválido no detienen al resto |
| 7–8 | CU4 · Analítica y reprocesamiento | Los indicadores explican fallos y un grupo nuevo incorpora el histórico |

**Idea central para explicar:** Kafka conserva eventos; MongoDB guarda vistas derivadas; Next.js las consulta. Los cinco consumidores base tienen posiciones de lectura independientes. En el paso 8 se agrega un sexto consumidor, sin reiniciar los anteriores.

## Preparar el entorno

Desde la carpeta `rutasur-tp7`, con Docker Engine y Compose v2 o posterior, ejecutar en una terminal:

```bash
docker compose --profile projector --profile business --profile sla --profile web --profile simulator up -d --build
```

Abrir **http://localhost:3000** y dejar otra terminal disponible para los comandos de caída y recuperación. Esperar que haya envíos en la torre de control. La primera construcción puede requerir descargar imágenes y dependencias; hacerla antes de la defensa. Los datos existentes se conservan.

Para una demo separada de los datos existentes, antes del comando anterior se puede usar otro proyecto y puertos libres:

```bash
export COMPOSE_PROJECT_NAME=rutasur-defensa
export KAFKA_EXTERNAL_PORT=19093
export MONGODB_EXTERNAL_PORT=27029
export WEB_PORT=3002
export KAFKA_BROKERS=localhost:19093
export MONGODB_URI='mongodb://localhost:27029/rutasur?replicaSet=rs0&directConnection=true'
```

En ese caso abrir http://localhost:3002. Los volúmenes se llaman `rutasur-defensa-kafka-data` y `rutasur-defensa-mongo-data`; no se reutilizan los del proyecto predeterminado. Mantener esas variables en las terminales de la demo.

No activar aún el perfil `hubs`: el cuarto indicador se incorpora en el paso 8, con el sistema en marcha. Si el indicador ya se ejecutó en esta base, usar un proyecto nuevo para repetir ese primer arranque histórico.

## 1. Arranque y operación en vivo — 2 minutos

**Mostrar:** los contadores y los nuevos envíos del flujo continuo. Abrir una fila y su tracking público.

**Resultado esperado:** ambas vistas reflejan el mismo envío, pero el tracking público no muestra información operativa interna.

**Explicar:** el simulador publica en Kafka, el proyector actualiza MongoDB y Next.js consulta esa vista. Los change streams generan avisos SSE para actualizar el navegador. El PDF pide menos de dos segundos entre evento y pantalla; observarlo en la demo no sustituye una medición registrada.

Para que los escenarios siguientes sean fáciles de identificar, detener solo el flujo continuo:

```bash
docker compose --profile simulator stop simulator
```

Los cinco consumidores y la interfaz siguen activos.

## 2. Envío feliz — 1 minuto

**Acción:** en `/demo`, dejar la semilla vacía y pulsar **Envío feliz**. Abrir el enlace devuelto y después **Ver tracking público**.

**Resultado esperado:** estado `ENTREGADO` e historial `CREADO → INGRESADO_HUB → CLASIFICADO → EN_REPARTO → ENTREGADO`.

La publicación de los estados es rápida; el historial permite revisar el recorrido aunque el último estado ya se haya proyectado al abrir la página. Mostrar que el tracking no expone hub, repartidor, alertas ni avisos internos.

**Explicar (CU1):** el productor informa qué ocurrió; el proyector construye la vista de lectura. La clave `shipmentId` agrupa los eventos de un envío por partición dentro de cada topic; la secuencia evita aplicar estados antiguos.

## 3. Destinatario ausente — 2 minutos

**Acción:** disparar **Ausente**. En el detalle y en `/analitica`, comprobar:

- Último estado `ENTREGA_FALLIDA`.
- Aviso simulado registrado.
- Decisión `PROGRAMAR`, siguiente día hábil y franja opuesta.
- Incremento del fallo y de su motivo en `/analitica` al actualizar.

**Explicar (CU2):** un solo `ENTREGA_FALLIDA` llega a tres grupos independientes. Notificaciones registra el aviso simulado; reprogramación decide; analítica suma el fallo. Ninguno espera a otro.

La decisión `PROGRAMAR` no genera automáticamente un nuevo reparto: el simulador representa los sistemas de origen. Para los otros motivos, dirección incorrecta se retiene y rechazo se devuelve; el tercer fallo se devuelve cualquiera sea el motivo. `ZONA_INACCESIBLE` se retiene para intervención: es una decisión de implementación ante una regla no especificada en el PDF.

## 4. Demora y alerta preventiva — 1 minuto

**Acción:** disparar **Demorado** y abrir su detalle.

**Resultado esperado:** el paquete queda en el hub y su promesa está a tres horas simuladas. Debe aparecer `SIN_SALIDA`, prioridad alta, también en la torre de control: todavía se puede intervenir antes de vencer.

**Explicar (CU3):** no hace falta recibir un nuevo estado para detectar riesgo; el monitor evalúa también la ausencia de avances.

El monitor evalúa por evento y por minuto simulado. A aceleración 120, un minuto simulado equivale a 500 ms reales. Si transcurre el vencimiento sin entrega aparece también la regla crítica `VENCIDO`; cada regla se registra una vez y se cierra al entregar.

## 5. Caída de notificaciones — 2 minutos

**Acción:** detener únicamente notificaciones:

```bash
docker compose --profile business stop notifications
```

Disparar otro **Ausente**. Mostrar que el tracking, la reprogramación, la analítica y el monitor siguen funcionando, pero todavía no hay aviso por ese fallo.

Consultar el atraso del grupo:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notificaciones
```

Reiniciar el mismo servicio y grupo:

```bash
docker compose --profile business start notifications
```

**Resultado esperado:** recupera lo pendiente y deja un solo aviso lógico por fallo, sin reiniciar los demás consumidores.

**Explicar:** cada grupo conserva su offset. La transacción MongoDB guarda el efecto, el marcador y la outbox de publicaciones pendientes; el offset se confirma después de persistir/publicar. Si hay una republicación física, conserva el ID y el proyector la deduplica. Esto es entrega al menos una vez con efectos idempotentes, no publicación física exactamente una vez.

## 6. Evento inválido — 1 minuto

Antes de dispararlo, abrir una terminal que espere mensajes nuevos de la DLQ:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic envios.estados.dlq --timeout-ms 15000 --max-messages 5 --property print.key=true
```

**Acción:** pulsar **Malformado** mientras la terminal anterior sigue esperando. Disparar después otro envío feliz.

**Resultado esperado:** la DLQ contiene el motivo de validación y el payload original; el nuevo feliz se entrega normalmente. Cada consumidor detecta y registra el inválido de forma independiente.

**Explicar:** el evento inválido se aparta para revisión, sin bloquear los mensajes posteriores.

Opcional: **Duplicado** conserva una sola entrada lógica en el historial; **Fuera de orden** no hace retroceder el estado; **Tercer fallo** termina en una decisión de devolución.

## 7. Analítica — 1 minuto

**Acción:** abrir `/analitica` y actualizar. Comparar zona/franja, motivos y cumplimiento de SLA. Aplicar un filtro de zona o centro y un rango de fechas.

**Resultado esperado (CU4):** identificar dónde falla el primer intento, por qué falla y qué centro incumple la promesa. Si los datos muestran más ausencias en una zona/franja, plantear la mejora del PDF: ofrecer una franja vespertina allí. Es una hipótesis de negocio, no una conclusión garantizada por cualquier corrida aleatoria.

Explicar los denominadores: primeros intentos con resultado y entregas con promesa conocida. Los porcentajes se calculan sumando numeradores y denominadores; no se promedian porcentajes por fila. La tabla se limita visualmente a 200 filas, pero los indicadores usan todo el período.

## 8. Nuevo indicador leyendo el histórico — 2 minutos

**Acción:** con los servicios anteriores aún activos, incorporar el cuarto indicador:

```bash
docker compose --profile hubs up -d --no-deps --build hub-analytics
```

`--no-deps` conserva la infraestructura ya activa y evita volver a ejecutar sus inicializadores.

Es un grupo nuevo, `analitica-hubs`, sobre el mismo topic `envios.estados`, desde el inicio retenido. No se modifican los offsets de los otros consumidores.

Ver su progreso:

```bash
docker compose --profile hubs logs --tail=30 hub-analytics
```

Esperar `Histórico de hubs completo.`. Actualizar `/analitica`: **Tiempo en el centro** debe incluir envíos anteriores a la creación de ese grupo. El promedio usa el primer `INGRESADO_HUB` y el primer `EN_REPARTO` posterior de cada envío.

**Resultado esperado:** hay muestras de envíos anteriores al arranque de `analitica-hubs`. Disparar otro feliz y actualizar: la cantidad de muestras aumenta sin reiniciar los demás servicios.

**Explicar (CU4):** no se está repitiendo la simulación. El grupo nuevo lee los eventos retenidos con sus propios offsets y continúa en vivo al alcanzar el histórico. Solo puede reconstruir lo que Kafka todavía conserva. Los pasos 5 y 8 muestran aislamiento y lectura independiente, no una cadena de llamadas REST.

## Evidencia automatizada para acompañar la defensa

```bash
npm ci
npm --prefix apps/web ci
npm run verify:all
npm run verify:compose
```

Estos comandos son verificaciones complementarias, no pasos de la presentación de 15 minutos. `verify:all` usa la infraestructura local y bases/grupos temporales. Incluye la comparación con semilla fija, dos replays de hubs y la reconstrucción de `envios` después de borrarla **solo en la base temporal del test**. `verify:compose` crea un proyecto Docker con puertos y volúmenes propios, levanta el sistema con un comando y elimina únicamente sus recursos al terminar.

El navegador es opcional en los verificadores; la configuración Chromium usada durante el desarrollo está en el [README](../README.md). Al habilitarla, se prueba también el cambio visible, la reconexión y el indicador histórico en pantalla. Los resultados ya registrados y sus límites se consultan en [EVIDENCIAS.md](EVIDENCIAS.md); ejecutar el guion no implica haber repetido toda esa evidencia.

## Cierre de la defensa

Cerrar con la cadena de valor del PDF: **saber dónde está el envío → reaccionar al fallo → anticipar el incumplimiento → aprender del histórico**. Kafka es el log de eventos; MongoDB es el modelo de lectura reconstruible, no la fuente de verdad.

Esta demo muestra el recorrido; no cubre por sí sola todas las prioridades SLA, motivos de fallo, reconstrucción de `envios` ni la comparación manual con semilla fija. Esas comprobaciones corresponden a los verificadores y a la evidencia complementaria. No hay correo/SMS real, GPS, autenticación ni optimización de rutas, conforme al alcance.

Para detener conservando datos:

```bash
docker compose --profile projector --profile business --profile sla --profile web --profile simulator --profile hubs stop
```
