# RutaSur: guion de defensa y demo

Duración orientativa: 15 minutos. La historia recorre seguimiento, fallo, SLA y analítica; los pasos de caída y replay muestran el desacople entre grupos.

## Preparar el entorno

Desde la raíz del proyecto, con Docker y Compose disponibles:

```bash
docker compose --profile projector --profile business --profile sla --profile web --profile simulator up -d --build
```

Abrir http://localhost:3000. Esperar que haya envíos en la torre de control. La primera construcción puede requerir descargar imágenes y dependencias; hacerla antes de la defensa.

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

Mostrar los contadores y los nuevos envíos del flujo continuo. Abrir una fila y su tracking público. Explicar que Kafka conserva los eventos; MongoDB contiene vistas reconstruibles y Next.js lee esas vistas. El navegador recibe avisos SSE a partir de change streams.

Para que los escenarios siguientes sean fáciles de identificar, detener solo el flujo continuo:

```bash
docker compose --profile simulator stop simulator
```

Los cinco consumidores y la interfaz siguen activos.

## 2. Envío feliz — 1 minuto

En `/demo`, dejar la semilla vacía y pulsar **Envío feliz**. Abrir el enlace devuelto y después **Ver tracking público**. Mostrar `ENTREGADO` y el historial de movimientos.

La publicación de los estados es rápida; el historial permite revisar el recorrido aunque el último estado ya se haya proyectado al abrir la página. Mostrar que el tracking no expone hub, repartidor, alertas ni avisos internos.

Conceptos: productor, consumidor, orden por envío y separación entre escritura y lectura.

## 3. Destinatario ausente — 2 minutos

Disparar **Ausente**. En el detalle comprobar:

- Último estado `ENTREGA_FALLIDA`.
- Aviso simulado registrado.
- Decisión `PROGRAMAR`, siguiente día hábil y franja opuesta.
- Incremento del fallo y de su motivo en `/analitica` al actualizar.

La decisión no inventa un nuevo estado de reparto: el simulador representa los sistemas de origen. Explicar que el aviso, la decisión y el agregado los generan tres procesos y grupos independientes.

## 4. Demora y alerta preventiva — 1 minuto

Disparar **Demorado** y abrir su detalle. Su promesa está a tres horas simuladas y el paquete queda en el hub: debe aparecer `SIN_SALIDA`, prioridad alta. La alerta también aparece en la torre de control.

El monitor evalúa por evento y por minuto simulado. A aceleración 120, un minuto simulado equivale a 500 ms reales. Si transcurre el vencimiento sin entrega aparece también la regla crítica `VENCIDO`; cada regla se registra una vez y se cierra al entregar.

## 5. Caída de notificaciones — 2 minutos

Detener únicamente notificaciones:

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

Mostrar que recupera lo pendiente y deja un solo aviso lógico. La transacción guarda el efecto y el marcador; la outbox conserva la publicación pendiente. El offset se confirma después de persistir/publicar. Una republicación física conserva el ID para que el proyector la deduplique.

## 6. Evento inválido — 1 minuto

Antes de dispararlo, abrir una terminal que espere mensajes nuevos de la DLQ:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic envios.estados.dlq --timeout-ms 15000 --max-messages 5 --property print.key=true
```

Pulsar **Malformado**. Mostrar el motivo de validación y el payload original. Cada consumidor detecta y registra el inválido de forma independiente. Disparar después otro envío feliz para mostrar que el flujo continúa.

Opcional: **Duplicado** conserva una sola entrada lógica en el historial; **Fuera de orden** no hace retroceder el estado; **Tercer fallo** termina en una decisión de devolución.

## 7. Analítica — 1 minuto

Abrir `/analitica` y actualizar. Comparar zona/franja, motivos y cumplimiento de SLA. Aplicar un filtro de zona o centro y un rango de fechas.

Explicar los denominadores: primeros intentos con resultado y entregas con promesa conocida. Los porcentajes se calculan sumando numeradores y denominadores; no se promedian porcentajes por fila. La tabla se limita visualmente a 200 filas, pero los indicadores usan todo el período.

## 8. Nuevo indicador leyendo el histórico — 2 minutos

Con los servicios anteriores aún activos:

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

Disparar otro feliz y actualizar. La cantidad de muestras aumenta sin reiniciar los demás servicios: el nuevo grupo continúa consumiendo en vivo después del histórico.

## Evidencia automatizada para acompañar la defensa

```bash
npm ci
npm --prefix apps/web ci
npm run verify:all
npm run verify:compose
```

`verify:all` usa la infraestructura local y bases/grupos temporales. Incluye la comparación con semilla fija, dos replays de hubs y la reconstrucción de `envios` después de borrarla **solo en la base temporal del test**. `verify:compose` crea un proyecto Docker con puertos y volúmenes propios, levanta el sistema con un comando y elimina únicamente sus recursos al terminar.

El navegador es opcional en los verificadores; la configuración Chromium usada durante el desarrollo está en el README. Con ella se prueba también el cambio visible, la reconexión y el indicador histórico en pantalla.

## Cierre de la defensa

Relacionar los problemas de RutaSur con los resultados: tracking confiable, reacciones independientes a fallos, intervención antes del vencimiento y mejora basada en eventos históricos. No hay correo/SMS real, GPS, autenticación ni optimización de rutas, conforme al alcance.

Para detener conservando datos:

```bash
docker compose --profile projector --profile business --profile sla --profile web --profile simulator --profile hubs stop
```
