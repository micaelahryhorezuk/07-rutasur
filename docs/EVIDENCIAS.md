# RutaSur: evidencias de verificación

Fecha de revisión: 23 de octubre de 2026, hora de Argentina. Las pruebas usan servicios locales y datos de prueba. No equivalen a una certificación externa ni a una prueba formal de carga.

## Comandos de cierre

```bash
npm ci
npm --prefix apps/web ci
npm run verify:all
npm run verify:compose
```

Ejecutar las dos verificaciones integrales **por separado**. La suite usa el Kafka/MongoDB habitual; Compose crea otro broker y otra base, por lo que correr ambos a la vez aumenta mucho el uso de memoria. En este entorno, la ejecución simultánea saturó memoria/swap y produjo fallos de coordinación; se repitió de forma secuencial.

El runner de Compose limita únicamente los recursos del proyecto temporal (heap Kafka de 256 MB, caché WiredTiger de 0,25 GB y límites de heap Node). No cambia la configuración de los servicios habituales. Kafka y MongoDB quedan dentro de su red; Docker asigna un puerto local libre a la interfaz. Su segundo paso usa `--no-deps`, porque la infraestructura ya fue validada al arrancar. La limpieza incluye todos los perfiles del proyecto temporal.

## Cobertura

| Verificación | Qué comprueba |
|---|---|
| `npm test` | Simulador, reloj, contratos, reglas de reprogramación, contribuciones analíticas, privacidad y filtros, reglas SLA, pares ingreso-salida y límites de replay |
| `npm run web:build` | Compilación de producción de todas las pantallas y route handlers de Next.js |
| `npm run verify:projector` | Duplicados, secuencias antiguas, promesas en ambos órdenes, DLQ, caída después de transacción, offset fallido y reconstrucciones iguales |
| `npm run verify:business` | Cuatro motivos y tercer fallo, avisos, incrementos analíticos, versiones de promesa, aislamiento, lag, outbox y DLQ |
| `npm run verify:sla` | Cuatro reglas/prioridades, seis aperturas/cierres, tiempo sin eventos, reinicio del reloj, salida pendiente y avisos |
| `npm run verify:web` | SSR, HTML/JSON público, filtros y prioridades, agregados completos, promedio de hubs ponderado, SSE y panel → simulador → proyector |
| `npm run verify:acceptance` | Corrida con semilla fija, fallos y SLA con consumidores reales, lag, incorporación tardía del grupo histórico, dos replays y borrado/reconstrucción de `envios` |
| `npm run verify:compose` | Arranque con un comando, puertos/volúmenes nuevos, flujo y reacciones reales, nuevo grupo sin reiniciar los cinco consumidores, limpieza propia |

La prueba de hubs de la interfaz incluye un caso donde promediar promedios daría un valor incorrecto: un hub con una muestra de 1 minuto y otro con tres muestras de 3 minutos. El total correcto es 2,5 minutos sobre cuatro muestras.

## Recuento independiente con semilla fija

Configuración de `verify:acceptance`: semilla `tp7-aceptacion-v1`, inicio `2026-10-23T08:00:00.000Z`, aceleración 120. Escenarios: feliz, ausente, demorado, tercer fallo, duplicado, fuera de orden y malformado.

| Concepto | Recuento manual esperado |
|---|---:|
| Envíos distintos | 7 |
| Primeros intentos con resultado válido | 5 |
| Primer intento exitoso | 3; tasa 60% |
| Fallos válidos | 4; todos por ausencia |
| Entregas con promesa / en plazo | 3 / 3; tasa 100% |
| Parejas iniciales de hub | 6 |
| Suma de permanencias | 360.000 ms |
| Promedio histórico | 60.000 ms = 1 minuto |

El demorado no tiene resultado final y el malformado no tiene un fallo válido. El malformado sí aporta una pareja válida de ingreso/salida. El duplicado no suma otro resultado ni otra pareja. Los reintentos del tercer fallo suman fallos, pero no otra permanencia inicial.

Después de activar el grupo nuevo se agrega un feliz: el indicador pasa a siete muestras y conserva el promedio de un minuto. Se comparan dos generaciones finitas con esa vista. El consumidor original del indicador y los otros grupos siguen funcionando.

## Reconstrucción de vistas

La aceptación detiene únicamente su proyector de prueba, toma la vista de negocio, borra la colección `envios` y los marcadores de ese consumidor **dentro de su base temporal exclusiva**, y reprocesa con otro grupo hasta offsets capturados. El resultado de los diez envíos coincide con el original al excluir `_id` y la hora técnica de creación de la proyección. Los demás consumidores permanecen activos.

Los replays de hubs escriben colecciones distintas por generación; el modo finito no cambia la vista activa. Solo el modo vivo activa una nueva generación al completar el histórico y luego continúa con los eventos siguientes.

## Interfaz y latencia

Con Chromium se comprueban el cambio visible sin recargar, la reconexión después de pasar a modo sin conexión, la navegación móvil sin desborde horizontal, el botón de envío feliz y la presencia del cuarto indicador.

La corrida de etapa 7 midió 211 ms de Kafka a SSE/API y 640 ms al cambio visible. Tres envíos a ritmo de 20/min se vieron en 775, 500 y 593 ms. En otra corrida con la comprobación Docker ejecutándose en paralelo, los tiempos fueron 1.127, 995 y 707 ms. Todos están por debajo de 2 segundos; son muestras locales pequeñas. El verificador imprime los tiempos de cada ejecución y si utilizó navegador.

Sin `WEB_VERIFY_BROWSER_MODULE`, las integraciones HTTP/SSE siguen funcionando, pero no certifican un cambio visual en Chromium. La configuración opcional del navegador se encuentra en el README.

Capturas de QA: `/tmp/rutasur-control-desktop.png`, `/tmp/rutasur-control-mobile.png` y `/tmp/rutasur-analitica-final.png`. Contienen datos de bases temporales, no datos insertados en la operación habitual.

## Estado de cierre

- Aceptación integral individual con navegador: aprobada; siete referencias, seis muestras históricas más una nueva, dos replays iguales y diez envíos reconstruidos.
- Suite completa secuencial: aprobada (`test`, `web:build`, `verify:projector`, `verify:business`, `verify:sla`, `verify:web`, `verify:acceptance`).
- Compose con volúmenes nuevos: aprobado; cinco envíos registrados, cinco muestras de hubs y los cinco consumidores existentes conservaron sus contenedores y tiempos de inicio. Proyecto de prueba: `rutasur-verify-188d7a38`. Es un arranque sin datos previos en este host, no una prueba en otra máquina.

Las bases, procesos y contenedores propios del test se eliminan al terminar. Se conservan los eventos publicados en el Kafka habitual, conforme a las verificaciones anteriores. Los volúmenes habituales no se borran.

El guion humano está en [DEMO.md](DEMO.md). Las decisiones adoptadas ante ambigüedades del enunciado permanecen en [PLAN.md](PLAN.md) y [CONTRATOS.md](CONTRATOS.md).
