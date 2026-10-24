import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { MongoClient } from "mongodb";
import { TOPICS, validateEvent } from "../packages/contracts/src/index.js";
import { createKafkaClient, createProducer } from "../services/simulator/src/kafka.js";

const runId = randomUUID();
const databaseName = `rutasur_verify_sla_${runId.replaceAll("-", "")}`;
const labels = ["sin-ingreso", "sin-salida", "fallo", "vencido", "timer", "crash", "invalid", "promise-first", "delivered-first"];
const shipmentId = label => `SLA-${runId}-${label}`;
const ids = labels.map(shipmentId).join(",");
const children = [];
const baseTime = "2026-10-23T08:00:00.000Z";
function event(label, tipo = "CREADO", secuencia = 1, extra = {}) {
  return { eventId: randomUUID(), shipmentId: shipmentId(label), tipo, secuencia, ocurridoEn: baseTime, hubId: "HUB-SLA", zonaId: "ZONA-SLA", repartidorId: null, version: 1, franja: "MANIANA", ...extra };
}
function promise(label, due, extra = {}) {
  return event(label, "PROMESA_CREADA", 1, { versionPromesa: 1, venceEn: due, ...extra });
}
async function waitFor(check, description, timeout = 60000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (!description.startsWith("limpieza")) {
      const failed = children.find(record => record.done && record.code !== 0 && !(record.expectedCrash && record.code === 86));
      if (failed) throw new Error(`${failed.folder}: ${failed.output}`);
    }
    const result = await check();
    if (result) return result;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw new Error(`Timeout ${description}\n${children.filter(record => !record.done).map(record => `${record.folder}: ${record.output.slice(-5000)}`).join("\n")}`);
}
function launch(folder, extra = {}) {
  const child = spawn(process.execPath, [`services/${folder}/src/cli.js`, ...(folder === "projector" ? ["run"] : [])], {
    env: {
      ...process.env,
      MONGODB_URI: process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true",
      MONGODB_DATABASE: databaseName,
      KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? "localhost:19092",
      SLA_GROUP_ID: `verify-sla-monitor-${runId}`,
      PROJECTOR_GROUP_ID: `verify-sla-projector-${runId}`,
      BUSINESS_GROUP_ID: `verify-sla-notifications-${runId}`,
      SLA_VERIFY_SHIPMENT_IDS: ids,
      PROJECTOR_VERIFY_SHIPMENT_IDS: ids,
      BUSINESS_VERIFY_SHIPMENT_IDS: ids,
      SLA_FROM_BEGINNING: "false", PROJECTOR_FROM_BEGINNING: "false", BUSINESS_FROM_BEGINNING: "false",
      SIMULATOR_ACCELERATION: "120", SLA_EVALUATION_INTERVAL_SIMULATED_MS: "60000", BUSINESS_UTC_OFFSET_MINUTES: "-180",
      ...extra,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const record = { child, folder, output: "", joined: false, done: false, expectedCrash: Boolean(extra.SLA_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID) };
  for (const stream of [child.stdout, child.stderr]) {
    stream.setEncoding("utf8");
    stream.on("data", chunk => {
      record.output = (record.output + chunk).slice(-24000);
      if (["Monitor SLA unido.", "Grupo de proyección unido.", "Servicio unido."].some(message => record.output.includes(`"message":"${message}"`))) record.joined = true;
    });
  }
  record.exited = new Promise(resolve => {
    child.once("exit", (code, signal) => { record.done = true; record.code = code; record.signal = signal; resolve(); });
    child.once("error", error => { record.done = true; record.output += error.message; resolve(); });
  });
  children.push(record);
  return record;
}
async function ready(record) { await waitFor(() => record.joined, `inicio ${record.folder}`); }
async function stop(record) {
  if (!record.done) record.child.kill("SIGTERM");
  await waitFor(() => record.done, `cierre ${record.folder}`, 25000);
  assert.equal(record.code, 0, record.output);
}

async function main() {
  const mongo = new MongoClient(process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true", { serverSelectionTimeoutMS: 5000 });
  const producer = createProducer(createKafkaClient({ clientId: `verify-sla-${runId}`, brokers: (process.env.KAFKA_BROKERS ?? "localhost:19092").split(",") }));
  let database;
  async function send(topic, value) {
    assert.equal(validateEvent(topic, value).valid, true, JSON.stringify(value));
    await producer.send({ topic, acks: -1, messages: [{ key: value.shipmentId, value: JSON.stringify(value) }] });
  }
  const progress = message => console.log(JSON.stringify({ message }));
  try {
    await mongo.connect(); database = mongo.db(databaseName); await producer.connect();
    let monitor = launch("sla-monitor");
    const projector = launch("projector");
    const notifier = launch("notifications");
    await Promise.all([monitor, projector, notifier].map(ready));
    const cases = [
      ["sin-ingreso", "CREADO", "2026-10-23T17:00:00Z", "SIN_INGRESO", "MEDIA"],
      ["sin-salida", "INGRESADO_HUB", "2026-10-23T11:00:00Z", "SIN_SALIDA", "ALTA"],
      ["fallo", "ENTREGA_FALLIDA", "2026-10-23T16:00:00Z", "FALLO_DIA_LIMITE", "ALTA"],
      ["vencido", "EN_REPARTO", "2026-10-23T07:59:00Z", "VENCIDO", "CRITICA"],
    ];
    for (const [label, tipo, due, rule, priority] of cases) {
      const state = event(label, tipo, 1, { intento: 1, ...(tipo === "ENTREGA_FALLIDA" ? { motivo: "AUSENTE" } : {}) });
      await send(TOPICS.states, state);
      await waitFor(async () => database.collection("estado_monitor_sla").findOne({ _id: state.shipmentId }), `estado ${label}`);
      const promised = promise(label, due);
      await send(TOPICS.promises, promised);
      await send(TOPICS.promises, promised);
      await send(TOPICS.states, state);
      await send(TOPICS.states, { ...state, eventId: randomUUID(), secuencia: 0 });
      const alert = await waitFor(async () => database.collection("alertas").findOne({ shipmentId: state.shipmentId, regla: rule }), `regla ${rule}`);
      assert.equal(alert.prioridad, priority);
      assert.equal(alert.estado, "ABIERTA");
      assert.equal(await database.collection("alertas").countDocuments({ shipmentId: state.shipmentId }), 1);
    }
    progress("Cuatro reglas y prioridades verificadas.");
    await send(TOPICS.promises, promise("promise-first", "2026-10-24T08:00:00Z"));
    await waitFor(async () => (await database.collection("estado_monitor_sla").findOne({ _id: shipmentId("promise-first") }))?.promise, "promesa antes del estado");
    await send(TOPICS.states, event("promise-first", "INGRESADO_HUB"));
    await send(TOPICS.states, event("delivered-first", "ENTREGADO"));
    await waitFor(async () => (await database.collection("estado_monitor_sla").findOne({ _id: shipmentId("delivered-first") }))?.delivered, "entrega antes de promesa");
    await send(TOPICS.promises, promise("delivered-first", "2026-10-23T07:00:00Z"));
    await send(TOPICS.states, event("timer", "INGRESADO_HUB"));
    await waitFor(async () => database.collection("estado_monitor_sla").findOne({ _id: shipmentId("timer") }), "estado previo a prueba periódica");
    const currentClock = await database.collection("reloj_monitor_sla").findOne({ _id: "clock" });
    const timedPromise = promise("timer", new Date(Date.parse(currentClock.simulatedAt) + 4 * 3600000 + 15 * 60000).toISOString(), { ocurridoEn: currentClock.simulatedAt });
    await send(TOPICS.promises, timedPromise);
    await waitFor(async () => (await database.collection("estado_monitor_sla").findOne({ _id: shipmentId("timer") }))?.promise, "promesa periódica aplicada");
    assert.equal(await database.collection("alertas").countDocuments({ shipmentId: shipmentId("timer") }), 0);
    await waitFor(async () => database.collection("alertas").findOne({ shipmentId: shipmentId("timer"), regla: "SIN_SALIDA" }), "alerta por paso del tiempo sin más eventos");
    progress("Evaluación sin eventos verificada.");
    await send(TOPICS.states, event("crash", "EN_REPARTO", 1, { intento: 1 }));
    await waitFor(async () => database.collection("estado_monitor_sla").findOne({ _id: shipmentId("crash") }), "estado antes de caída");
    await stop(monitor);
    const savedClock = await database.collection("reloj_monitor_sla").findOne({ _id: "clock" });
    const crashPromise = promise("crash", "2026-10-23T07:00:00Z");
    monitor = launch("sla-monitor", { SLA_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID: crashPromise.eventId });
    await ready(monitor);
    await send(TOPICS.promises, crashPromise);
    await waitFor(() => monitor.done, "caída luego de persistir alerta y antes de publicar");
    assert.equal(monitor.code, 86, monitor.output);
    assert.equal(await database.collection("outbox_monitor_sla").countDocuments({ "event.shipmentId": shipmentId("crash"), published: false }), 1);
    monitor = launch("sla-monitor", { SLA_FROM_BEGINNING: "true" });
    await ready(monitor);
    await waitFor(async () => database.collection("alertas").findOne({ shipmentId: shipmentId("crash"), regla: "VENCIDO" }), "alerta pendiente publicada al reiniciar");
    await send(TOPICS.promises, crashPromise);
    const restoredClock = await database.collection("reloj_monitor_sla").findOne({ _id: "clock" });
    assert.ok(Date.parse(restoredClock.simulatedAt) >= Date.parse(savedClock.simulatedAt));
    for (const [label] of cases) assert.equal(await database.collection("alertas").countDocuments({ shipmentId: shipmentId(label) }), 1);
    assert.equal(await database.collection("alertas").countDocuments({ shipmentId: shipmentId("delivered-first") }), 0);
    progress("Reinicio, reloj y outbox recuperados sin duplicar reglas.");
    const invalidId = shipmentId("invalid");
    await producer.send({ topic: TOPICS.states, messages: [{ key: invalidId, value: "{" }] });
    const valid = event("invalid");
    await send(TOPICS.states, valid);
    await waitFor(async () => database.collection("outbox_monitor_sla").findOne({ topic: TOPICS.deadLetter, published: true }), "DLQ del monitor");
    await waitFor(async () => database.collection("eventos_procesados").findOne({ consumer: "monitor-sla", eventId: valid.eventId }), "consumo continúa después de inválido");
    for (const label of [...cases.map(([name]) => name), "timer", "crash"]) {
      await send(TOPICS.states, event(label, "ENTREGADO", 10));
      await waitFor(async () => await database.collection("alertas").countDocuments({ shipmentId: shipmentId(label), estado: "ABIERTA" }) === 0, `cierre ${label}`);
      await send(TOPICS.states, event(label, "CREADO", 2));
    }
    await waitFor(async () => await database.collection("notificaciones").countDocuments({ categoria: "ALERTA_SLA" }) === 6, "seis avisos de apertura");
    // An obsolete promise must not replace the accepted version or reopen alerts.
    const revision = promise("sin-ingreso", "2026-10-23T07:00:00Z", { tipo: "PROMESA_ACTUALIZADA", versionPromesa: 2 });
    await send(TOPICS.promises, revision);
    await send(TOPICS.promises, promise("sin-ingreso", "2026-10-24T08:00:00Z"));
    await waitFor(async () => database.collection("eventos_procesados").findOne({ consumer: "monitor-sla", eventId: revision.eventId }), "revisión tras entrega");
    for (const record of [monitor, notifier, projector]) await stop(record);
    assert.equal(await database.collection("alertas").countDocuments(), 6);
    assert.equal(await database.collection("alertas").countDocuments({ estado: "CERRADA" }), 6);
    assert.equal(await database.collection("outbox_monitor_sla").countDocuments({ topic: TOPICS.slaAlerts, "event.tipo": "ALERTA_SLA_ABIERTA" }), 6);
    assert.equal(await database.collection("outbox_monitor_sla").countDocuments({ topic: TOPICS.slaAlerts, "event.tipo": "ALERTA_SLA_CERRADA" }), 6);
    const alerts = await database.collection("alertas").find().toArray();
    for (const alert of alerts) assert.ok(alert.abiertaEn && alert.cerradaEn);
    progress("Etapa 6 verificada: cuatro reglas, seis aperturas y seis cierres, evaluación periódica, reinicio, outbox, DLQ y seis avisos de alerta.");
  } finally {
    for (const record of children) if (!record.done) record.child.kill("SIGTERM");
    for (const record of children) {
      if (!record.done) {
        try { await waitFor(() => record.done, "limpieza de proceso", 20000); }
        catch { record.child.kill("SIGKILL"); await record.exited; }
      }
    }
    if (database) await database.dropDatabase();
    await producer.disconnect().catch(() => {});
    await mongo.close();
  }
}
main().catch(error => { console.error(error.stack ?? error); process.exitCode = 1; });
