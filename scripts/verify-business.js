import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { MongoClient } from "mongodb";
import { createKafkaClient, createProducer } from "../services/simulator/src/kafka.js";
import { TOPICS, validateEvent } from "../packages/contracts/src/index.js";

const runId = randomUUID();
const databaseName = `rutasur_verify_business_${runId.replaceAll("-", "")}`;
const children = [];
const groups = new Map();
const failures = [];
for (const motivo of ["AUSENTE", "DIRECCION_INCORRECTA", "RECHAZADO", "ZONA_INACCESIBLE"]) {
  for (const intento of [1, 3]) failures.push(event(`${motivo}-${intento}`, "ENTREGA_FALLIDA", 5, { motivo, intento }));
}
const happy = event("happy", "ENTREGADO", 5);
const isolated = event("isolated", "ENTREGA_FALLIDA", 5, { motivo: "AUSENTE", intento: 1 });
const crash = event("crash", "ENTREGA_FALLIDA", 5, { motivo: "AUSENTE", intento: 1 });
const invalidId = `BUS-${runId}-invalid`;
const shipmentIds = [...failures, happy, isolated, crash].map(value => value.shipmentId).concat(invalidId);

function event(label, tipo, secuencia, extra = {}) {
  return {
    eventId: randomUUID(), shipmentId: `BUS-${runId}-${label}`, tipo, secuencia,
    ocurridoEn: "2026-10-23T21:00:00.000Z", hubId: "HUB-VERIFY", zonaId: "ZONA-VERIFY",
    repartidorId: "REP-VERIFY", franja: "TARDE", version: 1, ...extra,
  };
}

async function waitFor(check, description, timeout = 60000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (!description.startsWith("limpieza")) {
      const failed = children.find(record => record.done && record.code !== 0 && !(record.expectedCrash && record.code === 86));
      if (failed) throw new Error(`${failed.folder} terminó inesperadamente: ${failed.output}`);
    }
    const result = await check();
    if (result) return result;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw new Error(`Timeout: ${description}\n${children.filter(record => !record.done).map(record => `${record.folder}: ${record.output.slice(-5000)}`).join("\n")}`);
}

function launch(folder, extra = {}) {
  const groupId = groups.get(folder) ?? `verify-business-${folder}-${runId}`;
  groups.set(folder, groupId);
  const child = spawn(process.execPath, [`services/${folder}/src/cli.js`, ...(folder === "projector" ? ["run"] : [])], {
    env: {
      ...process.env,
      MONGODB_DATABASE: databaseName,
      MONGODB_URI: process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true",
      KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? "localhost:19092",
      BUSINESS_GROUP_ID: groupId,
      PROJECTOR_GROUP_ID: groupId,
      BUSINESS_FROM_BEGINNING: "false",
      PROJECTOR_FROM_BEGINNING: "false",
      BUSINESS_VERIFY_SHIPMENT_IDS: shipmentIds.join(","),
      PROJECTOR_VERIFY_SHIPMENT_IDS: shipmentIds.join(","),
      BUSINESS_UTC_OFFSET_MINUTES: "-180",
      BUSINESS_HOLIDAYS: "",
      ...extra,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const record = { child, folder, joined: false, output: "", done: false, expectedCrash: Boolean(extra.BUSINESS_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID) };
  for (const stream of [child.stdout, child.stderr]) {
    stream.setEncoding("utf8");
    stream.on("data", chunk => {
      record.output = (record.output + chunk).slice(-24000);
      if (record.output.includes('"message":"Servicio unido."') || record.output.includes('"message":"Grupo de proyección unido."')) record.joined = true;
    });
  }
  record.exited = new Promise(resolve => {
    child.once("exit", (code, signal) => {
      record.done = true; record.code = code; record.signal = signal;
      if (code !== 0 && !(record.expectedCrash && code === 86)) console.error(`${folder} terminó con ${code}: ${record.output}`);
      resolve(record);
    });
    child.once("error", error => { record.error = error; record.done = true; resolve(record); });
  });
  children.push(record);
  return record;
}

async function ready(record) {
  await waitFor(() => {
    if (record.done) throw new Error(`${record.folder} terminó: ${record.output}`);
    return record.joined;
  }, `inicio ${record.folder}`, 30000);
}

async function stop(record) {
  if (!record.done) record.child.kill("SIGTERM");
  await waitFor(() => record.done, `cierre ${record.folder}`, 25000);
  assert.equal(record.code, 0, record.output);
}

async function main() {
  const kafka = createKafkaClient({ clientId: `verify-business-${runId}`, brokers: (process.env.KAFKA_BROKERS ?? "localhost:19092").split(",") });
  const producer = createProducer(kafka);
  const admin = kafka.admin();
  const mongo = new MongoClient(process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true", { serverSelectionTimeoutMS: 5000 });
  let database;
  async function send(topic, value) {
    assert.equal(validateEvent(topic, value).valid, true, JSON.stringify(value));
    const [metadata] = await producer.send({ topic, acks: -1, messages: [{ key: value.shipmentId, value: JSON.stringify(value) }] });
    return metadata;
  }
  try {
    await mongo.connect();
    database = mongo.db(databaseName);
    await producer.connect();
    await admin.connect();
    let notifier = launch("notifications");
    const rescheduler = launch("rescheduling");
    const analytics = launch("analytics");
    const projector = launch("projector");
    await Promise.all([notifier, rescheduler, analytics, projector].map(ready));
    for (const value of failures) await send(TOPICS.states, value);
    await send(TOPICS.states, failures[0]);
    await send(TOPICS.states, { ...failures[0], eventId: randomUUID(), secuencia: 4 });
    await send(TOPICS.states, { ...happy, eventId: randomUUID(), tipo: "EN_REPARTO", secuencia: 4, intento: 1 });
    await send(TOPICS.states, happy);
    await waitFor(async () => await database.collection("envios").countDocuments({ "reprogramaciones.0": { $exists: true } }) === 8, "ocho decisiones proyectadas");
    for (const value of failures) {
      const shipment = await database.collection("envios").findOne({ shipmentId: value.shipmentId });
      assert.equal(shipment.reprogramaciones.length, 1);
      const result = shipment.reprogramacionActual;
      const expected = value.intento === 3 || value.motivo === "RECHAZADO" ? "DEVOLVER" : value.motivo === "AUSENTE" ? "PROGRAMAR" : "RETENER";
      assert.equal(result.accion, expected);
      if (expected === "PROGRAMAR") {
        assert.equal(result.programadaPara, "2026-10-26T12:00:00.000Z");
        assert.equal(result.franja, "MANIANA");
      }
    }
    await waitFor(async () => await database.collection("notificaciones").countDocuments() === 8, "un aviso por fallo, sin duplicados");
    const promise = { ...happy, eventId: randomUUID(), tipo: "PROMESA_CREADA", versionPromesa: 2, venceEn: "2026-10-23T22:00:00.000Z" };
    await send(TOPICS.promises, promise);
    const totals = async () => {
      const rows = await database.collection("metricas_diarias").find().toArray();
      return rows.reduce((sum, row) => {
        for (const field of ["fallos", "primerosIntentos", "entregasPrimerIntento", "entregasConPromesa", "entregasEnPlazo", "entregasFueraPlazo"]) sum[field] = (sum[field] ?? 0) + (row[field] ?? 0);
        return sum;
      }, {});
    };
    await waitFor(async () => (await totals()).entregasEnPlazo === 1, "promesa tardía cruza con entrega");
    assert.deepEqual(await totals(), { fallos: 8, primerosIntentos: 5, entregasPrimerIntento: 1, entregasConPromesa: 1, entregasEnPlazo: 1, entregasFueraPlazo: 0 });
    await send(TOPICS.promises, { ...promise, eventId: randomUUID(), tipo: "PROMESA_ACTUALIZADA", versionPromesa: 3, venceEn: "2026-10-23T20:00:00.000Z" });
    await waitFor(async () => (await totals()).entregasFueraPlazo === 1 && (await totals()).entregasEnPlazo === 0, "corrección incremental de SLA");
    await send(TOPICS.promises, { ...promise, eventId: randomUUID(), versionPromesa: 1 });
    const alert = { ...happy, eventId: randomUUID(), tipo: "ALERTA_SLA_ABIERTA", alertaId: `alert-${runId}`, regla: "SIN_SALIDA", prioridad: "ALTA", causa: "Prueba de aviso", origen: "monitor-sla" };
    await send(TOPICS.slaAlerts, alert);
    await send(TOPICS.slaAlerts, { ...alert, eventId: randomUUID(), tipo: "ALERTA_SLA_CERRADA" });
    await waitFor(async () => await database.collection("notificaciones").countDocuments({ categoria: "ALERTA_SLA" }) === 1, "aviso de apertura sin aviso de cierre");

    await stop(notifier);
    const isolatedMetadata = await send(TOPICS.states, isolated);
    await waitFor(async () => Boolean((await database.collection("envios").findOne({ shipmentId: isolated.shipmentId }))?.reprogramacionActual) && (await totals()).fallos === 9, "reprogramación y analítica continúan sin notificaciones");
    assert.equal(await database.collection("notificaciones").countDocuments({ shipmentId: isolated.shipmentId }), 0);
    const offsets = await admin.fetchOffsets({ groupId: groups.get("notifications") });
    const committed = offsets.find(entry => entry.topic === TOPICS.states).partitions.find(entry => entry.partition === isolatedMetadata.partition).offset;
    assert.ok(BigInt(committed) <= BigInt(isolatedMetadata.baseOffset ?? isolatedMetadata.offset), "El grupo detenido debe tener lag");
    notifier = launch("notifications");
    await ready(notifier);
    await waitFor(async () => await database.collection("notificaciones").countDocuments({ shipmentId: isolated.shipmentId }) === 1, "recuperación de avisos pendientes");
    await stop(notifier);
    notifier = launch("notifications", { BUSINESS_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID: crash.eventId });
    await ready(notifier);
    await send(TOPICS.states, crash);
    await waitFor(() => notifier.done, "caída luego de transacción y antes de publicar outbox");
    assert.equal(notifier.code, 86, notifier.output);
    assert.equal(await database.collection("outbox_notificaciones").countDocuments({ "event.eventoOrigenId": crash.eventId, published: false }), 1);
    notifier = launch("notifications", { BUSINESS_FROM_BEGINNING: "true" });
    await ready(notifier);
    await waitFor(async () => await database.collection("notificaciones").countDocuments({ shipmentId: crash.shipmentId }) === 1, "outbox pendiente publicada al reiniciar");

    await producer.send({ topic: TOPICS.states, messages: [{ key: invalidId, value: "{" }] });
    const validAfterInvalid = event("invalid", "CREADO", 1);
    await send(TOPICS.states, validAfterInvalid);
    for (const service of ["notificaciones", "reprogramacion", "analitica"]) {
      await waitFor(async () => Boolean(await database.collection(`outbox_${service}`).findOne({ topic: TOPICS.deadLetter, published: true })), `DLQ ${service}`);
      await waitFor(async () => Boolean(await database.collection("eventos_procesados").findOne({ consumer: service, eventId: validAfterInvalid.eventId })), `continúa después de inválido ${service}`);
    }
    await waitFor(async () => (await totals()).fallos === 10 && await database.collection("notificaciones").countDocuments() === 11, "totales finales");
    for (const record of [notifier, rescheduler, analytics, projector]) await stop(record);
    assert.deepEqual(await totals(), { fallos: 10, primerosIntentos: 7, entregasPrimerIntento: 1, entregasConPromesa: 1, entregasEnPlazo: 0, entregasFueraPlazo: 1 });
    const rows = await database.collection("metricas_diarias").find().toArray();
    const motives = rows.reduce((sum, row) => {
      for (const [reason, count] of Object.entries(row.motivos ?? {})) sum[reason] = (sum[reason] ?? 0) + count;
      return sum;
    }, {});
    assert.deepEqual(motives, { AUSENTE: 4, DIRECCION_INCORRECTA: 2, RECHAZADO: 2, ZONA_INACCESIBLE: 2 });
    assert.equal(await database.collection("notificaciones").countDocuments({ shipmentId: crash.shipmentId }), 1);
    assert.equal(await database.collection("envios").countDocuments({ "reprogramaciones.0": { $exists: true } }), 10);
    assert.equal(await database.collection("envios").countDocuments({ "reprogramaciones.1": { $exists: true } }), 0);
    console.log(JSON.stringify({ message: "Etapa 5 verificada.", failures: 10, notifications: 11, firstAttempts: 7, firstAttemptDeliveries: 1, lateDeliveries: 1, independentGroups: true, lagRecovered: true, outboxCrashRecovered: true, invalidMessagesSkipped: true }));
  } finally {
    for (const record of children) if (!record.done) record.child.kill("SIGTERM");
    for (const record of children) {
      if (!record.done) {
        try { await waitFor(() => record.done, "limpieza de proceso", 20000); }
        catch { record.child.kill("SIGKILL"); await record.exited; }
      }
    }
    // Only this run's dedicated database is removed; retained Kafka events remain.
    if (database) await database.dropDatabase();
    await admin.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    await mongo.close();
  }
}
main().catch(error => { console.error(error.stack ?? error); process.exitCode = 1; });
