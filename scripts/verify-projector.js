import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { MongoClient } from "mongodb";
import { Kafka } from "kafkajs";
import { loadConfig } from "../services/simulator/src/config.js";
import { createProducer, publishEvents } from "../services/simulator/src/kafka.js";
import { generateScenario } from "../services/simulator/src/generator.js";
import { collectionNamesForGeneration } from "../services/projector/src/store.js";
import { TOPICS } from "../packages/contracts/src/index.js";

const SOURCE_TOPICS = [
  TOPICS.states,
  TOPICS.promises,
  TOPICS.reschedules,
  TOPICS.slaAlerts,
  TOPICS.notifications,
];
const VERIFY_TIMEOUT_MS = 240000;

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function waitFor(check, description, timeoutMs = VERIFY_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    let timer;
    let result;
    try {
      result = await Promise.race([
        Promise.resolve().then(check),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error(`Timeout esperando: ${description}`)), Math.max(1, deadline - Date.now()));
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
    if (result) return result;
    await sleep(100);
  }
  throw new Error(`Timeout esperando: ${description}`);
}

function makeBaseEvent(shipmentId, tipo, sequence, extra = {}) {
  return {
    eventId: `verify-${randomUUID()}`,
    shipmentId,
    tipo,
    ocurridoEn: new Date().toISOString(),
    secuencia: sequence,
    hubId: "HUB-VERIFY",
    zonaId: "ZONA-VERIFY",
    repartidorId: null,
    version: 1,
    ...extra,
  };
}

async function sendRaw(producer, topic, key, value) {
  const [metadata] = await producer.send({
    topic,
    acks: -1,
    messages: [{ key, value }],
  });
  return metadata;
}

async function sendEvent(producer, topic, event) {
  return sendRaw(producer, topic, event.shipmentId, JSON.stringify(event));
}

function startProjector({ generation, groupId, cutoffs, startOffsets, extraEnvironment = {} }) {
  const args = [
    "services/projector/src/cli.js",
    "run",
    `--generation=${generation}`,
    `--group-id=${groupId}`,
  ];
  if (cutoffs) args.push(`--cutoffs=${JSON.stringify(cutoffs)}`);
  if (startOffsets) args.push(`--start-offsets=${JSON.stringify(startOffsets)}`);
  const child = spawn(process.execPath, args, {
    cwd: process.cwd(),
    env: {
      ...process.env,
      KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? "localhost:19092",
      MONGODB_URI: process.env.MONGODB_URI ??
        "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true",
      ...extraEnvironment,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stderrTail = "";
  let stdoutTail = "";
  let readyResolve;
  let readyReject;
  const ready = new Promise((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  let exitResolve;
  const exited = new Promise((resolve) => { exitResolve = resolve; });
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let stdoutBuffer = "";
  child.stdout.on("data", (chunk) => {
    stdoutTail = `${stdoutTail}${chunk}`.slice(-12000);
    stdoutBuffer += chunk;
    const lines = stdoutBuffer.split("\n");
    stdoutBuffer = lines.pop() ?? "";
    for (const line of lines) {
      if (line.includes('"message":"Grupo de proyección unido."')) readyResolve();
    }
  });
  child.stderr.on("data", (chunk) => {
    stderrTail = `${stderrTail}${chunk}`.slice(-6000);
  });
  child.once("error", readyReject);
  child.once("exit", (code, signal) => {
    const result = { code, signal, stdout: stdoutTail, stderr: stderrTail };
    if (code !== 0 && code !== 86 && !extraEnvironment.PROJECTOR_TEST_MONGO_FAIL_EVENT_ID) {
      console.error(JSON.stringify({ level: "error", message: "Proceso de prueba del proyector terminado.", groupId, ...result }));
    }
    readyReject(new Error(`El proyector terminó antes de unirse: ${JSON.stringify(result)}`));
    exitResolve(result);
  });

  return { child, ready, exited };
}

async function waitForExit(projector, description, timeoutMs = VERIFY_TIMEOUT_MS) {
  let timer;
  try {
    return await Promise.race([
      projector.exited,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Timeout esperando: ${description}`)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function stopProjector(projector) {
  if (projector.child.exitCode !== null || projector.child.signalCode !== null) {
    return projector.exited;
  }
  projector.child.kill("SIGTERM");
  const result = await waitForExit(projector, "cierre ordenado del proyector", 20000);
  assert.equal(result.code, 0, `Cierre del proyector: ${result.stderr}`);
  return result;
}

async function startDeadLetterObserver(kafkaConfig, shipmentId) {
  const kafka = new Kafka(kafkaConfig);
  const consumer = kafka.consumer({
    groupId: `verify-dlq-${randomUUID()}`,
    allowAutoTopicCreation: false,
  });
  let resolveFound;
  let rejectFound;
  const found = new Promise((resolve, reject) => {
    resolveFound = resolve;
    rejectFound = reject;
  });
  let resolveJoined;
  const joined = new Promise((resolve) => { resolveJoined = resolve; });
  consumer.on(consumer.events.GROUP_JOIN, resolveJoined);
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.deadLetter, fromBeginning: false });
  const runner = consumer.run({
    autoCommit: false,
    eachMessage: async ({ message }) => {
      try {
        const event = JSON.parse(message.value.toString("utf8"));
        if (event.shipmentId === shipmentId) resolveFound(event);
      } catch (error) {
        rejectFound(error);
      }
    },
  }).catch(rejectFound);
  await waitFor(() => joined, "unión del grupo verificador DLQ", 30000);
  return {
    found,
    async stop() {
      await consumer.stop().catch(() => {});
      await consumer.disconnect().catch(() => {});
      await runner.catch(() => {});
    },
  };
}

async function captureCutoffs(admin) {
  const cutoffs = {};
  for (const topic of SOURCE_TOPICS) {
    const offsets = await admin.fetchTopicOffsets(topic);
    cutoffs[topic] = offsets
      .filter((entry) => BigInt(entry.high ?? entry.offset) > BigInt(entry.low ?? "0"))
      .map((entry) => ({
        partition: entry.partition,
        nextOffset: entry.high ?? entry.offset,
      }));
  }
  return cutoffs;
}

async function captureIndexCutoffs(admin, topic, partition) {
  const offsets = await admin.fetchTopicOffsets(topic);
  const selected = offsets.find((entry) => entry.partition === partition);
  return selected?.high ?? selected?.offset;
}

function businessView(shipment) {
  if (!shipment) return null;
  return {
    shipmentId: shipment.shipmentId,
    estado: shipment.estado,
    historial: shipment.historial,
    intentos: shipment.intentos,
    historialIntentos: shipment.historialIntentos ?? [],
    ultimaSecuenciaAplicada: shipment.ultimaSecuenciaAplicada,
    promesa: shipment.promesa,
    historialPromesas: shipment.historialPromesas,
    reprogramacionActual: shipment.reprogramacionActual,
    reprogramaciones: shipment.reprogramaciones,
  };
}

async function verifyStoredIndex(database) {
  const indexes = await database.collection("eventos_procesados").indexes();
  const ttlIndex = indexes.find((index) => index.name === "processedAt_ttl_7d");
  assert.equal(ttlIndex?.expireAfterSeconds, 604800);
  assert.deepEqual(ttlIndex?.key, { processedAt: 1 });
}

async function main() {
  const runId = randomUUID();
  const kafkaConfig = {
    clientId: `rutasur-projector-verify-${runId}`,
    brokers: String(process.env.KAFKA_BROKERS ?? "localhost:19092")
      .split(",")
      .map((broker) => broker.trim()),
  };
  const mongoUri = process.env.MONGODB_URI ??
    "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true";
  const databaseName = process.env.MONGODB_DATABASE ?? "rutasur";
  const config = loadConfig(process.env, { seed: `projector-verify-${runId}` });
  const kafka = new Kafka(kafkaConfig);
  const producer = createProducer(kafka);
  const admin = kafka.admin();
  const mongo = new MongoClient(mongoUri, { serverSelectionTimeoutMS: 5000 });
  const database = (await mongo.connect()).db(databaseName);
  await producer.connect();
  await admin.connect();

  const generation = `verify-live-${runId}`;
  const groupId = `verify-proyector-${runId}`;
  const generationsToClean = new Set([generation]);
  const live = startProjector({
    generation,
    groupId,
    extraEnvironment: { PROJECTOR_FROM_BEGINNING: "false" },
  });
  let deadLetterObserver;
  let crashChild;
  let mongoFailureChild;

  try {
    const names = collectionNamesForGeneration(generation);
    const shipments = database.collection(names.shipments);
    const alerts = database.collection(names.alerts);
    const notifications = database.collection(names.notifications);

    await waitFor(() => live.ready.then(() => true), "unión del proyector de verificación", 30000);
    await verifyStoredIndex(database);

    const happyEvents = generateScenario("happy", config);
    const happyShipmentId = happyEvents[0].event.shipmentId;
    await publishEvents(producer, happyEvents, { scenario: "verify-happy", logEvents: false });
    const happy = await waitFor(
      async () => {
        const shipment = await shipments.findOne({ shipmentId: happyShipmentId });
        return shipment?.estado === "ENTREGADO" && shipment.promesa?.venceEn ? shipment : null;
      },
      "envío feliz ENTREGADO",
    );
    assert.deepEqual(
      happy.historial.map((event) => event.tipo),
      ["CREADO", "INGRESADO_HUB", "CLASIFICADO", "EN_REPARTO", "ENTREGADO"],
    );
    assert.ok(happy.promesa?.venceEn);

    const duplicateEvents = generateScenario("duplicate", config);
    const duplicateShipmentId = duplicateEvents[0].event.shipmentId;
    await publishEvents(producer, duplicateEvents, { scenario: "verify-duplicate", logEvents: false });
    const duplicateShipment = await waitFor(
      async () => {
        const shipment = await shipments.findOne({ shipmentId: duplicateShipmentId });
        return shipment?.estado === "ENTREGADO" ? shipment : null;
      },
      "envío duplicado ENTREGADO",
    );
    const duplicateState = duplicateEvents.find((entry) => entry.duplicateOf).event;
    assert.equal(
      duplicateShipment.historial.filter((event) => event.eventId === duplicateState.eventId).length,
      1,
    );

    const outOfOrderEvents = generateScenario("out-of-order", config);
    const outOfOrderShipmentId = outOfOrderEvents[0].event.shipmentId;
    await publishEvents(producer, outOfOrderEvents, { scenario: "verify-out-of-order", logEvents: false });
    const outOfOrderShipment = await waitFor(
      async () => {
        const shipment = await shipments.findOne({ shipmentId: outOfOrderShipmentId });
        return shipment?.estado === "ENTREGADO" ? shipment : null;
      },
      "envío fuera de orden ENTREGADO",
    );
    assert.equal(outOfOrderShipment.ultimaSecuenciaAplicada, 6);
    assert.equal(outOfOrderShipment.historial.some((event) => event.tipo === "CLASIFICADO"), false);

    const promiseFirstEvents = generateScenario("happy", loadConfig(process.env, {
      seed: `${config.seed}-promise-first`,
    }));
    const promiseFirstId = promiseFirstEvents[0].event.shipmentId;
    await publishEvents(producer, [promiseFirstEvents[0]], { scenario: "verify-promise-first", logEvents: false });
    await waitFor(
      async () => (await shipments.findOne({ shipmentId: promiseFirstId }))?.promesa,
      "promesa antes del primer estado",
    );
    await publishEvents(producer, promiseFirstEvents.slice(1), { scenario: "verify-promise-first", logEvents: false });
    await waitFor(
      async () => (await shipments.findOne({ shipmentId: promiseFirstId }))?.estado === "ENTREGADO",
      "estado posterior a promesa",
    );

    const stateFirstEvents = generateScenario("happy", loadConfig(process.env, {
      seed: `${config.seed}-state-first`,
    }));
    const stateFirstId = stateFirstEvents[0].event.shipmentId;
    await publishEvents(producer, [stateFirstEvents[1]], { scenario: "verify-state-first", logEvents: false });
    await waitFor(
      async () => (await shipments.findOne({ shipmentId: stateFirstId }))?.estado === "CREADO",
      "primer estado antes de promesa",
    );
    await publishEvents(producer, [stateFirstEvents[0]], { scenario: "verify-state-first", logEvents: false });
    await waitFor(
      async () => Boolean((await shipments.findOne({ shipmentId: stateFirstId }))?.promesa),
      "promesa posterior al primer estado",
    );
    await publishEvents(producer, stateFirstEvents.slice(2), { scenario: "verify-state-first", logEvents: false });

    const derivedShipmentId = `RS-PROJ-${runId}`;
    const causedBy = { eventId: `cause-${runId}`, secuencia: 1 };
    const reschedule = makeBaseEvent(derivedShipmentId, "REPROGRAMACION_DEFINIDA", 1, {
      accion: "PROGRAMAR",
      motivo: "AUSENTE",
      intento: 2,
      programadaPara: "2026-10-19T08:00:00.000Z",
      franja: "MANIANA",
      causadoPor: causedBy,
      origen: "reprogramacion",
    });
    await sendEvent(producer, TOPICS.reschedules, reschedule);

    const alertId = `alert-${runId}`;
    const opened = makeBaseEvent(derivedShipmentId, "ALERTA_SLA_ABIERTA", 1, {
      alertaId: alertId,
      regla: "SIN_SALIDA",
      prioridad: "ALTA",
      causa: "Evento de verificación del proyector",
      causadoPor: causedBy,
      origen: "monitor-sla",
    });
    const closed = makeBaseEvent(derivedShipmentId, "ALERTA_SLA_CERRADA", 1, {
      alertaId: alertId,
      regla: "SIN_SALIDA",
      prioridad: "ALTA",
      causa: "Evento de verificación del proyector",
      causadoPor: causedBy,
      origen: "monitor-sla",
    });
    await sendEvent(producer, TOPICS.slaAlerts, opened);
    await sendEvent(producer, TOPICS.slaAlerts, closed);

    const notification = makeBaseEvent(derivedShipmentId, "NOTIFICACION_REGISTRADA", 1, {
      notificacionId: `notification-${runId}`,
      eventoOrigenId: causedBy.eventId,
      categoria: "FALLO",
      canal: "SIMULADO",
      estado: "REGISTRADA",
      opcionesReprogramacion: ["MANIANA", "TARDE"],
      causadoPor: causedBy,
      origen: "notificaciones",
    });
    await sendEvent(producer, TOPICS.notifications, notification);

    const invalidShipmentId = `RS-BAD-${runId}`;
    const invalidRaw = `{"eventId":"bad-${runId}","shipmentId":"${invalidShipmentId}"`;
    deadLetterObserver = await startDeadLetterObserver(kafkaConfig, invalidShipmentId);
    await sendRaw(producer, TOPICS.states, invalidShipmentId, invalidRaw);
    const subsequentValid = makeBaseEvent(invalidShipmentId, "CREADO", 1, { franja: "MANIANA" });
    await sendEvent(producer, TOPICS.states, subsequentValid);
    const deadLetter = await waitFor(() => deadLetterObserver.found, "evento inválido en DLQ");
    assert.equal(deadLetter.payloadOriginal, invalidRaw);
    assert.equal(deadLetter.topicOriginal, TOPICS.states);
    assert.equal(deadLetter.grupoConsumidor, "proyector");
    await waitFor(
      async () => (await shipments.findOne({ shipmentId: invalidShipmentId }))?.estado === "CREADO",
      "evento válido posterior al inválido",
    );
    await deadLetterObserver.stop();
    deadLetterObserver = null;

    await waitFor(
      async () => Boolean((await shipments.findOne({ shipmentId: stateFirstId }))?.estado === "ENTREGADO"),
      "fin del ciclo de vida para promesa posterior",
    );
    await waitFor(
      async () => (await shipments.findOne({ shipmentId: derivedShipmentId }))?.reprogramaciones?.length === 1,
      "proyección de reprogramación",
    );
    await waitFor(
      async () => (await alerts.findOne({ alertaId: alertId }))?.estado === "CERRADA",
      "alerta abierta y cerrada",
    );
    assert.deepEqual((await alerts.findOne({ alertaId: alertId })).abiertaEn, new Date(opened.ocurridoEn));
    const notificationIdentity = `${derivedShipmentId}:${causedBy.eventId}:FALLO`;
    await waitFor(
      async () => notifications.findOne({ identidad: notificationIdentity }),
      "proyección de notificación",
    );

    const crashEvent = makeBaseEvent(`RS-CRASH-${runId}`, "CREADO", 1, { franja: "MANIANA" });
    const cutoffs = await captureCutoffs(admin);
    const startOffsets = cutoffs[TOPICS.states].map(({ partition, nextOffset }) => ({
      topic: TOPICS.states,
      partition,
      offset: nextOffset,
    }));
    const crashGeneration = `verify-crash-${runId}`;
    const crashGroup = `verify-crash-group-${runId}`;
    generationsToClean.add(crashGeneration);
    crashChild = startProjector({
      generation: crashGeneration,
      groupId: crashGroup,
      startOffsets,
      extraEnvironment: {
        PROJECTOR_FROM_BEGINNING: "false",
        PROJECTOR_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID: crashEvent.eventId,
        PROJECTOR_VERIFY_SHIPMENT_IDS: crashEvent.shipmentId,
      },
    });
    await waitFor(() => crashChild.ready.then(() => true), "unión del proyector antes del crash", 30000);
    const crashMetadata = await sendEvent(producer, TOPICS.states, crashEvent);
    const crashResult = await waitForExit(crashChild, "crash después de transacción");
    assert.equal(crashResult.code, 86, `Crash esperado: ${crashResult.stderr}`);
    const crashNames = collectionNamesForGeneration(crashGeneration);
    const crashProjection = database.collection(crashNames.shipments);
    assert.equal(
      (await crashProjection.findOne({ shipmentId: crashEvent.shipmentId }))?.historial?.length,
      1,
    );

    const wakeEvent = makeBaseEvent(crashEvent.shipmentId, "INGRESADO_HUB", 2, { franja: "MANIANA" });
    const beforeWakeCutoffs = await captureCutoffs(admin);
    const wakeStartOffsets = beforeWakeCutoffs[TOPICS.states].map(({ partition, nextOffset }) => ({
      topic: TOPICS.states,
      partition,
      offset: crashMetadata.partition === partition
        ? crashMetadata.baseOffset
        : nextOffset,
    }));
    crashChild = startProjector({
      generation: crashGeneration,
      groupId: crashGroup,
      startOffsets: wakeStartOffsets,
      extraEnvironment: {
        PROJECTOR_FROM_BEGINNING: "true",
        PROJECTOR_VERIFY_SHIPMENT_IDS: crashEvent.shipmentId,
      },
    });
    await waitFor(() => crashChild.ready.then(() => true), "unión del proyector recuperado", 30000);
    await sendEvent(producer, TOPICS.states, wakeEvent);
    await waitFor(
      async () => {
        if (crashChild.child.exitCode !== null) {
          throw new Error(`El proyector recuperado terminó: ${JSON.stringify(await crashChild.exited)}`);
        }
        return (await crashProjection.findOne({ shipmentId: crashEvent.shipmentId }))
          ?.historial?.some((event) => event.eventId === wakeEvent.eventId);
      },
      "proyector recuperado aplica el evento siguiente",
    );
    assert.equal(
      (await crashProjection.findOne({ shipmentId: crashEvent.shipmentId })).historial
        .filter((event) => event.eventId === crashEvent.eventId).length,
      1,
    );
    await stopProjector(crashChild);
    crashChild = null;

    const mongoFailureEvent = makeBaseEvent(`RS-MONGO-FAIL-${runId}`, "CREADO", 1, { franja: "MANIANA" });
    const failureStartOffsetsSnapshot = await captureCutoffs(admin);
    const failureStartOffsets = failureStartOffsetsSnapshot[TOPICS.states].map(({ partition, nextOffset }) => ({
      topic: TOPICS.states,
      partition,
      offset: nextOffset,
    }));
    const failureGeneration = `verify-mongo-fail-${runId}`;
    const failureGroup = `verify-mongo-fail-group-${runId}`;
    generationsToClean.add(failureGeneration);
    mongoFailureChild = startProjector({
      generation: failureGeneration,
      groupId: failureGroup,
      startOffsets: failureStartOffsets,
      extraEnvironment: {
        PROJECTOR_FROM_BEGINNING: "false",
        PROJECTOR_TEST_MONGO_FAIL_EVENT_ID: mongoFailureEvent.eventId,
        PROJECTOR_VERIFY_SHIPMENT_IDS: mongoFailureEvent.shipmentId,
      },
    });
    await waitFor(() => mongoFailureChild.ready.then(() => true), "unión del proyector con fallo Mongo", 30000);
    const mongoFailureMetadata = await sendEvent(producer, TOPICS.states, mongoFailureEvent);
    const mongoFailureResult = await waitForExit(mongoFailureChild, "fallo inyectado de escritura Mongo");
    assert.equal(mongoFailureResult.code, 1, mongoFailureResult.stderr);
    const failedGroupOffsets = await admin.fetchOffsets({ groupId: failureGroup });
    const failedPartitionOffset = failedGroupOffsets
      .find((entry) => entry.topic === TOPICS.states)
      ?.partitions.find((entry) => entry.partition === mongoFailureMetadata.partition)?.offset;
    assert.ok(
      failedPartitionOffset === undefined ||
      BigInt(failedPartitionOffset) <= BigInt(mongoFailureMetadata.baseOffset ?? mongoFailureMetadata.offset),
      `El grupo avanzó el offset del mensaje fallido: ${failedPartitionOffset}`,
    );

    const replayCutoffs = await captureCutoffs(admin);
    const replayGenerations = [
      `verify-replay-a-${runId}`,
      `verify-replay-b-${runId}`,
    ];
    const comparedShipmentIds = [
      happyShipmentId,
      duplicateShipmentId,
      outOfOrderShipmentId,
      promiseFirstId,
      stateFirstId,
      invalidShipmentId,
      crashEvent.shipmentId,
    ];
    const replayShipmentIds = comparedShipmentIds.join(",");
    const replayResults = new Map();
    for (const replayGeneration of replayGenerations) {
      generationsToClean.add(replayGeneration);
      const replay = startProjector({
        generation: replayGeneration,
        groupId: `verify-replay-group-${replayGeneration}`,
        cutoffs: replayCutoffs,
        extraEnvironment: { PROJECTOR_VERIFY_SHIPMENT_IDS: replayShipmentIds },
      });
      await waitFor(() => replay.ready.then(() => true), "unión del proyector de replay", 30000);
      const replayResult = await waitForExit(replay, `replay ${replayGeneration}`);
      assert.equal(replayResult.code, 0, replayResult.stderr);
      replayResults.set(replayGeneration, replayResult);
    }

    const replayNames = replayGenerations.map(collectionNamesForGeneration);
    for (const shipmentId of comparedShipmentIds) {
      const liveView = businessView(await shipments.findOne({ shipmentId }));
      for (const [replayIndex, namesForReplay] of replayNames.entries()) {
        const replayView = businessView(
          await database.collection(namesForReplay.shipments).findOne({ shipmentId }),
        );
        const replayGeneration = replayGenerations[replayIndex];
        const relevantLogs = replayResults.get(replayGeneration).stdout
          .split("\n")
          .filter((line) => line.includes(shipmentId));
        assert.deepEqual(
          replayView,
          liveView,
          `Vista de replay distinta para ${shipmentId} en ${replayGeneration}; logs=${relevantLogs.join("\n")}`,
        );
      }
    }

    console.log(JSON.stringify({
      level: "info",
      message: "Verificación del proyector completada.",
      liveGroup: groupId,
      replayGenerations,
      testedShipments: comparedShipmentIds.length,
      topics: SOURCE_TOPICS,
      dlqTopic: TOPICS.deadLetter,
      mongoWriteFailureOffsetNotCommitted: true,
      transactionCrashResumedWithoutDuplicate: true,
    }));
  } finally {
    if (deadLetterObserver) await deadLetterObserver.stop();
    if (crashChild && crashChild.child.exitCode === null) crashChild.child.kill("SIGTERM");
    if (mongoFailureChild && mongoFailureChild.child.exitCode === null) mongoFailureChild.child.kill("SIGTERM");
    if (live.child.exitCode === null && live.child.signalCode === null) {
      await stopProjector(live).catch(() => {});
    }
    for (const generation of generationsToClean) {
      const names = collectionNamesForGeneration(generation);
      await Promise.all([
        database.collection(names.shipments).drop().catch(() => {}),
        database.collection(names.alerts).drop().catch(() => {}),
        database.collection(names.notifications).drop().catch(() => {}),
      ]);
      await database.collection("eventos_procesados").deleteMany({
        consumer: "proyector",
        generation,
      });
    }
    await admin.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    await mongo.close();
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ level: "error", message: error.stack ?? error.message }));
  process.exitCode = 1;
});
