import { MongoClient } from "mongodb";
import { createKafkaClient, createProducer } from "../simulator/src/kafka.js";
import { TOPICS, validateEvent } from "../../packages/contracts/src/index.js";
import { calendarFromEnvironment, metricContributions, notification, reschedule, stableId, updateAnalytics } from "./domain.js";

const SERVICES = {
  notificaciones: [TOPICS.states, TOPICS.slaAlerts],
  reprogramacion: [TOPICS.states],
  analitica: [TOPICS.states, TOPICS.promises],
};

export class BusinessStore {
  constructor(database, service, calendar) {
    this.service = service;
    this.calendar = calendar;
    this.processed = database.collection("eventos_procesados");
    this.states = database.collection(`estado_${service}`);
    this.outbox = database.collection(`outbox_${service}`);
    this.metrics = database.collection("metricas_diarias");
  }

  async createIndexes() {
    await this.processed.createIndex({ consumer: 1, generation: 1, eventId: 1 }, { unique: true, name: "consumer_generation_eventId_unique" });
    await this.processed.createIndex({ processedAt: 1 }, { expireAfterSeconds: 604800, name: "processedAt_ttl_7d" });
    await this.outbox.createIndex({ published: 1 });
    if (this.service === "analitica") {
      await this.metrics.createIndex({ fecha: 1, zonaId: 1, hubId: 1, franja: 1 }, { unique: true, name: "dimensiones_unique" });
    }
  }

  async enqueue(topic, event, session) {
    const validation = validateEvent(topic, event);
    if (!validation.valid) throw new Error(`Salida inválida: ${validation.errors.join("; ")}`);
    const id = event.eventId ?? event.falloId;
    await this.outbox.updateOne({ _id: id }, { $setOnInsert: { topic, event, key: event.shipmentId, published: false } }, { upsert: true, session });
  }

  async apply(client, topic, event, invalid = false) {
    const session = client.startSession();
    try {
      return await session.withTransaction(async () => {
        const identity = { consumer: this.service, generation: "main", eventId: event.eventId ?? event.falloId };
        if (await this.processed.findOne(identity, { session })) return "duplicate";
        let outcome = "applied";
        if (invalid) {
          await this.enqueue(TOPICS.deadLetter, event, session);
          outcome = "dead_lettered";
        } else {
          const state = await this.states.findOne({ _id: event.shipmentId }, { session }) ?? { _id: event.shipmentId };
          if (topic === TOPICS.states && Number.isInteger(state.lastSequence) && event.secuencia <= state.lastSequence) {
            outcome = "out_of_order";
          } else {
            if (this.service === "analitica") {
              const before = metricContributions(state, this.calendar.offsetMinutes);
              updateAnalytics(state, topic, event);
              const after = metricContributions(state, this.calendar.offsetMinutes);
              for (const key of new Set([...before.keys(), ...after.keys()])) {
                const old = before.get(key);
                const next = after.get(key);
                const increments = {};
                for (const field of new Set([...Object.keys(old?.values ?? {}), ...Object.keys(next?.values ?? {})])) {
                  const delta = (next?.values[field] ?? 0) - (old?.values[field] ?? 0);
                  if (delta) increments[field] = delta;
                }
                if (Object.keys(increments).length) {
                  await this.metrics.updateOne({ _id: key }, { $setOnInsert: (next ?? old).dimensions, $inc: increments }, { upsert: true, session });
                }
              }
            } else {
              const output = this.service === "notificaciones" ? notification(event) : reschedule(event, this.calendar);
              if (output) await this.enqueue(this.service === "notificaciones" ? TOPICS.notifications : TOPICS.reschedules, output, session);
            }
            if (topic === TOPICS.states) state.lastSequence = event.secuencia;
            await this.states.replaceOne({ _id: state._id }, state, { upsert: true, session });
          }
        }
        await this.processed.insertOne({ ...identity, topic, processedAt: new Date() }, { session });
        return outcome;
      }, { readConcern: { level: "snapshot" }, writeConcern: { w: "majority" } });
    } finally {
      await session.endSession();
    }
  }

  async flush(producer) {
    for await (const record of this.outbox.find({ published: false }).sort({ _id: 1 })) {
      console.log(JSON.stringify({ service: this.service, message: "Publicando outbox.", eventId: record._id, topic: record.topic }));
      await producer.send({ topic: record.topic, acks: -1, messages: [{ key: record.key, value: JSON.stringify(record.event) }] });
      await this.outbox.updateOne({ _id: record._id }, { $set: { published: true, publishedAt: new Date() } });
    }
  }
}

export function decodeBusinessMessage(service, topic, partition, message) {
  const raw = message.value?.toString("utf8") ?? "";
  let event;
  let error;
  try { event = JSON.parse(raw); } catch (cause) { error = cause.message; }
  const validation = error ? { valid: false, errors: [error] } : validateEvent(topic, event);
  if (validation.valid) return { event, invalid: false };
  const id = stableId(service, topic, partition, message.offset);
  const timestamp = Number(message.timestamp);
  return {
    invalid: true,
    event: {
      falloId: `dlq-${id}`,
      shipmentId: typeof event?.shipmentId === "string" && event.shipmentId ? event.shipmentId : message.key?.toString("utf8") || `sin-shipmentId:${id}`,
      eventIdOriginal: typeof event?.eventId === "string" ? event.eventId : null,
      topicOriginal: topic,
      particionOriginal: partition,
      offsetOriginal: message.offset,
      grupoConsumidor: service,
      codigoError: error ? "JSON_INVALIDO" : "SCHEMA_INVALIDO",
      motivo: validation.errors.join("; "),
      falloEn: new Date(Number.isFinite(timestamp) ? timestamp : Date.now()).toISOString(),
      payloadOriginal: raw,
      version: 1,
    },
  };
}

export async function runBusinessService(service) {
  if (!SERVICES[service]) throw new Error(`Servicio desconocido: ${service}`);
  const calendar = calendarFromEnvironment();
  const mongo = new MongoClient(process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true", { serverSelectionTimeoutMS: 5000 });
  const kafka = createKafkaClient({ clientId: `rutasur-${service}`, brokers: (process.env.KAFKA_BROKERS ?? "localhost:19092").split(",").map(value => value.trim()) });
  const groupId = process.env.BUSINESS_GROUP_ID ?? service;
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false, retry: { restartOnFailure: async () => false } });
  const producer = createProducer(kafka);
  let resolveStop;
  let fatal;
  let stopping = false;
  const stopped = new Promise(resolve => { resolveStop = resolve; });
  const requestStop = () => { stopping = true; resolveStop(); };
  const fail = error => { fatal ??= error; requestStop(); };
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, requestStop);
  const filter = process.env.BUSINESS_VERIFY_SHIPMENT_IDS ? new Set(process.env.BUSINESS_VERIFY_SHIPMENT_IDS.split(",")) : null;
  try {
    await mongo.connect();
    const store = new BusinessStore(mongo.db(process.env.MONGODB_DATABASE ?? "rutasur"), service, calendar);
    await store.createIndexes();
    await producer.connect();
    await store.flush(producer);
    await consumer.connect();
    await consumer.subscribe({ topics: SERVICES[service], fromBeginning: process.env.BUSINESS_FROM_BEGINNING !== "false" });
    consumer.on(consumer.events.GROUP_JOIN, () => console.log(JSON.stringify({ message: "Servicio unido.", service, groupId })));
    consumer.on(consumer.events.CRASH, ({ payload }) => fail(payload.error));
    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, heartbeat, resolveOffset, isRunning, isStale }) => {
        const { topic, partition } = batch;
        let lastFiltered;
        let currentOffset;
        try {
          for (const message of batch.messages) {
            currentOffset = message.offset;
            if (stopping || !isRunning() || isStale()) return;
            if (filter && !filter.has(message.key?.toString("utf8"))) {
              resolveOffset(message.offset);
              lastFiltered = message;
              continue;
            }
            const { event, invalid } = decodeBusinessMessage(service, topic, partition, message);
            const outcome = await store.apply(mongo, topic, event, invalid);
            if (process.env.BUSINESS_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID && process.env.BUSINESS_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID === event.eventId) process.exit(86);
            await store.flush(producer);
            await heartbeat();
            console.log(JSON.stringify({ service, eventId: event.eventId ?? event.falloId, shipmentId: event.shipmentId, topic, partition, offset: message.offset, outcome }));
            await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
            resolveOffset(message.offset);
            lastFiltered = null;
          }
          if (lastFiltered) {
            await consumer.commitOffsets([{ topic, partition, offset: (BigInt(lastFiltered.offset) + 1n).toString() }]);
            await heartbeat();
          }
        } catch (error) {
          console.error(JSON.stringify({ service, topic, partition, offset: currentOffset, message: "Procesamiento detenido sin confirmar el mensaje fallido.", error: error.message }));
          fail(error);
          throw error;
        }
      },
    });
    if (stopping) resolveStop();
    await stopped;
    await consumer.stop();
    if (fatal) throw fatal;
  } finally {
    for (const signal of ["SIGINT", "SIGTERM"]) process.removeListener(signal, requestStop);
    await consumer.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    await mongo.close();
  }
}
