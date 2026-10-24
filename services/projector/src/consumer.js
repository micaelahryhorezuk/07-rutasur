import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { createKafkaClient, createProducer } from "../../simulator/src/kafka.js";
import { TOPICS, validateEvent } from "../../../packages/contracts/src/index.js";

const SOURCE_TOPICS = [
  TOPICS.states,
  TOPICS.promises,
  TOPICS.reschedules,
  TOPICS.slaAlerts,
  TOPICS.notifications,
];

const RETRY_DELAYS_MS = [100, 250, 500, 1000, 1500];

function hash(value) {
  return createHash("sha256").update(value).digest("hex");
}

function offsetAfter(offset) {
  return (BigInt(offset) + 1n).toString();
}

function messageIdentity(topic, partition, offset) {
  return `${topic}:${partition}:${offset}`;
}

function decodeMessage(message) {
  const raw = message.value?.toString("utf8") ?? "";
  try {
    return { raw, event: JSON.parse(raw), jsonError: null };
  } catch (error) {
    return { raw, event: null, jsonError: error.message };
  }
}

function buildDeadLetter({ topic, partition, message, raw, event, errors, jsonError }) {
  const identity = messageIdentity(topic, partition, message.offset);
  const digest = hash(identity);
  const shipmentId =
    typeof event?.shipmentId === "string" && event.shipmentId.length > 0
      ? event.shipmentId
      : message.key?.toString("utf8") || `sin-shipmentId:${digest}`;
  const timestamp = Number(message.timestamp);

  return {
    markerId: `dlq:${digest}`,
    key: shipmentId,
    event: {
      falloId: `dlq-${digest.slice(0, 24)}`,
      shipmentId,
      eventIdOriginal: typeof event?.eventId === "string" ? event.eventId : null,
      topicOriginal: topic,
      particionOriginal: partition,
      offsetOriginal: message.offset,
      grupoConsumidor: "proyector",
      codigoError: jsonError ? "JSON_INVALIDO" : "SCHEMA_INVALIDO",
      motivo: jsonError ?? errors.join("; "),
      falloEn: new Date(Number.isFinite(timestamp) ? timestamp : Date.now()).toISOString(),
      payloadOriginal: raw,
      version: 1,
    },
  };
}

async function retryControlled(operation, callback) {
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await callback();
    } catch (error) {
      const waitMs = RETRY_DELAYS_MS[attempt];
      if (waitMs === undefined) {
        throw new Error(`${operation} falló tras ${attempt + 1} intentos: ${error.message}`, {
          cause: error,
        });
      }
      console.warn(JSON.stringify({
        level: "warn",
        message: `${operation} falló; se reintentará.`,
        attempt: attempt + 1,
        waitMs,
        error: error.message,
      }));
      await delay(waitMs);
    }
  }
}

function parseCutoffs(value) {
  if (!value) return null;
  const cutoffs = typeof value === "string" ? JSON.parse(value) : value;
  const result = new Map();
  for (const [topic, partitions] of Object.entries(cutoffs)) {
    for (const { partition, nextOffset } of partitions) {
      result.set(`${topic}:${partition}`, BigInt(nextOffset));
    }
  }
  return result;
}

function parseStartOffsets(value) {
  if (!value) return [];
  return typeof value === "string" ? JSON.parse(value) : value;
}

export function hasReachedReplayCutoff(batch, cutoff) {
  const lastMessage = batch.messages.at(-1);
  return cutoff !== undefined && lastMessage !== undefined &&
    BigInt(lastMessage.offset) + 1n >= cutoff;
}

export async function runProjector({
  kafkaConfig,
  mongoClient,
  store,
  generation = "main",
  groupId = "proyector",
  cutoffs: cutoffInput = null,
  startOffsets: startOffsetsInput = null,
  signalHandlers = true,
}) {
  const kafka = createKafkaClient(kafkaConfig);
  const consumer = kafka.consumer({
    groupId,
    allowAutoTopicCreation: false,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  });
  const producer = createProducer(kafka);
  const cutoffs = parseCutoffs(cutoffInput);
  const startOffsets = parseStartOffsets(startOffsetsInput);
  const verifyShipmentIds = process.env.PROJECTOR_VERIFY_SHIPMENT_IDS
    ? new Set(process.env.PROJECTOR_VERIFY_SHIPMENT_IDS.split(",").filter(Boolean))
    : null;
  const pendingCutoffs = new Set(cutoffs?.keys() ?? []);

  let resolveStop;
  let resolveCutoff;
  let fatalError;
  let requestedStop = false;
  const stopRequested = new Promise((resolve) => { resolveStop = resolve; });
  const cutoffReached = new Promise((resolve) => { resolveCutoff = resolve; });
  if (cutoffs && pendingCutoffs.size === 0) resolveCutoff();

  function requestStop(error) {
    if (error && !fatalError) fatalError = error;
    requestedStop = true;
    resolveStop();
  }

  const signalHandlersMap = new Map();
  if (signalHandlers) {
    for (const signal of ["SIGINT", "SIGTERM"]) {
      const handler = () => requestStop();
      signalHandlersMap.set(signal, handler);
      process.once(signal, handler);
    }
  }

  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({
    topics: SOURCE_TOPICS,
    fromBeginning: process.env.PROJECTOR_FROM_BEGINNING !== "false",
  });
  consumer.on(consumer.events.GROUP_JOIN, () => {
    console.log(JSON.stringify({
      level: "info",
      message: "Grupo de proyección unido.",
      groupId,
      generation,
    }));
    for (const entry of startOffsets) consumer.seek(entry);
  });
  console.log(JSON.stringify({
    level: "info",
    message: "Proyector iniciado.",
    consumer: "proyector",
    groupId,
    generation,
    topics: SOURCE_TOPICS,
  }));

  const runner = consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    partitionsConsumedConcurrently: 1,
    restartOnFailure: async () => false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      const cutoffKey = `${batch.topic}:${batch.partition}`;
      const cutoff = cutoffs?.get(cutoffKey);
      let currentMessage = null;
      let lastFilteredMessage = null;
      try {
        if (cutoffs && cutoff === undefined) return;
        for (const message of batch.messages) {
          currentMessage = message;
          if (!isRunning() || isStale() || requestedStop) return;
          if (cutoff !== undefined && BigInt(message.offset) >= cutoff) {
            pendingCutoffs.delete(cutoffKey);
            if (pendingCutoffs.size === 0) resolveCutoff();
            return;
          }

          const messageKey = message.key?.toString("utf8");
          if (verifyShipmentIds && !verifyShipmentIds.has(messageKey)) {
            resolveOffset(message.offset);
            lastFilteredMessage = message;
            continue;
          }

          const { raw, event, jsonError } = decodeMessage(message);
          const validation = jsonError
            ? { valid: false, errors: [jsonError] }
            : validateEvent(batch.topic, event);
          let outcome;

          if (!validation.valid) {
            if (batch.topic !== TOPICS.states) {
              throw new Error(
                `Evento inválido en ${batch.topic}; no se envía a DLQ para evitar bucles. ${validation.errors.join("; ")}`,
              );
            }

            const deadLetter = buildDeadLetter({
              topic: batch.topic,
              partition: batch.partition,
              message,
              raw,
              event,
              errors: validation.errors,
              jsonError,
            });
            if (await store.hasProcessed(deadLetter.markerId)) {
              outcome = "dead_letter_duplicate";
            } else {
              await retryControlled("Publicación DLQ", () => producer.send({
                topic: TOPICS.deadLetter,
                acks: -1,
                messages: [{ key: deadLetter.key, value: JSON.stringify(deadLetter.event) }],
              }));
              await retryControlled("Marcador DLQ en MongoDB", () =>
                store.markInvalidProcessed(mongoClient, deadLetter.markerId, batch.topic),
              );
              outcome = "dead_lettered";
            }
          } else {
            if (
              process.env.PROJECTOR_TEST_MONGO_FAIL_EVENT_ID === event.eventId
            ) {
              throw new Error(`Fallo Mongo inyectado para ${event.eventId}.`);
            }
            outcome = await retryControlled(
              `Transacción MongoDB ${event.eventId}`,
              () => store.applyEvent(mongoClient, batch.topic, event),
            );
            if (process.env.PROJECTOR_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID === event.eventId) {
              console.error(JSON.stringify({
                level: "error",
                message: "Interrupción de prueba después de MongoDB y antes del commit Kafka.",
                eventId: event.eventId,
                topic: batch.topic,
                partition: batch.partition,
                offset: message.offset,
              }));
              process.exit(86);
            }
          }

          await consumer.commitOffsets([{
            topic: batch.topic,
            partition: batch.partition,
            offset: offsetAfter(message.offset),
          }]);
          lastFilteredMessage = null;
          resolveOffset(message.offset);
          await heartbeat();

          console.log(JSON.stringify({
            level: "info",
            message: "Evento proyectado.",
            eventId: event?.eventId ?? null,
            shipmentId: event?.shipmentId ?? message.key?.toString("utf8") ?? null,
            consumer: "proyector",
            generation,
            topic: batch.topic,
            partition: batch.partition,
            offset: message.offset,
            outcome,
          }));
        }

        if (lastFilteredMessage) {
          await consumer.commitOffsets([{
            topic: batch.topic,
            partition: batch.partition,
            offset: offsetAfter(lastFilteredMessage.offset),
          }]);
        }

        if (hasReachedReplayCutoff(batch, cutoff)) {
          pendingCutoffs.delete(cutoffKey);
          if (pendingCutoffs.size === 0) resolveCutoff();
        }
      } catch (error) {
        console.error(JSON.stringify({
          level: "error",
          message: "Proyección detenida; el offset del mensaje fallido no se confirma.",
          topic: batch.topic,
          partition: batch.partition,
          offset: currentMessage?.offset ?? null,
          error: error.message,
        }));
        requestStop(error);
        throw error;
      }
    },
  }).catch((error) => {
    requestStop(error);
  });

  try {
    if (cutoffs) {
      await Promise.race([cutoffReached, stopRequested]);
    } else {
      await stopRequested;
    }
    await consumer.stop();
    await runner;
    if (fatalError) throw fatalError;
  } finally {
    for (const [signal, handler] of signalHandlersMap) {
      process.removeListener(signal, handler);
    }
    await producer.disconnect().catch(() => {});
    await consumer.disconnect().catch(() => {});
  }
}
