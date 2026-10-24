import { Kafka, Partitioners } from "kafkajs";
import { validateEvent } from "../../../packages/contracts/src/index.js";

export function createKafkaClient(config) {
  return new Kafka({
    clientId: config.clientId,
    brokers: config.brokers,
    connectionTimeout: 5000,
    requestTimeout: 30000,
    retry: { retries: 5 },
  });
}

export function createProducer(kafka) {
  return kafka.producer({
    allowAutoTopicCreation: false,
    createPartitioner: Partitioners.LegacyPartitioner,
  });
}

export async function publishEvents(
  producer,
  events,
  { scenario = "manual", concurrency = 24, logEvents = true } = {},
) {
  const lanesByShipment = new Map();
  for (const entry of events) {
    const shipmentId = entry.event.shipmentId;
    if (!lanesByShipment.has(shipmentId)) lanesByShipment.set(shipmentId, []);
    lanesByShipment.get(shipmentId).push(entry);
  }

  const lanes = [...lanesByShipment.values()];
  const workerCount = Math.min(Math.max(1, concurrency), lanes.length);
  const workers = Array.from({ length: workerCount }, async () => {
    let lane = lanes.shift();
    while (lane) {
      for (const entry of lane) await publishOne(producer, entry, scenario, logEvents);
      lane = lanes.shift();
    }
  });
  await Promise.all(workers);
}

async function publishOne(producer, entry, scenario, logEvents) {
    const result = validateEvent(entry.topic, entry.event);
    if (entry.intentionallyInvalid) {
      if (result.valid) {
        throw new Error(`El evento ${entry.event.eventId} debía ser inválido, pero pasó la validación.`);
      }
      if (logEvents) {
        console.warn(
          JSON.stringify({
            level: "warn",
            message: "Publicando evento inválido deliberadamente para probar la futura DLQ.",
            eventId: entry.event.eventId,
            shipmentId: entry.event.shipmentId,
            errors: result.errors,
          }),
        );
      }
    } else if (!result.valid) {
      throw new Error(
        `Evento inválido ${entry.event.eventId}: ${result.errors.join("; ")}`,
      );
    }

    const value = JSON.stringify(entry.event);
    const [metadata] = await producer.send({
      topic: entry.topic,
      acks: -1,
      messages: [{ key: entry.event.shipmentId, value }],
    });

    if (logEvents) {
      console.log(
        JSON.stringify({
          level: "info",
          scenario,
          eventId: entry.event.eventId,
          shipmentId: entry.event.shipmentId,
          topic: entry.topic,
          partition: metadata.partition,
          offset: metadata.baseOffset,
          duplicateOf: entry.duplicateOf ?? null,
          intentionallyInvalid: Boolean(entry.intentionallyInvalid),
          intentionallyOutOfOrder: Boolean(entry.intentionallyOutOfOrder),
        }),
      );
    }
}