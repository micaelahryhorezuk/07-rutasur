import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { loadConfig } from "../services/simulator/src/config.js";
import { generateScenario } from "../services/simulator/src/generator.js";
import {
  createKafkaClient,
  createProducer,
  publishEvents,
} from "../services/simulator/src/kafka.js";
import { TOPICS, validateEvent } from "../packages/contracts/src/index.js";

async function withTimeout(promise, timeoutMs, message) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function main() {
  const config = loadConfig(process.env, {
    seed: `kafka-verification-${randomUUID()}`,
  });
  const scenarioNames = [
    "happy",
    "absent",
    "delayed",
    "third-failure",
    "burst",
    "duplicate",
    "out-of-order",
    "malformed",
  ];
  const scenarios = scenarioNames.map((scenario) => ({
    scenario,
    events: generateScenario(scenario, config),
  }));
  const expectedById = new Map();
  let expectedCount = 0;
  for (const { scenario, events } of scenarios) {
    for (const entry of events) {
      const validation = validateEvent(entry.topic, entry.event);
      assert.equal(validation.valid, !entry.intentionallyInvalid);
      const existing = expectedById.get(entry.event.eventId);
      if (existing) {
        assert.equal(existing.serialized, JSON.stringify(entry.event));
        existing.remaining += 1;
      } else {
        expectedById.set(entry.event.eventId, {
          event: entry.event,
          topic: entry.topic,
          scenario,
          serialized: JSON.stringify(entry.event),
          remaining: 1,
          intentionallyOutOfOrder: Boolean(entry.intentionallyOutOfOrder),
        });
      }
      expectedCount += 1;
    }
  }

  const groupId = `rutasur-verification-${randomUUID()}`;
  const kafka = createKafkaClient(config);
  const consumer = kafka.consumer({
    groupId,
    allowAutoTopicCreation: false,
  });
  const producer = createProducer(kafka);

  let resolveGroupJoined;
  let resolveAllEvents;
  let rejectAllEvents;
  let receivedCount = 0;
  const groupJoined = new Promise((resolve) => {
    resolveGroupJoined = resolve;
  });
  const allEventsReceived = new Promise((resolve, reject) => {
    resolveAllEvents = resolve;
    rejectAllEvents = reject;
  });
  const receivedById = new Map();
  const receivedSequencesByShipment = new Map();
  const outOfOrderShipments = new Set(
    [...expectedById.values()]
      .filter((expected) => expected.intentionallyOutOfOrder)
      .map((expected) => expected.event.shipmentId),
  );

  consumer.on(consumer.events.GROUP_JOIN, () => resolveGroupJoined());

  try {
    await consumer.connect();
    await consumer.subscribe({
      topics: [TOPICS.promises, TOPICS.states],
      fromBeginning: true,
    });
    consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, message }) => {
        const event = JSON.parse(message.value.toString());
        const expected = expectedById.get(event.eventId);
        if (!expected) return;

        try {
          assert.equal(message.key.toString(), expected.event.shipmentId);
          assert.equal(topic, expected.topic);
          assert.equal(message.value.toString(), expected.serialized);
          if (expected.remaining <= 0) {
            throw new Error(`Cantidad inesperada de copias para ${event.eventId}.`);
          }
          expected.remaining -= 1;
          receivedCount += 1;
          receivedById.set(event.eventId, (receivedById.get(event.eventId) ?? 0) + 1);

          if (topic === TOPICS.states) {
            if (!receivedSequencesByShipment.has(event.shipmentId)) {
              receivedSequencesByShipment.set(event.shipmentId, []);
            }
            const sequenceEvents = receivedSequencesByShipment.get(event.shipmentId);
            if (!sequenceEvents.some((item) => item.eventId === event.eventId)) {
              sequenceEvents.push({ eventId: event.eventId, secuencia: event.secuencia });
            }
          }

          if (receivedCount === expectedCount) resolveAllEvents();
        } catch (error) {
          rejectAllEvents(error);
        }
      },
    }).catch(rejectAllEvents);

    await withTimeout(groupJoined, 30000, "El grupo de verificación no se unió a Kafka.");
    await producer.connect();
    for (const { scenario, events } of scenarios) {
      await publishEvents(producer, events, {
        scenario: `verification-${scenario}`,
        logEvents: false,
      });
      console.log(
        JSON.stringify({
          level: "info",
          message: "Escenario publicado para verificación.",
          scenario,
          events: events.length,
          shipmentIds: new Set(events.map((entry) => entry.event.shipmentId)).size,
        }),
      );
    }
    await withTimeout(
      allEventsReceived,
      30000,
      `Kafka no entregó todos los eventos esperados; recibidos=${receivedCount}/${expectedCount}.`,
    );

    assert.equal(
      [...scenarios.find((item) => item.scenario === "burst").events]
        .map((entry) => entry.event.shipmentId)
        .filter((shipmentId, index, all) => all.indexOf(shipmentId) === index).length,
      500,
    );

    for (const [shipmentId, sequenceEvents] of receivedSequencesByShipment) {
      const hasDecrease = sequenceEvents.some(
        (entry, index) => index > 0 && entry.secuencia < sequenceEvents[index - 1].secuencia,
      );
      assert.equal(hasDecrease, outOfOrderShipments.has(shipmentId));
    }

    const duplicated = scenarios.find((item) => item.scenario === "duplicate").events
      .find((entry) => entry.duplicateOf);
    assert.equal(receivedById.get(duplicated.event.eventId), 2);
    const malformed = scenarios.find((item) => item.scenario === "malformed").events
      .find((entry) => entry.intentionallyInvalid);
    assert.equal(malformed.topic, TOPICS.states);

    console.log(
      JSON.stringify({
        level: "info",
        message: "Verificación Kafka completada.",
        groupId,
        events: receivedCount,
        uniqueEventIds: receivedById.size,
        groupIsolated: true,
        topics: [TOPICS.promises, TOPICS.states],
        burstShipmentIds: 500,
      }),
    );
  } finally {
    await producer.disconnect().catch(() => {});
    await consumer.stop().catch(() => {});
    await consumer.disconnect().catch(() => {});
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ level: "error", message: error.message }));
  process.exitCode = 1;
});