import { MongoClient } from "mongodb";
import { SimulatedClock, TOPICS } from "../../../packages/contracts/src/index.js";
import { createKafkaClient, createProducer } from "../../simulator/src/kafka.js";
import { calendarFromEnvironment } from "../../business/domain.js";
import { decodeBusinessMessage } from "../../business/runtime.js";
import { SlaStore } from "./store.js";

async function main() {
  const acceleration = Number(process.env.SIMULATOR_ACCELERATION ?? 120);
  const tickSimulatedMs = Number(process.env.SLA_EVALUATION_INTERVAL_SIMULATED_MS ?? 60000);
  if (!Number.isFinite(acceleration) || acceleration <= 0 || !Number.isFinite(tickSimulatedMs) || tickSimulatedMs <= 0) throw new Error("Aceleración e intervalo SLA deben ser positivos.");
  const mongo = new MongoClient(process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true", { serverSelectionTimeoutMS: 5000 });
  const kafka = createKafkaClient({ clientId: "rutasur-monitor-sla", brokers: (process.env.KAFKA_BROKERS ?? "localhost:19092").split(",").map(value => value.trim()) });
  const groupId = process.env.SLA_GROUP_ID ?? "monitor-sla";
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false, retry: { restartOnFailure: async () => false } });
  const producer = createProducer(kafka);
  let clock;
  let store;
  let timer;
  let fatal;
  let stopping = false;
  let resolveStop;
  let queue = Promise.resolve();
  const stopped = new Promise(resolve => { resolveStop = resolve; });
  const requestStop = () => { stopping = true; clearTimeout(timer); resolveStop(); };
  const fail = error => { fatal ??= error; requestStop(); };
  function serialized(work) {
    const result = queue.then(work);
    queue = result.catch(() => {});
    return result;
  }
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, requestStop);
  const filter = process.env.SLA_VERIFY_SHIPMENT_IDS ? new Set(process.env.SLA_VERIFY_SHIPMENT_IDS.split(",")) : null;

  function schedule() {
    if (stopping) return;
    timer = setTimeout(async () => {
      try {
        await serialized(async () => {
          if (!clock || stopping) return;
          const snapshot = clock.snapshot();
          const emitted = await store.tick(mongo, snapshot);
          await store.flush(producer);
          if (emitted) console.log(JSON.stringify({ service: "monitor-sla", message: "Evaluación periódica SLA.", simulatedAt: snapshot.simulatedAt, emitted }));
        });
      } catch (error) { fail(error); }
      schedule();
    }, Math.max(1, tickSimulatedMs / acceleration));
  }

  try {
    await mongo.connect();
    store = new SlaStore(mongo.db(process.env.MONGODB_DATABASE ?? "rutasur"), calendarFromEnvironment());
    await store.createIndexes();
    const snapshot = await store.restore();
    if (snapshot) {
      if (snapshot.acceleration !== acceleration) throw new Error("La aceleración debe coincidir con el reloj SLA persistido.");
      clock = new SimulatedClock({ startAt: snapshot.simulatedAt, snapshot, acceleration });
    }
    await producer.connect();
    await store.flush(producer);
    await consumer.connect();
    await consumer.subscribe({ topics: [TOPICS.states, TOPICS.promises], fromBeginning: process.env.SLA_FROM_BEGINNING !== "false" });
    consumer.on(consumer.events.GROUP_JOIN, () => console.log(JSON.stringify({ message: "Monitor SLA unido.", groupId })));
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
            const { event, invalid } = decodeBusinessMessage("monitor-sla", topic, partition, message);
            const outcome = await serialized(async () => {
              if (!invalid) {
                clock ??= new SimulatedClock({ startAt: event.ocurridoEn, acceleration });
                clock.observeEvent(event.ocurridoEn, Number(message.timestamp));
              }
              const result = await store.apply(mongo, topic, event, invalid, clock?.snapshot());
              if (process.env.SLA_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID && event.eventId === process.env.SLA_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID) process.exit(86);
              await store.flush(producer);
              return result;
            });
            await heartbeat();
            await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
            resolveOffset(message.offset);
            lastFiltered = null;
            console.log(JSON.stringify({ service: "monitor-sla", eventId: event.eventId ?? event.falloId, shipmentId: event.shipmentId, topic, partition, offset: message.offset, outcome }));
          }
          if (lastFiltered) {
            await consumer.commitOffsets([{ topic, partition, offset: (BigInt(lastFiltered.offset) + 1n).toString() }]);
            await heartbeat();
          }
        } catch (error) {
          console.error(JSON.stringify({ service: "monitor-sla", topic, partition, offset: currentOffset, message: "Monitor detenido sin confirmar el mensaje fallido.", error: error.message }));
          fail(error);
          throw error;
        }
      },
    });
    schedule();
    await stopped;
    await consumer.stop();
    await queue;
    if (fatal) throw fatal;
    if (clock) await store.saveClock(clock.snapshot());
  } finally {
    requestStop();
    for (const signal of ["SIGINT", "SIGTERM"]) process.removeListener(signal, requestStop);
    await consumer.disconnect().catch(() => {});
    await queue;
    await producer.disconnect().catch(() => {});
    await mongo.close();
  }
}

main().catch(error => { console.error(JSON.stringify({ service: "monitor-sla", message: error.stack ?? error.message })); process.exitCode = 1; });
