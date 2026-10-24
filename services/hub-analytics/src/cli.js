import { MongoClient } from "mongodb";
import { createKafkaClient, createProducer } from "../../simulator/src/kafka.js";
import { calendarFromEnvironment } from "../../business/domain.js";
import { decodeBusinessMessage } from "../../business/runtime.js";
import { TOPICS } from "../../../packages/contracts/src/index.js";
import { generationSuffix, reachedCutoff } from "./domain.js";
import { HubStore } from "./store.js";

function argumentsFor(argv) {
  const [mode = "run", ...flags] = argv;
  if (!["run", "replay"].includes(mode)) throw new Error("Usar run o replay.");
  const options = {};
  for (const flag of flags) {
    const match = /^--(generation|group-id|cutoffs)=(.+)$/.exec(flag);
    if (!match) throw new Error(`Opción inválida: ${flag}. Usar --nombre=valor.`);
    options[match[1]] = match[2];
  }
  const generation = options.generation ?? process.env.HUB_GENERATION ?? "main";
  generationSuffix(generation);
  if (mode === "replay" && generation === "main") throw new Error("replay requiere una generación nueva con --generation=ID.");
  return { mode, generation, groupId: options["group-id"] ?? process.env.HUB_GROUP_ID ?? (generation === "main" ? "analitica-hubs" : `analitica-hubs-${generation}`), cutoffs: options.cutoffs ? JSON.parse(options.cutoffs) : null };
}

async function main() {
  const { mode, generation, groupId, cutoffs: suppliedCutoffs } = argumentsFor(process.argv.slice(2));
  const mongo = new MongoClient(process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true", { serverSelectionTimeoutMS: 5000 });
  const kafka = createKafkaClient({ clientId: "rutasur-analitica-hubs", brokers: (process.env.KAFKA_BROKERS ?? "localhost:19092").split(",").map(value => value.trim()) });
  const admin = kafka.admin();
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false, retry: { restartOnFailure: async () => false } });
  const producer = createProducer(kafka);
  const filter = process.env.HUB_VERIFY_SHIPMENT_IDS ? new Set(process.env.HUB_VERIFY_SHIPMENT_IDS.split(",")) : null;
  let stopping = false;
  let fatal;
  let completed = false;
  let resolveStop;
  const stopped = new Promise(resolve => { resolveStop = resolve; });
  const requestStop = () => { stopping = true; resolveStop(); };
  const fail = error => { fatal ??= error; requestStop(); };
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, requestStop);
  try {
    await mongo.connect();
    const store = new HubStore(mongo.db(process.env.MONGODB_DATABASE ?? "rutasur"), calendarFromEnvironment(), generation);
    await store.createIndexes();
    await producer.connect(); await admin.connect(); await store.flush(producer);
    const offsets = await admin.fetchTopicOffsets(TOPICS.states);
    const cutoffs = suppliedCutoffs ?? { [TOPICS.states]: offsets.filter(item => BigInt(item.high ?? item.offset) > BigInt(item.low ?? "0")).map(item => ({ partition: item.partition, nextOffset: item.high ?? item.offset })) };
    const targets = new Map((cutoffs[TOPICS.states] ?? []).map(item => [item.partition, BigInt(item.nextOffset)]));
    const pending = new Set(targets.keys());
    const committed = await admin.fetchOffsets({ groupId });
    for (const entry of committed.find(item => item.topic === TOPICS.states)?.partitions ?? []) {
      if (targets.has(entry.partition) && BigInt(entry.offset) >= targets.get(entry.partition)) pending.delete(entry.partition);
    }
    await store.progress.updateOne({ _id: generation }, { $set: { status: "REPROCESANDO", cutoffs, groupId, startedAt: new Date() } }, { upsert: true });
    async function maybeComplete() {
      if (!completed && pending.size === 0) {
        await store.complete(groupId, mode === "run");
        completed = true;
        console.log(JSON.stringify({ message: "Histórico de hubs completo.", generation, groupId, mode }));
        if (mode === "replay") requestStop();
      }
    }
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.states, fromBeginning: true });
    consumer.on(consumer.events.GROUP_JOIN, () => console.log(JSON.stringify({ message: "Analítica de hubs unida.", groupId, generation })));
    consumer.on(consumer.events.CRASH, ({ payload }) => fail(payload.error));
    await consumer.run({
      autoCommit: false, eachBatchAutoResolve: false,
      eachBatch: async ({ batch, heartbeat, resolveOffset, isRunning, isStale }) => {
        let lastFiltered;
        let currentOffset;
        const cutoff = targets.get(batch.partition);
        try {
          for (const message of batch.messages) {
            currentOffset = message.offset;
            if (stopping || !isRunning() || isStale()) return;
            if (mode === "replay" && (cutoff === undefined || BigInt(message.offset) >= cutoff)) {
              if (lastFiltered) await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: (BigInt(lastFiltered.offset) + 1n).toString() }]);
              consumer.pause([{ topic: batch.topic, partitions: [batch.partition] }]);
              pending.delete(batch.partition); await maybeComplete(); return;
            }
            if (filter && !filter.has(message.key?.toString("utf8"))) {
              lastFiltered = message;
              resolveOffset(message.offset);
            } else {
              const { event, invalid } = decodeBusinessMessage("analitica-hubs", batch.topic, batch.partition, message);
              const outcome = await store.apply(mongo, event, invalid);
              if (process.env.HUB_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID && event.eventId === process.env.HUB_TEST_CRASH_AFTER_TRANSACTION_EVENT_ID) process.exit(86);
              await store.flush(producer);
              await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: (BigInt(message.offset) + 1n).toString() }]);
              resolveOffset(message.offset); lastFiltered = null; await heartbeat();
              console.log(JSON.stringify({ service: "analitica-hubs", generation, eventId: event.eventId ?? event.falloId, shipmentId: event.shipmentId, topic: batch.topic, partition: batch.partition, offset: message.offset, outcome }));
            }
            if (cutoff !== undefined && reachedCutoff(message.offset, cutoff)) pending.delete(batch.partition);
          }
          if (lastFiltered) {
            await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: (BigInt(lastFiltered.offset) + 1n).toString() }]);
            await heartbeat();
          }
          if (mode === "replay" && !pending.has(batch.partition)) consumer.pause([{ topic: batch.topic, partitions: [batch.partition] }]);
          await maybeComplete();
        } catch (error) {
          console.error(JSON.stringify({ service: "analitica-hubs", partition: batch.partition, offset: currentOffset, error: error.message }));
          fail(error); throw error;
        }
      },
    });
    await maybeComplete();
    await stopped;
    await consumer.stop();
    if (fatal) throw fatal;
  } finally {
    for (const signal of ["SIGINT", "SIGTERM"]) process.removeListener(signal, requestStop);
    await consumer.disconnect().catch(() => {}); await producer.disconnect().catch(() => {}); await admin.disconnect().catch(() => {}); await mongo.close();
  }
}
main().catch(error => { console.error(error.stack ?? error.message); process.exitCode = 1; });
