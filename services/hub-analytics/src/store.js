import { BusinessStore } from "../../business/runtime.js";
import { stableId } from "../../business/domain.js";
import { hubContribution, rememberTransition, generationSuffix } from "./domain.js";
import { TOPICS } from "../../../packages/contracts/src/index.js";

export class HubStore extends BusinessStore {
  constructor(database, calendar, generation = "main") {
    super(database, "analitica-hubs", calendar);
    this.database = database;
    this.generation = generation;
    const suffix = generationSuffix(generation);
    this.states = database.collection(`estado_hubs${suffix}`);
    this.metrics = database.collection(`metricas_hubs${suffix}`);
    this.outbox = database.collection(`outbox_hubs${suffix}`);
    this.progress = database.collection("reprocesamientos_hubs");
  }

  async createIndexes() {
    await super.createIndexes();
    await this.metrics.createIndex({ fecha: 1, hubId: 1, zonaId: 1, franja: 1 }, { unique: true, name: "dimensiones_hub_unique" });
  }

  async apply(client, event, invalid) {
    const session = client.startSession();
    try {
      return await session.withTransaction(async () => {
        const identity = { consumer: this.service, generation: this.generation, eventId: event.eventId ?? event.falloId };
        if (await this.processed.findOne(identity, { session })) return "duplicate";
        let outcome = "ignored";
        if (invalid) {
          await this.enqueue(TOPICS.deadLetter, event, session);
          outcome = "dead_lettered";
        } else if (["INGRESADO_HUB", "EN_REPARTO"].includes(event.tipo)) {
          const state = await this.states.findOne({ _id: event.shipmentId }, { session }) ?? { _id: event.shipmentId, transitions: [] };
          const previous = hubContribution(state, this.calendar.offsetMinutes);
          rememberTransition(state, event);
          const next = hubContribution(state, this.calendar.offsetMinutes);
          const deltas = new Map();
          for (const [sample, sign] of [[previous, -1], [next, 1]]) {
            if (!sample) continue;
            const id = stableId(sample.dimensions);
            const delta = deltas.get(id) ?? { dimensions: sample.dimensions, count: 0, duration: 0 };
            delta.count += sign;
            delta.duration += sign * sample.duration;
            deltas.set(id, delta);
          }
          for (const [id, delta] of deltas) if (delta.count || delta.duration) {
            await this.metrics.updateOne({ _id: id }, { $setOnInsert: delta.dimensions, $inc: { enviosConSalida: delta.count, duracionTotalMs: delta.duration } }, { upsert: true, session });
          }
          await this.states.replaceOne({ _id: state._id }, state, { upsert: true, session });
          outcome = next ? "sampled" : "waiting_for_pair";
        }
        await this.processed.insertOne({ ...identity, topic: TOPICS.states, processedAt: new Date() }, { session });
        return outcome;
      }, { readConcern: { level: "snapshot" }, writeConcern: { w: "majority" } });
    } finally { await session.endSession(); }
  }

  async complete(groupId, live) {
    await this.progress.updateOne({ _id: this.generation }, { $set: { status: live ? "EN_VIVO" : "COMPLETO", completedAt: new Date(), groupId } });
    if (live) await this.database.collection("vistas_activas").updateOne({ _id: "hubs" }, { $set: { generation: this.generation, collection: this.metrics.collectionName, activatedAt: new Date() } }, { upsert: true });
  }
}
