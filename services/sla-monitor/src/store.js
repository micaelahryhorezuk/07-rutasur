import { BusinessStore } from "../../business/runtime.js";
import { TOPICS } from "../../../packages/contracts/src/index.js";
import { evaluateSla, updateSlaState } from "./domain.js";

export class SlaStore extends BusinessStore {
  constructor(database, calendar) {
    super(database, "monitor-sla", calendar);
    this.states = database.collection("estado_monitor_sla");
    this.outbox = database.collection("outbox_monitor_sla");
    this.meta = database.collection("reloj_monitor_sla");
    this.cache = new Map();
  }

  async restore() {
    for await (const state of this.states.find()) this.cache.set(state._id, state);
    return this.meta.findOne({ _id: "clock" });
  }

  async saveClock(snapshot, session) {
    if (!snapshot) return;
    await this.meta.replaceOne({ _id: "clock" }, { _id: "clock", ...snapshot }, { upsert: true, session });
  }

  async evaluateAndSave(state, now, session) {
    const outputs = evaluateSla(state, now, this.calendar.offsetMinutes);
    for (const event of outputs) await this.enqueue(TOPICS.slaAlerts, event, session);
    await this.states.replaceOne({ _id: state._id }, state, { upsert: true, session });
    return outputs.length;
  }

  async apply(client, topic, event, invalid, snapshot) {
    const session = client.startSession();
    try {
      const result = await session.withTransaction(async () => {
        const identity = { consumer: this.service, generation: "main", eventId: event.eventId ?? event.falloId };
        if (await this.processed.findOne(identity, { session })) return { outcome: "duplicate" };
        let state;
        let outcome = "dead_lettered";
        if (invalid) await this.enqueue(TOPICS.deadLetter, event, session);
        else {
          state = await this.states.findOne({ _id: event.shipmentId }, { session }) ?? { _id: event.shipmentId };
          outcome = updateSlaState(state, topic, event);
          await this.evaluateAndSave(state, snapshot.simulatedAt, session);
        }
        await this.processed.insertOne({ ...identity, topic, processedAt: new Date() }, { session });
        await this.saveClock(snapshot, session);
        return { outcome, state };
      }, { readConcern: { level: "snapshot" }, writeConcern: { w: "majority" } });
      if (result.state) this.cache.set(result.state._id, result.state);
      return result.outcome;
    } finally { await session.endSession(); }
  }

  async tick(client, snapshot) {
    let emitted = 0;
    for (const [shipmentId, cached] of this.cache) {
      if (!cached.promise || cached.delivered) continue;
      if (evaluateSla(structuredClone(cached), snapshot.simulatedAt, this.calendar.offsetMinutes).length === 0) continue;
      const session = client.startSession();
      try {
        const result = await session.withTransaction(async () => {
          const state = await this.states.findOne({ _id: shipmentId }, { session });
          const count = await this.evaluateAndSave(state, snapshot.simulatedAt, session);
          await this.saveClock(snapshot, session);
          return { state, count };
        }, { readConcern: { level: "snapshot" }, writeConcern: { w: "majority" } });
        this.cache.set(shipmentId, result.state);
        emitted += result.count;
      } finally { await session.endSession(); }
    }
    await this.saveClock(snapshot);
    return emitted;
  }
}
