import { performance } from "node:perf_hooks";

export class SimulatedClock {
  constructor({
    startAt,
    acceleration = 120,
    snapshot,
    wallNow = Date.now,
    monotonicNow = () => performance.now(),
  }) {
    const startTime = Date.parse(startAt);
    if (!Number.isFinite(startTime)) throw new Error("startAt debe ser una fecha válida.");
    if (!Number.isFinite(acceleration) || acceleration <= 0) {
      throw new Error("acceleration debe ser mayor que cero.");
    }

    this.acceleration = acceleration;
    this.wallNow = wallNow;
    this.monotonicNow = monotonicNow;

    const currentWallTime = wallNow();
    const snapshotTime = snapshot ? Date.parse(snapshot.simulatedAt) : startTime;
    if (!Number.isFinite(snapshotTime)) {
      throw new Error("snapshot.simulatedAt debe ser una fecha válida.");
    }
    const downtimeMs = snapshot
      ? Math.max(0, currentWallTime - snapshot.wallAnchorMs)
      : 0;

    this.anchorSimulatedTime = Math.max(
      startTime,
      snapshotTime + downtimeMs * acceleration,
    );
    this.anchorWallTime = currentWallTime;
    this.anchorMonotonicTime = monotonicNow();
  }

  atWallOffset(offsetMs) {
    if (!Number.isFinite(offsetMs) || offsetMs < 0) {
      throw new Error("El offset del reloj debe ser un número no negativo.");
    }
    return new Date(this.anchorSimulatedTime + offsetMs * this.acceleration);
  }

  now(monotonicTime = this.monotonicNow()) {
    const elapsedMs = Math.max(0, monotonicTime - this.anchorMonotonicTime);
    return new Date(this.anchorSimulatedTime + elapsedMs * this.acceleration);
  }

  observeEvent(occurredAt, kafkaTimestampMs = this.wallNow(), monotonicTime = this.monotonicNow()) {
    const eventTime = Date.parse(occurredAt);
    if (!Number.isFinite(eventTime)) throw new Error("occurredAt debe ser una fecha válida.");

    const processTime = this.now(monotonicTime).getTime();
    const kafkaElapsedMs = Math.max(0, kafkaTimestampMs - this.anchorWallTime);
    const kafkaProjectedTime = this.anchorSimulatedTime + kafkaElapsedMs * this.acceleration;
    this.anchorSimulatedTime = Math.max(processTime, kafkaProjectedTime, eventTime);
    this.anchorWallTime = Math.max(this.anchorWallTime, kafkaTimestampMs);
    this.anchorMonotonicTime = monotonicTime;
    return new Date(this.anchorSimulatedTime);
  }

  snapshot(wallTimeMs = this.wallNow()) {
    return {
      simulatedAt: this.now().toISOString(),
      wallAnchorMs: wallTimeMs,
      acceleration: this.acceleration,
    };
  }
}