import { createHash } from "node:crypto";
import { SimulatedClock, TOPICS } from "../../../packages/contracts/src/index.js";

const STAGE_OFFSETS_MS = [0, 250, 500, 750, 1000, 1250, 1500, 1750, 2000];
const NORMAL_STATES = [
  "CREADO",
  "INGRESADO_HUB",
  "CLASIFICADO",
  "EN_REPARTO",
  "ENTREGADO",
];

function hash(value, length = 24) {
  return createHash("sha256").update(String(value)).digest("hex").slice(0, length);
}

function sortObject(value) {
  if (Array.isArray(value)) return value.map(sortObject);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, sortObject(value[key])]),
    );
  }
  return value;
}

function configFingerprint(config) {
  const {
    brokers,
    clientId,
    ...businessConfig
  } = config;
  return hash(JSON.stringify(sortObject(businessConfig)), 12);
}

function seedRandom(seed) {
  let state = Number.parseInt(hash(seed, 8), 16) >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}

function chooseWeighted(random, weights) {
  const target = random();
  let cumulative = 0;
  const entries = Object.entries(weights);
  for (const [value, weight] of entries) {
    cumulative += weight;
    if (target < cumulative) return value;
  }
  return entries.at(-1)[0];
}

function chooseZone(random, config) {
  const zones = Object.keys(config.zoneFailureRates);
  return zones[Math.floor(random() * zones.length)];
}

function shipmentIdentifier(scenario, config, index) {
  const scenarioPart = scenario.toUpperCase().replaceAll(/[^A-Z0-9]+/g, "-");
  return `RS-${scenarioPart}-${hash(`${config.seed}|${configFingerprint(config)}`, 8).toUpperCase()}-${String(index).padStart(4, "0")}`;
}

function createEventFactory({ scenario, shipmentId, zoneId, config }) {
  const clock = new SimulatedClock({
    startAt: config.startAt,
    acceleration: config.acceleration,
  });
  const fingerprint = configFingerprint(config);
  let sequence = 0;

  function commonEvent(topic, tipo, wallOffsetMs, extra = {}) {
    sequence += 1;
    const eventId = `evt-${hash(
      `${fingerprint}|${scenario}|${shipmentId}|${topic}|${sequence}|${tipo}`,
    )}`;
    return {
      eventId,
      shipmentId,
      tipo,
      ocurridoEn: clock.atWallOffset(wallOffsetMs).toISOString(),
      secuencia: sequence,
      hubId: "HUB-01",
      zonaId: zoneId,
      repartidorId: sequence >= 4 ? "REP-01" : null,
      version: 1,
      ...extra,
    };
  }

  function promise(wallOffsetMs, hoursUntilDue = 12) {
    const dueOffsetMs = wallOffsetMs + (hoursUntilDue * 60 * 60 * 1000) / config.acceleration;
    return {
      topic: TOPICS.promises,
      event: commonEvent(TOPICS.promises, "PROMESA_CREADA", wallOffsetMs, {
        venceEn: clock.atWallOffset(dueOffsetMs).toISOString(),
        versionPromesa: 1,
      }),
      intentionallyInvalid: false,
    };
  }

  function status(tipo, wallOffsetMs, attempt, motive) {
    const event = commonEvent(TOPICS.states, tipo, wallOffsetMs, {
      franja: clock.atWallOffset(wallOffsetMs).getUTCHours() < 12 ? "MANIANA" : "TARDE",
    });
    if (tipo === "EN_REPARTO" || tipo === "ENTREGA_FALLIDA") event.intento = attempt;
    if (tipo === "ENTREGA_FALLIDA" && motive !== undefined) event.motivo = motive;
    return { topic: TOPICS.states, event, intentionallyInvalid: false };
  }

  return { promise, status };
}

function baseShipmentEvents({
  scenario,
  index,
  config,
  zoneId,
  statusPlan,
  deadlineHours = 12,
  shipmentWallOffsetMs = 0,
}) {
  const shipmentId = shipmentIdentifier(scenario, config, index);
  const factory = createEventFactory({ scenario, shipmentId, zoneId, config });
  const events = [factory.promise(shipmentWallOffsetMs, deadlineHours)];
  for (const [stateIndex, state] of statusPlan.entries()) {
    events.push(
      factory.status(
        state.tipo,
        shipmentWallOffsetMs + STAGE_OFFSETS_MS[stateIndex],
        state.intento,
        state.motivo,
      ),
    );
  }
  return events;
}

function makeHappyShipment({ scenario, index, config, zoneId }) {
  return baseShipmentEvents({
    scenario,
    index,
    config,
    zoneId,
    statusPlan: NORMAL_STATES.map((tipo, stateIndex) => ({
      tipo,
      intento: tipo === "EN_REPARTO" ? 1 : undefined,
      stateIndex,
    })),
  });
}

function makeAbsentShipment({ scenario, index, config, zoneId }) {
  return baseShipmentEvents({
    scenario,
    index,
    config,
    zoneId,
    statusPlan: [
      { tipo: "CREADO" },
      { tipo: "INGRESADO_HUB" },
      { tipo: "CLASIFICADO" },
      { tipo: "EN_REPARTO", intento: 1 },
      { tipo: "ENTREGA_FALLIDA", intento: 1, motivo: "AUSENTE" },
    ],
  });
}

function makeDelayedShipment({ scenario, index, config, zoneId, shipmentWallOffsetMs = 0 }) {
  return baseShipmentEvents({
    scenario,
    index,
    config,
    zoneId,
    shipmentWallOffsetMs,
    deadlineHours: 3,
    statusPlan: [{ tipo: "CREADO" }, { tipo: "INGRESADO_HUB" }],
  });
}

function makeThirdFailureShipment({ scenario, index, config, zoneId }) {
  return baseShipmentEvents({
    scenario,
    index,
    config,
    zoneId,
    statusPlan: [
      { tipo: "CREADO" },
      { tipo: "INGRESADO_HUB" },
      { tipo: "CLASIFICADO" },
      { tipo: "EN_REPARTO", intento: 1 },
      { tipo: "ENTREGA_FALLIDA", intento: 1, motivo: "AUSENTE" },
      { tipo: "EN_REPARTO", intento: 2 },
      { tipo: "ENTREGA_FALLIDA", intento: 2, motivo: "AUSENTE" },
      { tipo: "EN_REPARTO", intento: 3 },
      { tipo: "ENTREGA_FALLIDA", intento: 3, motivo: "AUSENTE" },
    ],
  });
}

function makeMalformedShipment({ scenario, index, config, zoneId }) {
  const events = makeAbsentShipment({ scenario, index, config, zoneId });
  const invalidFailure = events.at(-1);
  delete invalidFailure.event.motivo;
  invalidFailure.intentionallyInvalid = true;
  return events;
}

function applyContinuousProbabilities(events, config, random) {
  const statusEvents = events.filter((entry) => entry.topic === TOPICS.states);
  if (random() < config.outOfOrderProbability && statusEvents.length >= 4) {
    const earlier = events.indexOf(statusEvents[2]);
    const later = events.indexOf(statusEvents[3]);
    [events[earlier], events[later]] = [events[later], events[earlier]];
    events[earlier].intentionallyOutOfOrder = true;
    events[later].intentionallyOutOfOrder = true;
  }

  if (random() < config.malformedProbability) {
    const terminal = events.filter((entry) => entry.topic === TOPICS.states).at(-1);
    terminal.event.tipo = "ENTREGA_FALLIDA";
    terminal.event.intento ??= 1;
    delete terminal.event.motivo;
    terminal.intentionallyInvalid = true;
  }

  if (random() < config.duplicateProbability) {
    const candidate = events.filter((entry) => entry.topic === TOPICS.states)[0];
    events.splice(events.indexOf(candidate) + 1, 0, {
      ...candidate,
      duplicateOf: candidate.event.eventId,
    });
  }
  return events;
}

function makeRandomShipment({ index, config, random }) {
  const zoneId = chooseZone(random, config);
  const shipmentWallOffsetMs = ((index - 1) * 60_000) / config.shipmentsPerMinute;
  if (random() < config.hubDelayProbability) {
    return makeDelayedShipment({
      scenario: "continuous",
      index,
      config,
      zoneId,
      shipmentWallOffsetMs,
    });
  }

  const failed = random() < config.zoneFailureRates[zoneId];
  const states = failed
    ? [
        ...NORMAL_STATES.slice(0, 4),
        "ENTREGA_FALLIDA",
      ].map((tipo, stateIndex) => ({
        tipo,
        intento: tipo === "EN_REPARTO" || tipo === "ENTREGA_FALLIDA" ? 1 : undefined,
        motivo:
          tipo === "ENTREGA_FALLIDA"
            ? chooseWeighted(random, config.failureReasons)
            : undefined,
        stateIndex,
      }))
    : NORMAL_STATES.map((tipo, stateIndex) => ({
        tipo,
        intento: tipo === "EN_REPARTO" ? 1 : undefined,
        stateIndex,
      }));

  const events = baseShipmentEvents({
    scenario: "continuous",
    index,
    config,
    random,
    zoneId,
    statusPlan: states,
    shipmentWallOffsetMs,
  });
  return applyContinuousProbabilities(events, config, random);
}

export function generateScenario(scenario, config) {
  const random = seedRandom(`${config.seed}:${scenario}`);
  const zoneId = chooseZone(random, config);

  if (scenario === "happy") {
    return makeHappyShipment({ scenario, index: 1, config, zoneId });
  }
  if (scenario === "absent") {
    return makeAbsentShipment({ scenario, index: 1, config, zoneId });
  }
  if (scenario === "delayed") {
    return makeDelayedShipment({ scenario, index: 1, config, zoneId });
  }
  if (scenario === "third-failure") {
    return makeThirdFailureShipment({ scenario, index: 1, config, zoneId });
  }
  if (scenario === "malformed") {
    return makeMalformedShipment({ scenario, index: 1, config, zoneId });
  }
  if (scenario === "duplicate") {
    const events = makeHappyShipment({ scenario, index: 1, config, zoneId });
    const candidate = events.find(
      (entry) => entry.event.tipo === "EN_REPARTO",
    );
    events.splice(events.indexOf(candidate) + 1, 0, {
      ...candidate,
      duplicateOf: candidate.event.eventId,
    });
    return events;
  }
  if (scenario === "out-of-order") {
    const events = makeHappyShipment({ scenario, index: 1, config, zoneId });
    const earlier = events.findIndex((entry) => entry.event.tipo === "CLASIFICADO");
    const later = events.findIndex((entry) => entry.event.tipo === "EN_REPARTO");
    [events[earlier], events[later]] = [events[later], events[earlier]];
    events[earlier].intentionallyOutOfOrder = true;
    events[later].intentionallyOutOfOrder = true;
    return events;
  }
  if (scenario === "burst") {
    const burstEvents = [];
    for (let index = 1; index <= 500; index += 1) {
      burstEvents.push(makeRandomShipment({ index, config, random }));
    }
    return burstEvents.flat();
  }
  throw new Error(`Escenario desconocido: ${scenario}`);
}

export function createContinuousGenerator(config) {
  const random = seedRandom(`${config.seed}:continuous`);
  let index = 0;
  return {
    next() {
      index += 1;
      return makeRandomShipment({ index, config, random });
    },
    intervalMs: 60_000 / config.shipmentsPerMinute,
  };
}

export const SCENARIOS = Object.freeze([
  "continuous",
  "happy",
  "absent",
  "delayed",
  "third-failure",
  "burst",
  "duplicate",
  "out-of-order",
  "malformed",
]);