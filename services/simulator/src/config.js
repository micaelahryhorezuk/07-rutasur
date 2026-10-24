const DEFAULTS = {
  brokers: ["localhost:19092"],
  clientId: "rutasur-simulator",
  seed: "2026",
  startAt: "2026-10-18T08:00:00.000Z",
  acceleration: 120,
  shipmentsPerMinute: 20,
  zoneFailureRates: {
    "ZONA-NORTE": 0.05,
    "ZONA-CENTRO": 0.15,
    "ZONA-SUR": 0.3,
  },
  failureReasons: {
    AUSENTE: 0.6,
    DIRECCION_INCORRECTA: 0.2,
    RECHAZADO: 0.15,
    ZONA_INACCESIBLE: 0.05,
  },
  hubDelayProbability: 0.1,
  duplicateProbability: 0.02,
  outOfOrderProbability: 0.02,
  malformedProbability: 0.01,
};

function parseProbability(value, name) {
  const probability = Number(value);
  if (!Number.isFinite(probability) || probability < 0 || probability > 1) {
    throw new Error(`${name} debe estar entre 0 y 1.`);
  }
  return probability;
}

function parseWeights(value, fallback, name) {
  if (value === undefined || value === "") return { ...fallback };
  const result = {};
  for (const pair of String(value).split(",")) {
    const separator = pair.lastIndexOf("=");
    if (separator < 1) throw new Error(`${name} debe usar clave=probabilidad.`);
    result[pair.slice(0, separator)] = parseProbability(
      pair.slice(separator + 1),
      name,
    );
  }
  if (Object.keys(result).length === 0) throw new Error(`${name} no puede estar vacío.`);
  return result;
}

function firstDefined(...values) {
  return values.find((value) => value !== undefined && value !== "");
}

export function loadConfig(environment = process.env, overrides = {}) {
  const startAt = firstDefined(overrides.startAt, environment.SIMULATOR_START_AT, DEFAULTS.startAt);
  if (!Number.isFinite(Date.parse(startAt))) {
    throw new Error("SIMULATOR_START_AT debe ser una fecha ISO 8601 válida.");
  }

  const acceleration = Number(
    firstDefined(overrides.acceleration, environment.SIMULATOR_ACCELERATION, DEFAULTS.acceleration),
  );
  const shipmentsPerMinute = Number(
    firstDefined(overrides.shipmentsPerMinute, environment.SIMULATOR_SHIPMENTS_PER_MINUTE, DEFAULTS.shipmentsPerMinute),
  );
  if (!Number.isFinite(acceleration) || acceleration <= 0) {
    throw new Error("SIMULATOR_ACCELERATION debe ser mayor que cero.");
  }
  if (!Number.isFinite(shipmentsPerMinute) || shipmentsPerMinute <= 0) {
    throw new Error("SIMULATOR_SHIPMENTS_PER_MINUTE debe ser mayor que cero.");
  }

  const zoneFailureRates = parseWeights(
    firstDefined(overrides.zoneFailureRates, environment.SIMULATOR_ZONE_FAILURE_RATES),
    DEFAULTS.zoneFailureRates,
    "SIMULATOR_ZONE_FAILURE_RATES",
  );
  const failureReasons = parseWeights(
    firstDefined(overrides.failureReasons, environment.SIMULATOR_FAILURE_REASONS),
    DEFAULTS.failureReasons,
    "SIMULATOR_FAILURE_REASONS",
  );
  const reasonTotal = Object.values(failureReasons).reduce((sum, value) => sum + value, 0);
  if (Math.abs(reasonTotal - 1) > 0.000001) {
    throw new Error("SIMULATOR_FAILURE_REASONS debe sumar 1.");
  }

  return {
    brokers: String(firstDefined(overrides.brokers, environment.KAFKA_BROKERS, DEFAULTS.brokers.join(",")))
      .split(",")
      .map((broker) => broker.trim())
      .filter(Boolean),
    clientId: firstDefined(overrides.clientId, environment.KAFKA_CLIENT_ID, DEFAULTS.clientId),
    seed: String(firstDefined(overrides.seed, environment.SIMULATOR_SEED, DEFAULTS.seed)),
    startAt: new Date(startAt).toISOString(),
    acceleration,
    shipmentsPerMinute,
    zoneFailureRates,
    failureReasons,
    hubDelayProbability: parseProbability(
      firstDefined(overrides.hubDelayProbability, environment.SIMULATOR_HUB_DELAY_PROBABILITY, DEFAULTS.hubDelayProbability),
      "SIMULATOR_HUB_DELAY_PROBABILITY",
    ),
    duplicateProbability: parseProbability(
      firstDefined(overrides.duplicateProbability, environment.SIMULATOR_DUPLICATE_PROBABILITY, DEFAULTS.duplicateProbability),
      "SIMULATOR_DUPLICATE_PROBABILITY",
    ),
    outOfOrderProbability: parseProbability(
      firstDefined(overrides.outOfOrderProbability, environment.SIMULATOR_OUT_OF_ORDER_PROBABILITY, DEFAULTS.outOfOrderProbability),
      "SIMULATOR_OUT_OF_ORDER_PROBABILITY",
    ),
    malformedProbability: parseProbability(
      firstDefined(overrides.malformedProbability, environment.SIMULATOR_MALFORMED_PROBABILITY, DEFAULTS.malformedProbability),
      "SIMULATOR_MALFORMED_PROBABILITY",
    ),
  };
}

export const DEFAULT_SIMULATOR_CONFIG = Object.freeze({
  ...DEFAULTS,
  brokers: [...DEFAULTS.brokers],
  zoneFailureRates: { ...DEFAULTS.zoneFailureRates },
  failureReasons: { ...DEFAULTS.failureReasons },
});