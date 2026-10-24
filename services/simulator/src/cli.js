import { setTimeout as delay } from "node:timers/promises";
import { performance } from "node:perf_hooks";
import { loadConfig } from "./config.js";
import { createContinuousGenerator, generateScenario, SCENARIOS } from "./generator.js";
import { createKafkaClient, createProducer, publishEvents } from "./kafka.js";

const OPTION_NAMES = {
  "--seed": "seed",
  "--start-at": "startAt",
  "--acceleration": "acceleration",
  "--shipments-per-minute": "shipmentsPerMinute",
  "--zone-failure-rates": "zoneFailureRates",
  "--failure-reasons": "failureReasons",
  "--hub-delay-probability": "hubDelayProbability",
  "--duplicate-probability": "duplicateProbability",
  "--out-of-order-probability": "outOfOrderProbability",
  "--malformed-probability": "malformedProbability",
};

function parseArguments(argumentsList) {
  const [scenario = "continuous", ...options] = argumentsList;
  const overrides = {};
  for (let index = 0; index < options.length; index += 1) {
    const separator = options[index].indexOf("=");
    const flag = separator < 0 ? options[index] : options[index].slice(0, separator);
    const inlineValue = separator < 0 ? undefined : options[index].slice(separator + 1);
    const configKey = OPTION_NAMES[flag];
    if (!configKey) throw new Error(`Opción desconocida: ${flag}`);
    const value = inlineValue ?? options[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`Falta valor para ${flag}.`);
    }
    overrides[configKey] = value;
    if (inlineValue === undefined) index += 1;
  }
  return { scenario, overrides };
}

function setOverrideTypes(overrides) {
  for (const key of [
    "acceleration",
    "shipmentsPerMinute",
    "hubDelayProbability",
    "duplicateProbability",
    "outOfOrderProbability",
    "malformedProbability",
  ]) {
    if (overrides[key] !== undefined) overrides[key] = Number(overrides[key]);
  }
  return overrides;
}

async function runContinuous(producer, config, shutdownSignal) {
  const generator = createContinuousGenerator(config);
  let shipmentCount = 0;
  let nextScheduledAt = performance.now();
  while (!shutdownSignal.aborted) {
    const events = generator.next();
    await publishEvents(producer, events, { scenario: "continuous" });
    shipmentCount += 1;
    console.log(
      JSON.stringify({
        level: "info",
        message: "Envío continuo publicado.",
        shipmentCount,
        shipmentsPerMinute: config.shipmentsPerMinute,
      }),
    );
    nextScheduledAt += generator.intervalMs;
    try {
      await delay(
        Math.max(0, nextScheduledAt - performance.now()),
        undefined,
        { signal: shutdownSignal },
      );
    } catch (error) {
      if (error.name !== "AbortError") throw error;
    }
  }
}

async function main() {
  const { scenario, overrides: rawOverrides } = parseArguments(process.argv.slice(2));
  if (!SCENARIOS.includes(scenario)) {
    throw new Error(`Escenario inválido: ${scenario}. Disponibles: ${SCENARIOS.join(", ")}`);
  }

  const config = loadConfig(process.env, setOverrideTypes(rawOverrides));
  const kafka = createKafkaClient(config);
  const producer = createProducer(kafka);
  const shutdown = new AbortController();
  const handleSignal = () => shutdown.abort();
  process.once("SIGINT", handleSignal);
  process.once("SIGTERM", handleSignal);

  try {
    await producer.connect();
    console.log(
      JSON.stringify({
        level: "info",
        message: "Simulador conectado a Kafka.",
        scenario,
        brokers: config.brokers,
        seed: config.seed,
        startAt: config.startAt,
        acceleration: config.acceleration,
      }),
    );

    if (scenario === "continuous") {
      await runContinuous(producer, config, shutdown.signal);
    } else {
      const events = generateScenario(scenario, config);
      await publishEvents(producer, events, { scenario });
      console.log(
        JSON.stringify({
          level: "info",
          message: "Escenario finalizado.",
          scenario,
          events: events.length,
          shipmentIds: new Set(events.map((entry) => entry.event.shipmentId)).size,
        }),
      );
    }
  } finally {
    shutdown.abort();
    process.removeListener("SIGINT", handleSignal);
    process.removeListener("SIGTERM", handleSignal);
    await producer.disconnect();
    console.log(JSON.stringify({ level: "info", message: "Conexión Kafka cerrada." }));
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ level: "error", message: error.message }));
  process.exitCode = 1;
});