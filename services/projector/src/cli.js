import { MongoClient } from "mongodb";
import { createKafkaClient } from "../../simulator/src/kafka.js";
import { runProjector } from "./consumer.js";
import { ProjectionStore } from "./store.js";

function parseArguments(argumentsList) {
  const [command = "run", ...options] = argumentsList;
  const parsed = {};
  for (let index = 0; index < options.length; index += 1) {
    const separator = options[index].indexOf("=");
    const flag = separator < 0 ? options[index] : options[index].slice(0, separator);
    const inlineValue = separator < 0 ? undefined : options[index].slice(separator + 1);
    if (!["--generation", "--group-id", "--cutoffs", "--start-offsets"].includes(flag)) {
      throw new Error(`Opción desconocida: ${flag}`);
    }
    const value = inlineValue ?? options[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`Falta valor para ${flag}.`);
    }
    parsed[flag.slice(2)] = value;
    if (inlineValue === undefined) index += 1;
  }
  return { command, options: parsed };
}

async function main() {
  const { command, options } = parseArguments(process.argv.slice(2));
  if (command === "help") {
    console.log("Uso: node services/projector/src/cli.js [run|replay] [--generation=ID] [--group-id=ID] [--cutoffs=JSON]");
    return;
  }
  if (command !== "run" && command !== "replay") {
    throw new Error(`Comando inválido: ${command}. Usar run o replay.`);
  }

  const generation = options.generation ?? process.env.PROJECTOR_GENERATION ?? "main";
  const cutoffs = options.cutoffs ?? process.env.PROJECTOR_CUTOFFS ?? null;
  const startOffsets = options["start-offsets"] ?? process.env.PROJECTOR_START_OFFSETS ?? null;
  if (command === "replay" && (!options.generation || !cutoffs)) {
    throw new Error("replay requiere --generation nueva y --cutoffs JSON.");
  }

  const config = {
    brokers: String(process.env.KAFKA_BROKERS ?? "localhost:19092")
      .split(",")
      .map((broker) => broker.trim())
      .filter(Boolean),
    clientId: process.env.KAFKA_CLIENT_ID ?? "rutasur-projector",
  };
  const groupId = options["group-id"] ??
    process.env.PROJECTOR_GROUP_ID ??
    (generation === "main" ? "proyector" : `proyector-${generation}`);
  const mongoUri = process.env.MONGODB_URI ??
    "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true";
  const databaseName = process.env.MONGODB_DATABASE ?? "rutasur";
  const mongoClient = new MongoClient(mongoUri, {
    serverSelectionTimeoutMS: 5000,
    maxPoolSize: 20,
  });

  try {
    await mongoClient.connect();
    const store = new ProjectionStore(mongoClient.db(databaseName), { generation });
    await store.createIndexes();
    await runProjector({
      kafkaConfig: config,
      mongoClient,
      store,
      generation,
      groupId,
      cutoffs,
      startOffsets,
    });
  } finally {
    await mongoClient.close();
  }
}

main().catch((error) => {
  console.error(JSON.stringify({
    level: "error",
    message: "El proyector terminó sin confirmar el offset del mensaje fallido.",
    error: error.message,
  }));
  process.exitCode = 1;
});