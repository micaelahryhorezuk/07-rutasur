import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { randomUUID } from "node:crypto";
import { MongoClient } from "mongodb";
import { loadConfig } from "../services/simulator/src/config.js";
import { generateScenario } from "../services/simulator/src/generator.js";
import { createKafkaClient, createProducer, publishEvents } from "../services/simulator/src/kafka.js";
import { TOPICS } from "../packages/contracts/src/index.js";

const runId = randomUUID();
const databaseName = `rutasur_verify_acceptance_${runId.replaceAll("-", "")}`;
const config = loadConfig({}, { seed: "tp7-aceptacion-v1", startAt: "2026-10-23T08:00:00.000Z", acceleration: 120 });
const scenarios = ["happy", "absent", "delayed", "third-failure", "duplicate", "out-of-order", "malformed"];
const reference = scenarios.flatMap(scenario => generateScenario(scenario, config));
const referenceIds = [...new Set(reference.map(entry => entry.event.shipmentId))];
const byScenario = scenario => generateScenario(scenario, config)[0].event.shipmentId;
const lagEvents = generateScenario("absent", loadConfig({}, { seed: runId, startAt: "2026-10-23T08:10:00.000Z" }));
const futureEvents = generateScenario("happy", loadConfig({}, { seed: runId, startAt: "2026-10-23T08:30:00.000Z" }));
const allIds = [...new Set([...reference, ...lagEvents, ...futureEvents].map(entry => entry.event.shipmentId))];
const children = [];
const groups = {};
let common;
function progress(message, fields = {}) { console.log(JSON.stringify({ message, ...fields })); }
async function freePort() {
  const server = createServer(); await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port; await new Promise(resolve => server.close(resolve)); return port;
}
async function waitFor(check, description, timeout = 120000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const failed = children.find(child => child.done && child.code !== 0 && !child.expectedStop);
    if (failed && !description.startsWith("limpieza")) throw new Error(`${failed.name}: ${failed.output}`);
    const value = await check(); if (value) return value;
    await new Promise(resolve => setTimeout(resolve, 75));
  }
  throw new Error(`Timeout ${description}\n${children.filter(child => !child.done).map(child => `${child.name}: ${child.output.slice(-4000)}`).join("\n")}`);
}
function launch(folder, args = [], environment = {}) {
  groups[folder] ??= `verify-acceptance-${folder}-${runId}`;
  const child = spawn(process.execPath, [`services/${folder}/src/cli.js`, ...args], { env: {
    ...process.env, ...common,
    PROJECTOR_GROUP_ID: groups[folder], BUSINESS_GROUP_ID: groups[folder], SLA_GROUP_ID: groups[folder], HUB_GROUP_ID: groups[folder],
    PROJECTOR_FROM_BEGINNING: "false", BUSINESS_FROM_BEGINNING: "false", SLA_FROM_BEGINNING: "false",
    BUSINESS_VERIFY_SHIPMENT_IDS: allIds.join(","), SLA_VERIFY_SHIPMENT_IDS: allIds.join(","),
    ...environment,
  }, stdio: ["ignore", "pipe", "pipe"] });
  return register(folder, child);
}
function register(name, child) {
  const record = { name, child, done: false, output: "" };
  for (const stream of [child.stdout, child.stderr]) { stream.setEncoding("utf8"); stream.on("data", chunk => { record.output = (record.output + chunk).slice(-30000); }); }
  record.exited = new Promise(resolve => {
    child.once("exit", (code, signal) => { record.done = true; record.code = code; record.signal = signal; resolve(); });
    child.once("error", error => { record.done = true; record.output += error.message; resolve(); });
  });
  children.push(record); return record;
}
async function ready(record) { await waitFor(() => ["Servicio unido.", "Grupo de proyección unido.", "Monitor SLA unido.", "Analítica de hubs unida."].some(value => record.output.includes(`"message":"${value}"`)), `inicio ${record.name}`); }
async function stop(record) {
  record.expectedStop = true;
  if (!record.done) record.child.kill("SIGTERM");
  await waitFor(() => record.done, `cierre ${record.name}`, 25000);
  assert.ok(record.code === 0 || (record.code === null && record.signal === "SIGTERM") || record.code === 143, `${record.name} code=${record.code} signal=${record.signal}: ${record.output}`);
}
function businessView(value) { const { _id, creadoEnProyeccion, ...view } = value; return JSON.parse(JSON.stringify(view)); }
async function main() {
  const webPort = await freePort(); const controlPort = await freePort();
  common = { MONGODB_URI: process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true", MONGODB_DATABASE: databaseName, KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? "localhost:19092", SIMULATOR_ACCELERATION: "120", BUSINESS_UTC_OFFSET_MINUTES: "-180", BUSINESS_HOLIDAYS: "", NEXT_TELEMETRY_DISABLED: "1" };
  const base = `http://127.0.0.1:${webPort}`;
  const mongo = new MongoClient(common.MONGODB_URI, { serverSelectionTimeoutMS: 5000 });
  const kafka = createKafkaClient({ clientId: `acceptance-${runId}`, brokers: common.KAFKA_BROKERS.split(",") });
  const producer = createProducer(kafka); const admin = kafka.admin();
  let db; let browser;
  async function api(path) { const response = await fetch(`${base}${path}`); assert.equal(response.status, 200, path); return response.json(); }
  async function offsets() {
    const result = {};
    for (const folder of ["projector", "notifications", "rescheduling", "analytics", "sla-monitor"]) result[folder] = await admin.fetchOffsets({ groupId: groups[folder] });
    return result;
  }
  try {
    await mongo.connect(); db = mongo.db(databaseName); await producer.connect(); await admin.connect();
    let projector = launch("projector", ["run"]);
    let notifier = launch("notifications");
    const rescheduler = launch("rescheduling"); const analytics = launch("analytics"); const monitor = launch("sla-monitor");
    await Promise.all([projector, notifier, rescheduler, analytics, monitor].map(ready));
    const web = register("web", spawn(process.execPath, ["apps/web/node_modules/next/dist/bin/next", "start", "apps/web", "--hostname", "127.0.0.1", "--port", String(webPort)], { env: { ...process.env, ...common, SIMULATOR_CONTROL_URL: `http://127.0.0.1:${controlPort}` }, stdio: ["ignore", "pipe", "pipe"] }));
    const control = register("control", spawn(process.execPath, ["services/simulator/src/control.js"], { env: { ...process.env, ...common, SIMULATOR_START_AT: config.startAt, SIMULATOR_CONTROL_PORT: String(controlPort), SIMULATOR_CONTROL_HOST: "127.0.0.1" }, stdio: ["ignore", "pipe", "pipe"] }));
    await waitFor(async () => { try { return (await fetch(`${base}/api/health`)).ok && control.output.includes("Control del simulador listo."); } catch { return false; } }, "web y control");
    await publishEvents(producer, reference, { logEvents: false, scenario: "aceptacion-semilla-fija" });
    const happyId = byScenario("happy"); const absentId = byScenario("absent"); const thirdId = byScenario("third-failure");
    await waitFor(async () => (await db.collection("envios").findOne({ shipmentId: happyId }))?.estado === "ENTREGADO", "envío feliz");
    assert.equal((await api(`/api/tracking/${happyId}`)).estado, "ENTREGADO");
    await waitFor(async () => (await db.collection("envios").findOne({ shipmentId: absentId }))?.reprogramacionActual?.accion === "PROGRAMAR", "ausente reprogramado");
    await waitFor(async () => (await db.collection("envios").findOne({ shipmentId: thirdId }))?.reprogramacionActual?.accion === "DEVOLVER", "tercer fallo devuelve");
    await waitFor(async () => db.collection("alertas").findOne({ shipmentId: byScenario("delayed"), regla: "SIN_SALIDA", prioridad: "ALTA" }), "alerta preventiva demorado");
    await waitFor(async () => await db.collection("notificaciones").countDocuments({ categoria: "FALLO" }) === 4, "cuatro avisos por fallos");
    await waitFor(async () => (await api("/api/analytics")).totals.entregasConPromesa === 3, "métricas completas");
    const metrics = (await api("/api/analytics")).totals;
    assert.deepEqual(metrics, { primerosIntentos: 5, entregasPrimerIntento: 3, fallos: 4, entregasConPromesa: 3, entregasEnPlazo: 3, entregasFueraPlazo: 0, motivos: { AUSENTE: 4, DIRECCION_INCORRECTA: 0, RECHAZADO: 0, ZONA_INACCESIBLE: 0 } });
    const duplicate = await db.collection("envios").findOne({ shipmentId: byScenario("duplicate") });
    assert.equal(new Set(duplicate.historial.map(value => value.eventId)).size, duplicate.historial.length);
    for (const name of ["outbox_notificaciones", "outbox_reprogramacion", "outbox_analitica", "outbox_monitor_sla"]) await waitFor(async () => db.collection(name).findOne({ topic: TOPICS.deadLetter, published: true }), `DLQ ${name}`);
    progress("Semilla fija: 5 primeros intentos, 3 entregas, 4 fallos; tracking, reprogramación, SLA y DLQ correctos.");
    await stop(notifier);
    await publishEvents(producer, lagEvents, { logEvents: false, scenario: "aceptacion-lag" });
    const lagId = lagEvents[0].event.shipmentId;
    await waitFor(async () => (await db.collection("envios").findOne({ shipmentId: lagId }))?.reprogramacionActual?.accion === "PROGRAMAR" && (await api("/api/analytics")).totals.fallos === 5, "otros consumidores siguen activos");
    assert.equal(await db.collection("notificaciones").countDocuments({ shipmentId: lagId, categoria: "FALLO" }), 0);
    const lagOffsets = (await admin.fetchOffsets({ groupId: groups.notifications })).find(value => value.topic === TOPICS.states)?.partitions ?? [];
    const latest = await admin.fetchTopicOffsets(TOPICS.states);
    assert.ok(latest.some(value => BigInt(value.high) > BigInt(lagOffsets.find(item => item.partition === value.partition)?.offset ?? "-1")), "Debe crecer el lag del grupo detenido");
    notifier = launch("notifications", [], { BUSINESS_FROM_BEGINNING: "true" }); await ready(notifier);
    await waitFor(async () => await db.collection("notificaciones").countDocuments({ shipmentId: lagId, categoria: "FALLO" }) === 1, "recuperación sin duplicar aviso");
    progress("Aislamiento y recuperación del lag verificados.");
    const before = await offsets();
    const retained = [projector, notifier, rescheduler, analytics, monitor];
    const hubs = launch("hub-analytics", ["run"], { HUB_VERIFY_SHIPMENT_IDS: [...referenceIds, futureEvents[0].event.shipmentId].join(",") });
    await ready(hubs);
    await waitFor(async () => (await api("/api/analytics")).hubTimes.status === "EN_VIVO", "replay histórico cuarto indicador");
    const historic = (await api("/api/analytics")).hubTimes;
    assert.equal(historic.samples, 6); assert.equal(historic.durationMs, 360000); assert.equal(historic.meanMs, 60000);
    assert.ok(retained.every(record => !record.done), "No se reiniciaron los consumidores existentes");
    const after = await offsets();
    for (const [folder, topics] of Object.entries(before)) for (const topic of topics) for (const partition of topic.partitions) {
      const advanced = after[folder].find(item => item.topic === topic.topic)?.partitions.find(item => item.partition === partition.partition)?.offset ?? "-1";
      assert.ok(BigInt(advanced) >= BigInt(partition.offset), `${folder} retrocedió su offset`);
    }
    await publishEvents(producer, futureEvents, { logEvents: false, scenario: "aceptacion-despues-de-replay" });
    await waitFor(async () => (await api("/api/analytics")).hubTimes.samples === 7, "el grupo nuevo sigue incorporando envíos");
    assert.equal((await api("/api/analytics")).hubTimes.meanMs, 60000);
    progress("Cuarto indicador: 6 muestras históricas y 1 nueva, promedio 1 minuto; grupos anteriores intactos.");
    const mainHubs = await db.collection("metricas_hubs").find({}, { projection: { _id: 0 } }).sort({ fecha: 1, hubId: 1, zonaId: 1, franja: 1 }).toArray();
    for (const generation of ["replay-a", "replay-b"]) {
      const replay = launch("hub-analytics", ["replay", `--generation=${generation}`], { HUB_GROUP_ID: `acceptance-${generation}-${runId}`, HUB_VERIFY_SHIPMENT_IDS: [...referenceIds, futureEvents[0].event.shipmentId].join(",") });
      await waitFor(() => replay.done, `replay ${generation}`);
      assert.equal(replay.code, 0, replay.output);
      const rebuilt = await db.collection(`metricas_hubs__${generation}`).find({}, { projection: { _id: 0 } }).sort({ fecha: 1, hubId: 1, zonaId: 1, franja: 1 }).toArray();
      assert.deepEqual(rebuilt, mainHubs);
    }
    assert.equal((await db.collection("vistas_activas").findOne({ _id: "hubs" })).generation, "main");
    const response = await fetch(`${base}/api/demo`, { method: "POST", headers: { "Content-Type": "application/json", Origin: base }, body: JSON.stringify({ scenario: "happy", seed: runId }) });
    assert.equal(response.status, 200); const demo = await response.json();
    await waitFor(async () => (await db.collection("envios").findOne({ shipmentId: demo.shipmentIds[0] }))?.estado === "ENTREGADO", "panel demo → simulador → tracking");
    if (process.env.WEB_VERIFY_BROWSER_MODULE) {
      const { chromium } = await import(process.env.WEB_VERIFY_BROWSER_MODULE); browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
      const page = await browser.newPage(); await page.goto(`${base}/analitica`);
      await page.getByRole("heading", { name: "Tiempo en el centro" }).waitFor();
      await page.getByText("7 envíos con ambos movimientos", { exact: true }).waitFor();
      await page.screenshot({ path: "/tmp/rutasur-analitica-final.png", fullPage: true });
      await browser.close(); browser = undefined;
    }
    await stop(projector);
    const original = (await db.collection("envios").find().sort({ shipmentId: 1 }).toArray()).map(businessView);
    const ids = original.map(value => value.shipmentId);
    const cutoffs = {};
    for (const topic of [TOPICS.states, TOPICS.promises, TOPICS.reschedules, TOPICS.slaAlerts, TOPICS.notifications]) cutoffs[topic] = (await admin.fetchTopicOffsets(topic)).filter(value => BigInt(value.high) > BigInt(value.low)).map(value => ({ partition: value.partition, nextOffset: value.high }));
    // Destructive operations are restricted to this run's dedicated test DB.
    assert.ok(db.databaseName.startsWith("rutasur_verify_acceptance_"));
    await db.collection("envios").drop();
    await db.collection("eventos_procesados").deleteMany({ consumer: "proyector", generation: "main" });
    const restored = launch("projector", ["run", `--cutoffs=${JSON.stringify(cutoffs)}`], { PROJECTOR_GROUP_ID: `acceptance-rebuild-${runId}`, PROJECTOR_FROM_BEGINNING: "true", PROJECTOR_VERIFY_SHIPMENT_IDS: ids.join(",") });
    await waitFor(() => restored.done, "reconstrucción de envios borrados"); assert.equal(restored.code, 0, restored.output);
    const reconstructed = (await db.collection("envios").find().sort({ shipmentId: 1 }).toArray()).map(businessView);
    assert.deepEqual(reconstructed, original);
    assert.ok([notifier, rescheduler, analytics, monitor, hubs].every(record => !record.done));
    progress("Reconstrucción tras borrar envios: misma vista de negocio, otros grupos activos.");
    for (const record of [notifier, rescheduler, analytics, monitor, hubs, web, control]) await stop(record);
    progress("Aceptación integral verificada.", { seed: config.seed, referenceShipments: 7, historicalHubSamples: 6, currentHubSamples: 7, hubMeanMs: 60000, hubReplays: 2, deletedShipmentsRebuilt: original.length, browserVerified: Boolean(process.env.WEB_VERIFY_BROWSER_MODULE) });
  } finally {
    await browser?.close().catch(() => {});
    for (const record of children) if (!record.done) record.child.kill("SIGTERM");
    for (const record of children) if (!record.done) {
      try { await waitFor(() => record.done, "limpieza procesos", 20000); }
      catch { record.child.kill("SIGKILL"); await record.exited; }
    }
    if (db) await db.dropDatabase();
    await producer.disconnect().catch(() => {}); await admin.disconnect().catch(() => {}); await mongo.close();
  }
}
main().catch(error => { console.error(error.stack ?? error); process.exitCode = 1; });
