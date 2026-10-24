import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { MongoClient } from "mongodb";
import { createKafkaClient, createProducer } from "../services/simulator/src/kafka.js";
import { TOPICS } from "../packages/contracts/src/index.js";
import { createContinuousGenerator } from "../services/simulator/src/generator.js";
import { loadConfig } from "../services/simulator/src/config.js";
import { publishEvents } from "../services/simulator/src/kafka.js";
import { label } from "../apps/web/lib/format.js";

const runId = randomUUID();
const databaseName = `rutasur_verify_web_${runId.replaceAll("-", "")}`;
const id = `RS-WEB-${runId}`;
const children = [];
const streams = [];
async function freePort() {
  const server = createServer();
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}
async function waitFor(check, description, timeout = 30000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const failed = children.find(record => record.done && record.code !== 0);
    if (failed && !description.startsWith("limpieza")) throw new Error(`${failed.name}: ${failed.output}`);
    const result = await check();
    if (result) return result;
    await new Promise(resolve => setTimeout(resolve, 40));
  }
  throw new Error(`Timeout ${description}\n${children.map(record => `${record.name}: ${record.output.slice(-3000)}`).join("\n")}`);
}
function launch(name, args, environment) {
  const child = spawn(process.execPath, args, { env: { ...process.env, ...environment }, stdio: ["ignore", "pipe", "pipe"] });
  const record = { name, child, output: "", done: false };
  for (const stream of [child.stdout, child.stderr]) {
    stream.setEncoding("utf8");
    stream.on("data", chunk => { record.output = (record.output + chunk).slice(-24000); });
  }
  record.exited = new Promise(resolve => {
    child.once("exit", (code, signal) => { record.done = true; record.code = code; record.signal = signal; resolve(); });
    child.once("error", error => { record.done = true; record.output += error.message; resolve(); });
  });
  children.push(record); return record;
}
async function openStream(url) {
  const abort = new AbortController();
  const response = await fetch(url, { signal: abort.signal });
  assert.equal(response.status, 200);
  assert.ok(response.headers.get("content-type").includes("text/event-stream"));
  const reader = response.body.getReader();
  const record = { events: [], abort, reader };
  const decoder = new TextDecoder();
  let buffer = "";
  record.pump = (async () => {
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) return;
        buffer += decoder.decode(value, { stream: true });
        const chunks = buffer.split("\n\n"); buffer = chunks.pop();
        record.events.push(...chunks.filter(value => value.includes("event:")));
      }
    } catch (error) { if (!abort.signal.aborted) record.error = error; }
  })();
  streams.push(record);
  await waitFor(() => record.events.some(value => value.includes("event: ready")), "suscripción SSE");
  return record;
}
async function main() {
  const webPort = await freePort();
  const controlPort = await freePort();
  const base = `http://127.0.0.1:${webPort}`;
  const mongo = new MongoClient(process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true", { serverSelectionTimeoutMS: 5000 });
  const producer = createProducer(createKafkaClient({ clientId: `verify-web-${runId}`, brokers: (process.env.KAFKA_BROKERS ?? "localhost:19092").split(",") }));
  let database;
  let browser;
  const progress = message => console.log(JSON.stringify({ message }));
  async function json(path, options) { const response = await fetch(`${base}${path}`, options); return { response, data: await response.json() }; }
  try {
    await mongo.connect(); database = mongo.db(databaseName); await producer.connect();
    const common = { MONGODB_URI: process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true", MONGODB_DATABASE: databaseName, KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? "localhost:19092", NEXT_TELEMETRY_DISABLED: "1" };
    const source = { eventId: `initial-${runId}`, shipmentId: id, tipo: "INGRESADO_HUB", ocurridoEn: "2026-10-23T08:00:00Z", secuencia: 2, hubId: "HUB-PRIVATE", zonaId: "ZONA-SUR", repartidorId: "REP-PRIVATE", version: 1, franja: "MANIANA" };
    await database.collection("envios").insertOne({ shipmentId: id, estado: source.tipo, historial: [source], ultimaSecuenciaAplicada: 2, intentos: 0, zonaId: source.zonaId, hubId: source.hubId, promesa: { venceEn: "2026-10-23T20:00:00Z" }, actualizadoEn: new Date(source.ocurridoEn) });
    await database.collection("envios").insertMany(Array.from({ length: 8 }, (_, index) => ({ shipmentId: `RS-DEMO-${index + 1}`, estado: ["CREADO", "CLASIFICADO", "EN_REPARTO", "ENTREGADO", "ENTREGA_FALLIDA"][index % 5], zonaId: index % 2 ? "ZONA-NORTE" : "ZONA-SUR", hubId: "HUB-02", intentos: 1, historial: [], actualizadoEn: new Date("2026-10-23T07:00:00Z") })));
    await database.collection("alertas").insertMany(["MEDIA", "CRITICA", "ALTA"].map((priority, index) => ({ alertaId: `test-alert-${index}`, shipmentId: id, estado: "ABIERTA", prioridad: priority, regla: ["SIN_INGRESO", "VENCIDO", "SIN_SALIDA"][index], causa: "Alerta de verificación", abiertaEn: new Date("2026-10-23T09:00:00Z") })));
    await database.collection("metricas_diarias").insertMany(Array.from({ length: 201 }, (_, index) => ({ fecha: "2026-10-23", zonaId: `ZONA-${index}`, hubId: "HUB-01", franja: "MANIANA", primerosIntentos: index === 0 ? 9 : 1, entregasPrimerIntento: index === 0 ? 1 : 1, fallos: index === 0 ? 8 : 0, motivos: { AUSENTE: index === 0 ? 8 : 0 }, entregasConPromesa: 1, entregasEnPlazo: 1 })));
    await database.collection("metricas_hubs").insertMany([
      { fecha: "2026-10-23", hubId: "HUB-01", zonaId: "ZONA-0", franja: "MANIANA", enviosConSalida: 1, duracionTotalMs: 60000 },
      { fecha: "2026-10-23", hubId: "HUB-02", zonaId: "ZONA-1", franja: "MANIANA", enviosConSalida: 3, duracionTotalMs: 540000 },
    ]);
    const web = launch("web", ["apps/web/node_modules/next/dist/bin/next", "start", "apps/web", "--hostname", "127.0.0.1", "--port", String(webPort)], { ...common, SIMULATOR_CONTROL_URL: `http://127.0.0.1:${controlPort}` });
    const projector = launch("projector", ["services/projector/src/cli.js", "run"], { ...common, PROJECTOR_GROUP_ID: `verify-web-${runId}`, PROJECTOR_FROM_BEGINNING: "false" });
    const control = launch("simulator-control", ["services/simulator/src/control.js"], { ...common, SIMULATOR_CONTROL_HOST: "127.0.0.1", SIMULATOR_CONTROL_PORT: String(controlPort) });
    await waitFor(async () => { try { return (await fetch(`${base}/api/health`)).ok; } catch { return false; } }, "inicio Next.js");
    await waitFor(() => projector.output.includes('"message":"Grupo de proyección unido."') && control.output.includes("Control del simulador listo."), "inicio consumidores y control");
    for (const path of ["/", "/analitica", "/demo", "/tracking", `/envios/${id}`, `/tracking/${id}`]) {
      const response = await fetch(`${base}${path}`); assert.equal(response.status, 200, path); const html = await response.text(); assert.ok(!html.includes("Application error"), path);
    }
    const tracking = await json(`/api/tracking/${id}`);
    assert.deepEqual(Object.keys(tracking.data).sort(), ["estado", "fechaEstimada", "historial", "shipmentId"].sort());
    assert.ok(!JSON.stringify(tracking.data).includes("PRIVATE"));
    const publicHtml = await (await fetch(`${base}/tracking/${id}`)).text(); assert.ok(!publicHtml.includes("HUB-PRIVATE") && !publicHtml.includes("REP-PRIVATE"));
    assert.equal((await fetch(`${base}/api/tracking/not-found`)).status, 404);
    assert.equal((await fetch(`${base}/api/envios/not-found`)).status, 404);
    assert.equal((await json("/api/control?q=.*")).data.total, 0, "La búsqueda debe tratar metacaracteres como texto literal");
    const filtered = (await json("/api/control?estado=EN_REPARTO&zona=ZONA-SUR")).data;
    assert.ok(filtered.shipments.every(value => value.estado === "EN_REPARTO" && value.zonaId === "ZONA-SUR"));
    const overview = (await json("/api/control")).data;
    assert.deepEqual(overview.alerts.map(value => value.prioridad), ["CRITICA", "ALTA", "MEDIA"]);
    const analytics = (await json("/api/analytics")).data;
    assert.equal(analytics.hubTimes.meanMs, 150000, "El promedio global debe ponderar las muestras de cada hub");
    assert.equal(analytics.hubTimes.samples, 4);
    assert.equal(analytics.rows.length, 200); assert.equal(analytics.truncated, true); assert.equal(analytics.totals.primerosIntentos, 209); assert.equal(analytics.totals.entregasPrimerIntento, 201);
    const filteredAnalytics = (await json("/api/analytics?zona=ZONA-0&hub=HUB-01&desde=2026-10-23&hasta=2026-10-23")).data;
    assert.equal(filteredAnalytics.totals.primerosIntentos, 9); assert.equal(filteredAnalytics.totals.entregasPrimerIntento, 1);
    progress("SSR, privacidad, filtros, prioridades y agregados completos verificados.");
    const publicStream = await openStream(`${base}/api/events?scope=tracking&id=${id}`);
    const controlStream = await openStream(`${base}/api/events?scope=control`);
    const publicCount = publicStream.events.length;
    const controlCount = controlStream.events.length;
    await database.collection("envios").updateOne({ shipmentId: "RS-DEMO-1" }, { $set: { actualizadoEn: new Date() } });
    await waitFor(() => controlStream.events.length > controlCount, "SSE de operación");
    assert.equal(publicStream.events.length, publicCount, "El tracking no recibe movimientos de otros envíos");
    let page;
    if (process.env.WEB_VERIFY_BROWSER_MODULE) {
      const { chromium } = await import(process.env.WEB_VERIFY_BROWSER_MODULE);
      browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
      page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
      await page.goto(`${base}/tracking/${id}`);
      await page.locator(".live-indicator.connected").waitFor();
      await page.waitForTimeout(400);
    }
    const before = publicStream.events.length;
    const start = performance.now();
    await producer.send({ topic: TOPICS.states, acks: -1, messages: [{ key: id, value: JSON.stringify({ ...source, eventId: `live-${runId}`, tipo: "EN_REPARTO", intento: 1, secuencia: 3, ocurridoEn: "2026-10-23T10:00:00Z" }) }] });
    await waitFor(async () => publicStream.events.length > before && (await json(`/api/tracking/${id}`)).data.estado === "EN_REPARTO", "Kafka → proyección → SSE", 2000);
    const sseLatencyMs = Math.round(performance.now() - start);
    let screenLatencyMs;
    const continuousLatenciesMs = [];
    if (page) {
      await page.getByRole("heading", { name: "Ya está en camino." }).waitFor({ timeout: 2000 });
      screenLatencyMs = Math.round(performance.now() - start);
      assert.ok(screenLatencyMs < 2000, `Latencia en pantalla: ${screenLatencyMs}ms`);
      const browserErrors = [];
      page.on("pageerror", error => browserErrors.push(error.message));
      await page.goto(base);
      await page.locator(".live-indicator.connected").waitFor();
      await page.context().setOffline(true);
      await page.locator(".live-indicator.reconnecting").waitFor({ timeout: 10000 });
      await page.context().setOffline(false);
      await page.locator(".live-indicator.connected").waitFor({ timeout: 15000 });
      const generator = createContinuousGenerator(loadConfig({}, { seed: runId, shipmentsPerMinute: 20 }));
      let nextScheduled = performance.now();
      for (let index = 0; index < 3; index += 1) {
        await new Promise(resolve => setTimeout(resolve, Math.max(0, nextScheduled - performance.now())));
        const events = generator.next();
        const finalState = events.filter(entry => entry.topic === TOPICS.states && !entry.intentionallyInvalid).sort((a, b) => a.event.secuencia - b.event.secuencia).at(-1).event;
        const started = performance.now();
        await publishEvents(producer, events, { logEvents: false, scenario: "verify-web-continuous" });
        await page.locator("table tbody tr").filter({ hasText: finalState.shipmentId }).getByText(label(finalState.tipo), { exact: true }).waitFor({ timeout: 2000 });
        const latency = Math.round(performance.now() - started);
        assert.ok(latency < 2000, `Flujo continuo: ${latency}ms`);
        continuousLatenciesMs.push(latency);
        nextScheduled += generator.intervalMs;
      }
      await page.screenshot({ path: "/tmp/rutasur-control-desktop.png", fullPage: true });
      await page.setViewportSize({ width: 390, height: 844 });
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), "La vista móvil no debe desbordar horizontalmente");
      await page.screenshot({ path: "/tmp/rutasur-control-mobile.png", fullPage: true });
      await page.goto(`${base}/analitica`);
      await page.getByRole("heading", { name: "Entender para mejorar" }).waitFor();
      await page.goto(`${base}/demo`);
      await page.getByRole("button", { name: "Envío feliz" }).click();
      await page.getByRole("heading", { name: "Escenario publicado ✓" }).waitFor({ timeout: 30000 });
      assert.deepEqual(browserErrors, []);
    }
    for (const value of [...publicStream.events, ...controlStream.events]) assert.ok(!value.includes("PRIVATE") && !value.includes("fullDocument") && !value.includes("resumeToken"));
    assert.equal((await json("/api/demo", { method: "POST", headers: { "Content-Type": "application/json", Origin: "https://invalid.example" }, body: JSON.stringify({ scenario: "happy" }) })).response.status, 403);
    assert.equal((await json("/api/demo", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ scenario: "unknown" }) })).response.status, 400);
    const demo = await json("/api/demo", { method: "POST", headers: { "Content-Type": "application/json", Origin: base }, body: JSON.stringify({ scenario: "happy", seed: runId }) });
    assert.equal(demo.response.status, 200, JSON.stringify(demo.data));
    const demoId = demo.data.shipmentIds[0];
    await waitFor(async () => (await database.collection("envios").findOne({ shipmentId: demoId }))?.estado === "ENTREGADO", "escenario publicado desde panel/proxy/simulador");
    console.log(JSON.stringify({ message: "Etapa 7 verificada.", sseLatencyMs, screenLatencyMs, continuousLatenciesMs, browserVerified: Boolean(page), publicPrivacy: true, demoEndToEnd: true, completeAnalyticsTotals: true }));
  } finally {
    await browser?.close().catch(() => {});
    for (const record of streams) { record.abort.abort(); await record.reader.cancel().catch(() => {}); await record.pump; }
    for (const record of children) if (!record.done) record.child.kill("SIGTERM");
    for (const record of children) if (!record.done) {
      try { await waitFor(() => record.done, "limpieza procesos", 20000); }
      catch { record.child.kill("SIGKILL"); await record.exited; }
    }
    if (database) await database.dropDatabase();
    await producer.disconnect().catch(() => {}); await mongo.close();
  }
}
main().catch(error => { console.error(error.stack ?? error); process.exitCode = 1; });
