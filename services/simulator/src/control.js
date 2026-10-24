import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { loadConfig } from "./config.js";
import { SCENARIOS, generateScenario } from "./generator.js";
import { createKafkaClient, createProducer, publishEvents } from "./kafka.js";
import { SimulatedClock } from "../../../packages/contracts/src/index.js";

const allowed = SCENARIOS.filter(scenario => scenario !== "continuous");
async function main() {
  const config = loadConfig();
  const clock = new SimulatedClock({ startAt: config.startAt, acceleration: config.acceleration });
  const producer = createProducer(createKafkaClient(config));
  const port = Number(process.env.SIMULATOR_CONTROL_PORT ?? 3001);
  const host = process.env.SIMULATOR_CONTROL_HOST ?? "127.0.0.1";
  await producer.connect();
  let busy = false;
  const server = createServer(async (request, response) => {
    function reply(status, value) { response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" }); response.end(JSON.stringify(value)); }
    if (request.method === "GET" && request.url === "/health") return reply(200, { status: "ok" });
    if (request.method !== "POST" || request.url !== "/scenarios") return reply(404, { error: "Ruta no encontrada" });
    if (busy) return reply(409, { error: "Hay un escenario en curso. Esperá a que termine." });
    let ownsJob = false;
    try {
      let raw = "";
      for await (const chunk of request) {
        raw += chunk;
        if (raw.length > 4096) return reply(413, { error: "Solicitud demasiado grande" });
      }
      let body;
      try { body = JSON.parse(raw); } catch { return reply(400, { error: "Solicitud inválida" }); }
      if (!body || !allowed.includes(body.scenario) || (body.seed !== undefined && (typeof body.seed !== "string" || body.seed.length > 100))) return reply(400, { error: "Escenario o semilla inválidos" });
      if (busy) return reply(409, { error: "Hay un escenario en curso. Esperá a que termine." });
      busy = true;
      ownsJob = true;
      const seed = body.seed?.trim() || randomUUID();
      const startAt = clock.now().toISOString();
      const scenarioConfig = loadConfig(process.env, { seed, startAt });
      const events = generateScenario(body.scenario, scenarioConfig);
      await publishEvents(producer, events, { scenario: body.scenario });
      const latest = Math.max(...events.map(entry => Date.parse(entry.event.ocurridoEn)));
      clock.observeEvent(new Date(latest).toISOString());
      reply(200, { scenario: body.scenario, seed, startAt, events: events.length, shipmentIds: [...new Set(events.map(entry => entry.event.shipmentId))] });
    } catch (error) {
      console.error(JSON.stringify({ message: "No se pudo publicar el escenario", error: error.message }));
      reply(503, { error: "No se pudo publicar el escenario. Revisá el simulador." });
    } finally { if (ownsJob) busy = false; }
  });
  server.requestTimeout = 15000;
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(port, host, resolve); });
  console.log(JSON.stringify({ message: "Control del simulador listo.", host, port }));
  let closing = false;
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, async () => {
    if (closing) return;
    closing = true;
    await new Promise(resolve => server.close(resolve));
    await producer.disconnect();
  });
}
main().catch(error => { console.error(error.stack ?? error.message); process.exitCode = 1; });
