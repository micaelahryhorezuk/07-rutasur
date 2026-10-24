import { getDatabase } from "../../../lib/mongo.js";
import { validId } from "../../../lib/data.js";
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  const { searchParams } = new URL(request.url);
  const scope = searchParams.get("scope") ?? "control";
  const id = searchParams.get("id");
  if (!["control", "shipment", "tracking"].includes(scope) || (scope !== "control" && !validId(id))) return Response.json({ error: "Seguimiento inválido" }, { status: 400 });
  let changes;
  let first;
  try {
    const db = await getDatabase();
    const collections = scope === "tracking" ? ["envios"] : ["envios", "alertas", "notificaciones"];
    const match = { "ns.coll": { $in: collections } };
    if (scope !== "control") {
      const existing = await db.collection("envios").findOne({ shipmentId: id }, { projection: { _id: 1 } });
      match.$or = [{ "fullDocument.shipmentId": id }];
      if (scope === "shipment") match.$or.push({ operationType: "delete" });
      else if (existing) match.$or.push({ operationType: "delete", "documentKey._id": existing._id });
    }
    changes = db.watch([{ $match: match }], { fullDocument: "updateLookup", maxAwaitTimeMS: 250 });
    // Open the cursor before announcing readiness; the client refreshes once
    // at that point to cover changes between server render and subscription.
    first = await changes.tryNext();
  } catch {
    await changes?.close().catch(() => {});
    return Response.json({ error: "Las actualizaciones en vivo no están disponibles." }, { status: 503 });
  }
  let closed = false;
  let heartbeat;
  let controller;
  const encoder = new TextEncoder();
  async function cleanup() {
    if (closed) return;
    closed = true;
    clearInterval(heartbeat);
    request.signal.removeEventListener("abort", abort);
    await changes.close().catch(() => {});
  }
  function abort() {
    void cleanup();
    try { controller?.close(); } catch {}
  }
  function send(value) {
    if (!closed) {
      try { controller.enqueue(encoder.encode(value)); } catch { abort(); }
    }
  }
  const body = new ReadableStream({
    start(value) {
      controller = value;
      request.signal.addEventListener("abort", abort, { once: true });
      if (request.signal.aborted) { abort(); return; }
      send('retry: 3000\nevent: ready\ndata: {"connected":true}\n\n');
      if (first) send('event: changed\ndata: {"changed":true}\n\n');
      heartbeat = setInterval(() => { try { send(": heartbeat\n\n"); } catch { abort(); } }, 15000);
      void (async () => {
        try {
          for await (const change of changes) {
            if (closed) break;
            // Never send database documents, resume tokens or internal fields.
            send('event: changed\ndata: {"changed":true}\n\n');
          }
        } catch {
          if (!closed) send('event: unavailable\ndata: {"connected":false}\n\n');
        } finally {
          await cleanup();
          try { controller.close(); } catch {}
        }
      })();
    },
    async cancel() { await cleanup(); },
  });
  return new Response(body, { headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no", Connection: "keep-alive" } });
}
