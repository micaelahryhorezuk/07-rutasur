export const runtime = "nodejs";
const scenarios = ["happy", "absent", "delayed", "third-failure", "burst", "duplicate", "out-of-order", "malformed"];
export async function POST(request) {
  const origin = request.headers.get("origin");
  if (origin) {
    try {
      const supplied = new URL(origin);
      const host = request.headers.get("host") ?? new URL(request.url).host;
      if (!["http:", "https:"].includes(supplied.protocol) || supplied.host !== host) return Response.json({ error: "Origen inválido" }, { status: 403 });
    } catch { return Response.json({ error: "Origen inválido" }, { status: 403 }); }
  }
  let body;
  try { body = await request.json(); } catch { return Response.json({ error: "Solicitud inválida" }, { status: 400 }); }
  if (!body || !scenarios.includes(body.scenario) || (body.seed !== undefined && (typeof body.seed !== "string" || body.seed.length > 100))) return Response.json({ error: "Escenario o semilla inválidos" }, { status: 400 });
  try {
    const result = await fetch(`${process.env.SIMULATOR_CONTROL_URL ?? "http://127.0.0.1:3001"}/scenarios`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ scenario: body.scenario, seed: body.seed }), signal: AbortSignal.timeout(60000), cache: "no-store" });
    return Response.json(await result.json(), { status: result.status });
  } catch { return Response.json({ error: "El simulador no está disponible. Iniciá el control del simulador y volvé a intentar." }, { status: 503 }); }
}
