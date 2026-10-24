import { getDatabase } from "../../../lib/mongo.js";
export const dynamic = "force-dynamic";
export async function GET() {
  try { await (await getDatabase()).command({ ping: 1 }); return Response.json({ status: "ok" }); }
  catch { return Response.json({ status: "unavailable" }, { status: 503 }); }
}
