import { getShipment } from "../../../../lib/data.js";
export const dynamic = "force-dynamic";
export async function GET(request, { params }) {
  try {
    const { id } = await params;
    const value = await getShipment(id);
    return Response.json(value ?? { error: "Envío no encontrado" }, { status: value ? 200 : 404, headers: { "Cache-Control": "no-store" } });
  } catch { return Response.json({ error: "No se pudo consultar el envío." }, { status: 503 }); }
}
