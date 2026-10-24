export async function respond(read) {
  try { return Response.json(await read(), { headers: { "Cache-Control": "no-store" } }); }
  catch (error) { console.error("Consulta de proyecciones fallida:", error.message); return Response.json({ error: "No se pudo consultar la operación. Intentá nuevamente." }, { status: 503 }); }
}
