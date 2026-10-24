export async function getHubAnalytics(database, match) {
  const active = await database.collection("vistas_activas").findOne({ _id: "hubs" });
  const generation = active?.generation ?? "main";
  const name = active?.collection ?? "metricas_hubs";
  if (!/^metricas_hubs(?:__[A-Za-z0-9_-]{1,64})?$/.test(name)) throw new Error("Vista de hubs inválida");
  const [progress, rows, zones, hubs] = await Promise.all([
    database.collection("reprocesamientos_hubs").findOne({ _id: generation }),
    database.collection(name).aggregate([
      { $match: match },
      { $group: { _id: "$hubId", samples: { $sum: "$enviosConSalida" }, durationMs: { $sum: "$duracionTotalMs" } } },
      { $match: { samples: { $gt: 0 } } }, { $sort: { _id: 1 } },
    ]).toArray(),
    database.collection(name).distinct("zonaId"),
    database.collection(name).distinct("hubId"),
  ]);
  const samples = rows.reduce((sum, row) => sum + row.samples, 0);
  const durationMs = rows.reduce((sum, row) => sum + row.durationMs, 0);
  return { status: progress?.status ?? "NO_INICIADO", samples, durationMs, meanMs: samples ? durationMs / samples : null, zones: zones.filter(Boolean), hubs: hubs.filter(Boolean), rows: rows.map(row => ({ hubId: row._id, samples: row.samples, durationMs: row.durationMs, meanMs: row.durationMs / row.samples })) };
}
