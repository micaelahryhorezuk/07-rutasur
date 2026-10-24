import { getDatabase } from "./mongo.js";
import { STATES } from "./format.js";
import { getHubAnalytics } from "./hubs.js";

const plain = value => JSON.parse(JSON.stringify(value));
const text = value => typeof value === "string" ? value.trim().slice(0, 120) : "";
export function validId(value) { return typeof value === "string" && value.length > 0 && value.length <= 120 && /^[A-Za-z0-9_.:-]+$/.test(value); }
export function controlFilters(input = {}) {
  return { estado: STATES.includes(input.estado) ? input.estado : "", zona: text(input.zona), q: text(input.q), page: Math.min(10000, Math.max(1, Math.floor(Number(input.page)) || 1)) };
}
export function publicShipment(shipment) {
  if (!shipment) return null;
  return {
    shipmentId: shipment.shipmentId,
    estado: shipment.estado,
    fechaEstimada: shipment.reprogramacionActual?.accion === "PROGRAMAR" ? shipment.reprogramacionActual.programadaPara : shipment.promesa?.venceEn ?? null,
    historial: (shipment.historial ?? []).map(event => ({ tipo: event.tipo, ocurridoEn: event.ocurridoEn })),
  };
}
export async function getTracking(id) {
  if (!validId(id)) return null;
  const db = await getDatabase();
  return publicShipment(await db.collection("envios").findOne({ shipmentId: id }, { projection: { _id: 0, shipmentId: 1, estado: 1, promesa: 1, reprogramacionActual: 1, "historial.tipo": 1, "historial.ocurridoEn": 1 } }));
}
export async function getShipment(id) {
  if (!validId(id)) return null;
  const db = await getDatabase();
  const [shipment, alerts, notifications] = await Promise.all([
    db.collection("envios").findOne({ shipmentId: id }, { projection: { _id: 0 } }),
    db.collection("alertas").find({ shipmentId: id }, { projection: { _id: 0 } }).sort({ abiertaEn: -1 }).toArray(),
    db.collection("notificaciones").find({ shipmentId: id }, { projection: { _id: 0 } }).sort({ ocurridaEn: -1 }).toArray(),
  ]);
  return shipment ? plain({ shipment, alerts, notifications }) : null;
}
export async function getControl(input = {}) {
  const filters = controlFilters(input);
  const match = {};
  if (filters.estado) match.estado = filters.estado;
  if (filters.zona) match.zonaId = filters.zona;
  if (filters.q) match.shipmentId = { $regex: filters.q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), $options: "i" };
  const db = await getDatabase();
  const [states, shipments, total, alerts, openAlerts, zones, recent] = await Promise.all([
    db.collection("envios").aggregate([{ $group: { _id: "$estado", count: { $sum: 1 } } }]).toArray(),
    db.collection("envios").find(match, { projection: { _id: 0, shipmentId: 1, estado: 1, zonaId: 1, hubId: 1, intentos: 1, promesa: 1, actualizadoEn: 1, reprogramacionActual: 1 } }).sort({ actualizadoEn: -1, shipmentId: 1 }).skip((filters.page - 1) * 20).limit(20).toArray(),
    db.collection("envios").countDocuments(match),
    db.collection("alertas").aggregate([
      { $match: { estado: "ABIERTA" } },
      { $addFields: { weight: { $switch: { branches: [{ case: { $eq: ["$prioridad", "CRITICA"] }, then: 0 }, { case: { $eq: ["$prioridad", "ALTA"] }, then: 1 }], default: 2 } } } },
      { $sort: { weight: 1, abiertaEn: -1 } }, { $limit: 8 }, { $project: { _id: 0, weight: 0 } },
    ]).toArray(),
    db.collection("alertas").countDocuments({ estado: "ABIERTA" }),
    db.collection("envios").distinct("zonaId"),
    db.collection("envios").aggregate([{ $sort: { actualizadoEn: -1 } }, { $limit: 40 }, { $unwind: "$historial" }, { $sort: { "historial.ocurridoEn": -1 } }, { $limit: 6 }, { $project: { _id: 0, shipmentId: 1, tipo: "$historial.tipo", ocurridoEn: "$historial.ocurridoEn" } }]).toArray(),
  ]);
  return plain({ filters, counts: Object.fromEntries(states.map(item => [item._id ?? "SIN_ESTADO", item.count])), shipments, total, alerts, openAlerts, zones: zones.filter(Boolean).sort(), recent });
}
export function analyticsFilters(input = {}) {
  const validDate = value => /^\d{4}-\d{2}-\d{2}$/.test(value ?? "") && Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0, 10) === value ? value : "";
  const filters = { zona: text(input.zona), hub: text(input.hub), desde: validDate(input.desde), hasta: validDate(input.hasta) };
  if (filters.desde && filters.hasta && filters.desde > filters.hasta) [filters.desde, filters.hasta] = [filters.hasta, filters.desde];
  return filters;
}
export async function getAnalytics(input = {}) {
  const filters = analyticsFilters(input);
  const match = {};
  if (filters.zona) match.zonaId = filters.zona;
  if (filters.hub) match.hubId = filters.hub;
  if (filters.desde || filters.hasta) match.fecha = { ...(filters.desde ? { $gte: filters.desde } : {}), ...(filters.hasta ? { $lte: filters.hasta } : {}) };
  const db = await getDatabase();
  const fields = ["primerosIntentos", "entregasPrimerIntento", "fallos", "entregasConPromesa", "entregasEnPlazo", "entregasFueraPlazo"];
  const motives = ["AUSENTE", "DIRECCION_INCORRECTA", "RECHAZADO", "ZONA_INACCESIBLE"];
  const totalGroup = { _id: null, ...Object.fromEntries(fields.map(field => [field, { $sum: `$${field}` }])), ...Object.fromEntries(motives.map(reason => [reason, { $sum: `$motivos.${reason}` }])) };
  const [rows, zones, hubs, totalsRows, grouped, rowCount, hubTimes] = await Promise.all([
    db.collection("metricas_diarias").find(match, { projection: { _id: 0 } }).sort({ fecha: -1, zonaId: 1, hubId: 1, franja: 1 }).limit(200).toArray(),
    db.collection("metricas_diarias").distinct("zonaId"), db.collection("metricas_diarias").distinct("hubId"),
    db.collection("metricas_diarias").aggregate([{ $match: match }, { $group: totalGroup }]).toArray(),
    db.collection("metricas_diarias").aggregate([{ $match: match }, { $group: { _id: { zonaId: "$zonaId", franja: "$franja" }, primerosIntentos: { $sum: "$primerosIntentos" }, entregasPrimerIntento: { $sum: "$entregasPrimerIntento" }, fallos: { $sum: "$fallos" } } }, { $sort: { fallos: -1 } }]).toArray(),
    db.collection("metricas_diarias").countDocuments(match),
    getHubAnalytics(db, match),
  ]);
  const source = totalsRows[0] ?? {};
  const totals = { ...Object.fromEntries(fields.map(field => [field, source[field] ?? 0])), motivos: Object.fromEntries(motives.map(reason => [reason, source[reason] ?? 0])) };
  const groups = grouped.map(({ _id, ...group }) => ({ ..._id, ...group }));
  return plain({ filters, totals, rows, groups, zones: [...new Set([...zones.filter(Boolean), ...hubTimes.zones])].sort(), hubs: [...new Set([...hubs.filter(Boolean), ...hubTimes.hubs])].sort(), truncated: rowCount > 200, hubTimes });
}
