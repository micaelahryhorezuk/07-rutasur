import { createHash } from "node:crypto";
import { TOPICS } from "../../packages/contracts/src/index.js";

export function stableId(...parts) {
  return createHash("sha256").update(JSON.stringify(parts)).digest("hex");
}

export function localDate(instant, offsetMinutes = -180) {
  return new Date(Date.parse(instant) + offsetMinutes * 60000).toISOString().slice(0, 10);
}

export function calendarFromEnvironment(env = process.env) {
  const offsetMinutes = Number(env.BUSINESS_UTC_OFFSET_MINUTES ?? -180);
  if (!Number.isInteger(offsetMinutes) || offsetMinutes < -720 || offsetMinutes > 840) {
    throw new Error("BUSINESS_UTC_OFFSET_MINUTES debe estar entre -720 y 840.");
  }
  const holidays = (env.BUSINESS_HOLIDAYS ?? "").split(",").map((value) => value.trim()).filter(Boolean);
  for (const day of holidays) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(day) || new Date(`${day}T00:00:00Z`).toISOString().slice(0, 10) !== day) {
      throw new Error(`Feriado inválido: ${day}`);
    }
  }
  return { offsetMinutes, holidays };
}

export function nextWorkingSlot(event, { offsetMinutes = -180, holidays = [] } = {}) {
  const date = new Date(`${localDate(event.ocurridoEn, offsetMinutes)}T00:00:00Z`);
  do {
    date.setUTCDate(date.getUTCDate() + 1);
  } while ([0, 6].includes(date.getUTCDay()) || holidays.includes(date.toISOString().slice(0, 10)));
  const franja = event.franja === "MANIANA" ? "TARDE" : "MANIANA";
  date.setUTCHours(franja === "MANIANA" ? 9 : 15);
  return { franja, programadaPara: new Date(date.getTime() - offsetMinutes * 60000).toISOString() };
}

function derived(event, service, tipo) {
  return {
    eventId: stableId(service, event.shipmentId, event.eventId),
    shipmentId: event.shipmentId,
    tipo,
    ocurridoEn: event.ocurridoEn,
    secuencia: event.secuencia,
    hubId: event.hubId,
    zonaId: event.zonaId,
    repartidorId: event.repartidorId,
    version: 1,
    causadoPor: { eventId: event.eventId, secuencia: event.secuencia },
    origen: service,
  };
}

export function reschedule(event, calendar) {
  if (event.tipo !== "ENTREGA_FALLIDA") return null;
  let accion = "RETENER";
  let slot = { programadaPara: null, franja: null };
  if (event.intento >= 3 || event.motivo === "RECHAZADO") accion = "DEVOLVER";
  else if (event.motivo === "AUSENTE") {
    accion = "PROGRAMAR";
    slot = nextWorkingSlot(event, calendar);
  }
  return {
    ...derived(event, "reprogramacion", "REPROGRAMACION_DEFINIDA"),
    accion,
    motivo: event.motivo,
    intento: accion === "PROGRAMAR" ? event.intento + 1 : event.intento,
    ...slot,
  };
}

export function notification(event) {
  const failure = event.tipo === "ENTREGA_FALLIDA";
  if (!failure && event.tipo !== "ALERTA_SLA_ABIERTA") return null;
  const result = derived(event, "notificaciones", "NOTIFICACION_REGISTRADA");
  return {
    ...result,
    notificacionId: result.eventId,
    eventoOrigenId: event.eventId,
    categoria: failure ? "FALLO" : "ALERTA_SLA",
    canal: "SIMULADO",
    estado: "REGISTRADA",
    opcionesReprogramacion: failure && event.motivo === "AUSENTE" && event.intento < 3 ? ["MANIANA", "TARDE"] : [],
  };
}

// Contribution of one shipment. Replacing it with a delta makes late promises
// and later attempts correct without counting a first attempt twice.
export function metricContributions(state, offsetMinutes = -180) {
  const entries = new Map();
  function add(event, values) {
    const dimensions = {
      fecha: localDate(event.ocurridoEn, offsetMinutes),
      zonaId: event.zonaId,
      hubId: event.hubId,
      franja: event.franja,
    };
    const key = stableId(dimensions);
    const entry = entries.get(key) ?? { dimensions, values: {} };
    for (const [field, amount] of Object.entries(values)) entry.values[field] = (entry.values[field] ?? 0) + amount;
    entries.set(key, entry);
  }
  const first = state.firstOutcome;
  if (first) add(first, { primerosIntentos: 1, entregasPrimerIntento: first.tipo === "ENTREGADO" ? 1 : 0 });
  for (const failure of state.failures ?? []) add(failure, { fallos: 1, [`motivos.${failure.motivo}`]: 1 });
  if (state.delivery && state.promise) {
    const onTime = Date.parse(state.delivery.ocurridoEn) <= Date.parse(state.promise.venceEn);
    add(state.delivery, { entregasConPromesa: 1, entregasEnPlazo: onTime ? 1 : 0, entregasFueraPlazo: onTime ? 0 : 1 });
  }
  return entries;
}

export function updateAnalytics(state, topic, event) {
  if (topic === TOPICS.promises) {
    if (!state.promise || event.versionPromesa > state.promise.versionPromesa) state.promise = event;
  } else {
    if (event.tipo === "EN_REPARTO") state.currentAttempt = event.intento;
    const attempt = event.intento ?? state.currentAttempt ?? 1;
    if ((event.tipo === "ENTREGADO" || event.tipo === "ENTREGA_FALLIDA") && attempt === 1 && !state.firstOutcome) state.firstOutcome = event;
    if (event.tipo === "ENTREGA_FALLIDA") (state.failures ??= []).push(event);
    if (event.tipo === "ENTREGADO") state.delivery = event;
  }
  return state;
}
