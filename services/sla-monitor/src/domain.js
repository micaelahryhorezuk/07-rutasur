import { TOPICS } from "../../../packages/contracts/src/index.js";
import { localDate, stableId } from "../../business/domain.js";

const HOUR = 3600000;
export const SLA_RULES = Object.freeze({
  SIN_INGRESO: { prioridad: "MEDIA", causa: "Faltan menos de 10 horas y no ingresó al hub" },
  SIN_SALIDA: { prioridad: "ALTA", causa: "Faltan menos de 4 horas y no está en reparto" },
  FALLO_DIA_LIMITE: { prioridad: "ALTA", causa: "Entrega fallida en el día de vencimiento de la promesa" },
  VENCIDO: { prioridad: "CRITICA", causa: "La promesa venció sin entrega" },
});

export function updateSlaState(state, topic, event) {
  if (topic === TOPICS.promises) {
    if (state.promise && state.promise.versionPromesa >= event.versionPromesa) return "older_promise_version";
    state.promise = event;
  } else {
    if (Number.isInteger(state.lastSequence) && event.secuencia <= state.lastSequence) return "out_of_order";
    state.lastSequence = event.secuencia;
    state.lastState = event;
    if (["INGRESADO_HUB", "CLASIFICADO", "EN_REPARTO", "ENTREGADO", "ENTREGA_FALLIDA"].includes(event.tipo)) state.enteredHub = true;
    if (event.tipo === "ENTREGA_FALLIDA") state.lastFailure = event;
    if (event.tipo === "ENTREGADO") state.delivered = true;
  }
  return "applied";
}

export function matchingSlaRules(state, now, offsetMinutes = -180) {
  if (!state.promise || state.delivered) return [];
  const remaining = Date.parse(state.promise.venceEn) - Date.parse(now);
  const rules = [];
  if (remaining >= 0 && remaining < 10 * HOUR && !state.enteredHub) rules.push("SIN_INGRESO");
  if (remaining >= 0 && remaining < 4 * HOUR && state.lastState?.tipo !== "EN_REPARTO") rules.push("SIN_SALIDA");
  if (state.lastFailure && localDate(state.lastFailure.ocurridoEn, offsetMinutes) === localDate(state.promise.venceEn, offsetMinutes)) rules.push("FALLO_DIA_LIMITE");
  if (remaining < 0) rules.push("VENCIDO");
  return rules;
}

function makeAlert(state, rule, now, closed) {
  const causal = state.lastState ?? state.promise;
  const alertaId = stableId("monitor-sla", state._id, rule);
  return {
    eventId: stableId(alertaId, closed ? "closed" : "opened"),
    shipmentId: state._id,
    tipo: closed ? "ALERTA_SLA_CERRADA" : "ALERTA_SLA_ABIERTA",
    ocurridoEn: now,
    secuencia: causal.secuencia,
    hubId: causal.hubId,
    zonaId: causal.zonaId,
    repartidorId: causal.repartidorId,
    version: 1,
    alertaId,
    regla: rule,
    prioridad: SLA_RULES[rule].prioridad,
    causa: closed ? "Envío entregado" : SLA_RULES[rule].causa,
    causadoPor: { eventId: causal.eventId, secuencia: causal.secuencia },
    origen: "monitor-sla",
  };
}

// An emitted rule stays recorded for the shipment's lifetime, including after
// closure or a promise revision. Later ticks never reopen it.
export function evaluateSla(state, now, offsetMinutes = -180) {
  state.alerts ??= {};
  const outputs = [];
  if (state.delivered) {
    for (const [rule, alert] of Object.entries(state.alerts)) {
      if (!alert.closed) {
        const event = makeAlert(state, rule, now, true);
        alert.closed = true;
        alert.closedAt = now;
        outputs.push(event);
      }
    }
  } else {
    for (const rule of matchingSlaRules(state, now, offsetMinutes)) {
      if (!state.alerts[rule]) {
        const event = makeAlert(state, rule, now, false);
        state.alerts[rule] = { alertaId: event.alertaId, openedAt: now, closed: false };
        outputs.push(event);
      }
    }
  }
  return outputs;
}
