import assert from "node:assert/strict";
import test from "node:test";
import { evaluateSla, matchingSlaRules, updateSlaState } from "../../services/sla-monitor/src/domain.js";
import { SimulatedClock, TOPICS, validateEvent } from "../../packages/contracts/src/index.js";

const now = "2026-10-23T08:00:00.000Z";
function event(extra = {}) {
  return { eventId: "source-1", shipmentId: "RS-SLA", tipo: "CREADO", secuencia: 1, ocurridoEn: now, hubId: "HUB-1", zonaId: "SUR", repartidorId: null, version: 1, franja: "MANIANA", ...extra };
}
function state(due, tipo = "CREADO") {
  const value = { _id: "RS-SLA" };
  updateSlaState(value, TOPICS.states, event({ tipo }));
  updateSlaState(value, TOPICS.promises, event({ eventId: "promise-1", tipo: "PROMESA_CREADA", versionPromesa: 1, venceEn: due }));
  return value;
}

test("umbrales estrictos de 10 h, 4 h y vencimiento con prioridades del PDF", () => {
  const noHub = state("2026-10-23T18:00:00.000Z");
  assert.deepEqual(matchingSlaRules(noHub, now), []);
  assert.deepEqual(matchingSlaRules(noHub, "2026-10-23T08:00:00.001Z"), ["SIN_INGRESO"]);
  const hub = state("2026-10-23T12:00:00.000Z", "INGRESADO_HUB");
  assert.deepEqual(matchingSlaRules(hub, now), []);
  assert.deepEqual(matchingSlaRules(hub, "2026-10-23T08:00:00.001Z"), ["SIN_SALIDA"]);
  assert.deepEqual(matchingSlaRules(hub, "2026-10-23T12:00:00.001Z"), ["VENCIDO"]);
  const route = state(now, "EN_REPARTO");
  assert.deepEqual(matchingSlaRules(route, now), []);
  assert.deepEqual(matchingSlaRules(route, "2026-10-23T08:00:00.001Z"), ["VENCIDO"]);
  const cases = [[noHub, "2026-10-23T09:00:00Z", "MEDIA"], [hub, "2026-10-23T09:00:00Z", "ALTA"], [route, "2026-10-23T09:00:00Z", "CRITICA"]];
  for (const [value, at, priority] of cases) {
    const [alert] = evaluateSla(value, at);
    assert.equal(alert.prioridad, priority);
    assert.equal(validateEvent(TOPICS.slaAlerts, alert).valid, true);
  }
});

test("fallo en día límite usa la fecha local del evento, incluso si llegó tarde", () => {
  const value = state("2026-10-24T01:00:00Z", "ENTREGA_FALLIDA");
  assert.ok(matchingSlaRules(value, now).includes("FALLO_DIA_LIMITE"));
  assert.ok(!matchingSlaRules(value, now, 0).includes("FALLO_DIA_LIMITE"));
  const [alert] = evaluateSla(value, now);
  assert.equal(alert.regla, "FALLO_DIA_LIMITE");
  assert.equal(alert.prioridad, "ALTA");
});

test("una regla se emite una sola vez, cierra al entregar y no reabre tras revisión", () => {
  const value = state("2026-10-23T09:00:00Z");
  const opened = evaluateSla(value, now);
  assert.equal(opened.length, 2);
  assert.deepEqual(evaluateSla(value, now), []);
  updateSlaState(value, TOPICS.states, event({ eventId: "delivered", tipo: "ENTREGADO", secuencia: 5 }));
  const closed = evaluateSla(value, "2026-10-23T08:30:00Z");
  assert.equal(closed.length, 2);
  for (const alert of closed) {
    assert.equal(alert.tipo, "ALERTA_SLA_CERRADA");
    assert.equal(alert.alertaId, opened.find(item => item.regla === alert.regla).alertaId);
    assert.notEqual(alert.eventId, opened.find(item => item.regla === alert.regla).eventId);
    assert.equal(validateEvent(TOPICS.slaAlerts, alert).valid, true);
  }
  updateSlaState(value, TOPICS.promises, event({ tipo: "PROMESA_ACTUALIZADA", versionPromesa: 2, venceEn: now }));
  assert.deepEqual(evaluateSla(value, "2026-10-24T08:00:00Z"), []);
  assert.deepEqual(evaluateSla(structuredClone(value), "2026-10-24T09:00:00Z"), []);
});

test("orden entre topics, versiones de promesa y secuencias antiguas no deshacen el estado", () => {
  const value = { _id: "RS-SLA" };
  updateSlaState(value, TOPICS.states, event({ tipo: "ENTREGADO", secuencia: 5 }));
  updateSlaState(value, TOPICS.promises, event({ tipo: "PROMESA_CREADA", versionPromesa: 2, venceEn: now }));
  assert.equal(updateSlaState(value, TOPICS.states, event({ tipo: "CREADO", secuencia: 1 })), "out_of_order");
  assert.equal(updateSlaState(value, TOPICS.promises, event({ tipo: "PROMESA_CREADA", versionPromesa: 1, venceEn: now })), "older_promise_version");
  assert.deepEqual(evaluateSla(value, now), []);
  const classified = state("2026-10-23T15:00:00Z", "CLASIFICADO");
  assert.deepEqual(matchingSlaRules(classified, now), []);
  assert.deepEqual(matchingSlaRules({ _id: "without-promise" }, now), []);
});

test("el reloj avanza sin eventos y reanuda el tiempo transcurrido tras reinicio", () => {
  let wall = 0;
  let monotonic = 0;
  const clock = new SimulatedClock({ startAt: now, acceleration: 120, wallNow: () => wall, monotonicNow: () => monotonic });
  const value = state("2026-10-23T12:01:00Z", "INGRESADO_HUB");
  assert.deepEqual(evaluateSla(value, clock.now().toISOString()), []);
  monotonic = wall = 1000;
  assert.equal(evaluateSla(value, clock.now().toISOString())[0].regla, "SIN_SALIDA");
  const snapshot = clock.snapshot();
  wall += 120000;
  const resumed = new SimulatedClock({ startAt: snapshot.simulatedAt, snapshot, acceleration: 120, wallNow: () => wall, monotonicNow: () => monotonic });
  assert.ok(evaluateSla(value, resumed.now().toISOString()).some(alert => alert.regla === "VENCIDO"));
  const before = resumed.now().getTime();
  resumed.observeEvent("2026-10-22T08:00:00Z", wall);
  assert.ok(resumed.now().getTime() >= before);
});
