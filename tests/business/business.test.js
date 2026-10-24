import assert from "node:assert/strict";
import test from "node:test";
import { calendarFromEnvironment, localDate, metricContributions, notification, reschedule, updateAnalytics } from "../../services/business/domain.js";
import { decodeBusinessMessage } from "../../services/business/runtime.js";
import { TOPICS, validateEvent } from "../../packages/contracts/src/index.js";

function failure(extra = {}) {
  return { eventId: "failure-1", shipmentId: "RS-TEST", tipo: "ENTREGA_FALLIDA", ocurridoEn: "2026-10-23T21:00:00Z", secuencia: 5, hubId: "HUB-1", zonaId: "SUR", repartidorId: "REP-1", version: 1, franja: "TARDE", motivo: "AUSENTE", intento: 1, ...extra };
}

test("ausente: día hábil siguiente, franja opuesta y calendario local configurable", () => {
  const result = reschedule(failure(), { offsetMinutes: -180, holidays: ["2026-10-26"] });
  assert.equal(result.programadaPara, "2026-10-27T12:00:00.000Z");
  assert.equal(result.franja, "MANIANA");
  assert.equal(result.intento, 2);
  const morning = reschedule(failure({ franja: "MANIANA", ocurridoEn: "2026-10-24T01:00:00Z" }));
  assert.equal(morning.programadaPara, "2026-10-26T18:00:00.000Z");
  assert.equal(morning.franja, "TARDE");
  assert.equal(localDate("2026-10-24T01:00:00Z"), "2026-10-23");
});

test("cada motivo aplica su política y el tercer intento siempre devuelve", () => {
  const expected = { AUSENTE: "PROGRAMAR", DIRECCION_INCORRECTA: "RETENER", RECHAZADO: "DEVOLVER", ZONA_INACCESIBLE: "RETENER" };
  for (const [motivo, action] of Object.entries(expected)) {
    const result = reschedule(failure({ motivo }));
    assert.equal(result.accion, action);
    assert.equal(validateEvent(TOPICS.reschedules, result).valid, true);
    if (action !== "PROGRAMAR") assert.equal(result.programadaPara, null);
    assert.equal(reschedule(failure({ motivo, intento: 3 })).accion, "DEVOLVER");
  }
  assert.equal(reschedule(failure({ tipo: "CREADO" })), null);
  assert.deepEqual(reschedule(failure()), reschedule(failure()));
});

test("avisos solo para fallos y aperturas, IDs estables y opciones según política", () => {
  const result = notification(failure());
  assert.equal(validateEvent(TOPICS.notifications, result).valid, true);
  assert.deepEqual(result.opcionesReprogramacion, ["MANIANA", "TARDE"]);
  assert.deepEqual(result, notification(failure()));
  assert.deepEqual(notification(failure({ intento: 3 })).opcionesReprogramacion, []);
  assert.equal(notification(failure({ tipo: "ALERTA_SLA_CERRADA" })), null);
  assert.equal(notification(failure({ tipo: "ALERTA_SLA_ABIERTA" })).categoria, "ALERTA_SLA");
});

test("analítica infiere intento de ENTREGADO, separa fallos y espera una promesa tardía", () => {
  let state = {};
  state = updateAnalytics(state, TOPICS.states, failure());
  state = updateAnalytics(state, TOPICS.states, failure({ tipo: "EN_REPARTO", intento: 2 }));
  state = updateAnalytics(state, TOPICS.states, failure({ tipo: "ENTREGADO", intento: undefined, ocurridoEn: "2026-10-23T22:00:00Z" }));
  let values = [...metricContributions(state).values()][0].values;
  assert.equal(values.primerosIntentos, 1);
  assert.equal(values.entregasPrimerIntento, 0);
  assert.equal(values.fallos, 1);
  assert.equal(values["motivos.AUSENTE"], 1);
  assert.equal(values.entregasConPromesa, undefined);
  updateAnalytics(state, TOPICS.promises, { versionPromesa: 2, venceEn: "2026-10-23T21:30:00Z" });
  updateAnalytics(state, TOPICS.promises, { versionPromesa: 1, venceEn: "2026-10-24T21:30:00Z" });
  values = [...metricContributions(state).values()][0].values;
  assert.equal(values.entregasFueraPlazo, 1);
  updateAnalytics(state, TOPICS.promises, { versionPromesa: 3, venceEn: "2026-10-24T21:30:00Z" });
  values = [...metricContributions(state).values()][0].values;
  assert.equal(values.entregasEnPlazo, 1);
  assert.equal(values.entregasFueraPlazo, 0);
  const happy = updateAnalytics({}, TOPICS.states, failure({ tipo: "ENTREGADO", intento: undefined }));
  assert.equal([...metricContributions(happy).values()][0].values.entregasPrimerIntento, 1);
});

test("dimensiones incluyen hub, fecha local y franja; no se mezclan hubs", () => {
  const state = { failures: [failure(), failure({ hubId: "HUB-2" })] };
  assert.equal(metricContributions(state).size, 2);
  assert.throws(() => calendarFromEnvironment({ BUSINESS_UTC_OFFSET_MINUTES: "oops" }));
  assert.throws(() => calendarFromEnvironment({ BUSINESS_HOLIDAYS: "2026-02-30" }));
});

test("cada consumidor deriva inválidos a DLQ con identidad estable y payload original", () => {
  for (const raw of ["{", JSON.stringify({ shipmentId: "RS-TEST" }), "null"]) {
    const message = { value: Buffer.from(raw), key: null, offset: "19", timestamp: "0" };
    const decoded = decodeBusinessMessage("analitica", TOPICS.states, 1, message);
    assert.equal(decoded.invalid, true);
    assert.equal(decoded.event.payloadOriginal, raw);
    assert.equal(validateEvent(TOPICS.deadLetter, decoded.event).valid, true);
    assert.deepEqual(decoded, decodeBusinessMessage("analitica", TOPICS.states, 1, message));
    assert.notEqual(decoded.event.falloId, decodeBusinessMessage("notificaciones", TOPICS.states, 1, message).event.falloId);
  }
});
