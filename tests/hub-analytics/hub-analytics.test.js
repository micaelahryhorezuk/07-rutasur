import assert from "node:assert/strict";
import test from "node:test";
import { rememberTransition, hubContribution, generationSuffix, reachedCutoff } from "../../services/hub-analytics/src/domain.js";
import { duration } from "../../apps/web/lib/format.js";
const entry = { tipo: "INGRESADO_HUB", secuencia: 2, ocurridoEn: "2026-10-23T10:00:00Z", hubId: "HUB-1", zonaId: "SUR", franja: "MANIANA" };
const departure = { ...entry, tipo: "EN_REPARTO", secuencia: 4, ocurridoEn: "2026-10-23T12:00:00Z" };
test("la primera pareja aporta una muestra, aunque llegue invertida o duplicada", () => {
  const state = {};
  rememberTransition(state, departure);
  assert.equal(hubContribution(state), null);
  rememberTransition(state, entry);
  rememberTransition(state, entry);
  rememberTransition(state, { ...departure, secuencia: 6, ocurridoEn: "2026-10-24T12:00:00Z" });
  assert.equal(state.transitions.length, 3);
  assert.deepEqual(hubContribution(state), { dimensions: { fecha: "2026-10-23", hubId: "HUB-1", zonaId: "SUR", franja: "MANIANA" }, duration: 7200000 });
});
test("una fase anterior tardía corrige la contribución; no se cuentan intervalos negativos", () => {
  const state = {};
  rememberTransition(state, { ...entry, secuencia: 3, ocurridoEn: "2026-10-23T11:00:00Z" });
  rememberTransition(state, departure);
  assert.equal(hubContribution(state).duration, 3600000);
  rememberTransition(state, entry);
  assert.equal(hubContribution(state).duration, 7200000);
  assert.equal(hubContribution({ transitions: [entry, { ...departure, ocurridoEn: "2026-10-23T09:00:00Z" }] }), null);
  assert.equal(hubContribution({ transitions: [entry] }), null);
});
test("la dimensión toma la fecha local de salida y el hub de ingreso", () => {
  const state = { transitions: [entry, { ...departure, hubId: "HUB-2", ocurridoEn: "2026-10-24T01:00:00Z" }] };
  assert.equal(hubContribution(state).dimensions.hubId, "HUB-1");
  assert.equal(hubContribution(state).dimensions.fecha, "2026-10-23");
});
test("replay usa generaciones distintas y decide su final por offsets procesados", () => {
  assert.equal(generationSuffix("main"), "");
  assert.notEqual(generationSuffix("replay-a"), generationSuffix("replay-b"));
  assert.throws(() => generationSuffix("../main"));
  assert.equal(reachedCutoff("49", "100"), false);
  assert.equal(reachedCutoff("99", "100"), true);
  assert.equal(reachedCutoff("9007199254740993", "9007199254740994"), true);
  assert.equal(duration(null), "Sin datos");
  assert.equal(duration(7200000), "2 h");
});
