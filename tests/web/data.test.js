import assert from "node:assert/strict";
import test from "node:test";
import { publicShipment, validId, controlFilters, analyticsFilters } from "../../apps/web/lib/data.js";
import { percent } from "../../apps/web/lib/format.js";

test("el tracking público no contiene dimensiones, alertas, causas ni IDs internos", () => {
  const source = { shipmentId: "RS-1", estado: "EN_REPARTO", _id: "secret", hubId: "hub", zonaId: "zona", repartidorId: "repartidor", intentos: 3, promesa: { venceEn: "2026-10-23T20:00:00Z", eventId: "secret" }, historial: [{ tipo: "CREADO", ocurridoEn: "2026-10-23T10:00:00Z", eventId: "secret", motivo: "AUSENTE", secuencia: 5, hubId: "secret" }], reprogramacionActual: { accion: "PROGRAMAR", programadaPara: "2026-10-26T12:00:00Z", motivo: "AUSENTE" } };
  assert.deepEqual(publicShipment(source), { shipmentId: "RS-1", estado: "EN_REPARTO", fechaEstimada: "2026-10-26T12:00:00Z", historial: [{ tipo: "CREADO", ocurridoEn: "2026-10-23T10:00:00Z" }] });
  assert.equal(publicShipment(null), null);
  assert.ok(!JSON.stringify(publicShipment(source)).includes("secret"));
});
test("los filtros aceptan texto acotado y descartan operadores y estados desconocidos", () => {
  assert.deepEqual(controlFilters({ estado: { $ne: "ENTREGADO" }, zona: { $ne: null }, q: ["RS"], page: -2 }), { estado: "", zona: "", q: "", page: 1 });
  assert.equal(controlFilters({ page: "200000" }).page, 10000);
  assert.equal(controlFilters({ estado: "ENTREGADO" }).estado, "ENTREGADO");
  assert.ok(validId("RS-HAPPY-ABC-1"));
  assert.ok(!validId("../admin"));
  assert.ok(!validId("x".repeat(121)));
});
test("filtros de analítica normalizan rangos y tasas vacías no inventan porcentajes", () => {
  assert.deepEqual(analyticsFilters({ desde: "2026-10-24", hasta: "2026-10-22", zona: " SUR ", hub: "HUB-1" }), { desde: "2026-10-22", hasta: "2026-10-24", zona: "SUR", hub: "HUB-1" });
  assert.equal(analyticsFilters({ desde: "bad" }).desde, "");
  assert.equal(analyticsFilters({ desde: "2026-02-30" }).desde, "");
  assert.equal(percent(0, 0), "Sin datos");
  assert.equal(percent(1, 4).replace(/\s/g, ""), "25%");
});
