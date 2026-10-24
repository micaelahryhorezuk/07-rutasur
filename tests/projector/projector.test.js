import assert from "node:assert/strict";
import test from "node:test";
import { hasReachedReplayCutoff } from "../../services/projector/src/consumer.js";
import {
  collectionNamesForGeneration,
  isStateSequenceStale,
  notificationIdentity,
  PROCESSED_TTL_SECONDS_EXPORT,
} from "../../services/projector/src/store.js";

test("el replay espera los lotes pendientes aunque el broker anuncie todo el histórico", () => {
  assert.equal(hasReachedReplayCutoff({ highWatermark: "100", messages: [{ offset: "49" }] }, 100n), false);
  assert.equal(hasReachedReplayCutoff({ highWatermark: "100", messages: [{ offset: "99" }] }, 100n), true);
  assert.equal(hasReachedReplayCutoff({ highWatermark: "100", messages: [] }, 100n), false);
  assert.equal(hasReachedReplayCutoff({ messages: [{ offset: "99" }] }, undefined), false);
  assert.equal(hasReachedReplayCutoff({ messages: [{ offset: "9007199254740993" }] }, 9007199254740994n), true);
});

test("secuencia igual o menor se descarta solo dentro del stream de estados", () => {
  assert.equal(isStateSequenceStale(null, 0), false);
  assert.equal(isStateSequenceStale(4, 4), true);
  assert.equal(isStateSequenceStale(4, 3), true);
  assert.equal(isStateSequenceStale(4, 5), false);
});

test("las identidades de notificación deduplican por causa y categoría", () => {
  assert.equal(
    notificationIdentity("RS-1", "failure-1", "FALLO"),
    notificationIdentity("RS-1", "failure-1", "FALLO"),
  );
  assert.notEqual(
    notificationIdentity("RS-1", "failure-1", "FALLO"),
    notificationIdentity("RS-1", "failure-1", "ALERTA_SLA"),
  );
});

test("generaciones de replay escriben colecciones aisladas y TTL es de siete días", () => {
  assert.equal(collectionNamesForGeneration("main").shipments, "envios");
  assert.notEqual(
    collectionNamesForGeneration("replay-a").shipments,
    collectionNamesForGeneration("replay-b").shipments,
  );
  assert.equal(PROCESSED_TTL_SECONDS_EXPORT, 604800);
});
