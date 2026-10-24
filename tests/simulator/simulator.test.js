import assert from "node:assert/strict";
import test from "node:test";
import {
  SimulatedClock,
  validateEvent,
  TOPICS,
} from "../../packages/contracts/src/index.js";
import { loadConfig } from "../../services/simulator/src/config.js";
import {
  createContinuousGenerator,
  generateScenario,
} from "../../services/simulator/src/generator.js";

const testConfig = loadConfig({}, {
  seed: "test-seed",
  startAt: "2026-10-18T08:00:00.000Z",
});

test("misma configuración produce idénticos eventos, IDs y tiempos", () => {
  const firstRun = generateScenario("third-failure", testConfig);
  const secondRun = generateScenario("third-failure", testConfig);
  assert.deepEqual(firstRun, secondRun);
});

test("una nueva fecha/configuración evita colisiones de eventId", () => {
  const firstEvent = generateScenario("happy", testConfig)[0].event;
  const laterConfig = loadConfig({}, {
    seed: "test-seed",
    startAt: "2026-10-19T08:00:00.000Z",
  });
  const laterEvent = generateScenario("happy", laterConfig)[0].event;
  assert.notEqual(firstEvent.eventId, laterEvent.eventId);
  assert.notEqual(firstEvent.shipmentId, laterEvent.shipmentId);
});

test("agenda continua progresa en tiempo simulado según frecuencia y aceleración", () => {
  const generator = createContinuousGenerator(testConfig);
  const firstPromise = generator.next().find((entry) => entry.topic === TOPICS.promises);
  const secondPromise = generator.next().find((entry) => entry.topic === TOPICS.promises);
  const delta = Date.parse(secondPromise.event.ocurridoEn) - Date.parse(firstPromise.event.ocurridoEn);
  assert.equal(delta, (60_000 / testConfig.shipmentsPerMinute) * testConfig.acceleration);
});

test("eventos de promesa y estado válidos pasan los esquemas", () => {
  const events = generateScenario("happy", testConfig);
  assert.ok(events.some((entry) => entry.topic === TOPICS.promises));
  assert.ok(events.some((entry) => entry.topic === TOPICS.states));
  for (const entry of events) {
    assert.equal(validateEvent(entry.topic, entry.event).valid, true);
    assert.equal(typeof entry.event.shipmentId, "string");
  }
});

test("el escenario malformado falla validación, sin convertirse en DLQ", () => {
  const events = generateScenario("malformed", testConfig);
  const invalidEvent = events.find((entry) => entry.intentionallyInvalid);
  assert.ok(invalidEvent);
  assert.equal(invalidEvent.topic, TOPICS.states);
  assert.equal(validateEvent(invalidEvent.topic, invalidEvent.event).valid, false);
});

test("un duplicado repite exactamente eventId y contenido", () => {
  const events = generateScenario("duplicate", testConfig);
  const copies = new Map();
  for (const entry of events) {
    const existing = copies.get(entry.event.eventId);
    if (existing) {
      assert.equal(JSON.stringify(entry.event), JSON.stringify(existing.event));
      assert.equal(entry.duplicateOf, entry.event.eventId);
    } else {
      copies.set(entry.event.eventId, entry);
    }
  }
  assert.ok([...copies.values()].some((entry) =>
    events.filter((candidate) => candidate.event.eventId === entry.event.eventId).length === 2,
  ));
});

test("solo el escenario fuera de orden retrocede secuencia de publicación", () => {
  const normalEvents = generateScenario("happy", testConfig)
    .filter((entry) => entry.topic === TOPICS.states)
    .map((entry) => entry.event.secuencia);
  assert.ok(normalEvents.every((value, index) => index === 0 || value > normalEvents[index - 1]));

  const reordered = generateScenario("out-of-order", testConfig)
    .filter((entry) => entry.topic === TOPICS.states)
    .map((entry) => entry.event.secuencia);
  assert.ok(reordered.some((value, index) => index > 0 && value < reordered[index - 1]));
});

test("ráfaga genera 500 shipmentId distintos", () => {
  const events = generateScenario("burst", testConfig);
  assert.equal(new Set(events.map((entry) => entry.event.shipmentId)).size, 500);
});

test("reloj avanza sin eventos y no retrocede al observar un evento atrasado", () => {
  let wallTime = 1000;
  let monotonicTime = 0;
  const clock = new SimulatedClock({
    startAt: testConfig.startAt,
    acceleration: 120,
    wallNow: () => wallTime,
    monotonicNow: () => monotonicTime,
  });
  wallTime += 5000;
  monotonicTime += 5000;
  const advanced = clock.now();
  assert.equal(advanced.getTime(), Date.parse(testConfig.startAt) + 600000);

  const observed = clock.observeEvent(testConfig.startAt, wallTime, monotonicTime);
  assert.equal(observed.getTime(), advanced.getTime());
  wallTime += 1000;
  monotonicTime += 1000;
  assert.equal(clock.now().getTime(), advanced.getTime() + 120000);
});

test("el reloj común recupera el tiempo de inactividad desde su snapshot", () => {
  let wallTime = 1000;
  let monotonicTime = 50;
  const original = new SimulatedClock({
    startAt: testConfig.startAt,
    acceleration: 120,
    wallNow: () => wallTime,
    monotonicNow: () => monotonicTime,
  });
  wallTime += 1000;
  monotonicTime += 1000;
  const snapshot = original.snapshot(wallTime);

  wallTime += 2000;
  monotonicTime = 10;
  const restored = new SimulatedClock({
    startAt: testConfig.startAt,
    acceleration: 120,
    snapshot,
    wallNow: () => wallTime,
    monotonicNow: () => monotonicTime,
  });
  assert.equal(
    restored.now().getTime(),
    Date.parse(snapshot.simulatedAt) + 240000,
  );
});

test("escenarios exponen solo fallos de origen; el ausente no decide reprogramación", () => {
  const absent = generateScenario("absent", testConfig);
  const failure = absent.find((entry) => entry.event.tipo === "ENTREGA_FALLIDA");
  assert.equal(failure.event.motivo, "AUSENTE");
  assert.equal(absent.some((entry) => entry.topic === TOPICS.reschedules), false);

  const thirdFailures = generateScenario("third-failure", testConfig)
    .filter((entry) => entry.event.tipo === "ENTREGA_FALLIDA");
  assert.deepEqual(thirdFailures.map((entry) => entry.event.intento), [1, 2, 3]);
});