const PROCESSED_TTL_SECONDS = 7 * 24 * 60 * 60;
const CONSUMER_NAME = "proyector";

function safeGeneration(generation) {
  if (generation === "main") return "";
  const normalized = generation.replaceAll(/[^A-Za-z0-9_-]/g, "-").slice(0, 48);
  if (!normalized) throw new Error("La generación debe tener caracteres válidos.");
  return `__${normalized}`;
}

export function isStateSequenceStale(lastAppliedSequence, eventSequence) {
  return Number.isInteger(lastAppliedSequence) && eventSequence <= lastAppliedSequence;
}

export function notificationIdentity(shipmentId, sourceEventId, category) {
  return `${shipmentId}:${sourceEventId}:${category}`;
}

export class ProjectionStore {
  constructor(database, { generation = "main" } = {}) {
    const suffix = safeGeneration(generation);
    this.database = database;
    this.generation = generation;
    this.collections = {
      shipments: database.collection(`envios${suffix}`),
      alerts: database.collection(`alertas${suffix}`),
      notifications: database.collection(`notificaciones${suffix}`),
      processed: database.collection("eventos_procesados"),
    };
  }

  async createIndexes() {
    const { shipments, alerts, notifications, processed } = this.collections;
    await Promise.all([
      shipments.createIndex({ shipmentId: 1 }, { unique: true, name: "shipmentId_unique" }),
      shipments.createIndex({ estado: 1, zonaId: 1 }, { name: "estado_zonaId" }),
      shipments.createIndex({ "promesa.venceEn": 1 }, { name: "promesa_venceEn" }),
      alerts.createIndex({ estado: 1, prioridad: 1 }, { name: "estado_prioridad" }),
      alerts.createIndex({ shipmentId: 1 }, { name: "alertas_shipmentId" }),
      alerts.createIndex({ alertaId: 1 }, { unique: true, name: "alertaId_unique" }),
      notifications.createIndex({ shipmentId: 1 }, { name: "notificaciones_shipmentId" }),
      notifications.createIndex({ identidad: 1 }, { unique: true, name: "identidad_unique" }),
      processed.createIndex(
        { consumer: 1, generation: 1, eventId: 1 },
        { unique: true, name: "consumer_generation_eventId_unique" },
      ),
      processed.createIndex(
        { processedAt: 1 },
        { expireAfterSeconds: PROCESSED_TTL_SECONDS, name: "processedAt_ttl_7d" },
      ),
    ]);
  }

  async hasProcessed(eventId) {
    return Boolean(
      await this.collections.processed.findOne({
        consumer: CONSUMER_NAME,
        generation: this.generation,
        eventId,
      }),
    );
  }

  async markProcessed(eventId, topic, session) {
    await this.collections.processed.insertOne(
      {
        consumer: CONSUMER_NAME,
        generation: this.generation,
        eventId,
        topic,
        processedAt: new Date(),
      },
      { session },
    );
  }

  async applyEvent(client, topic, event) {
    const session = client.startSession();
    try {
      return await session.withTransaction(async () => {
        if (await this.hasProcessedInSession(event.eventId, session)) return "duplicate";

        let outcome;
        switch (topic) {
          case "envios.estados":
            outcome = await this.applyState(event, session);
            break;
          case "envios.promesas":
            outcome = await this.applyPromise(event, session);
            break;
          case "envios.reprogramaciones":
            outcome = await this.applyReschedule(event, session);
            break;
          case "alertas.sla":
            outcome = await this.applyAlert(event, session);
            break;
          case "notificaciones.enviadas":
            outcome = await this.applyNotification(event, session);
            break;
          default:
            throw new Error(`El proyector no admite el topic ${topic}.`);
        }

        await this.markProcessed(event.eventId, topic, session);
        return outcome;
      }, {
        readConcern: { level: "snapshot" },
        writeConcern: { w: "majority" },
        readPreference: "primary",
      });
    } finally {
      await session.endSession();
    }
  }

  async hasProcessedInSession(eventId, session) {
    return Boolean(
      await this.collections.processed.findOne(
        {
          consumer: CONSUMER_NAME,
          generation: this.generation,
          eventId,
        },
        { session },
      ),
    );
  }

  async markInvalidProcessed(client, eventId, topic) {
    const session = client.startSession();
    try {
      return await session.withTransaction(async () => {
        if (await this.hasProcessedInSession(eventId, session)) return "duplicate";
        await this.markProcessed(eventId, topic, session);
        return "dead_lettered";
      }, {
        readConcern: { level: "snapshot" },
        writeConcern: { w: "majority" },
        readPreference: "primary",
      });
    } finally {
      await session.endSession();
    }
  }

  async ensureShipment(shipmentId, session) {
    await this.collections.shipments.updateOne(
      { shipmentId },
      {
        $setOnInsert: {
          shipmentId,
          estado: null,
          historial: [],
          intentos: 0,
          ultimaSecuenciaAplicada: null,
          promesa: null,
          historialPromesas: [],
          reprogramacionActual: null,
          reprogramaciones: [],
          creadoEnProyeccion: new Date(),
        },
      },
      { upsert: true, session },
    );
  }

  async applyState(event, session) {
    const { shipments } = this.collections;
    const existing = await shipments.findOne({ shipmentId: event.shipmentId }, { session });
    if (isStateSequenceStale(existing?.ultimaSecuenciaAplicada, event.secuencia)) {
      return "out_of_order";
    }

    await this.ensureShipment(event.shipmentId, session);
    const update = {
      $set: {
        estado: event.tipo,
        zonaId: event.zonaId,
        hubId: event.hubId,
        repartidorId: event.repartidorId,
        franja: event.franja,
        ultimaSecuenciaAplicada: event.secuencia,
        actualizadoEn: new Date(event.ocurridoEn),
      },
      $push: { historial: event },
    };
    if (Number.isInteger(event.intento)) {
      update.$max = { intentos: event.intento };
      if (event.tipo === "EN_REPARTO" || event.tipo === "ENTREGA_FALLIDA") {
        update.$push.historialIntentos = event;
      }
    }
    await shipments.updateOne({ shipmentId: event.shipmentId }, update, { session });
    return "applied";
  }

  async applyPromise(event, session) {
    const { shipments } = this.collections;
    await this.ensureShipment(event.shipmentId, session);
    const existing = await shipments.findOne({ shipmentId: event.shipmentId }, { session });
    if (
      existing.promesa &&
      existing.promesa.versionPromesa >= event.versionPromesa
    ) {
      return "older_promise_version";
    }

    await shipments.updateOne(
      { shipmentId: event.shipmentId },
      {
        $set: { promesa: event },
        $push: { historialPromesas: event },
      },
      { session },
    );
    return "applied";
  }

  async applyReschedule(event, session) {
    await this.ensureShipment(event.shipmentId, session);
    await this.collections.shipments.updateOne(
      { shipmentId: event.shipmentId },
      {
        $set: { reprogramacionActual: event },
        $push: { reprogramaciones: event },
      },
      { session },
    );
    return "applied";
  }

  async applyAlert(event, session) {
    const isClosed = event.tipo === "ALERTA_SLA_CERRADA";
    const fields = {
      alertaId: event.alertaId,
      shipmentId: event.shipmentId,
      estado: isClosed ? "CERRADA" : "ABIERTA",
      prioridad: event.prioridad,
      regla: event.regla,
      causa: event.causa,
      hubId: event.hubId,
      zonaId: event.zonaId,
      cerradaEn: isClosed ? new Date(event.ocurridoEn) : null,
      ultimoEvento: event,
    };
    if (!isClosed) fields.abiertaEn = new Date(event.ocurridoEn);
    await this.collections.alerts.updateOne(
      { alertaId: event.alertaId },
      {
        $set: fields,
      },
      { upsert: true, session },
    );
    return isClosed ? "closed" : "opened";
  }

  async applyNotification(event, session) {
    const identity = notificationIdentity(
      event.shipmentId,
      event.eventoOrigenId,
      event.categoria,
    );
    await this.collections.notifications.updateOne(
      { identidad: identity },
      {
        $setOnInsert: {
          identidad: identity,
          shipmentId: event.shipmentId,
          notificacionId: event.notificacionId,
          eventoOrigenId: event.eventoOrigenId,
          categoria: event.categoria,
          estado: event.estado,
          canal: event.canal,
          ocurridaEn: new Date(event.ocurridoEn),
          evento: event,
        },
      },
      { upsert: true, session },
    );
    return "applied";
  }
}

export function collectionNamesForGeneration(generation) {
  const suffix = safeGeneration(generation);
  return {
    shipments: `envios${suffix}`,
    alerts: `alertas${suffix}`,
    notifications: `notificaciones${suffix}`,
  };
}

export const PROCESSED_TTL_SECONDS_EXPORT = PROCESSED_TTL_SECONDS;
