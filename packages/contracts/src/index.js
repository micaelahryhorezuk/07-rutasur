import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
export { SimulatedClock } from "./simulated-clock.js";

export const TOPICS = Object.freeze({
  states: "envios.estados",
  promises: "envios.promesas",
  reschedules: "envios.reprogramaciones",
  slaAlerts: "alertas.sla",
  notifications: "notificaciones.enviadas",
  deadLetter: "envios.estados.dlq",
});

export const EVENT_SCHEMA = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  $id: "https://rutasur.local/contracts/events.schema.json",
  $defs: {
    BaseEvent: {
      type: "object",
      properties: {
        eventId: { type: "string", minLength: 1 },
        shipmentId: { type: "string", minLength: 1 },
        tipo: { type: "string", minLength: 1 },
        ocurridoEn: { type: "string", format: "date-time" },
        secuencia: { type: "integer", minimum: 0 },
        hubId: { type: ["string", "null"] },
        zonaId: { type: ["string", "null"] },
        repartidorId: { type: ["string", "null"] },
        version: { type: "integer", minimum: 1 },
      },
      required: [
        "eventId",
        "shipmentId",
        "tipo",
        "ocurridoEn",
        "secuencia",
        "hubId",
        "zonaId",
        "repartidorId",
        "version",
      ],
    },
    CausadoPor: {
      type: "object",
      properties: {
        eventId: { type: "string", minLength: 1 },
        secuencia: { type: "integer", minimum: 0 },
      },
      required: ["eventId", "secuencia"],
      additionalProperties: false,
    },
    EnvioEstado: {
      allOf: [
        { $ref: "#/$defs/BaseEvent" },
        {
          type: "object",
          properties: {
            tipo: {
              enum: [
                "CREADO",
                "INGRESADO_HUB",
                "CLASIFICADO",
                "EN_REPARTO",
                "ENTREGADO",
                "ENTREGA_FALLIDA",
              ],
            },
            motivo: {
              enum: [
                "AUSENTE",
                "DIRECCION_INCORRECTA",
                "RECHAZADO",
                "ZONA_INACCESIBLE",
              ],
            },
            intento: { type: "integer", minimum: 1 },
            franja: { enum: ["MANIANA", "TARDE"] },
          },
          required: ["franja"],
          allOf: [
            {
              if: {
                properties: { tipo: { const: "ENTREGA_FALLIDA" } },
                required: ["tipo"],
              },
              then: {
                properties: {
                  motivo: {
                    enum: [
                      "AUSENTE",
                      "DIRECCION_INCORRECTA",
                      "RECHAZADO",
                      "ZONA_INACCESIBLE",
                    ],
                  },
                  intento: { type: "integer", minimum: 1 },
                },
                required: ["motivo", "intento"],
              },
            },
            {
              if: {
                properties: { tipo: { const: "EN_REPARTO" } },
                required: ["tipo"],
              },
              then: {
                properties: { intento: { type: "integer", minimum: 1 } },
                required: ["intento"],
              },
            },
          ],
        },
      ],
    },
    Promesa: {
      allOf: [
        { $ref: "#/$defs/BaseEvent" },
        {
          type: "object",
          properties: {
            tipo: { enum: ["PROMESA_CREADA", "PROMESA_ACTUALIZADA"] },
            venceEn: { type: "string", format: "date-time" },
            versionPromesa: { type: "integer", minimum: 1 },
          },
          required: ["venceEn", "versionPromesa"],
        },
      ],
    },
    Reprogramacion: {
      allOf: [
        { $ref: "#/$defs/BaseEvent" },
        {
          type: "object",
          properties: {
            tipo: { const: "REPROGRAMACION_DEFINIDA" },
            accion: { enum: ["PROGRAMAR", "RETENER", "DEVOLVER"] },
            motivo: {
              enum: [
                "AUSENTE",
                "DIRECCION_INCORRECTA",
                "RECHAZADO",
                "ZONA_INACCESIBLE",
              ],
            },
            intento: { type: "integer", minimum: 1 },
            programadaPara: {
              anyOf: [
                { type: "string", format: "date-time" },
                { type: "null" },
              ],
            },
            franja: { enum: ["MANIANA", "TARDE", null] },
            causadoPor: { $ref: "#/$defs/CausadoPor" },
            origen: { const: "reprogramacion" },
          },
          required: [
            "accion",
            "motivo",
            "intento",
            "programadaPara",
            "franja",
            "causadoPor",
            "origen",
          ],
        },
      ],
    },
    AlertaSla: {
      allOf: [
        { $ref: "#/$defs/BaseEvent" },
        {
          type: "object",
          properties: {
            tipo: { enum: ["ALERTA_SLA_ABIERTA", "ALERTA_SLA_CERRADA"] },
            alertaId: { type: "string", minLength: 1 },
            regla: {
              enum: [
                "SIN_INGRESO",
                "SIN_SALIDA",
                "FALLO_DIA_LIMITE",
                "VENCIDO",
              ],
            },
            prioridad: { enum: ["MEDIA", "ALTA", "CRITICA"] },
            causa: { type: "string", minLength: 1 },
            causadoPor: { $ref: "#/$defs/CausadoPor" },
            origen: { const: "monitor-sla" },
          },
          required: ["alertaId", "regla", "prioridad", "causa", "origen"],
        },
      ],
    },
    Notificacion: {
      allOf: [
        { $ref: "#/$defs/BaseEvent" },
        {
          type: "object",
          properties: {
            tipo: { const: "NOTIFICACION_REGISTRADA" },
            notificacionId: { type: "string", minLength: 1 },
            eventoOrigenId: { type: "string", minLength: 1 },
            categoria: { enum: ["FALLO", "ALERTA_SLA"] },
            canal: { const: "SIMULADO" },
            estado: { const: "REGISTRADA" },
            opcionesReprogramacion: {
              type: "array",
              items: { enum: ["MANIANA", "TARDE"] },
            },
            causadoPor: { $ref: "#/$defs/CausadoPor" },
            origen: { const: "notificaciones" },
          },
          required: [
            "notificacionId",
            "eventoOrigenId",
            "categoria",
            "canal",
            "estado",
            "origen",
          ],
        },
      ],
    },
    DeadLetter: {
      type: "object",
      properties: {
        falloId: { type: "string", minLength: 1 },
        shipmentId: { type: "string", minLength: 1 },
        eventIdOriginal: { type: ["string", "null"] },
        topicOriginal: { type: "string", minLength: 1 },
        particionOriginal: { type: "integer", minimum: 0 },
        offsetOriginal: { type: "string", minLength: 1 },
        grupoConsumidor: { type: "string", minLength: 1 },
        codigoError: { type: "string", minLength: 1 },
        motivo: { type: "string", minLength: 1 },
        falloEn: { type: "string", format: "date-time" },
        payloadOriginal: { type: "string" },
        version: { type: "integer", minimum: 1 },
      },
      required: [
        "falloId",
        "shipmentId",
        "topicOriginal",
        "particionOriginal",
        "offsetOriginal",
        "grupoConsumidor",
        "codigoError",
        "motivo",
        "falloEn",
        "payloadOriginal",
        "version",
      ],
    },
  },
};

const schemaNameByTopic = new Map([
  [TOPICS.states, "EnvioEstado"],
  [TOPICS.promises, "Promesa"],
  [TOPICS.reschedules, "Reprogramacion"],
  [TOPICS.slaAlerts, "AlertaSla"],
  [TOPICS.notifications, "Notificacion"],
  [TOPICS.deadLetter, "DeadLetter"],
]);

const ajv = new Ajv2020({ allErrors: true, strict: true });
addFormats(ajv);
ajv.addSchema(EVENT_SCHEMA);

const validators = new Map(
  [...schemaNameByTopic].map(([topic, schemaName]) => [
    topic,
    ajv.compile({ $ref: `${EVENT_SCHEMA.$id}#/$defs/${schemaName}` }),
  ]),
);

export function validateEvent(topic, event) {
  const validator = validators.get(topic);
  if (!validator) {
    return { valid: false, errors: [`Topic sin esquema: ${topic}`] };
  }

  const valid = validator(event);
  return {
    valid,
    errors: valid
      ? []
      : validator.errors.map(
          (error) => `${error.instancePath || "/"} ${error.message}`,
        ),
  };
}