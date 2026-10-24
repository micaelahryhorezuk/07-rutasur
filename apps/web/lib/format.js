export const STATES = ["CREADO", "INGRESADO_HUB", "CLASIFICADO", "EN_REPARTO", "ENTREGADO", "ENTREGA_FALLIDA"];
export const LABELS = {
  CREADO: "Creado", INGRESADO_HUB: "En el centro", CLASIFICADO: "Clasificado", EN_REPARTO: "En reparto", ENTREGADO: "Entregado", ENTREGA_FALLIDA: "Entrega fallida",
  AUSENTE: "Destinatario ausente", DIRECCION_INCORRECTA: "Dirección incorrecta", RECHAZADO: "Entrega rechazada", ZONA_INACCESIBLE: "Zona inaccesible",
  SIN_INGRESO: "Sin ingreso al centro", SIN_SALIDA: "Pendiente de salida", FALLO_DIA_LIMITE: "Fallo en día límite", VENCIDO: "Promesa vencida",
  MEDIA: "Media", ALTA: "Alta", CRITICA: "Crítica", ABIERTA: "Abierta", CERRADA: "Cerrada", MANIANA: "Mañana", TARDE: "Tarde", PROGRAMAR: "Nuevo intento", RETENER: "Retenido", DEVOLVER: "Devolución", FALLO: "Entrega fallida", ALERTA_SLA: "Alerta de entrega",
};
export const label = value => LABELS[value] ?? value ?? "Sin datos";
export function dateTime(value) {
  if (!value || !Number.isFinite(new Date(value).getTime())) return "Sin fecha";
  return new Intl.DateTimeFormat("es-AR", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", timeZone: "America/Argentina/Buenos_Aires" }).format(new Date(value));
}
export function percent(numerator, denominator) {
  return denominator ? new Intl.NumberFormat("es-AR", { style: "percent", maximumFractionDigits: 1 }).format(numerator / denominator) : "Sin datos";
}

export function duration(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "Sin datos";
  const minutes = value / 60000;
  return minutes < 60 ? `${new Intl.NumberFormat("es-AR", { maximumFractionDigits: 1 }).format(minutes)} min` : `${new Intl.NumberFormat("es-AR", { maximumFractionDigits: 2 }).format(minutes / 60)} h`;
}
