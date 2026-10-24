import { localDate } from "../../business/domain.js";

export function rememberTransition(state, event) {
  if (!["INGRESADO_HUB", "EN_REPARTO"].includes(event.tipo)) return;
  state.transitions ??= [];
  if (!state.transitions.some(item => item.secuencia === event.secuencia)) state.transitions.push(event);
}

// The first departure after the first hub entry is one sample per shipment.
// Selecting by causal sequence also works when one phase arrives late.
export function hubContribution(state, offsetMinutes = -180) {
  const ordered = [...(state.transitions ?? [])].sort((a, b) => a.secuencia - b.secuencia);
  const entry = ordered.find(event => event.tipo === "INGRESADO_HUB");
  const departure = entry && ordered.find(event => event.tipo === "EN_REPARTO" && event.secuencia > entry.secuencia);
  if (!departure) return null;
  const duration = Date.parse(departure.ocurridoEn) - Date.parse(entry.ocurridoEn);
  if (!Number.isFinite(duration) || duration < 0) return null;
  return {
    dimensions: { fecha: localDate(departure.ocurridoEn, offsetMinutes), hubId: entry.hubId ?? departure.hubId, zonaId: departure.zonaId ?? entry.zonaId, franja: departure.franja },
    duration,
  };
}

export function generationSuffix(generation) {
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(generation)) throw new Error("La generación debe tener de 1 a 64 letras, números, guiones o guiones bajos.");
  return generation === "main" ? "" : `__${generation}`;
}

export function reachedCutoff(offset, cutoff) { return BigInt(offset) + 1n >= BigInt(cutoff); }
