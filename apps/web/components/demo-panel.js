"use client";
import Link from "next/link";
import { useState } from "react";
const scenarios = [
  ["happy", "01", "Un camino sin incidentes", "Un envío recorre todos sus estados hasta llegar a destino.", "Envío feliz"],
  ["absent", "02", "El destinatario no está", "Un fallo por ausencia activa aviso, reprogramación y analítica.", "Ausente"],
  ["delayed", "03", "Un envío que se demora", "El paquete queda en el centro y el monitor detecta el riesgo.", "Demorado"],
  ["third-failure", "04", "Hasta el tercer intento", "Tres intentos fallidos terminan en una decisión de devolución.", "Tercer fallo"],
  ["burst", "05", "Una operación en movimiento", "500 envíos para observar cómo se acumula y recupera el trabajo.", "Ráfaga de 500"],
  ["duplicate", "06", "El mismo evento, otra vez", "La copia de un evento no debe duplicar sus efectos.", "Duplicado"],
  ["out-of-order", "07", "Un movimiento atrasado", "Un evento antiguo no debe hacer retroceder el envío.", "Fuera de orden"],
  ["malformed", "08", "Un evento para revisar", "Un mensaje que incumple el contrato termina en la DLQ.", "Malformado"],
];
export default function DemoPanel() {
  const [seed, setSeed] = useState("");
  const [busy, setBusy] = useState(null);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);
  async function run(scenario) {
    setBusy(scenario); setError(""); setResult(null);
    try {
      const response = await fetch("/api/demo", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ scenario, ...(seed.trim() ? { seed: seed.trim() } : {}) }) });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error ?? "No se pudo iniciar el escenario.");
      setResult(data);
    } catch (error) { setError(error.message); }
    finally { setBusy(null); }
  }
  return <><div className="demo-settings panel"><div><h2>Una historia, distintos recorridos</h2><p className="muted">Elegí un escenario y seguí sus resultados en la operación.</p></div><label>Semilla opcional<input value={seed} onChange={event => setSeed(event.target.value)} placeholder="Automática si la dejás vacía" maxLength={100} /></label></div>{error && <div className="form-error" role="alert">{error}</div>}{result && <section className="demo-result" role="status"><h2>Escenario publicado ✓</h2><p>{result.shipmentIds.length} envíos · {result.events} eventos. Los resultados aparecen a medida que se procesan.</p><div className="result-links">{result.shipmentIds.slice(0, 6).map(id => <Link key={id} href={`/envios/${encodeURIComponent(id)}`}>{id} ↗</Link>)}</div><small>Semilla: {result.seed}</small><Link className="text-link" href="/">Ir a la torre de control →</Link></section>}<div className="demo-grid">{scenarios.map(([scenario, number, title, description, action]) => <article className="scenario-card" key={scenario}><span className="scenario-number">{number}</span><h2>{title}</h2><p>{description}</p><button className="button" disabled={Boolean(busy)} onClick={() => run(scenario)}>{busy === scenario ? "Publicando…" : action}<span aria-hidden="true">↗</span></button></article>)}</div></>;
}
