import Link from "next/link";
import { notFound } from "next/navigation";
import { getShipment } from "../../../lib/data.js";
import { dateTime, label } from "../../../lib/format.js";
import { Badge, Empty, Heading, Panel, Timeline } from "../../../components/ui.js";
import LiveUpdates from "../../../components/live-updates.js";
export const dynamic = "force-dynamic";
export const metadata = { title: "Detalle del envío" };
export default async function ShipmentPage({ params }) {
  const { id } = await params;
  const data = await getShipment(id);
  if (!data) notFound();
  const { shipment, alerts, notifications } = data;
  const decision = shipment.reprogramacionActual;
  return <><Link href="/" className="back-link">← Volver a la operación</Link><Heading eyebrow="OPERACIÓN / DETALLE DEL ENVÍO" title={shipment.shipmentId} description="El recorrido completo, en un solo lugar."><LiveUpdates scope="shipment" shipmentId={id} /><Link className="button" href={`/tracking/${encodeURIComponent(id)}`}>Ver tracking público ↗</Link></Heading>
    <div className="shipment-summary"><div><span className="muted small">Estado actual</span><Badge value={shipment.estado} /></div><div><span className="muted small">Promesa de entrega</span><strong>{dateTime(shipment.promesa?.venceEn)}</strong></div><div><span className="muted small">Zona / centro</span><strong>{shipment.zonaId ?? "Sin asignar"} · {shipment.hubId ?? "Sin asignar"}</strong></div><div><span className="muted small">Intentos</span><strong>{shipment.intentos ?? 0}</strong></div></div>
    {decision && <section className={`decision-banner decision-${decision.accion.toLowerCase()}`}><span aria-hidden="true">↳</span><div><strong>{label(decision.accion)}</strong><p>{decision.accion === "PROGRAMAR" ? `Próximo intento: ${dateTime(decision.programadaPara)} · ${label(decision.franja)}` : decision.accion === "DEVOLVER" ? "El envío quedó marcado para devolución al comercio." : "El envío queda retenido y requiere intervención."} · {label(decision.motivo)}</p></div></section>}
    <div className="detail-grid"><Panel title="Recorrido del envío" hint={`${shipment.historial?.length ?? 0} movimientos`}><Timeline events={shipment.historial} internal /></Panel><div className="right-column"><Panel title="Alertas" hint={`${alerts.filter(alert => alert.estado === "ABIERTA").length} abiertas`}>{alerts.length ? <ul className="alert-list">{alerts.map(alert => <li key={alert.alertaId}><div className="row-between"><Badge value={alert.prioridad} /><Badge value={alert.estado} /></div><h3>{label(alert.regla)}</h3><p>{alert.causa}</p><small className="muted">Abierta: {dateTime(alert.abiertaEn)}{alert.cerradaEn ? ` · Cerrada: ${dateTime(alert.cerradaEn)}` : ""}</small></li>)}</ul> : <Empty title="Sin alertas para este envío">Las alertas de entrega aparecerán acá.</Empty>}</Panel><Panel title="Avisos al destinatario" hint={`${notifications.length} registrados`}>{notifications.length ? <ul className="notice-list">{notifications.map(notice => <li key={notice.identidad}><span className="notice-icon" aria-hidden="true">↗</span><div><strong>{label(notice.categoria)}</strong><p>Aviso simulado registrado</p><time>{dateTime(notice.ocurridaEn)}</time></div><span className="muted small">✓</span></li>)}</ul> : <p className="muted padded">Todavía no se registraron avisos.</p>}</Panel></div></div>
  </>;
}
