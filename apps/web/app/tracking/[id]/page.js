import Link from "next/link";
import { notFound } from "next/navigation";
import { getTracking } from "../../../lib/data.js";
import { dateTime } from "../../../lib/format.js";
import { Badge, Timeline } from "../../../components/ui.js";
import LiveUpdates from "../../../components/live-updates.js";
export const dynamic = "force-dynamic";
export const metadata = { title: "Seguimiento del envío" };
export default async function TrackingDetail({ params }) {
  const { id } = await params;
  const shipment = await getTracking(id);
  if (!shipment) notFound();
  return <div className="tracking-detail"><Link href="/tracking" className="back-link">← Buscar otro envío</Link><div className="row-between"><p className="eyebrow">TU ENVÍO, PASO A PASO</p><LiveUpdates scope="tracking" shipmentId={id} /></div><h1>{shipment.estado === "ENTREGADO" ? "Llegó a destino." : shipment.estado === "EN_REPARTO" ? "Ya está en camino." : shipment.estado === "ENTREGA_FALLIDA" ? "No pudimos entregarlo." : "Estamos moviendo tu envío."}</h1><p className="tracking-code">{shipment.shipmentId}</p><div className="tracking-status"><Badge value={shipment.estado} /><div><span className="muted small">{shipment.estado === "ENTREGADO" ? "Fecha de entrega prevista" : "Entrega estimada"}</span><strong>{dateTime(shipment.fechaEstimada)}</strong></div></div><section className="tracking-history"><h2>El recorrido</h2><Timeline events={shipment.historial} /></section><p className="tracking-help">Los movimientos se actualizan automáticamente. No necesitás recargar la página.</p></div>;
}
