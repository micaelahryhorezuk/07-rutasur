import Link from "next/link";
import { dateTime, label } from "../lib/format.js";
export function Badge({ value }) { return <span className={`badge badge-${value?.toLowerCase() ?? "neutral"}`}>{label(value)}</span>; }
export function Empty({ title = "Todavía no hay datos", children }) { return <div className="empty"><span className="empty-mark" aria-hidden="true">↗</span><h3>{title}</h3><p>{children ?? "Los nuevos envíos y sus cambios van a aparecer acá."}</p></div>; }
export function Heading({ eyebrow, title, description, children }) { return <header className="page-heading"><div><p className="eyebrow">{eyebrow}</p><h1>{title}</h1>{description && <p className="muted">{description}</p>}</div><div className="heading-actions">{children}</div></header>; }
export function Panel({ title, hint, children, className = "" }) { return <section className={`panel ${className}`}><div className="panel-heading"><h2>{title}</h2>{hint && <span className="muted small">{hint}</span>}</div>{children}</section>; }
export function Stat({ title, value, detail, accent }) { return <article className={`stat ${accent ? "stat-accent" : ""}`}><span className="stat-label">{title}</span><strong>{value}</strong><span className="stat-detail">{detail}</span></article>; }
export function ShipmentLink({ id }) { return <Link className="shipment-link" href={`/envios/${encodeURIComponent(id)}`}>{id}<span aria-hidden="true">↗</span></Link>; }
export function Timeline({ events = [], internal = false }) {
  return events.length ? <ol className="timeline">{events.map((event, index) => <li key={event.eventId ?? `${event.tipo}-${index}`} className={event.tipo === "ENTREGADO" ? "complete" : ""}><span className="timeline-dot" /><div><strong>{label(event.tipo)}</strong><time dateTime={event.ocurridoEn}>{dateTime(event.ocurridoEn)}</time>{internal && <p className="muted small">{event.hubId ?? "Sin centro asignado"}{event.intento ? ` · Intento ${event.intento}` : ""}{event.motivo ? ` · ${label(event.motivo)}` : ""}</p>}</div></li>)}</ol> : <Empty title="Esperando el primer movimiento" />;
}
