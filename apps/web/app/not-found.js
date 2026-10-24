import Link from "next/link";
export default function NotFound() { return <div className="empty"><p className="eyebrow">NO ENCONTRADO</p><h1>No encontramos ese envío.</h1><p>Revisá el código e intentá nuevamente.</p><Link className="button primary" href="/tracking">Buscar envío</Link></div>; }
