import TrackingSearch from "../../components/tracking-search.js";
export const metadata = { title: "Seguí tu envío" };
export default function TrackingPage() { return <div className="tracking-landing"><span className="tracking-art" aria-hidden="true">↗<span /></span><p className="eyebrow">CADA VEZ MÁS CERCA</p><h1>Tu envío tiene<br />un camino. Seguilo.</h1><p className="muted">Consultá dónde está y cuándo está prevista la entrega.</p><TrackingSearch /><p className="tracking-help">Encontrás el código en la confirmación de tu envío.</p></div>; }
