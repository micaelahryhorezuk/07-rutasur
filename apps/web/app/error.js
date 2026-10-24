"use client";
export default function ErrorPage({ reset }) { return <section className="error-state" role="alert"><p className="eyebrow">OPERACIÓN NO DISPONIBLE</p><h1>No pudimos cargar los datos.</h1><p>Comprobá que el entorno esté activo y volvé a intentar.</p><button className="button primary" onClick={() => reset()}>Volver a intentar</button></section>; }
