"use client";
import { useRouter } from "next/navigation";
import { useState } from "react";
export default function TrackingSearch({ initial = "" }) {
  const router = useRouter();
  const [value, setValue] = useState(initial);
  return <form className="tracking-search" onSubmit={event => { event.preventDefault(); const id = value.trim(); if (id) router.push(`/tracking/${encodeURIComponent(id)}`); }}><label htmlFor="tracking-code">Código de seguimiento</label><div><input id="tracking-code" value={value} onChange={event => setValue(event.target.value)} placeholder="Ej.: RS-HAPPY-…" required maxLength={120} autoComplete="off" /><button className="button primary" type="submit">Seguir envío →</button></div></form>;
}
