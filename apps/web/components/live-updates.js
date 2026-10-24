"use client";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

export default function LiveUpdates({ scope = "control", shipmentId }) {
  const router = useRouter();
  const [status, setStatus] = useState("connecting");
  useEffect(() => {
    const query = new URLSearchParams({ scope, ...(shipmentId ? { id: shipmentId } : {}) });
    let source;
    let pending;
    function refresh() {
      if (pending) return;
      pending = setTimeout(() => { pending = undefined; router.refresh(); }, 250);
    }
    function connect() {
      source?.close();
      if (!navigator.onLine) { setStatus("reconnecting"); return; }
      setStatus("connecting");
      source = new EventSource(`/api/events?${query}`);
      source.addEventListener("ready", () => { setStatus("connected"); refresh(); });
      source.addEventListener("changed", refresh);
      source.addEventListener("unavailable", () => setStatus("reconnecting"));
      source.onerror = () => setStatus("reconnecting");
    }
    function offline() {
      source?.close();
      clearTimeout(pending);
      pending = undefined;
      setStatus("reconnecting");
    }
    window.addEventListener("online", connect);
    window.addEventListener("offline", offline);
    connect();
    return () => {
      window.removeEventListener("online", connect);
      window.removeEventListener("offline", offline);
      clearTimeout(pending);
      source?.close();
    };
  }, [scope, shipmentId, router]);
  return <span className={`live-indicator ${status}`} role="status"><span />{status === "connected" ? "En vivo" : status === "connecting" ? "Conectando" : "Reconectando"}</span>;
}
