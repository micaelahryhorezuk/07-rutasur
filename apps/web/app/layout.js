import Shell from "../components/shell.js";
import "./globals.css";
export const metadata = { title: { default: "RutaSur · Torre de control", template: "%s · RutaSur" }, description: "Seguimiento de envíos, alertas y analítica de última milla." };
export default function RootLayout({ children }) { return <html lang="es"><body><a className="skip-link" href="#contenido">Ir al contenido</a><Shell>{children}</Shell></body></html>; }
