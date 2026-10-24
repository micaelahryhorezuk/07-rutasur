import { Heading } from "../../components/ui.js";
import DemoPanel from "../../components/demo-panel.js";
export const metadata = { title: "Panel de demo" };
export default function DemoPage() { return <><Heading eyebrow="DEMOSTRACIÓN / ESCENARIOS" title="Poné la operación en marcha" description="Probá un recorrido y observá cómo reacciona cada parte del sistema." /><DemoPanel /></>; }
