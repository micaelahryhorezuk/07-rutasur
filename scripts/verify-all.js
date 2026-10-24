import { spawn } from "node:child_process";
const commands = ["test", "web:build", "verify:projector", "verify:business", "verify:sla", "verify:web", "verify:acceptance"];
for (const command of commands) {
  console.log(JSON.stringify({ message: "Iniciando verificación", command }));
  const start = Date.now();
  const code = await new Promise((resolve, reject) => {
    const child = spawn("npm", ["run", command], { stdio: "inherit", env: process.env });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve(signal ? 1 : code));
  });
  if (code !== 0) { process.exitCode = code ?? 1; break; }
  console.log(JSON.stringify({ message: "Verificación aprobada", command, elapsedMs: Date.now() - start }));
}
if (!process.exitCode) console.log(JSON.stringify({ message: "Suite integral aprobada.", commands }));
