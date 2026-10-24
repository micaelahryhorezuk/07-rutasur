"""Start the full stack with fresh, exclusively owned Docker volumes."""
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

project = "rutasur-verify-" + uuid.uuid4().hex[:8]
assert project.startswith("rutasur-verify-") and len(project) == 23
ports = set()
def free_port():
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            value = sock.getsockname()[1]
        if value not in ports:
            ports.add(value)
            return value

environment = dict(os.environ)
environment.update(COMPOSE_PROJECT_NAME=project, COMPOSE_PROFILES="", KAFKA_EXTERNAL_PORT=str(free_port()), MONGODB_EXTERNAL_PORT=str(free_port()), WEB_PORT=str(free_port()), SIMULATOR_SEED="tp7-compose-2026")
base = ""
# Keep the disposable stack bounded while the user's regular Kafka/Mongo remain up.
override = Path("/tmp") / (project + ".override.yaml")
lines = ["services:", "  kafka:", "    ports: !reset []", "    environment:", "      KAFKA_HEAP_OPTS: '-Xms128m -Xmx256m'", "  kafka-init:", "    environment:", "      KAFKA_HEAP_OPTS: '-Xms64m -Xmx256m'", "  mongo:", "    ports: !reset []", "    command: ['mongod', '--replSet', 'rs0', '--bind_ip_all', '--port', '27017', '--wiredTigerCacheSizeGB', '0.25']"]
for name in ["projector", "notifications", "rescheduling", "analytics", "sla-monitor", "simulator", "simulator-control", "hub-analytics"]:
    lines.extend(["  " + name + ":", "    environment:", "      NODE_OPTIONS: '--max-old-space-size=96'"])
lines.extend(["  web:", "    ports: !override", "      - '127.0.0.1::3000'", "    environment:", "      NODE_OPTIONS: '--max-old-space-size=192'"])
override.write_text("\n".join(lines) + "\n")
command = ["docker", "compose", "--project-name", project, "--file", str(Path("docker-compose.yml").resolve()), "--file", str(override)]
log_path = Path("/tmp") / (project + ".log")
validated = False

def compose(*args, capture=False):
    if capture:
        return subprocess.check_output(command + list(args), env=environment, text=True)
    with log_path.open("a") as log:
        subprocess.run(command + list(args), env=environment, stdout=log, stderr=subprocess.STDOUT, check=True)

def request(path, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(base + path, data=body, headers={"Content-Type": "application/json", "Origin": base})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)

def wait_for(check, description, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, urllib.error.URLError, KeyError):
            pass
        time.sleep(1)
    raise RuntimeError("Timeout: " + description)

def consumers():
    names = ["projector", "notifications", "rescheduling", "analytics", "sla-monitor"]
    ids = compose("ps", "--quiet", *names, capture=True).split()
    assert len(ids) == 5, "Faltan consumidores"
    return {value: subprocess.check_output(["docker", "inspect", "--format", "{{.State.StartedAt}}", value], text=True).strip() for value in ids}

try:
    config = json.loads(compose("--profile", "projector", "--profile", "business", "--profile", "sla", "--profile", "web", "--profile", "simulator", "config", "--format", "json", capture=True))
    expected = {"rutasur-kafka-data": project + "-kafka-data", "rutasur-mongo-data": project + "-mongo-data"}
    for key, name in expected.items():
        assert config["volumes"][key]["name"] == name, "Se rechazó un volumen ajeno a la verificación"
    validated = True
    # This is the single startup command being verified. Other projects stay up.
    compose("--profile", "projector", "--profile", "business", "--profile", "sla", "--profile", "web", "--profile", "simulator", "up", "-d", "--build")
    binding = compose("port", "web", "3000", capture=True).strip().splitlines()[0]
    base = "http://" + binding
    print("Arranque completo solicitado; esperando interfaz y flujo.", flush=True)
    wait_for(lambda: request("/api/health").get("status") == "ok", "Next.js y MongoDB")
    wait_for(lambda: request("/api/control").get("total", 0) > 0, "flujo continuo en la torre de control")
    wait_for(lambda: request("/api/analytics")["totals"]["primerosIntentos"] > 0, "analítica del flujo")
    before = consumers()
    scenario = request("/api/demo", {"scenario": "absent", "seed": project})
    shipment_id = scenario["shipmentIds"][0]
    def failure_reactions():
        detail = request("/api/envios/" + shipment_id)
        return (detail["shipment"].get("reprogramacionActual", {}) or {}).get("accion") == "PROGRAMAR" and any(item["categoria"] == "FALLO" for item in detail["notifications"]) and any(item["regla"] == "FALLO_DIA_LIMITE" for item in detail["alerts"])
    wait_for(failure_reactions, "reprogramación, aviso y alerta del fallo")
    print("Flujo, reprogramación, notificación y SLA verificados.", flush=True)
    compose("--profile", "simulator", "stop", "simulator")
    compose("--profile", "hubs", "up", "-d", "--no-deps", "hub-analytics")
    wait_for(lambda: request("/api/analytics")["hubTimes"]["status"] == "EN_VIVO" and request("/api/analytics")["hubTimes"]["samples"] > 0, "histórico de hubs")
    assert consumers() == before, "El nuevo grupo reinició otros consumidores"
    print(json.dumps({"message": "Compose integral verificado con volúmenes nuevos.", "project": project, "registeredShipments": request("/api/control")["total"], "hubSamples": request("/api/analytics")["hubTimes"]["samples"], "existingConsumersUnchanged": True, "log": str(log_path)}), flush=True)
except Exception:
    if validated:
        try: compose("logs", "--tail=30")
        except Exception: pass
    if log_path.exists():
        print(log_path.read_text()[-8000:], flush=True)
    raise
finally:
    if validated:
        # Only volumes whose names were checked above belong to this project.
        compose("--profile", "*", "down", "--volumes", "--remove-orphans")
        print("Contenedores y volúmenes temporales eliminados: " + project, flush=True)
    override.unlink(missing_ok=True)
