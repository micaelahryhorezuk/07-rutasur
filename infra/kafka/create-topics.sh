#!/usr/bin/env bash
set -Eeuo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
replication_factor=1

create_and_check_topic() {
  local topic="$1"
  local partitions="$2"
  local config_key="$3"
  local config_value="$4"
  local description
  local configs
  local actual_config

  echo "Configurando topic ${topic}..."
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap_servers" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$replication_factor" \
    --config "${config_key}=${config_value}"

  description="$(/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap_servers" --describe --topic "$topic")"
  if ! grep -Eq "PartitionCount: ${partitions}([[:space:]]|$)" <<<"$description"; then
    echo "ERROR: ${topic} debe tener ${partitions} particiones. Descripción: ${description}" >&2
    return 1
  fi
  if ! grep -Eq "ReplicationFactor: ${replication_factor}([[:space:]]|$)" <<<"$description"; then
    echo "ERROR: ${topic} debe tener factor de replicación ${replication_factor}. Descripción: ${description}" >&2
    return 1
  fi

  configs="$(/opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "$bootstrap_servers" \
    --entity-type topics --entity-name "$topic" --describe --all)"
  actual_config="$(awk -v key="$config_key" 'index($1, key "=") == 1 { sub(/^[^=]*=/, "", $1); print $1; exit }' <<<"$configs")"
  if [[ "$actual_config" != "$config_value" ]]; then
    echo "ERROR: configuración incompatible para ${topic}: ${config_key}=${actual_config:-<no definida>}; se esperaba ${config_key}=${config_value}." >&2
    return 1
  fi

  echo "OK: ${topic} particiones=${partitions}, réplicas=${replication_factor}, ${config_key}=${config_value}."
}

create_and_check_topic envios.estados 3 retention.ms 2592000000
create_and_check_topic envios.promesas 3 cleanup.policy compact
create_and_check_topic envios.reprogramaciones 3 retention.ms 2592000000
create_and_check_topic alertas.sla 3 retention.ms 604800000
create_and_check_topic notificaciones.enviadas 3 retention.ms 604800000
create_and_check_topic envios.estados.dlq 1 retention.ms 2592000000

echo "Inicialización de topics finalizada."