#!/usr/bin/env bash
# Одноразовое создание топиков. Скрипт идемпотентен: повторный запуск не падает
# и приводит конфиги существующих топиков к ожидаемым значениям.
set -euo pipefail

BIN=/opt/kafka/bin
BOOTSTRAP="${BOOTSTRAP_SERVERS:?BOOTSTRAP_SERVERS is required}"
EXPECTED_BROKERS="${EXPECTED_BROKERS:-3}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
RF="${TOPIC_REPLICATION_FACTOR:-3}"
MIN_ISR="${TOPIC_MIN_ISR:-2}"
EVENTS_TOPIC="${EVENTS_TOPIC:-events}"
DLT_TOPIC="${DLT_TOPIC:-events.dlt}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-120}"

log() { echo "[kafka-init] $(date -u +%H:%M:%S) $*"; }

# Healthcheck видит только открытый порт. Здесь ждём, пока все брокеры
# зарегистрируются в кластере: иначе RF=3 не создать.
wait_for_brokers() {
  local deadline=$((SECONDS + WAIT_TIMEOUT_SEC)) count
  while :; do
    count=$("$BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" 2>/dev/null \
      | grep -cE '^[^ ]+:[0-9]+ \(id: [0-9]+' || true)
    if [ "$count" -ge "$EXPECTED_BROKERS" ]; then
      log "brokers online: $count/$EXPECTED_BROKERS"
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      log "timeout: brokers online $count/$EXPECTED_BROKERS"
      return 1
    fi
    log "waiting for brokers: $count/$EXPECTED_BROKERS"
    sleep 2
  done
}

# create_topic <name> <config...>
create_topic() {
  local topic="$1"; shift
  local cfg_args=() cfg_csv=""
  for c in "$@"; do
    cfg_args+=(--config "$c")
    cfg_csv="${cfg_csv:+$cfg_csv,}$c"
  done

  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$RF" "${cfg_args[@]}"

  # --if-not-exists не меняет конфиг уже существующего топика, поэтому выравниваем явно.
  "$BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --alter \
    --entity-type topics --entity-name "$topic" --add-config "$cfg_csv" >/dev/null

  # Количество партиций проверяем. Если меньше ожидаемого, падаем: менять его молча нельзя,
  # это сломает соответствие ключ → партиция.
  local actual
  actual=$("$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic" \
    | grep -c 'Partition: ' || true)
  if [ "$actual" -ne "$PARTITIONS" ]; then
    log "ERROR: topic $topic has $actual partitions, expected $PARTITIONS (run 'make reset')"
    return 1
  fi
  log "topic $topic ready"
}

wait_for_brokers

create_topic "$EVENTS_TOPIC" "min.insync.replicas=$MIN_ISR"
# Задание требует для DLT только RF=3. min.insync.replicas=2 задан явно, чтобы ядовитое
# событие не потерялось при отказе брокера: консьюмер закоммитит оффсет только после
# записи в DLT, подтверждённой двумя репликами.
create_topic "$DLT_TOPIC" "min.insync.replicas=$MIN_ISR"

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$EVENTS_TOPIC"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$DLT_TOPIC"
log "done"
