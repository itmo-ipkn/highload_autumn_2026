# Подключается из каталога hw2 командой source ./experiments.sh.
cli=kafka-3

group() {
  docker compose exec -T "$cli" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server "$cli:9092" --describe --group events-processor
}

topic() {
  docker compose exec -T "$cli" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$cli:9092" --describe --topic events
}

messages() {
  docker compose logs --no-color --since "$fault" "$@" |
    sed -nE 's/^([^ ]+).*timestamp=([^[:space:]]+).*text=(.*)$/\1 \2 \3/p'
}

duplicates() {
  docker compose exec -T prometheus wget -qO- http://observer:8080/metrics |
    grep '^hw4_sink_duplicates_total '
}

stop_producers() {
  docker compose stop producer-1 producer-2 producer-3
}

start_run() {
  stop_producers || return 1
  since=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
  KEY_MODE="$1" docker compose up -d --no-deps --force-recreate producer-1 producer-2 producer-3
}

wait_lag_zero() {
  local report attempt
  for ((attempt=0; attempt<30; attempt++)); do
    if report=$(group) && awk '$1 == "events-processor" && $2 == "events" && $6 == "0" {n++} END {exit n != 3}' <<< "$report"; then
      printf '%s\n' "$report"
      return 0
    fi
    sleep 2
  done
  printf '%s\n' "$report" >&2
  return 1
}
