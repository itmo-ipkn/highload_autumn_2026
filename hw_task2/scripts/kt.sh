#!/usr/bin/env bash
# Run Kafka CLI tools from an alive broker container.
#   scripts/kt.sh topics        - describe events / events.dlt (leaders, ISR)
#   scripts/kt.sh group         - describe consumer group events-processor (offsets, lag, owners)
#   scripts/kt.sh <tool> <args> - any /opt/kafka/bin/<tool>.sh
set -euo pipefail
BS=kafka-1:19092,kafka-2:19092,kafka-3:19092
for b in kafka-1 kafka-2 kafka-3; do
  if [ "$(docker compose ps -q --status running "$b" 2>/dev/null)" ]; then C=$b; break; fi
done
: "${C:?no running broker}"
run() { docker compose exec -T "$C" "/opt/kafka/bin/$1.sh" --bootstrap-server "$BS" "${@:2}"; }
case "${1:-topics}" in
  topics) run kafka-topics --describe --topic events; run kafka-topics --describe --topic events.dlt ;;
  group)  run kafka-consumer-groups --describe --group events-processor ;;
  *)      run "$@" ;;
esac
