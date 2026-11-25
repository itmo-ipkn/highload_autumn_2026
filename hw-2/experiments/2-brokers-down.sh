#!/usr/bin/env bash
# Эксперимент 2: отказ брокеров.
#   фаза A: стоп kafka-1 (ISR {2,3}, кворум KRaft 2 из 3 жив), затем min.insync.replicas
#           топика events поднимается до 3 - живых реплик меньше, чем требует acks=all;
#           через 40 с возвращается 2;
#   фаза B: стоп kafka-2 - остаётся один узел, кворум контроллеров потерян;
#   возврат kafka-1 и kafka-2.
EXP=2
. "$(dirname "$0")/lib.sh"

fresh 60

describe kafka-1 >"$RESULTS/2-describe-0-before.txt"

log "stop kafka-1"
mark stop_kafka1
docker compose stop kafka-1 >/dev/null 2>&1
sleep 45
describe kafka-2 >"$RESULTS/2-describe-1-kafka1-down.txt"

log "min.insync.replicas=3"
mark min_isr_3
min_isr kafka-2 3 >"$RESULTS/2-min-isr.txt"
sleep 40
describe kafka-2 >"$RESULTS/2-describe-2-min-isr-3.txt"
log "min.insync.replicas=2"
mark min_isr_2
min_isr kafka-2 2 >>"$RESULTS/2-min-isr.txt"
sleep 30

log "stop kafka-2"
mark stop_kafka2
docker compose stop kafka-2 >/dev/null 2>&1
sleep 60
describe kafka-3 >"$RESULTS/2-describe-3-one-broker.txt"
quorum kafka-3 >"$RESULTS/2-quorum-one-broker.txt"

log "start kafka-1 kafka-2"
mark start_brokers
docker compose start kafka-1 kafka-2 >/dev/null 2>&1
sleep 90
describe kafka-1 >"$RESULTS/2-describe-4-after.txt"
quorum kafka-1 >"$RESULTS/2-quorum-after.txt"
mark recovered

stop_and_verify "$RESULTS/2.txt" 5m

logs_grep 'produce batch rejected|batch in a produce request failed|produce failed|"stats"|"stopped"|moving producing partition' \
  "$RESULTS/2-producers.log" producer-1 producer-2 producer-3
logs_grep '"level":"(WARN|ERROR)"|partitions (assigned|revoked|lost)|partition resumed' "$RESULTS/2-consumers.log" \
  consumer-1 consumer-2 consumer-3
log "done"
