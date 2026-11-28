#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data

client() { docker compose run --rm -T --no-deps lab "$@"; }
recover_service=""
recover() {
  if [ -n "$recover_service" ]; then
    docker compose start "$recover_service"
    client kafka _recovered
  fi
}

case "${1:-help}" in
  prepare)
    docker compose config --quiet
    docker compose pull rabbit kafka1 kafka2 kafka3
    docker compose build lab
    docker compose up -d --wait --wait-timeout 180 rabbit kafka1 kafka2 kafka3
    client check
    ;;
  up)
    docker compose up -d --wait --wait-timeout 180 rabbit kafka1 kafka2 kafka3
    client check
    ;;
  check) client check ;;
  rabbit) client rabbit "${2:?Specify routing, redelivery, idempotency, dlq or prefetch}" ;;
  kafka)
    case "${2:-}" in
      failover|min-isr)
        docker compose up -d --wait --wait-timeout 180 kafka1 kafka2 kafka3
        client kafka baseline
        stopped_id="$(client kafka _leader)"
        case "$stopped_id" in 1|2|3) ;; *) echo "Unexpected broker ID: $stopped_id" >&2; exit 1 ;; esac
        recover_service="kafka${stopped_id}"
        trap recover EXIT
        trap 'exit 130' INT
        trap 'exit 143' TERM
        docker compose stop -t 10 "$recover_service"
        client kafka _after-stop
        if [ "$2" = "min-isr" ]; then client kafka _min-isr; fi
        ;;
      groups|replay|commit|baseline) client kafka "$2" ;;
      *) echo 'kafka: groups | replay | commit | baseline | failover | min-isr' >&2; exit 2 ;;
    esac
    ;;
  all)
    client check
    for area in rabbit kafka; do
      if [ "$area" = rabbit ]; then
        steps="routing redelivery idempotency dlq"
      else
        steps="groups replay commit baseline failover min-isr"
      fi
      for step in $steps; do
        printf 'RUN %s.%s\n' "$area" "$step"
        check_log="$(mktemp)"
        if LAB_DEMO_DELAY=0 ./lab.sh "$area" "$step" >"$check_log" 2>&1; then
          printf 'OK  %s.%s\n' "$area" "$step"
          rm -f "$check_log"
        else
          code=$?
          cat "$check_log"
          rm -f "$check_log"
          exit "$code"
        fi
      done
    done
    ;;
  stop) docker compose down ;;
  export) docker compose run --rm -T --no-deps --entrypoint tar lab -C /data -cf - . | tar -xf - -C data ;;
  help|*) echo './lab.sh prepare | up | check | rabbit STEP | kafka STEP | all | export | stop' ;;
esac
