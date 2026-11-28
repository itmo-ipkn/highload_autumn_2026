#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname -- "$0")"
check() {
  local expected=$1 actual
  shift
  actual=$(printf '%s\n' "$@" | awk -v group=g -v topic=t -f lag.awk)
  [[ $actual == "$expected" ]] || { echo "expected '$expected', got '$actual'" >&2; exit 1; }
}
header='GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID'
rows=('g t 0 15 15 0 c1' 'g t 2 12 12 0 c3')
check 0 "$header" "${rows[@]}" 'g t 1 10 10 0 c2' 'other t 1 10 25 15 c2' 'g other 0 1 9 8 c1'
check 5 "$header" "${rows[@]}" 'g t 1 10 15 5 c2'
check unknown "$header" "${rows[@]}"
check unknown "$header" "${rows[@]}" 'g t 1 - 15 - c2'
check unknown "$header" "${rows[@]}" "${rows[0]}" 'g t 1 10 10 0 c2'
check 0 'PARTITION LAG TOPIC GROUP' '2 0 t g' '0 0 t g' '1 0 t g'
echo 'Kafka CLI lag fixtures: PASS'
