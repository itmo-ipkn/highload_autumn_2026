#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "$0")/lib.sh"

begin_experiment exp1
reset_stack
wait_for_seconds

# Sarama uses the same client-id for all pods. Join Kafka's HOST to the
# Compose network address rather than guessing ownership from a member UUID.
ip_of() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$("${COMPOSE[@]}" ps -q "$1")"
}
row_for_host() {
  awk -v host="/$1" -v topic="${EVENTS_TOPIC:-events}" '
    $1=="GROUP" {for(i=1;i<=NF;i++) col[$i]=i; next}
    col["HOST"] && $(col["HOST"])==host && $(col["TOPIC"])==topic {
      print $(col["PARTITION"]), $(col["CURRENT-OFFSET"])
    }' "$2"
}
victim=$(ip_of consumer-1)
# Freeze the process so it cannot advance the commit between describe and KILL.
# Complete describe within the consumer session timeout (otherwise no owner).
"${COMPOSE[@]}" kill -s SIGSTOP consumer-1
group_state | tee "$ARTIFACT_DIR/group-before.txt"
row=$(row_for_host "$victim" "$ARTIFACT_DIR/group-before.txt")
[[ $row =~ ^([0-9]+)[[:space:]]([0-9]+)$ ]] || { echo 'Missing unique committed partition of consumer-1' >&2; exit 1; }
partition=${BASH_REMATCH[1]}; committed=${BASH_REMATCH[2]}
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
"${COMPOSE[@]}" kill -s SIGKILL consumer-1

deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
owner=''
while (( SECONDS < deadline )); do
  if group_state > "$ARTIFACT_DIR/group-after.txt"; then
    for pod in consumer-2 consumer-3; do
      if row_for_host "$(ip_of "$pod")" "$ARTIFACT_DIR/group-after.txt" | awk -v p="$partition" '$1==p {found=1} END {exit !found}'; then
        owner=$pod; break 2
      fi
    done
  fi
  sleep 1
done
[[ -n $owner ]] || { echo 'No takeover observed' >&2; exit 1; }
cat "$ARTIFACT_DIR/group-after.txt"
"${COMPOSE[@]}" logs --since "$since" --no-log-prefix "$owner" | jq -Rc --argjson p "$partition" --arg t "${EVENTS_TOPIC:-events}" 'fromjson? | select(.msg=="first claim message" and .partition==$p and .topic==$t)' > "$ARTIFACT_DIR/claim.jsonl"
first=$(jq -sr '.[0].offset // empty' "$ARTIFACT_DIR/claim.jsonl")
printf 'partition=%s old=consumer-1 committed_next=%s new=%s first_read=%s\n' "$partition" "$committed" "$owner" "$first" | tee "$ARTIFACT_DIR/takeover.txt"
[[ $first == "$committed" ]] || { echo 'Resume offset not confirmed' >&2; exit 1; }
"${COMPOSE[@]}" start consumer-1
wait_for_seconds
stop_producers
wait_for_zero_lag
verify_passes 2>&1 | tee "$ARTIFACT_DIR/exp1-verify.txt"
