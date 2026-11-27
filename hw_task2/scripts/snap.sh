#!/usr/bin/env bash
# Export the Grafana dashboard as PNG via grafana-image-renderer.
#   scripts/snap.sh docs/img/exp1.png 10m        # last 10 minutes
#   scripts/snap.sh docs/img/exp1.png 19:40 19:52 # time window (today, local time)
set -euo pipefail
out=$1; shift
if [ $# -eq 2 ]; then
  from=$(( $(date -d "$1" +%s) * 1000 )); to=$(( $(date -d "$2" +%s) * 1000 ))
else
  from="now-${1:-15m}"; to=now
fi
curl -sf -o "$out" "http://localhost:3000/render/d/kafka-hw2/hw2-kafka-events?orgId=1&from=$from&to=$to&width=1600&height=1300&kiosk&tz=Europe%2FMoscow"
echo "saved $out"
