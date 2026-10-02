#!/usr/bin/env bash
# Save an evidence bundle for ONE run. No cloud calls and no secret dumps.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=${1:-$(cat results/.last-run.txt)}
[[ -d "$RUN" && -f "$RUN/end.txt" && -f "$RUN/start.txt" ]] || { echo 'Expected a completed run directory' >&2; exit 2; }
OUT="$RUN/observations"
[[ ! -e "$OUT" ]] || { echo 'Observations already exist; refusing overwrite' >&2; exit 2; }
mkdir "$OUT"
# Anchor to actual k6 request starts, not delayed Docker container teardown.
AT=$(python3 - "$RUN/summary.json" <<'PYTIME'
import json,math,sys
x=json.load(open(sys.argv[1]))['summary']['metrics']['client_request_start_unix_ms']['values']
first,last=float(x['min']),float(x['max'])
if not (math.isfinite(first) and math.isfinite(last) and first <= last):
    raise SystemExit('Invalid k6 request time bounds')
# Leave two seconds before the final request. Rate still uses its own 1m window.
print(math.floor(last/1000)-2)
PYTIME
)
printf '%s\n' "$AT" > "$OUT/evaluation-unix.txt"
curl -fsS --max-time 10 http://127.0.0.1:8080/metrics > "$OUT/app.prom"
curl -fsS --max-time 10 http://127.0.0.1:9090/api/v1/targets > "$OUT/targets.json"
curl -fsS --max-time 10 http://127.0.0.1:9090/api/v1/alerts > "$OUT/alerts.json"
docker compose ps > "$OUT/compose-ps.txt"
docker compose images > "$OUT/images.txt"
IDS=$(docker compose --profile host ps -q)
if [[ -n "$IDS" ]]; then docker stats --no-stream $IDS > "$OUT/docker-stats.txt"; fi
docker compose logs --no-color --tail 120 app > "$OUT/app.log"
for KEY in started_rps completed_rps success_rps good_rps errors mean p50 p95 p99 good up inflight active waiting capacity queue_p95 service_mean cpu rss host_cpu host_ram host_busy_core mode resets; do
  bash scripts/query.sh "$KEY" "$AT" > "$OUT/$KEY.json"
done
# A 60 s range ending at AT preserves the transition/plateau instead of one number.
python3 - "$AT" <<'PYRANGE' > "$OUT/good-range.json"
import json,sys,urllib.parse,urllib.request
at=int(sys.argv[1]); query=json.load(open('prometheus/queries.json'))['good']
url='http://127.0.0.1:9090/api/v1/query_range?' + urllib.parse.urlencode({'query':query,'start':at-60,'end':at,'step':5})
with urllib.request.urlopen(url,timeout=10) as r: x=json.load(r)
assert x.get('status')=='success', x
print(json.dumps(x,indent=2))
PYRANGE
echo "$OUT"
