#!/usr/bin/env bash
# Run the verification CLI as a one-shot compose service and keep the evidence.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo 'Copy .env.example to .env and run python scripts/configure.py'; exit 1; }
# GitHub login printed on the first output line (HW requirement). An exported value
# overrides .env for this run only; keep .env in sync for the other scripts.
export GITHUB_LOGIN=${GITHUB_LOGIN:-tpl20000}
LABEL=${1:-manual}
[[ "$LABEL" =~ ^[a-zA-Z0-9_-]{1,60}$ ]] || { echo 'Unsafe run label' >&2; exit 2; }
# Plain `bash` on Windows is WSL bash, whose docker CLI has no compose plugin; the
# scripts must run through Git Bash ("C:\Program Files\Git\bin\bash.exe").
docker compose version >/dev/null 2>&1 || {
  echo 'docker compose is not available in this shell (WSL bash has no compose plugin).' >&2
  echo 'Run it through Git Bash: "C:\Program Files\Git\bin\bash.exe" scripts/verify.sh '"$LABEL" >&2
  exit 2
}
mkdir -p results docs/evidence
OUT="results/verify-$(date -u +%Y%m%dT%H%M%SZ)-$LABEL.txt"
set +e
docker compose run --rm -T verify | tee "$OUT"
RC=${PIPESTATUS[0]}
set -e
cp "$OUT" docs/evidence/verify-$LABEL.txt
echo "verify exit=$RC; evidence: $OUT" >&2
exit "$RC"
