#!/usr/bin/env bash
# Delete only exact recorded IDs, after label/name/folder verification.
set -euo pipefail
cd "$(dirname "$0")/.."
source cloud/env.local
: "${LAB_NAME:?}" "${YC_FOLDER_ID:?}"
STATE="cloud/.state/$LAB_NAME"
[[ -d "$STATE" ]] || { echo 'No local state; refusing discovery-based deletion'; exit 2; }
[[ $(cat "$STATE/folder-id.txt") == "$YC_FOLDER_ID" ]] || { echo 'Folder mismatch'; exit 2; }
[[ "${CONFIRM_DELETE:-}" == "$LAB_NAME" ]] || { echo "Deletion not approved. Set CONFIRM_DELETE exactly to $LAB_NAME after saving evidence."; exit 0; }
id_of() { python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["id"])' "$1"; }
for ITEM in instance security-group subnet network; do
  FILE="$STATE/$ITEM.json"
  [[ -s "$FILE" ]] || continue
  [[ -f "$STATE/$ITEM.deleted" ]] && continue
  ID=$(id_of "$FILE")
  if [[ "$ITEM" == instance ]]; then CMD=(compute instance); else CMD=(vpc "$ITEM"); fi
  yc --profile "$YC_PROFILE" "${CMD[@]}" get --id "$ID" --folder-id "$YC_FOLDER_ID" --format json > "$STATE/$ITEM.before-delete.json"
  python3 - "$STATE/$ITEM.before-delete.json" "$LAB_NAME" "$YC_FOLDER_ID" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); assert x.get('labels',{}).get('lab')==sys.argv[2], 'Label mismatch'
assert x.get('folder_id')==sys.argv[3], 'Folder mismatch'
PY
  yc --profile "$YC_PROFILE" "${CMD[@]}" delete --id "$ID" --folder-id "$YC_FOLDER_ID"
  date -u +%FT%TZ > "$STATE/$ITEM.deleted"
done
if [[ -s "$STATE/boot-disk-id.txt" ]]; then
  DISK_ID=$(cat "$STATE/boot-disk-id.txt")
  set +e
  yc --profile "$YC_PROFILE" compute disk get --id "$DISK_ID" \
    --folder-id "$YC_FOLDER_ID" --format json \
    > "$STATE/disk-after.json" 2> "$STATE/disk-after.stderr"
  DISK_RC=$?
  set -e
  if (( DISK_RC == 0 )); then
    echo "Boot disk $DISK_ID still exists. Inspect this exact ID; not deleting it silently." >&2
    exit 1
  fi
  # Accept only the explicit gRPC NotFound status, never auth/network/permission errors.
  if ! grep -Eq 'code[[:space:]]*=[[:space:]]*NotFound' "$STATE/disk-after.stderr"; then
    echo 'Disk absence is NOT verified. Inspect disk-after.stderr / current CLI error format.' >&2
    exit 1
  fi
  date -u +%FT%TZ > "$STATE/boot-disk.absence-verified"
fi
echo 'Recorded VM/network resources deleted. Verify boot disk ID is absent (auto-delete) and review remaining billable resources.'
echo 'Do not delete any other disk or network merely because its name looks similar.'
