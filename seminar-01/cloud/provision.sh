#!/usr/bin/env bash
# Same commands as docs/01_from_zero.md. Run on the laptop, not the VM.
set -euo pipefail
cd "$(dirname "$0")/.."
source cloud/env.local
# STEP 1 — validate the target and show the exact resource envelope.
: "${YC_PROFILE:?}" "${YC_CLOUD_ID:?}" "${YC_FOLDER_ID:?}" "${YC_ZONE:?}"
: "${LAB_NAME:?}" "${SSH_CIDR:?}" "${SSH_PUBLIC_KEY_FILE:?}"
[[ "$LAB_NAME" =~ ^[a-z][a-z0-9-]{3,39}$ ]] || { echo 'Invalid LAB_NAME'; exit 2; }
[[ -f "$SSH_PUBLIC_KEY_FILE" ]] || { echo 'Public SSH key missing'; exit 2; }
python3 - "$SSH_CIDR" <<'PYCIDR'
import ipaddress, sys
net = ipaddress.ip_network(sys.argv[1], strict=True)
if not (net.version == 4 and net.prefixlen == 32 and net.network_address.is_global):
    raise SystemExit('SSH_CIDR must be your public IPv4/32, not 0.0.0.0/0')
PYCIDR
command -v yc >/dev/null
yc version
yc --profile "$YC_PROFILE" resource-manager folder get --id "$YC_FOLDER_ID" --format json |
  python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["cloud_id"]==sys.argv[1], "Cloud/folder mismatch"; print("Folder:", x["name"], x["id"])' "$YC_CLOUD_ID"
yc --profile "$YC_PROFILE" compute zone get "$YC_ZONE" --format json >/dev/null
printf 'PLAN: cloud=%s folder=%s zone=%s name=%s\n' "$YC_CLOUD_ID" "$YC_FOLDER_ID" "$YC_ZONE" "$LAB_NAME"
echo 'ONE VM: 4 vCPU/100%, 8 GB RAM, 30 GB network-ssd auto-delete boot disk.'
echo 'ONE dynamic public IPv4, own network/subnet/security group; no service account.'
printf 'Ingress: TCP/22 from %s only. This creates billable resources.\n' "$SSH_CIDR"
[[ "${APPROVE_CLOUD_CREATE:-NO}" == YES ]] || { echo 'Plan only. Review costs; set APPROVE_CLOUD_CREATE=YES to create.'; exit 0; }
# STEP 2 — create an append-only local record of exactly OUR resources.
umask 077
STATE="$PWD/cloud/.state/$LAB_NAME"
[[ ! -e "$STATE" ]] || { echo "State exists: $STATE. Inspect it; never erase it to retry."; exit 2; }
mkdir -p "$STATE"
printf '%s\n' "$YC_FOLDER_ID" > "$STATE/folder-id.txt"
printf '%s\n' "$LAB_NAME" > "$STATE/lab-name.txt"
yc version > "$STATE/yc-version.txt"
date -u +%FT%TZ > "$STATE/create-start.txt"
trap 'echo "Stopped: inspect $STATE and actual yc resources/operations before retrying." >&2' ERR
id_of() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$1"; }
# STEP 3 — resolve the image and prepare first-boot instructions, with no secrets.
yc --profile "$YC_PROFILE" compute image get-latest-from-family ubuntu-2404-lts \
  --folder-id standard-images --format json > "$STATE/image.json"
IMAGE_ID=$(id_of "$STATE/image.json")
python3 cloud/render_cloud_init.py "$SSH_PUBLIC_KEY_FILE" "$STATE/cloud-init.json"
# STEP 4 — create the network and a subnet in the SAME zone as the VM.
yc --profile "$YC_PROFILE" vpc network create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME-net" --labels "course=highload-s1,lab=$LAB_NAME" \
  --format json > "$STATE/network.json"
NETWORK_ID=$(id_of "$STATE/network.json")
yc --profile "$YC_PROFILE" vpc subnet create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME-subnet" --zone "$YC_ZONE" --network-id "$NETWORK_ID" \
  --range 10.231.0.0/24 --labels "course=highload-s1,lab=$LAB_NAME" \
  --format json > "$STATE/subnet.json"
SUBNET_ID=$(id_of "$STATE/subnet.json")
# STEP 5 — only SSH comes IN; package/image downloads may go OUT.
yc --profile "$YC_PROFILE" vpc security-group create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME-sg" --network-id "$NETWORK_ID" \
  --labels "course=highload-s1,lab=$LAB_NAME" \
  --rule "direction=ingress,protocol=tcp,port=22,v4-cidrs=$SSH_CIDR" \
  --rule "direction=egress,protocol=any,from-port=0,to-port=65535,v4-cidrs=0.0.0.0/0" \
  --format json > "$STATE/security-group.json"
SG_ID=$(id_of "$STATE/security-group.json")
# STEP 6 — create a FRESH VM; Docker installs during first boot via cloud-init.
yc --profile "$YC_PROFILE" compute instance create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME" --zone "$YC_ZONE" --platform standard-v3 \
  --cores 4 --core-fraction 100 --memory 8 \
  --labels "course=highload-s1,lab=$LAB_NAME" \
  --create-boot-disk "name=$LAB_NAME-boot,type=network-ssd,size=30,image-id=$IMAGE_ID,auto-delete=true" \
  --network-interface "subnet-id=$SUBNET_ID,nat-ip-version=ipv4,security-group-ids=$SG_ID" \
  --metadata-from-file "user-data=$STATE/cloud-init.json" \
  --format json > "$STATE/instance.json"
# STEP 7 — record the returned VM/disk/IP, not names guessed from a console screen.
python3 - "$STATE/instance.json" "$STATE" <<'PYSTATE'
import json, sys
from pathlib import Path
x = json.load(open(sys.argv[1])); p = Path(sys.argv[2])
ip = x['network_interfaces'][0]['primary_v4_address']['one_to_one_nat']['address']
(p/'ip.txt').write_text(ip+'\n')
(p/'instance-id.txt').write_text(x['id']+'\n')
(p/'boot-disk-id.txt').write_text(x['boot_disk']['disk_id']+'\n')
print('VM:', x['id'], 'IP:', ip)
PYSTATE
date -u +%FT%TZ > "$STATE/create-returned.txt"
echo 'VM resource created. This does NOT prove cloud-init, Docker, or the application is ready.'
