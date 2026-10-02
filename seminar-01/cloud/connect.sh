#!/usr/bin/env bash
# Run AFTER verifying and accepting the host key (docs/01_from_zero.md).
set -euo pipefail
cd "$(dirname "$0")/.."
source cloud/env.local
: "${LAB_NAME:?}" "${SSH_PRIVATE_KEY_FILE:?}"
IP=$(cat "cloud/.state/$LAB_NAME/ip.txt")
exec ssh -i "$SSH_PRIVATE_KEY_FILE" -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$PWD/cloud/known_hosts" \
  -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -L "127.0.0.1:${LOCAL_GRAFANA_PORT:-3000}:127.0.0.1:3000" \
  -L "127.0.0.1:${LOCAL_PROMETHEUS_PORT:-9090}:127.0.0.1:9090" \
  -L "127.0.0.1:${LOCAL_API_PORT:-8080}:127.0.0.1:8080" \
  -L "127.0.0.1:${LOCAL_ADMIN_PORT:-8081}:127.0.0.1:8081" \
  "lab@$IP"
