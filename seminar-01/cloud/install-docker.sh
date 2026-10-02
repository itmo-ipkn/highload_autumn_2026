#!/usr/bin/env bash
# Execute on the dedicated Ubuntu VM. Uses the official signed Docker apt repository.
set -euo pipefail
source /etc/os-release
[[ "$ID" == ubuntu && "$VERSION_ID" == 24.04 ]] || { echo 'Expected Ubuntu 24.04. Review installation for this OS.'; exit 2; }
if command -v docker >/dev/null; then
  sudo docker version; sudo docker compose version
  echo 'Existing Docker detected; no package replacement performed.'
  exit 0
fi
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 install -y ca-certificates curl python3
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
ARCH=$(dpkg --print-architecture)
printf '%s\n' \
  'Types: deb' 'URIs: https://download.docker.com/linux/ubuntu' \
  "Suites: $VERSION_CODENAME" 'Components: stable' "Architectures: $ARCH" \
  'Signed-By: /etc/apt/keyrings/docker.asc' | sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
# Docker group is root-equivalent. Only this isolated lab user gets it.
sudo usermod -aG docker lab
sudo docker version
sudo docker compose version
echo 'Open a NEW SSH session before using docker without sudo.'
