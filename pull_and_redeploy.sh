#!/usr/bin/env bash
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Error: this script must be run as root." >&2
  exit 1
fi

cd GitHub-Projects-Showcase-Plus-AI-Chat_SaaS-Multi-tenant
git pull
cd infra/compose
sudo docker compose up --build -d
