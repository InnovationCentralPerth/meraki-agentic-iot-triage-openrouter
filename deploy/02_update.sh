#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# 02_update.sh — Sync code to your server and restart services.
#
# Edit REMOTE and APP_DIR below, then run from your local machine (project root):
#   bash deploy/02_update.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REMOTE="<user>@<your-server>"   # ← change to your server
APP_DIR="/opt/meraki-iot-triage"  # ← change to your deployment path
RSYNC_EXCLUDE=(
  --exclude='.venv'
  --exclude='__pycache__'
  --exclude='*.pyc'
  --exclude='.env'
  --exclude='.git'
)

echo "=== Syncing code to $REMOTE:$APP_DIR ==="
ssh "$REMOTE" "mkdir -p $APP_DIR"
rsync -az --delete "${RSYNC_EXCLUDE[@]}" ./ "$REMOTE:$APP_DIR/"

echo "=== Restarting services ==="
ssh "$REMOTE" "sudo systemctl restart mcp_stub server"

echo "=== Service status ==="
ssh "$REMOTE" "systemctl is-active server mcp_stub"
echo "Done — dashboard: http://<your-server>:8090"
