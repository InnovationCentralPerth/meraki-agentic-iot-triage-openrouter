#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# 01_bootstrap.sh — One-time setup on your Ubuntu server.
#
# Edit APP_DIR and APP_USER below to match your server, then run once via SSH
# after the first code sync:
#   ssh <user>@<your-server> "sudo bash <APP_DIR>/deploy/01_bootstrap.sh"
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

APP_DIR="/opt/meraki-iot-triage"   # ← change to your deployment path
APP_USER="<user>"                  # ← change to the OS user that owns the app

echo "=== System packages ==="
apt-get update -q
apt-get install -y -q python3 python3-pip python3-venv curl

echo "=== Checking .env ==="
if [[ ! -f "$APP_DIR/.env" ]]; then
  echo "ERROR: $APP_DIR/.env not found."
  echo "Copy your .env to the server first:"
  echo "  scp .env <user>@<your-server>:$APP_DIR/.env"
  exit 1
fi
chmod 600 "$APP_DIR/.env"

echo "=== Python venv + dependencies ==="
python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install --quiet --upgrade pip
"$APP_DIR/.venv/bin/pip" install --quiet \
  "langgraph>=0.2" "langchain>=0.3" "langchain-openai>=0.2" \
  "httpx>=0.27" "python-dotenv>=1.0" "pydantic>=2.7" "rich>=13" \
  "fastapi>=0.115" "uvicorn[standard]>=0.30" "sse-starlette>=2.1" \
  "fastmcp>=0.9" "slack-sdk>=3.27"

echo "=== Installing systemd services ==="
for svc in server mcp_stub; do
  cp "$APP_DIR/deploy/systemd/${svc}.service" /etc/systemd/system/
done
systemctl daemon-reload
systemctl enable server mcp_stub

echo "=== Starting services ==="
systemctl start mcp_stub
sleep 2
systemctl start server

echo ""
echo "=== Bootstrap complete ==="
systemctl is-active --quiet server   && echo "server   : running" || echo "server   : FAILED — check: journalctl -u server -n 30"
systemctl is-active --quiet mcp_stub && echo "mcp_stub : running" || echo "mcp_stub : FAILED — check: journalctl -u mcp_stub -n 30"
echo ""
echo "Dashboard : http://<your-server>:8090"
echo "MCP stub  : http://localhost:8889/mcp  (local only)"
