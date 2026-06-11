# Meraki IoT Triage Agent

A multi-agent IoT monitoring and triage pipeline for Cisco Meraki MT10 environmental sensors. Detects anomalies in temperature, humidity, and CO₂ readings, runs parallel AI analysis via OpenRouter, and delivers Slack alerts with automatic model fallback on provider failure.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Application Requirements](#2-application-requirements)
3. [System Design](#3-system-design)
4. [Implementation](#4-implementation)
5. [Configuration Reference](#5-configuration-reference)
6. [Local Development](#6-local-development)
7. [Production Deployment](#7-production-deployment)
8. [Operations](#8-operations)

---

## 1. Overview

### Problem

Cisco Meraki MT10 sensors generate continuous environmental telemetry. When thresholds are breached — high temperature, excess CO₂, abnormal humidity — a human operator needs immediate context: what happened, how severe is it, what likely caused it, and what action to take. Manual monitoring is not viable at scale.

### Solution

An autonomous triage agent that polls the sensor (or simulator), detects threshold breaches, runs three specialised AI sub-agents in parallel, and sends a structured Slack alert. The pipeline recovers gracefully when any AI provider is unavailable by falling back to a secondary model automatically.

### Key Capabilities

- Real Meraki MT10 readings via the Meraki Dashboard API, with a built-in simulator when no API key is configured
- Parallel AI analysis across three specialist agents (anomaly analysis, alert composition, root cause advisory)
- OpenRouter integration — single API key, access to multiple free LLM providers with automatic fallback
- Operator-controlled chaos injection to test resilience in production
- Live SSE event stream powering a real-time dashboard
- Slack alerts sent only on state transitions (alert → clear → alert), never duplicated

---

## 2. Application Requirements

### 2.1 Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-01 | Fetch latest readings from a Cisco Meraki MT10 sensor via the Meraki Dashboard API |
| FR-02 | Fall back to a deterministic simulator when `MERAKI_API_KEY` is not set or the API is unreachable |
| FR-03 | Evaluate readings against configurable thresholds for temperature (high/low), humidity (high/low), and CO₂ (high) |
| FR-04 | Run anomaly analysis, alert composition, and root cause advisory as parallel AI agents |
| FR-05 | Route all LLM calls through OpenRouter using separate models per agent role |
| FR-06 | Fall back to a secondary model automatically when a primary model returns an error or rate-limit response |
| FR-07 | Send a Slack alert on anomaly detection and a separate "all clear" when conditions normalise |
| FR-08 | Suppress duplicate alerts — only send on state transitions (normal→anomaly, anomaly→normal) |
| FR-09 | Expose a real-time SSE event stream for dashboard consumption |
| FR-10 | Allow manual triage cycles and sensor condition injection via REST API |
| FR-11 | Allow operators to inject failure modes (model timeouts, errors) to demonstrate resilience |
| FR-12 | Allow the polling interval to be adjusted without restarting the service |

### 2.2 Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR-01 | Pipeline must complete within 60 seconds under normal conditions |
| NFR-02 | LLM failure in any one agent must not prevent the pipeline from completing |
| NFR-03 | All LLM calls must be non-blocking (async) |
| NFR-04 | Services must restart automatically after a crash (`Restart=always` in systemd) |
| NFR-05 | The `.env` file must not be committed to version control |
| NFR-06 | MCP stub must only bind to `127.0.0.1` — not publicly accessible |
| NFR-07 | The entire application (dashboard + API) must run from a single `uvicorn` command |
| NFR-08 | Runs on Python 3.11+ |

### 2.3 External Dependencies

| Service | Purpose | Required |
|---------|---------|----------|
| OpenRouter | LLM inference gateway (multi-provider, single API key) | Yes |
| Cisco Meraki Dashboard API | Live MT10 sensor readings | No — simulator used if absent |
| Slack Bot API | Alert delivery via `chat:write` | Yes (for alerts) |

---

## 3. System Design

### 3.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Client Browser                                                 │
│  http://<your-server>:8090                                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │ SSE + REST
┌───────────────────────────▼─────────────────────────────────────┐
│  server.py  (FastAPI, port 8090)                                │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │  Dashboard  │  │  REST API    │  │  SSE /events           │ │
│  │  (HTML/JS)  │  │  /triage     │  │  sensor · llm · chaos  │ │
│  └─────────────┘  │  /chaos      │  │  fallback · pipeline   │ │
│                   │  /inject_*   │  └────────────────────────┘ │
│                   └──────┬───────┘                              │
└──────────────────────────┼──────────────────────────────────────┘
                           │ asyncio.create_task
┌──────────────────────────▼──────────────────────────────────────┐
│  src/agent.py  —  LangGraph Triage Pipeline                     │
│                                                                 │
│  load_sensor                                                    │
│       │                                                         │
│       ├──────────────────────────────────┐                      │
│       │                                  │                      │
│  anomaly_analyst   alert_composer   root_cause_advisor          │
│  (parallel via Send())                                          │
│       │                                  │                      │
│       └──────────────┬───────────────────┘                      │
│                      │                                          │
│               merge_results                                     │
│                      │                                          │
│               send_mcp_alert                                    │
└──────────────┬───────────────────────────┬──────────────────────┘
               │                           │
┌──────────────▼──────────┐   ┌────────────▼──────────────────────┐
│  src/meraki_client.py   │   │  src/chaos.py                     │
│                         │   │  OpenRouter via ChatOpenAI         │
│  1. CRITICAL_INJECT     │   │                                   │
│  2. Meraki API (MT10)   │   │  primary model                    │
│  3. Simulator fallback  │   │    │ on error/429                 │
└─────────────────────────┘   │    └──▶ fallback model            │
                              │         │ on error                │
                              │         └──▶ degraded string      │
                              └───────────────────────────────────┘
                                           │ OpenRouter API
                              ┌────────────▼──────────────────────┐
                              │  https://openrouter.ai/api/v1     │
                              │  analyst  : gpt-oss-120b:free     │
                              │  composer : kimi-k2.6:free        │
                              │  advisor  : nemotron-550b:free    │
                              │  fallback : gpt-oss-20b:free      │
                              └───────────────────────────────────┘

               │ HTTP POST /mcp (localhost only)
┌──────────────▼──────────────────────────────────────────────────┐
│  mcp_stub.py  (FastMCP, port 8889 — 127.0.0.1 only)            │
│  tools: send_alert · get_status                                 │
└──────────────┬──────────────────────────────────────────────────┘
               │ Slack API
┌──────────────▼──────────────────────────────────────────────────┐
│  Slack  #<your-alerts-channel>                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 LangGraph Pipeline

The triage pipeline is a compiled LangGraph `StateGraph` with the following topology:

```
START
  └─▶ load_sensor
        └─▶ fan_out (Send × 3, parallel)
              ├─▶ anomaly_analyst
              ├─▶ alert_composer
              └─▶ root_cause_advisor
                    └─▶ merge_results  (waits for all 3)
                          └─▶ send_mcp_alert
                                └─▶ END
```

**State (`TriageState`)** carries the sensor reading, per-agent reports, model names, fallback flags, the final summary, alert result, and a merged error list across the full cycle.

**Fan-out** uses LangGraph's `Send()` primitive so all three sub-agents execute concurrently within the same event loop, not sequentially.

### 3.3 Resilience — Model Fallback Chain

Every LLM call is routed through `chaos.invoke_with_chaos()`:

```
invoke_with_chaos(model, chaos_flag, prompt)
  │
  ├─ chaos flag active? → raise RuntimeError (simulated failure)
  │
  ├─▶ primary model (OpenRouter)
  │     success → return (text, model, fallback=False)
  │     error   ──────────────────────────────────────────────────┐
  │                                                               │
  └─▶ fallback model (openai/gpt-oss-20b:free)                   │
        success → return (text, fallback_model, fallback=True)   │
        error   ──────────────────────────────────────────────────┘
                    │
                    └─▶ return degraded string, model="degraded"
```

The dashboard labels any fallback-sourced output with `*(fallback)*` so the operator always knows which model produced each section.

### 3.4 Alert State Machine

Slack messages are sent only on state transitions to prevent alert fatigue:

```
State        │  None (startup)  │  False (normal)  │  True (alert)
─────────────┼──────────────────┼──────────────────┼──────────────
→ any        │  skip (init)     │                  │
→ normal     │                  │  skip (quiet)    │  send "all clear"
→ anomaly    │                  │  send alert      │  skip (already sent)
```

### 3.5 Sensor Reading Priority

```
get_latest_reading()
  1. NORMAL_INJECT flag set?   → return warm non-critical reading (once)
  2. CRITICAL_INJECT flag set? → return all-thresholds-breached reading (once)
  3. MERAKI_API_KEY set?       → fetch live MT10 via Meraki Dashboard API
       API failure?            → fall through to simulator
  4. Simulator                 → 100s deterministic cycle (normal → ramp → critical → recovery)
```

---

## 4. Implementation

### 4.1 Project Structure

```
meraki-iot-triage/
├── server.py               # FastAPI app — dashboard, SSE, REST endpoints
├── mcp_stub.py             # Local MCP server — Slack alert delivery
├── demo_runner.py          # CLI runner for manual pipeline invocation
├── pyproject.toml          # Package metadata and dependencies
├── .env.example            # Environment variable template
│
├── src/
│   ├── __init__.py
│   ├── agent.py            # LangGraph pipeline definition
│   ├── chaos.py            # OpenRouter LLM wrapper + chaos injection + event bus
│   └── meraki_client.py    # Meraki API client + simulator + injection flags
│
└── deploy/
    ├── 01_bootstrap.sh     # One-time server setup (venv, systemd, services)
    ├── 02_update.sh        # Code sync + service restart
    └── systemd/
        ├── server.service
        └── mcp_stub.service
```

### 4.2 Key Modules

#### `src/chaos.py` — LLM Gateway

Central wrapper for all LLM calls. Responsibilities:
- Builds `ChatOpenAI` instances pointed at `https://openrouter.ai/api/v1`
- Injects required OpenRouter headers (`HTTP-Referer`, `X-Title`)
- Implements the primary → fallback → degraded call chain
- Maintains the in-process SSE event bus (`_emit`, `subscribe_events`, `unsubscribe_events`)
- Manages the `ChaosState` singleton used by the operator dashboard

#### `src/agent.py` — Pipeline

Defines the LangGraph `StateGraph` and all node functions. Each sub-agent node calls `invoke_with_chaos()` with its assigned model and chaos flag. The `send_mcp_alert` node posts a JSON-RPC 2.0 `tools/call` request to the local MCP stub.

#### `src/meraki_client.py` — Sensor

Handles Meraki API discovery (org → device by MAC address) and polling. `SensorReading.anomalies()` evaluates thresholds read from environment variables at call time, so threshold changes take effect on the next cycle without restart.

#### `server.py` — API + Dashboard

Single-file FastAPI application. The dashboard HTML is embedded as a string constant — no static file serving required. SSE clients connect to `/events` and receive the full last-known state immediately on connect (replayed from `_last_events` in `chaos.py`), so a browser refresh shows current data without waiting for the next cycle.

#### `mcp_stub.py` — Slack Bridge

FastMCP server exposing two tools (`send_alert`, `get_status`) over streamable HTTP. Binds to `127.0.0.1:8889` only — not accessible from outside the server. Requires `Accept: application/json, text/event-stream` on all requests.

### 4.3 REST API Reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Dashboard HTML |
| `GET` | `/events` | SSE stream (pipeline, chaos, LLM events) |
| `GET` | `/reading` | Latest sensor reading (JSON) |
| `POST` | `/triage` | Trigger one triage cycle manually |
| `POST` | `/inject_critical` | Inject all-threshold-breached reading + trigger triage |
| `POST` | `/inject_normal` | Inject normal (non-critical) reading + trigger triage |
| `GET` | `/chaos` | Current active chaos flags |
| `POST` | `/chaos` | Set or clear a chaos flag `{"flag": "analyst_timeout", "active": true}` |
| `GET` | `/config` | Current triage interval |
| `POST` | `/config/interval` | Update triage interval `{"minutes": 5}` |
| `GET` | `/health` | Liveness probe `{"ok": true}` |

### 4.4 SSE Event Types

Events emitted on the `/events` stream:

| Type | Meaning |
|------|---------|
| `sensor` | New sensor reading fetched |
| `llm_ok` | Primary model responded successfully |
| `llm_error` | Primary model failed — fallback being attempted |
| `llm_fallback_ok` | Fallback model succeeded |
| `llm_fatal` | Both primary and fallback failed — degraded response returned |
| `chaos` | Operator-injected failure triggered |
| `merge_done` | All three sub-agents complete — summary ready |
| `pipeline_done` | Full triage cycle complete with timing |
| `slack_sent` | Slack alert or all-clear delivered |
| `mcp_ok` | MCP tool call succeeded |
| `mcp_error` | MCP tool call failed — alert queued locally |
| `ping` | Keep-alive (every 1s when no other events) |

---

## 5. Configuration Reference

Copy `.env.example` to `.env` and fill in the required values.

```bash
# ── OpenRouter ───────────────────────────────────────────────────────────────
OPENROUTER_API_KEY=sk-or-v1-...          # Required
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_REFERER=https://github.com/InnovationCentralPerth/meraki-agentic-iot-triage-openrouter

# ── Models (OpenRouter model IDs) ────────────────────────────────────────────
MODEL_ANALYST=openai/gpt-oss-120b:free
MODEL_COMPOSER=moonshotai/kimi-k2.6:free
MODEL_ADVISOR=nvidia/nemotron-3-ultra-550b-a55b:free
MODEL_FALLBACK=openai/gpt-oss-20b:free

# ── Local MCP stub ────────────────────────────────────────────────────────────
LOCAL_MCP_URL=http://localhost:8889/mcp

# ── Slack ─────────────────────────────────────────────────────────────────────
SLACK_BOT_TOKEN=xoxb-...                 # Required for alerts
SLACK_CHANNEL=your-alerts-channel

# ── Cisco Meraki MT10 ─────────────────────────────────────────────────────────
MERAKI_API_KEY=                          # Leave blank to use simulator
MERAKI_MT10_MAC=xx:xx:xx:xx:xx:xx        # MAC address of your MT10 sensor
MERAKI_BASE_URL=https://api.meraki.com/api/v1

# ── Alert thresholds ──────────────────────────────────────────────────────────
TEMP_HIGH_C=30.0
TEMP_LOW_C=15.0
HUMIDITY_HIGH_PCT=70.0
HUMIDITY_LOW_PCT=30.0
CO2_HIGH_PPM=1200.0

# ── App settings ──────────────────────────────────────────────────────────────
TRIAGE_INTERVAL_S=300                    # Polling interval in seconds
CHAOS_FLAGS=                             # Pre-active flags at startup (comma-separated)
```

### Chaos Flags

| Flag | Effect |
|------|--------|
| `analyst_timeout` | Simulates analyst LLM failure → triggers fallback |
| `composer_error` | Simulates composer LLM failure → triggers fallback |
| `advisor_error` | Simulates advisor LLM failure → triggers fallback |
| `mcp_error` | Simulates MCP stub unreachable → alert queued locally |

Each flag fires once (the next call recovers) to demonstrate auto-recovery. Flags can also be set at startup via `CHAOS_FLAGS=analyst_timeout,mcp_error`.

---

## 6. Local Development

### Prerequisites

- Python 3.11+
- An OpenRouter account with API key — sign up at https://openrouter.ai
- A Slack bot token with `chat:write` scope, invited to your alerts channel

### Setup

```bash
git clone https://github.com/InnovationCentralPerth/meraki-agentic-iot-triage-openrouter.git
cd meraki-agentic-iot-triage-openrouter

python3 -m venv .venv
.venv/bin/pip install -e .

cp .env.example .env
# Edit .env — fill in OPENROUTER_API_KEY, SLACK_BOT_TOKEN, SLACK_CHANNEL
```

> **No Meraki sensor?** Leave `MERAKI_API_KEY` blank. The simulator runs a 100-second deterministic cycle: normal → ramp → critical → recovery. All pipeline features work without real hardware.

### Run

Two terminals are required:

```bash
# Terminal 1 — MCP stub (Slack bridge)
.venv/bin/uvicorn mcp_stub:app --port 8889

# Terminal 2 — Main server
.venv/bin/uvicorn server:app --port 8090
```

Open `http://localhost:8090`.

### Test Slack connectivity (no LLM requests consumed)

```bash
curl -s -X POST http://localhost:8889/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {"name": "send_alert",
               "arguments": {"sensor_id": "TEST", "severity": "info",
                              "message": "connectivity check"}}}'
```

---

## 7. Production Deployment

### Infrastructure

| Component | Value |
|-----------|-------|
| Recommended OS | Ubuntu 22.04+ |
| Dashboard port | 8090 |
| MCP stub port | 8889 (localhost only) |
| Process manager | systemd |
| App directory | `/opt/meraki-iot-triage` (or any path you choose) |
| Environment file | `<APP_DIR>/.env` (chmod 600) |

> **Network access**: The dashboard binds to `0.0.0.0:8090`. Restrict access to trusted networks using a firewall rule, reverse proxy (nginx/Caddy), or a VPN (e.g. Tailscale) — whichever suits your infrastructure.

### Adapting the Deploy Scripts

Before running, set the two variables at the top of each script:

```bash
# deploy/02_update.sh
REMOTE="<user>@<your-server>"
APP_DIR="/opt/meraki-iot-triage"

# deploy/01_bootstrap.sh
APP_DIR="/opt/meraki-iot-triage"
APP_USER="<user>"
```

The systemd service files under `deploy/systemd/` reference the same `APP_DIR` and `APP_USER` — update them to match before running the bootstrap script.

### First Deploy

```bash
# 1. Copy environment config to server
scp .env <user>@<your-server>:/tmp/meraki.env
ssh <user>@<your-server> \
  "mkdir -p /opt/meraki-iot-triage && \
   mv /tmp/meraki.env /opt/meraki-iot-triage/.env && \
   chmod 600 /opt/meraki-iot-triage/.env"

# 2. Sync code
bash deploy/02_update.sh

# 3. Bootstrap — installs Python venv, deps, and systemd services (once only)
ssh <user>@<your-server> \
  "sudo bash /opt/meraki-iot-triage/deploy/01_bootstrap.sh"
```

### Subsequent Updates

```bash
bash deploy/02_update.sh
```

This rsyncs changed files (excluding `.venv`, `.env`, `__pycache__`) and restarts both services.

### Systemd Services

| Service | Description |
|---------|-------------|
| `server` | FastAPI dashboard — depends on `mcp_stub` |
| `mcp_stub` | Local MCP + Slack bridge — binds to 127.0.0.1 only |

Both services are set to `Restart=always` with a 5 second back-off.

---

## 8. Operations

### Service Management

```bash
# Status
sudo systemctl status server mcp_stub

# Logs (live)
sudo journalctl -u server -f
sudo journalctl -u mcp_stub -f

# Restart
sudo systemctl restart server mcp_stub

# Stop
sudo systemctl stop server mcp_stub
```

### Update Environment Variables

```bash
# Edit on the server directly
ssh <user>@<your-server> "nano /opt/meraki-iot-triage/.env"

# Then restart to pick up changes
ssh <user>@<your-server> "sudo systemctl restart server mcp_stub"
```

### Swap a Model

Edit `.env` on the server (or locally then re-sync) and set any of:

```bash
MODEL_ANALYST=<openrouter-model-id>
MODEL_COMPOSER=<openrouter-model-id>
MODEL_ADVISOR=<openrouter-model-id>
MODEL_FALLBACK=<openrouter-model-id>
```

No code change required — models are read from environment at startup. Find available free models at https://openrouter.ai/models?q=:free.

### Change Polling Interval

Without restarting — use the dashboard input field or the API:

```bash
curl -X POST http://<your-server>:8090/config/interval \
  -H "Content-Type: application/json" \
  -d '{"minutes": 10}'
```

### Check Port Usage

```bash
ssh <user>@<your-server> "sudo ss -tlnp | grep -E '8090|8889'"
```
