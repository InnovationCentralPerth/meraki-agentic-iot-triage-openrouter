"""
server.py
---------
FastAPI backend serving:
  GET  /           — static dashboard HTML (embedded below)
  GET  /events     — SSE stream of pipeline + chaos events
  GET  /reading    — latest sensor reading (JSON)
  POST /triage     — trigger one triage cycle manually
  POST /chaos      — set / clear chaos flags
  GET  /chaos      — current chaos state
  GET  /health     — liveness probe

The dashboard HTML is inlined so the entire app runs with a single
`uvicorn server:app` command with no static-file setup required.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

# ── Local imports (add src/ to path when running from repo root) ──────────
sys.path.insert(0, str(Path(__file__).parent))
from src.agent import run_triage
from src.chaos import CHAOS, subscribe_events, unsubscribe_events
from src.meraki_client import (get_latest_reading, set_inject_critical,
    get_inject_critical, set_inject_normal, get_inject_normal)


# ── Background triage loop ────────────────────────────────────────────────

TRIAGE_INTERVAL_S = float(os.getenv("TRIAGE_INTERVAL_S", "300"))
_triage_task: asyncio.Task | None = None
_interval_changed: asyncio.Event = asyncio.Event()


async def _triage_loop() -> None:
    # Brief pause so SSE clients can connect before the first cycle fires.
    await asyncio.sleep(3)
    while True:
        try:
            await run_triage()
        except Exception as exc:
            print(f"[triage loop] unhandled error: {exc}")
        _interval_changed.clear()
        try:
            await asyncio.wait_for(_interval_changed.wait(), timeout=TRIAGE_INTERVAL_S)
        except asyncio.TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _triage_task
    _triage_task = asyncio.create_task(_triage_loop())
    yield
    if _triage_task:
        _triage_task.cancel()


app = FastAPI(
    title="Meraki IoT Triage Agent",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── SSE event stream ──────────────────────────────────────────────────────

async def _event_generator(request: Request) -> AsyncGenerator:
    q = subscribe_events()
    try:
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(q.get(), timeout=1.0)
                yield {"data": json.dumps(event)}
            except asyncio.TimeoutError:
                yield {"data": json.dumps({"type": "ping"})}  # keep-alive
    finally:
        unsubscribe_events(q)


@app.get("/events")
async def events(request: Request):
    return EventSourceResponse(_event_generator(request))


# ── REST endpoints ────────────────────────────────────────────────────────

@app.get("/reading")
async def reading():
    r = await get_latest_reading()
    return r.to_dict()


@app.post("/triage")
async def trigger_triage():
    asyncio.create_task(run_triage())
    return {"status": "triage cycle started"}


class ChaosRequest(BaseModel):
    flag: str
    active: bool


@app.post("/chaos")
async def set_chaos(req: ChaosRequest):
    if req.active:
        CHAOS.set_flag(req.flag)
    else:
        CHAOS.clear_flag(req.flag)
    return {"chaos_flags": list(CHAOS.flags)}


@app.get("/chaos")
async def get_chaos():
    return {"chaos_flags": list(CHAOS.flags)}


@app.get("/config")
async def get_config():
    return {"triage_interval_s": TRIAGE_INTERVAL_S}


class IntervalUpdate(BaseModel):
    minutes: float


@app.post("/config/interval")
async def set_interval(body: IntervalUpdate):
    global TRIAGE_INTERVAL_S
    TRIAGE_INTERVAL_S = max(0.25, body.minutes) * 60
    _interval_changed.set()
    return {"triage_interval_s": TRIAGE_INTERVAL_S}


@app.post("/inject_critical")
async def inject_critical():
    set_inject_critical(True)
    asyncio.create_task(run_triage())
    return {"status": "critical conditions injected — triage triggered"}


@app.get("/inject_critical")
async def get_inject_critical_status():
    return {"pending": get_inject_critical()}


@app.post("/inject_normal")
async def inject_normal():
    set_inject_normal(True)
    asyncio.create_task(run_triage())
    return {"status": "normal conditions injected — triage triggered"}


@app.get("/health")
async def health():
    return {"ok": True}


# ── Embedded dashboard ────────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Meraki IoT Triage — OpenRouter Multi-Model Pipeline</title>
<style>
  :root{--bg:#0f1117;--surface:#1a1d27;--border:#2a2d3a;--text:#e0e0e8;--muted:#7a7d8a;
        --green:#2ecc71;--amber:#f39c12;--red:#e74c3c;--blue:#3498db;--purple:#9b59b6}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Inter','Segoe UI',sans-serif;font-size:14px;padding:16px}
  h1{font-size:20px;font-weight:600;margin-bottom:4px}
  .subtitle{color:var(--muted);font-size:12px;margin-bottom:16px}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px}
  .card h2{font-size:12px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px}
  .metric{display:flex;align-items:baseline;gap:6px;margin-bottom:6px}
  .metric .val{font-size:28px;font-weight:700}
  .metric .unit{font-size:13px;color:var(--muted)}
  .badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600}
  .ok{background:#1a3a25;color:var(--green)}.warn{background:#3a2a10;color:var(--amber)}.err{background:#3a1a1a;color:var(--red)}
  .chaos-panel{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:12px}
  .chaos-panel h2{font-size:12px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px}
  .chaos-row{display:flex;flex-wrap:wrap;gap:8px}
  .chaos-btn{padding:6px 14px;border:1px solid var(--border);border-radius:6px;background:transparent;color:var(--text);
             font-size:12px;cursor:pointer;transition:.15s}
  .chaos-btn:hover{border-color:var(--red);color:var(--red)}
  .chaos-btn.active{background:#3a1a1a;border-color:var(--red);color:var(--red)}
  .events-panel{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px}
  .events-panel h2{font-size:12px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px}
  .event-log{height:280px;overflow-y:auto;font-size:12px;font-family:'JetBrains Mono','Fira Code',monospace}
  .event-row{padding:3px 0;border-bottom:1px solid var(--border);display:flex;gap:8px}
  .event-ts{color:var(--muted);min-width:80px}
  .event-type-sensor{color:var(--blue)}.event-type-chaos{color:var(--red)}.event-type-llm_error{color:var(--red)}
  .event-type-llm_fallback_ok{color:var(--amber)}.event-type-llm_ok{color:var(--green)}.event-type-mcp_ok{color:var(--green)}
  .event-type-mcp_error{color:var(--red)}.event-type-pipeline_done{color:var(--purple)}.event-type-ping{color:var(--border)}
  .summary-panel{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:12px}
  .summary-panel h2{font-size:12px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px}
  .summary-text{font-size:12px;line-height:1.7;white-space:pre-wrap;color:var(--text)}
  .models-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
  .model-chip{background:#1a1a2a;border:1px solid var(--border);border-radius:6px;padding:3px 10px;font-size:11px;color:var(--muted)}
  .model-chip.fallback{border-color:var(--amber);color:var(--amber)}
  #status-dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;margin-right:4px}
  #status-dot.err{background:var(--red)}
  .full-width{grid-column:1/-1}
  @media(max-width:600px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<h1>⚡ Meraki IoT Triage Agent</h1>
<p class="subtitle">OpenRouter · Multi-Model Pipeline &nbsp;|&nbsp;
  <span id="status-dot"></span><span id="conn-label">connecting…</span></p>

<div class="grid">
  <!-- Sensor readings -->
  <div class="card">
    <h2>MT10 Sensor readings</h2>
    <div class="metric"><span class="val" id="temp">--</span><span class="unit">°C temp</span></div>
    <div class="metric"><span class="val" id="hum">--</span><span class="unit">% humidity</span></div>
    <div class="metric"><span class="val" id="co2">--</span><span class="unit">ppm CO₂</span></div>
    <div id="anomaly-badges" style="margin-top:8px;display:flex;flex-wrap:wrap;gap:6px"></div>
    <p style="margin-top:6px;font-size:11px;color:var(--muted)" id="sensor-source">source: —</p>
    <div style="display:flex;gap:8px;margin-top:12px">
      <button id="btn-normal" onclick="injectNormal()" style="flex:1;padding:7px 0;
        border:1px solid var(--amber);border-radius:6px;background:transparent;
        color:var(--amber);font-size:11px;font-weight:600;cursor:pointer;transition:.15s"
        onmouseover="this.style.background='#3a2a10'" onmouseout="this.style.background='transparent'">
        🌡 Inject Non-Critical<br><span style="font-weight:400;font-size:10px">35°C · 60% · 900ppm</span>
      </button>
      <button id="btn-critical" onclick="injectCritical()" style="flex:1;padding:7px 0;
        border:1px solid var(--red);border-radius:6px;background:transparent;
        color:var(--red);font-size:11px;font-weight:600;cursor:pointer;transition:.15s"
        onmouseover="this.style.background='#3a1a1a'" onmouseout="this.style.background='transparent'">
        🚨 Inject Critical<br><span style="font-weight:400;font-size:10px">35°C · 78% · 1450ppm</span>
      </button>
    </div>
  </div>

  <!-- Pipeline status -->
  <div class="card">
    <h2>Pipeline status</h2>
    <div class="metric"><span class="val" id="pipeline-ms">--</span><span class="unit">ms last run</span></div>
    <div style="margin-top:4px;font-size:11px;color:var(--muted)" id="pipeline-ts">--</div>
    <div class="models-row" id="models-row"></div>
    <div style="margin-top:10px">
      <span id="alert-badge" class="badge ok">no alerts</span>
    </div>
    <div style="margin-top:12px;padding-top:10px;border-top:1px solid var(--border);
                display:flex;align-items:center;gap:6px;font-size:11px;color:var(--muted)">
      <span>Polling cycle</span>
      <input type="number" id="interval-mins" value="5" min="0.25" max="60" step="0.25"
        style="width:52px;background:var(--bg);border:1px solid var(--border);
               color:var(--text);border-radius:4px;padding:3px 6px;
               font-size:11px;text-align:center"
        title="Minutes between triage cycles">
      <span>min</span>
    </div>
  </div>
</div>

<!-- Chaos control panel -->
<div class="chaos-panel">
  <h2>🔥 Chaos injection — click to toggle</h2>
  <div class="chaos-row">
    <button class="chaos-btn" data-flag="analyst_timeout">Analyst timeout</button>
    <button class="chaos-btn" data-flag="composer_error">Composer error</button>
    <button class="chaos-btn" data-flag="advisor_error">Advisor error</button>
    <button class="chaos-btn" data-flag="mcp_error">MCP error</button>
    <button class="chaos-btn" style="margin-left:auto;border-color:var(--muted)"
      onclick="clearAll()">Clear all</button>
  </div>
  <p style="margin-top:8px;font-size:11px;color:var(--muted)">
    Each flag fires once then auto-recovers — shows OpenRouter fallback in action.
  </p>
</div>

<!-- Last triage summary -->
<div class="summary-panel">
  <h2>Last triage summary</h2>
  <div class="summary-text" id="summary">Waiting for first triage cycle…</div>
</div>

<!-- Event log -->
<div class="events-panel">
  <h2>Live event stream</h2>
  <div class="event-log" id="event-log"></div>
</div>

<script>
const es = new EventSource('/events');
const log = document.getElementById('event-log');
const dot = document.getElementById('status-dot');
const connLabel = document.getElementById('conn-label');

es.onopen = () => { dot.className=''; connLabel.textContent='connected'; };
es.onerror= () => { dot.className='err'; connLabel.textContent='disconnected'; };

es.onmessage = (e) => {
  const ev = JSON.parse(e.data);
  if (ev.type === 'ping') return;
  appendEvent(ev);

  if (ev.type === 'sensor') updateSensor(ev);
  if (ev.type === 'pipeline_done') updatePipeline(ev);
  if (ev.type === 'merge_done') updateSummary(ev);
  if (ev.type === 'slack_sent') {
    const badge = document.getElementById('alert-badge');
    badge.className = 'badge ok';
    badge.textContent = '✓ Slack alert sent';
    setTimeout(() => { badge.className='badge ok'; badge.textContent='no alerts'; }, 8000);
  }
};

function appendEvent(ev) {
  const ts = new Date(ev.ts * 1000).toLocaleTimeString();
  const row = document.createElement('div');
  row.className = 'event-row';
  let msg;
  if (ev.type === 'llm_error') {
    const err = (ev.error || '').split('\\n')[0].slice(0, 80);
    msg = `${ev.model} FAILED — ${err} → trying fallback`;
  } else if (ev.type === 'llm_fallback_ok') {
    msg = `${ev.primary} ✗ primary → ✓ fallback: ${ev.fallback}`;
  } else if (ev.type === 'llm_fatal') {
    msg = `${ev.model} FATAL — ${(ev.error || '').slice(0, 80)}`;
  } else {
    msg = ev.message || ev.action || ev.error || ev.model || ev.flag || '';
  }
  row.innerHTML = `<span class="event-ts">${ts}</span>`
    + `<span class="event-type-${ev.type}">[${ev.type}]</span> ${msg}`;
  log.prepend(row);
  if (log.children.length > 200) log.removeChild(log.lastChild);
}

function updateSensor(ev) {
  const d = ev.data;
  document.getElementById('temp').textContent = d.temperature_c.toFixed(1);
  document.getElementById('hum').textContent  = d.humidity_pct.toFixed(1);
  document.getElementById('co2').textContent  = d.co2_ppm.toFixed(0);
  document.getElementById('sensor-source').textContent = 'source: ' + d.source;
  const badges = document.getElementById('anomaly-badges');
  badges.innerHTML = '';
  (d.anomalies || []).forEach(a => {
    const b = document.createElement('span');
    b.className = 'badge err'; b.textContent = a;
    badges.appendChild(b);
  });
  if (!d.anomalies?.length) {
    const b = document.createElement('span');
    b.className = 'badge ok'; b.textContent = 'Normal';
    badges.appendChild(b);
  }
}

function updatePipeline(ev) {
  document.getElementById('pipeline-ms').textContent = Math.round(ev.pipeline_ms);
  const t = new Date(ev.ts * 1000);
  document.getElementById('pipeline-ts').textContent =
    'last run ' + t.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit', second:'2-digit'});
}

function updateSummary(ev) {
  if (!ev.summary) return;
  // Escape HTML then render **bold** markers
  const safe = ev.summary
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const html = safe.replace(/\\*\\*(.+?)\\*\\*/g,
    '<strong style="color:var(--text);font-weight:600">$1</strong>');
  document.getElementById('summary').innerHTML = html;
}

// Chaos control
const btns = document.querySelectorAll('.chaos-btn[data-flag]');
btns.forEach(btn => {
  btn.addEventListener('click', async () => {
    const flag = btn.dataset.flag;
    const active = !btn.classList.contains('active');
    await fetch('/chaos', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({flag, active})
    });
    btn.classList.toggle('active', active);
    if (active) {
      // Trigger a triage cycle immediately so the audience sees the effect
      fetch('/triage', {method:'POST'});
    }
  });
});

async function clearAll() {
  const flags = ['analyst_timeout','composer_error','advisor_error','mcp_error'];
  for (const f of flags) {
    await fetch('/chaos', {method:'POST',headers:{'Content-Type':'application/json'},
      body: JSON.stringify({flag:f, active:false})});
  }
  btns.forEach(b => b.classList.remove('active'));
}

// Polling Cycle control
(async () => {
  const input = document.getElementById('interval-mins');
  // Initialise from server's current value
  const cfg = await fetch('/config').then(r => r.json()).catch(() => null);
  if (cfg) input.value = +(cfg.triage_interval_s / 60).toFixed(2);

  input.addEventListener('change', async () => {
    const mins = parseFloat(input.value);
    if (!isFinite(mins) || mins < 0.25) return;
    await fetch('/config/interval', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({minutes: mins})
    });
  });
})();

async function injectNormal() {
  const btn = document.getElementById('btn-normal');
  const orig = btn.innerHTML;
  btn.innerHTML = '⏳ Injecting...';
  btn.disabled = true;
  await fetch('/inject_normal', {method:'POST'});
  setTimeout(() => { btn.innerHTML = orig; btn.disabled = false; }, 3000);
}

async function injectCritical() {
  const btn = document.getElementById('btn-critical');
  const orig = btn.innerHTML;
  btn.innerHTML = '⏳ Injecting...';
  btn.disabled = true;
  await fetch('/inject_critical', {method:'POST'});
  setTimeout(() => { btn.innerHTML = orig; btn.disabled = false; }, 3000);
}
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8090, reload=False)
