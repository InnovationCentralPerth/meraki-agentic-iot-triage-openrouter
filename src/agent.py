"""
agent.py
--------
Multi-agent IoT triage pipeline built with LangGraph.

Graph topology:
  load_sensor → [anomaly_analyst ‖ alert_composer ‖ root_cause_advisor]
              → merge_results → send_mcp_alert → done

The three sub-agents run in parallel via LangGraph's Send() primitive.
Each sub-agent calls OpenRouter via chaos.invoke_with_chaos(), which
handles chaos simulation and primary→fallback model switching.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Annotated, Any, TypedDict

import httpx
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from .chaos import CHAOS, invoke_with_chaos, safe_mcp_call, _emit
from .meraki_client import SensorReading, get_latest_reading

# ── Models (OpenRouter model IDs) ────────────────────────────────────────
MODEL_ANALYST  = os.getenv("MODEL_ANALYST",  "openai/gpt-oss-120b:free")
MODEL_COMPOSER = os.getenv("MODEL_COMPOSER", "openai/gpt-oss-20b:free")
MODEL_ADVISOR  = os.getenv("MODEL_ADVISOR",  "qwen/qwen3-next-80b-a3b-instruct:free")

CHAOS_ANALYST  = "analyst_timeout"
CHAOS_COMPOSER = "composer_error"
CHAOS_ADVISOR  = "advisor_error"


# ── State ─────────────────────────────────────────────────────────────────

def _merge_lists(a: list, b: list) -> list:
    return a + b


class TriageState(TypedDict):
    reading: SensorReading
    anomalies: list[str]
    analyst_report: str
    analyst_model: str
    analyst_fallback: bool
    composer_report: str
    composer_model: str
    composer_fallback: bool
    advisor_report: str
    advisor_model: str
    advisor_fallback: bool
    final_summary: str
    alert_result: dict
    pipeline_ms: float
    errors: Annotated[list[str], _merge_lists]


# ── Nodes ─────────────────────────────────────────────────────────────────

async def load_sensor(state: TriageState) -> dict:
    t0 = time.time()
    reading = await get_latest_reading()
    await _emit({
        "type": "sensor",
        "data": reading.to_dict(),
        "anomalies": reading.anomalies(),
    })
    return {
        "reading": reading,
        "anomalies": reading.anomalies(),
        "pipeline_ms": (time.time() - t0) * 1000,
        "errors": [],
    }


async def anomaly_analyst(state: TriageState) -> dict:
    r = state["reading"]
    prompt = (
        f"You are an IoT sensor anomaly analyst. Analyse this MT10 reading and "
        f"give a concise structured report (3-5 bullet points, < 80 words):\n\n"
        f"Temperature: {r.temperature_c}°C\n"
        f"Humidity: {r.humidity_pct}%\n"
        f"CO₂: {r.co2_ppm} ppm\n"
        f"Anomalies flagged: {', '.join(state['anomalies']) or 'none'}\n"
        f"Timestamp: {r.ts}"
    )
    text, model, fallback = await invoke_with_chaos(
        MODEL_ANALYST, CHAOS_ANALYST, prompt
    )
    return {
        "analyst_report": text,
        "analyst_model": model,
        "analyst_fallback": fallback,
    }


async def alert_composer(state: TriageState) -> dict:
    r = state["reading"]
    anomalies_str = ", ".join(state["anomalies"]) if state["anomalies"] else "none detected"
    prompt = (
        f"You are an alert message writer. Draft a crisp Slack alert (< 60 words) "
        f"for an IoT sensor event. Include severity, sensor ID, and key values.\n\n"
        f"Sensor: {r.sensor_serial}\n"
        f"Anomalies: {anomalies_str}\n"
        f"Temp: {r.temperature_c}°C | Humidity: {r.humidity_pct}% | CO₂: {r.co2_ppm} ppm\n"
        f"Use emoji sparingly. Output ONLY the alert text."
    )
    text, model, fallback = await invoke_with_chaos(
        MODEL_COMPOSER, CHAOS_COMPOSER, prompt
    )
    return {
        "composer_report": text,
        "composer_model": model,
        "composer_fallback": fallback,
    }


async def root_cause_advisor(state: TriageState) -> dict:
    r = state["reading"]
    prompt = (
        f"You are an industrial IoT root-cause advisor. "
        f"Given these sensor readings, suggest the most likely physical causes "
        f"and one immediate mitigation step (< 60 words):\n\n"
        f"Temp: {r.temperature_c}°C | Humidity: {r.humidity_pct}% | CO₂: {r.co2_ppm} ppm\n"
        f"Anomalies: {', '.join(state['anomalies']) or 'none'}"
    )
    text, model, fallback = await invoke_with_chaos(
        MODEL_ADVISOR, CHAOS_ADVISOR, prompt
    )
    return {
        "advisor_report": text,
        "advisor_model": model,
        "advisor_fallback": fallback,
    }


async def merge_results(state: TriageState) -> dict:
    summary_parts = []
    if state.get("anomalies"):
        summary_parts.append(f"**Anomalies:** {', '.join(state['anomalies'])}")
    if state.get("analyst_report"):
        tag = " *(fallback)*" if state.get("analyst_fallback") else ""
        summary_parts.append(
            f"**Analyst [{state.get('analyst_model','')}]{tag}:**\n{state['analyst_report']}"
        )
    if state.get("composer_report"):
        tag = " *(fallback)*" if state.get("composer_fallback") else ""
        summary_parts.append(
            f"**Alert draft [{state.get('composer_model','')}]{tag}:**\n{state['composer_report']}"
        )
    if state.get("advisor_report"):
        tag = " *(fallback)*" if state.get("advisor_fallback") else ""
        summary_parts.append(
            f"**Root cause [{state.get('advisor_model','')}]{tag}:**\n{state['advisor_report']}"
        )

    summary_text = "\n\n".join(summary_parts) if summary_parts else "No anomalies — all readings normal."
    await _emit({
        "type": "merge_done",
        "anomaly_count": len(state.get("anomalies", [])),
        "summary": summary_text,
    })
    return {"final_summary": summary_text}


# ── Alert state tracker ──────────────────────────────────────────────────
# Tracks previous alert state to detect transitions:
#   None      → not yet initialised (first reading)
#   True      → last cycle had anomalies (alerted)
#   False     → last cycle was normal
_last_alert_state: bool | None = None


async def send_mcp_alert(state: TriageState) -> dict:
    """
    Send Slack messages only on state transitions:
      None  → *     : first cycle after restart → silence (just initialise state)
      False → True   : anomaly detected          → send alert
      True  → False  : anomalies resolved        → send "all clear"
      False → False  : still normal              → silence
      True  → True   : still alerting            → silence

    Calls local mcp_stub.py → Slack.
    Wrapped in safe_mcp_call() so the mcp_error chaos flag still intercepts it.
    """
    global _last_alert_state

    anomalies = state.get("anomalies", [])
    has_alert = bool(anomalies)
    reading   = state.get("reading")
    sensor_id = getattr(reading, "sensor_serial", "MT10-SIMULATOR")

    # ── Decide whether to send ────────────────────────────────────────────
    prev = _last_alert_state
    _last_alert_state = has_alert

    if prev is None:
        return {"alert_result": {"ok": True, "skipped": True, "reason": "startup initialisation"}}
    if prev is False and not has_alert:
        return {"alert_result": {"ok": True, "skipped": True, "reason": "still normal"}}
    if prev is True and has_alert:
        return {"alert_result": {"ok": True, "skipped": True, "reason": "already alerted"}}

    # ── Build message based on transition ─────────────────────────────────
    if has_alert:
        severity = "critical"
        message  = ", ".join(anomalies)
    else:
        severity = "info"
        message  = "All sensor readings back to normal"

    async def _do_send():
        mcp_url = os.environ.get("LOCAL_MCP_URL", "http://localhost:8889/mcp")
        channel = os.getenv("SLACK_CHANNEL", "alerts")

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "send_alert",
                "arguments": {
                    "sensor_id": sensor_id,
                    "severity": severity,
                    "message": message,
                },
            },
        }

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as http:
            resp = await http.post(
                mcp_url,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                },
                json=payload,
            )
            resp.raise_for_status()

            ct = resp.headers.get("content-type", "")
            if "text/event-stream" in ct:
                tool_result = {}
                for line in resp.text.splitlines():
                    if line.startswith("data: "):
                        try:
                            data = json.loads(line[6:])
                            if "result" in data:
                                tool_result = data["result"]
                                break
                        except json.JSONDecodeError:
                            pass
            else:
                data = resp.json()
                tool_result = data.get("result", {})

        await _emit({
            "type": "slack_sent",
            "channel": channel,
            "severity": severity,
            "via": "local MCP stub",
        })
        return {"status": "sent", "via": "local_mcp", "result": tool_result}

    result = await safe_mcp_call(_do_send)
    return {"alert_result": result}


# ── Router: fan out to all three sub-agents in parallel ───────────────────

def fan_out(state: TriageState) -> list[Send]:
    return [
        Send("anomaly_analyst",  state),
        Send("alert_composer",   state),
        Send("root_cause_advisor", state),
    ]


# ── Build graph ───────────────────────────────────────────────────────────

def build_graph() -> Any:
    g = StateGraph(TriageState)

    g.add_node("load_sensor",       load_sensor)
    g.add_node("anomaly_analyst",   anomaly_analyst)
    g.add_node("alert_composer",    alert_composer)
    g.add_node("root_cause_advisor", root_cause_advisor)
    g.add_node("merge_results",     merge_results)
    g.add_node("send_mcp_alert",    send_mcp_alert)

    g.add_edge(START, "load_sensor")
    g.add_conditional_edges("load_sensor", fan_out, ["anomaly_analyst", "alert_composer", "root_cause_advisor"])
    g.add_edge("anomaly_analyst",    "merge_results")
    g.add_edge("alert_composer",     "merge_results")
    g.add_edge("root_cause_advisor", "merge_results")
    g.add_edge("merge_results",      "send_mcp_alert")
    g.add_edge("send_mcp_alert",     END)

    return g.compile()


graph = build_graph()


# ── Convenience run function ───────────────────────────────────────────────

async def run_triage() -> TriageState:
    """Execute one full triage cycle and return the final state."""
    t0 = time.time()
    initial: TriageState = {
        "reading": None,          # type: ignore[assignment]
        "anomalies": [],
        "analyst_report": "",
        "analyst_model": "",
        "analyst_fallback": False,
        "composer_report": "",
        "composer_model": "",
        "composer_fallback": False,
        "advisor_report": "",
        "advisor_model": "",
        "advisor_fallback": False,
        "final_summary": "",
        "alert_result": {},
        "pipeline_ms": 0.0,
        "errors": [],
    }
    result = await graph.ainvoke(initial)
    result["pipeline_ms"] = (time.time() - t0) * 1000
    await _emit({"type": "pipeline_done", "pipeline_ms": result["pipeline_ms"]})
    return result
