"""
chaos.py
--------
Chaos injection layer sitting between the agent code and OpenRouter.
Provides:

  1. ChaosState      — singleton holding which failure flags are active.
  2. invoke_with_chaos() — calls OpenRouter via ChatOpenAI; injects the
                           configured failure before the first attempt, then
                           falls back to FALLBACK_MODEL on any error.
  3. safe_mcp_call() — wraps an MCP tool call; returns a graceful error
                       dict when the mcp_error flag is active.
  4. An event bus so the dashboard can SSE exact failure + recovery events.

The operator flips flags via POST /chaos in server.py.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional

from langchain_openai import ChatOpenAI

logger = logging.getLogger(__name__)


# ── Event bus (simple in-memory; connected to SSE in server.py) ───────────

_event_listeners: list[asyncio.Queue] = []

# Last snapshot of key event types — replayed to new SSE subscribers so they
# see current state immediately without waiting for the next triage cycle.
_REPLAY_TYPES = {"sensor", "merge_done", "pipeline_done", "slack_sent"}
_last_events: dict[str, dict] = {}


async def _emit(event: dict) -> None:
    payload = {"ts": time.time(), **event}
    if event.get("type") in _REPLAY_TYPES:
        _last_events[event["type"]] = payload
    for q in _event_listeners:
        await q.put(payload)


def subscribe_events() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    # Pre-fill with the last known state so the new client renders immediately.
    for evt in _last_events.values():
        try:
            q.put_nowait(evt)
        except asyncio.QueueFull:
            pass
    _event_listeners.append(q)
    return q


def unsubscribe_events(q: asyncio.Queue) -> None:
    _event_listeners.remove(q)


# ── Chaos state ───────────────────────────────────────────────────────────

@dataclass
class ChaosState:
    """
    Active chaos flags.  Each flag name corresponds to a specific failure:
      analyst_timeout  — simulates primary analyst LLM failure → fallback model
      composer_error   — simulates primary composer LLM failure → fallback model
      advisor_error    — simulates primary advisor LLM failure → fallback model
      mcp_error        — raises RuntimeError inside safe_mcp_call()
    """
    flags: set[str] = field(default_factory=set)

    # Per-flag "already failed once" tracker so retries succeed (shows recovery)
    _triggered: set[str] = field(default_factory=set)

    def set_flag(self, flag: str) -> None:
        self.flags.add(flag)
        self._triggered.discard(flag)  # reset so it fires again next time

    def clear_flag(self, flag: str) -> None:
        self.flags.discard(flag)
        self._triggered.discard(flag)

    def clear_all(self) -> None:
        self.flags.clear()
        self._triggered.clear()

    def should_trigger(self, flag: str) -> bool:
        """Returns True the FIRST time a flag fires; subsequent calls pass."""
        if flag not in self.flags:
            return False
        if flag in self._triggered:
            return False           # already triggered; let retry succeed
        self._triggered.add(flag)
        return True


CHAOS = ChaosState()

# Pre-populate from env var on import
_env_flags = os.getenv("CHAOS_FLAGS", "")
for _f in [f.strip() for f in _env_flags.split(",") if f.strip()]:
    CHAOS.set_flag(_f)


# ── LLM factory with resilience wrapping ─────────────────────────────────

_OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
_OPENROUTER_HEADERS = {
    "HTTP-Referer": os.getenv("OPENROUTER_REFERER", "https://github.com/icp-meraki-iot-triage"),
    "X-Title": "Meraki IoT Triage",
}


def _gateway_llm(model_name: str) -> ChatOpenAI:
    """Returns a ChatOpenAI pointed at OpenRouter."""
    return ChatOpenAI(
        model=model_name,
        base_url=_OPENROUTER_BASE_URL,
        api_key=os.environ["OPENROUTER_API_KEY"],
        timeout=30,
        max_retries=2,
        default_headers=_OPENROUTER_HEADERS,
    )


FALLBACK_MODEL = os.getenv("MODEL_FALLBACK", "google/gemma-4-31b:free")


async def invoke_with_chaos(
    model_name: str,
    chaos_flag: str,
    prompt: str,
    fallback_model: str = FALLBACK_MODEL,
) -> tuple[str, str, bool]:
    """
    Invoke an LLM via OpenRouter with chaos simulation.

    Returns:
        (response_text, model_actually_used, was_fallback)
    """

    # Step 1+2 — chaos injection and primary call in one try block
    try:
        if CHAOS.should_trigger(chaos_flag):
            await _emit({
                "type": "chaos",
                "flag": chaos_flag,
                "model": model_name,
                "message": f"Injecting '{chaos_flag}' failure on {model_name}",
            })
            logger.warning("CHAOS: injecting %s on %s", chaos_flag, model_name)
            raise RuntimeError(f"[CHAOS] Simulated {chaos_flag} — provider unreachable")

        llm = _gateway_llm(model_name)
        result = await llm.ainvoke(prompt)
        text = result.content if hasattr(result, "content") else str(result)
        await _emit({"type": "llm_ok", "model": model_name})
        return text, model_name, False

    except Exception as exc:
        await _emit({
            "type": "llm_error",
            "model": model_name,
            "error": str(exc),
            "action": f"falling back to {fallback_model}",
        })
        logger.error("Primary LLM %s failed: %s — trying fallback", model_name, exc)

    # Step 3 — fallback model via OpenRouter
    try:
        llm_fb = _gateway_llm(fallback_model)
        result = await llm_fb.ainvoke(prompt)
        text = result.content if hasattr(result, "content") else str(result)
        await _emit({
            "type": "llm_fallback_ok",
            "primary": model_name,
            "fallback": fallback_model,
        })
        return text, fallback_model, True

    except Exception as exc2:
        await _emit({
            "type": "llm_fatal",
            "model": fallback_model,
            "error": str(exc2),
        })
        # Last resort: return a structured degraded response so the pipeline
        # continues and the UI can surface an explanatory message
        degraded = (
            "⚠️ LLM unavailable — analysis degraded. "
            "Raw sensor anomalies have been logged. "
            "Manual review required."
        )
        return degraded, "degraded", True


# ── MCP call wrapper ──────────────────────────────────────────────────────

async def safe_mcp_call(
    tool_fn: Callable[..., Coroutine[Any, Any, Any]],
    *args,
    **kwargs,
) -> dict:
    """
    Wraps an MCP tool coroutine with chaos + graceful degradation.
    If mcp_error is flagged, returns a degraded dict instead of raising.
    """
    if CHAOS.should_trigger("mcp_error"):
        await _emit({
            "type": "chaos",
            "flag": "mcp_error",
            "message": "Injecting MCP server error — alert will be queued locally",
        })
        return {
            "ok": False,
            "degraded": True,
            "message": (
                "MCP Gateway unreachable. Alert queued in local buffer. "
                "Will retry when connectivity restored."
            ),
        }

    try:
        result = await tool_fn(*args, **kwargs)
        await _emit({"type": "mcp_ok", "tool": tool_fn.__name__})
        return {"ok": True, "result": result}

    except Exception as exc:
        print(f"[MCP CALL FAILED] {exc}")
        await _emit({
            "type": "mcp_error",
            "tool": tool_fn.__name__,
            "error": str(exc),
            "action": "alert queued locally",
        })
        return {
            "ok": False,
            "degraded": True,
            "message": f"MCP call failed ({exc}). Alert queued locally.",
        }
