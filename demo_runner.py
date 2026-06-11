"""
demo_runner.py
--------------
Self-contained demo runner for the pitch.
Does NOT require a real TFY_API_KEY — runs in DEMO_MODE with
instant canned responses so you can show the UX flow without live LLMs.

Usage:
    python demo_runner.py

What it does:
  1. Starts the simulator, shows 8 seconds of normal readings
  2. Fast-forwards to the CO₂ anomaly phase
  3. Injects chaos flags in sequence, shows recovery for each
  4. Prints a colour-coded timeline to the terminal

For the actual pitch, run `uvicorn server:app --port 8888` and open
http://localhost:8888 — the browser dashboard is the primary demo UI.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

# Force demo mode
os.environ.setdefault("DEMO_MODE", "1")
os.environ.setdefault("TFY_GATEWAY_URL", "http://localhost:9999/stub")
os.environ.setdefault("TFY_API_KEY", "demo-key")

from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

sys.path.insert(0, str(Path(__file__).parent))
from src.meraki_client import _simulated_reading
from src.chaos import CHAOS

console = Console()

CHAOS_SEQUENCE = [
    ("claude_timeout", "Claude provider timeout",        "Gateway retries → falls back to Bedrock Haiku"),
    ("openai_429",     "OpenAI rate-limit (429)",        "Gateway queues → auto-switches to next provider"),
    ("gemini_503",     "Gemini 503 service unavailable", "Gateway falls back to Bedrock Haiku"),
    ("mcp_error",      "MCP Slack Gateway error",        "Alert queued locally, user notified in UI"),
]

def show_reading(r):
    t = Table(show_header=False, box=None, padding=(0, 1))
    t.add_column("k", style="dim")
    t.add_column("v", style="bold")
    t.add_row("Temperature", f"{r.temperature_c}°C")
    t.add_row("Humidity",    f"{r.humidity_pct}%")
    t.add_row("CO₂",         f"{r.co2_ppm} ppm")
    anomalies = r.anomalies()
    t.add_row("Anomalies",   ", ".join(anomalies) if anomalies else "[green]None[/green]")
    console.print(t)


async def main():
    console.rule("[bold blue]Meraki IoT Triage — TrueFoundry Resilience Demo[/bold blue]")
    console.print("\n[dim]Phase 1: Normal operations[/dim]\n")

    for _ in range(3):
        r = _simulated_reading()
        show_reading(r)
        await asyncio.sleep(2)

    console.print("\n[dim]Phase 2: Anomalies detected[/dim]\n")
    # Fast-forward simulator to CO₂ spike window
    import src.meraki_client as mc
    mc._SIM_START -= 45  # puts us in the CO₂ spike phase

    r = _simulated_reading()
    show_reading(r)
    console.print(f"\n[bold red]⚠  {len(r.anomalies())} anomaly(s) detected — triage agents activated[/bold red]\n")
    await asyncio.sleep(1)

    console.print("[dim]Phase 3: Chaos injection — each failure recovers automatically[/dim]\n")

    for flag, label, expected_recovery in CHAOS_SEQUENCE:
        CHAOS.set_flag(flag)
        console.print(Panel(
            f"[red]INJECTING:[/red] {label}\n"
            f"[dim]Expected recovery:[/dim] {expected_recovery}",
            title=f"⚡ chaos: {flag}",
            border_style="red",
        ))
        await asyncio.sleep(1.5)

        # Simulate the trigger firing
        fired = CHAOS.should_trigger(flag)
        if fired:
            console.print(f"  [red]✗ {label} fired[/red]")
        await asyncio.sleep(0.5)
        console.print(f"  [green]✓ Recovery: {expected_recovery}[/green]\n")
        await asyncio.sleep(1)

    CHAOS.clear_all()
    console.print("[dim]Phase 4: Recovery — all systems nominal[/dim]\n")
    mc._SIM_START = time.time()  # reset simulator to normal phase
    r = _simulated_reading()
    show_reading(r)

    console.rule("[bold green]Demo complete — 3 LLMs, 1 Gateway, zero dropped alerts[/bold green]")


if __name__ == "__main__":
    asyncio.run(main())
