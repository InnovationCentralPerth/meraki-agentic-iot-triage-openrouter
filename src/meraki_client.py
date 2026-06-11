"""
meraki_client.py
----------------
Fetches real environmental readings from a Cisco Meraki MT10 sensor.
Uses the proven org→device discovery pattern with MAC address lookup,
then polls /organizations/{org_id}/sensor/readings/latest.

Falls back to simulator on any API failure (sensor offline, network error).
Supports a CRITICAL_INJECT flag for the demo dashboard button.
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from dataclasses import dataclass, field

import httpx

# ── Config ─────────────────────────────────────────────────────────────────
MERAKI_API_KEY  = os.getenv("MERAKI_API_KEY", "")
MERAKI_MT10_MAC = os.getenv("MERAKI_MT10_MAC", "")
MERAKI_BASE_URL = os.getenv("MERAKI_BASE_URL", "https://api.meraki.com/api/v1")
MERAKI_HEADERS  = {
    "X-Cisco-Meraki-API-Key": MERAKI_API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ── Discovered device cache ────────────────────────────────────────────────
_meraki_org_id: str = ""
_meraki_serial: str = ""

# ── Injection flags ───────────────────────────────────────────────────────
_inject_critical: bool = False
_inject_normal:   bool = False

def set_inject_critical(val: bool) -> None:
    global _inject_critical, _inject_normal
    _inject_critical = val

def get_inject_critical() -> bool:
    return _inject_critical

def set_inject_normal(val: bool) -> None:
    global _inject_normal
    _inject_normal = val

def get_inject_normal() -> bool:
    return _inject_normal


# ── SensorReading ──────────────────────────────────────────────────────────

@dataclass
class SensorReading:
    ts: float = field(default_factory=time.time)
    temperature_c: float = 22.0
    humidity_pct: float = 45.0
    co2_ppm: float = 600.0
    sensor_serial: str = "SIMULATED"
    source: str = "simulator"

    def anomalies(self) -> list[str]:
        issues: list[str] = []
        high_t   = float(os.getenv("TEMP_HIGH_C",      "30"))
        low_t    = float(os.getenv("TEMP_LOW_C",        "15"))
        high_h   = float(os.getenv("HUMIDITY_HIGH_PCT", "70"))
        low_h    = float(os.getenv("HUMIDITY_LOW_PCT",  "30"))
        high_co2 = float(os.getenv("CO2_HIGH_PPM",    "1200"))

        if self.temperature_c > high_t:
            issues.append(f"HIGH TEMP {self.temperature_c:.1f}°C > {high_t}°C")
        if self.temperature_c < low_t:
            issues.append(f"LOW TEMP {self.temperature_c:.1f}°C < {low_t}°C")
        if self.humidity_pct > high_h:
            issues.append(f"HIGH HUMIDITY {self.humidity_pct:.1f}% > {high_h}%")
        if self.humidity_pct < low_h:
            issues.append(f"LOW HUMIDITY {self.humidity_pct:.1f}% < {low_h}%")
        if self.co2_ppm > high_co2:
            issues.append(f"HIGH CO₂ {self.co2_ppm:.0f} ppm > {high_co2:.0f} ppm")
        return issues

    def to_dict(self) -> dict:
        return {
            "ts": self.ts,
            "temperature_c": round(self.temperature_c, 2),
            "humidity_pct": round(self.humidity_pct, 2),
            "co2_ppm": round(self.co2_ppm, 1),
            "sensor_serial": self.sensor_serial,
            "source": self.source,
            "anomalies": self.anomalies(),
        }


# ── Critical injection ─────────────────────────────────────────────────────

def _critical_reading() -> SensorReading:
    """Breaches all thresholds — used by the demo Inject button."""
    serial = _meraki_serial or "MT10-INJECTED"
    return SensorReading(
        temperature_c=35.0,
        humidity_pct=78.0,
        co2_ppm=1450.0,
        sensor_serial=serial,
        source="CRITICAL-INJECT",
    )


def _normal_reading() -> SensorReading:
    """Non-critical reading — warm but below all thresholds. Used by demo button."""
    serial = _meraki_serial or "MT10-NORMAL"
    return SensorReading(
        temperature_c=25.0,
        humidity_pct=60.0,
        co2_ppm=900.0,
        sensor_serial=serial,
        source="NORMAL-INJECT",
    )


# ── Simulator ──────────────────────────────────────────────────────────────

_SIM_START = time.time()

def _simulated_reading() -> SensorReading:
    t = (time.time() - _SIM_START) % 100
    if t < 20:
        temp = 22.0 + math.sin(t / 4) * 0.5
        hum  = 45.0 + math.sin(t / 6) * 2
        co2  = 600  + math.sin(t / 5) * 30
    elif t < 40:
        frac = (t - 20) / 20
        temp = 22.0 + frac * 11
        hum  = 48.0
        co2  = 650.0
    elif t < 60:
        frac = (t - 40) / 20
        temp = 28.0
        hum  = 52.0
        co2  = 650 + frac * 800
    elif t < 80:
        temp = 31.0
        hum  = 72.0
        co2  = 1250.0
    else:
        frac = (t - 80) / 20
        temp = 31.0 - frac * 9
        hum  = 72.0 - frac * 27
        co2  = 1250 - frac * 650
    return SensorReading(
        temperature_c=round(temp, 2),
        humidity_pct=round(hum, 2),
        co2_ppm=round(co2, 1),
        sensor_serial="SIMULATED-MT10",
        source="simulator",
    )


# ── Meraki discovery ───────────────────────────────────────────────────────

async def _discover() -> bool:
    """
    Walk orgs → devices to find MT10 serial by MAC.
    Caches org_id and serial for subsequent polls.
    Mirrors the proven pattern from the RPi5 project.
    """
    global _meraki_org_id, _meraki_serial

    if _meraki_serial:
        return True  # already discovered

    target_mac = MERAKI_MT10_MAC.lower().replace("-", ":").replace(".", ":")

    async with httpx.AsyncClient(
        headers=MERAKI_HEADERS, timeout=15, follow_redirects=True
    ) as client:
        try:
            r = await client.get(f"{MERAKI_BASE_URL}/organizations")
            r.raise_for_status()
            orgs = r.json()
        except Exception as e:
            print(f"[MERAKI] Cannot fetch orgs: {e}")
            return False

        for org in orgs:
            try:
                r = await client.get(
                    f"{MERAKI_BASE_URL}/organizations/{org['id']}/devices"
                )
                r.raise_for_status()
                devices = r.json()
            except Exception:
                continue

            for dev in devices:
                mac = dev.get("mac", "").lower().replace("-", ":").replace(".", ":")
                if mac == target_mac:
                    _meraki_org_id = org["id"]
                    _meraki_serial = dev["serial"]
                    print(
                        f"[MERAKI] MT10 discovered: serial={_meraki_serial} "
                        f"org={org['name']}"
                    )
                    return True

    print(f"[MERAKI] MT10 MAC {MERAKI_MT10_MAC} not found in any org")
    return False


# ── Meraki fetch ───────────────────────────────────────────────────────────

async def _fetch_readings() -> SensorReading:
    """
    Fetch latest sensor readings using the proven endpoint:
    GET /organizations/{org_id}/sensor/readings/latest?serials[]={serial}
    """
    async with httpx.AsyncClient(
        headers=MERAKI_HEADERS, timeout=10, follow_redirects=True
    ) as client:
        r = await client.get(
            f"{MERAKI_BASE_URL}/organizations/{_meraki_org_id}/sensor/readings/latest",
            params={"serials[]": _meraki_serial},
        )
        r.raise_for_status()
        data = r.json()

    temp = hum = co2 = None

    for sensor in data:
        if sensor.get("serial") == _meraki_serial:
            for reading in sensor.get("readings", []):
                metric = reading.get("metric")
                if metric == "temperature":
                    temp = reading["temperature"]["celsius"]
                elif metric == "humidity":
                    hum = reading["humidity"]["relativePercentage"]
                elif metric == "co2":
                    co2 = reading["co2"]["concentration"]
            break

    return SensorReading(
        temperature_c=round(float(temp), 2) if temp is not None else 22.0,
        humidity_pct=round(float(hum),   2) if hum  is not None else 45.0,
        co2_ppm=round(float(co2),         1) if co2  is not None else 600.0,
        sensor_serial=_meraki_serial,
        source="meraki-api",
    )


# ── Public interface ───────────────────────────────────────────────────────

async def get_latest_reading() -> SensorReading:
    """
    Priority:
      1. CRITICAL_INJECT override (demo button) — fires once then resets
      2. Real Meraki MT10 via API (if MERAKI_API_KEY set)
      3. Simulator fallback
    """
    global _inject_critical, _inject_normal

    # Priority 1a — normal injection (warm but no alert)
    if _inject_normal:
        _inject_normal = False
        r = _normal_reading()
        print(f"[INJECT] Normal: temp={r.temperature_c} hum={r.humidity_pct} co2={r.co2_ppm}")
        return r

    # Priority 1b — critical injection (breaches all thresholds)
    if _inject_critical:
        _inject_critical = False
        r = _critical_reading()
        print(f"[INJECT] Critical: {r.anomalies()}")
        return r

    # Priority 2 — real Meraki API
    if MERAKI_API_KEY:
        try:
            discovered = await _discover()
            if discovered:
                return await _fetch_readings()
        except Exception as exc:
            print(f"[MERAKI] Poll failed, using simulator: {exc}")

        # Discovery failed or poll failed — fall through to simulator
        r = _simulated_reading()
        r.source = "simulator (meraki fallback)"
        return r

    # Priority 3 — simulator only
    return _simulated_reading()


async def stream_readings(interval_s: float = 5.0):
    while True:
        yield await get_latest_reading()
        await asyncio.sleep(interval_s)
