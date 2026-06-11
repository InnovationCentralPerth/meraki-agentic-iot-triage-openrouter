"""
mcp_stub.py
-----------
Local MCP server (streamable HTTP) — sends real Slack alerts via slack-sdk.

Exposes two tools:
  send_alert  — posts a Slack message to the configured channel
  get_status  — returns the last alert sent

Run:
    uvicorn mcp_stub:app --port 8889

Requires in .env:
    SLACK_BOT_TOKEN=xoxb-...
    SLACK_CHANNEL=your-alerts-channel
"""

import os
import time

from dotenv import load_dotenv
from fastmcp import FastMCP
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

load_dotenv()

mcp = FastMCP("iot-alert-slack")


_slack   = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
_channel = os.getenv("SLACK_CHANNEL", "alerts")
_last: dict = {}


@mcp.tool()
def send_alert(sensor_id: str, severity: str, message: str) -> dict:
    """Send an IoT sensor alert to Slack."""
    global _last

    emoji = {
        "critical": ":red_circle:",
        "warning":  ":large_yellow_circle:",
        "info":     ":large_green_circle:",
    }.get(severity, ":white_circle:")

    try:
        resp = _slack.chat_postMessage(
            channel=_channel,
            text=f"{emoji} *[{severity.upper()}]* `{sensor_id}` — {message}",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"{emoji} *IoT Alert — {severity.upper()}*\n"
                            f"*Sensor:* `{sensor_id}`\n"
                            f"*Message:* {message}"
                        ),
                    },
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Meraki IoT Triage · {time.strftime('%Y-%m-%d %H:%M:%S')}",
                        }
                    ],
                },
            ],
        )
        _last = {
            "sensor_id": sensor_id,
            "severity": severity,
            "message": message,
            "ts": time.time(),
            "slack_ts": resp["ts"],
            "channel": _channel,
            "status": "sent",
        }
        print(f"[SLACK SENT] {severity.upper()} | {sensor_id} | {message}")

    except SlackApiError as e:
        error = e.response["error"]
        print(f"[SLACK ERROR] {error}")
        _last = {
            "sensor_id": sensor_id,
            "severity": severity,
            "message": message,
            "ts": time.time(),
            "status": f"failed: {error}",
        }

    return _last


@mcp.tool()
def get_status() -> dict:
    """Return the last alert sent to Slack."""
    return _last or {"status": "no alerts yet"}


app = mcp.http_app(path="/mcp", stateless_http=True)
