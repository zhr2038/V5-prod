#!/usr/bin/env python3
"""
Run smart alerts and forward them to Telegram when configured.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from configs.runtime_config import resolve_runtime_env_path
from src.monitoring.smart_alert import SmartAlertEngine


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_telegram_settings(project_root: Path | None = None) -> tuple[str | None, str | None]:
    root = (project_root or PROJECT_ROOT).resolve()
    env_path = Path(resolve_runtime_env_path(project_root=root))
    load_env_file(env_path)
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    return bot_token, chat_id


def format_alert_message(alert: dict[str, Any]) -> str:
    level_emoji = {
        "critical": "🚨",
        "high": "⚠️",
        "medium": "📳",
        "low": "ℹ️",
    }
    emoji = level_emoji.get(str(alert.get("level") or "").lower(), "🔔")
    return (
        f"{emoji} <b>V5智能告警</b>\n\n"
        f"<b>{alert['title']}</b>\n\n"
        f"{alert['message']}\n\n"
        f"📌 <b>建议:</b> {alert['suggestion']}\n\n"
        f"⏰ {alert.get('time', '')}"
    )


def send_telegram_alert(alert: dict[str, Any], *, project_root: Path | None = None) -> bool:
    try:
        bot_token, chat_id = load_telegram_settings(project_root)
        if not bot_token or not chat_id:
            print("[Alert] Telegram config not found, skipping send")
            return False

        response = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": format_alert_message(alert),
                "parse_mode": "HTML",
            },
            timeout=10,
        )
        if response.status_code == 200:
            print(f"[Alert] Sent: {alert['title']}")
            return True
        print(f"[Alert] Failed to send: {response.text}")
        return False
    except Exception as exc:
        print(f"[Alert] Send error: {exc}")
        return False


def main() -> int:
    print("[SmartAlert] Starting alert check...")

    engine = SmartAlertEngine(workspace=PROJECT_ROOT)
    alerts = engine.run_all_checks()

    if not alerts:
        print("[SmartAlert] No alerts, exiting silently")
        return 0

    print(f"[SmartAlert] Found {len(alerts)} alert(s)")
    sent_count = 0
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for alert in alerts:
        alert["time"] = now_text
        if send_telegram_alert(alert, project_root=PROJECT_ROOT):
            sent_count += 1

    print(f"[SmartAlert] Sent {sent_count}/{len(alerts)} alert(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
