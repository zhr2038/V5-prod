#!/usr/bin/env python3
"""
Trade monitor for the V5 production workspace.
"""

from __future__ import annotations

import os
import re
import shutil
import sqlite3
import subprocess
import sys
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence
from urllib import parse, request


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from configs.runtime_config import resolve_runtime_env_path


TELEGRAM_CHAT_ID = "5065024131"
LIVE_SERVICE_UNITS = ("v5-prod.user.service", "v5-live-20u.user.service")
ALERT_THRESHOLDS = {
    "no_trade_hours": 6,
    "no_trade_critical": 12,
}
FILL_SYNC_RE = re.compile(r"new_fills=(\d+)")
JOURNAL_TS_RE = re.compile(r"^(?P<month>[A-Z][a-z]{2})\s+(?P<day>\d{1,2})\s+(?P<clock>\d{2}:\d{2}:\d{2})")


@dataclass(frozen=True)
class MonitorPaths:
    project_root: Path
    reports_dir: Path
    logs_dir: Path
    fills_db_path: Path
    orders_db_path: Path
    env_path: Path
    alert_file: Path


def build_paths(project_root: Path | None = None) -> MonitorPaths:
    root = (project_root or PROJECT_ROOT).resolve()
    reports_dir = root / "reports"
    logs_dir = root / "logs"
    env_path = Path(resolve_runtime_env_path(project_root=root))
    return MonitorPaths(
        project_root=root,
        reports_dir=reports_dir,
        logs_dir=logs_dir,
        fills_db_path=reports_dir / "fills.sqlite",
        orders_db_path=reports_dir / "orders.sqlite",
        env_path=env_path,
        alert_file=reports_dir / "monitor_alert.txt",
    )


DEFAULT_PATHS = build_paths()


def run_command(cmd: Sequence[str], timeout: int = 30) -> str:
    try:
        result = subprocess.run(
            list(cmd),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return result.stdout
    except Exception as exc:
        print(f"[ERROR] command failed: {exc}")
        return ""


def _get_unit_load_state(unit: str) -> str:
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", unit, "--property=LoadState"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return ""

    for line in result.stdout.splitlines():
        if line.startswith("LoadState="):
            return line.split("=", 1)[1].strip()
    return ""


def resolve_live_service_unit_name() -> str:
    if shutil.which("systemctl") is None:
        return LIVE_SERVICE_UNITS[0]

    current_unit = LIVE_SERVICE_UNITS[0]
    if _get_unit_load_state(current_unit) not in {"", "not-found"}:
        return current_unit

    legacy_unit = LIVE_SERVICE_UNITS[1]
    if _get_unit_load_state(legacy_unit) not in {"", "not-found"}:
        return legacy_unit

    return LIVE_SERVICE_UNITS[0]


def _parse_journal_timestamp(line: str) -> datetime | None:
    match = JOURNAL_TS_RE.match(line)
    if not match:
        return None
    try:
        current_year = datetime.now().year
        date_text = "{month} {day} {clock} {year}".format(
            month=match.group("month"),
            day=match.group("day"),
            clock=match.group("clock"),
            year=current_year,
        )
        return datetime.strptime(date_text, "%b %d %H:%M:%S %Y")
    except Exception:
        return None


def _get_latest_fill_ts_ms(paths: MonitorPaths) -> int | None:
    try:
        if paths.fills_db_path.exists():
            conn = sqlite3.connect(str(paths.fills_db_path))
            try:
                row = conn.execute("SELECT MAX(ts_ms) FROM fills").fetchone()
            finally:
                conn.close()
            if row and row[0]:
                return int(row[0])
    except Exception as exc:
        print(f"[ERROR] failed to query fill store: {exc}")
    return None


def _get_latest_filled_order_ts_ms(paths: MonitorPaths) -> int | None:
    if not paths.orders_db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(paths.orders_db_path))
        try:
            try:
                row = conn.execute(
                    """
                    SELECT MAX(
                        CASE
                            WHEN COALESCE(updated_ts, 0) > 0 THEN updated_ts
                            ELSE created_ts
                        END
                    )
                    FROM orders
                    WHERE state = 'FILLED'
                    """
                ).fetchone()
            except sqlite3.OperationalError:
                row = conn.execute(
                    "SELECT MAX(created_ts) FROM orders WHERE state = 'FILLED'"
                ).fetchone()
        finally:
            conn.close()
        if row and row[0]:
            return int(row[0])
    except Exception as exc:
        print(f"[ERROR] failed to query order store: {exc}")
    return None


def get_last_trade_time(paths: MonitorPaths = DEFAULT_PATHS, *, service_unit: str | None = None) -> datetime | None:
    latest_ts_ms = max(
        (
            ts_ms
            for ts_ms in (
                _get_latest_fill_ts_ms(paths),
                _get_latest_filled_order_ts_ms(paths),
            )
            if ts_ms
        ),
        default=None,
    )
    if latest_ts_ms is not None:
        return datetime.fromtimestamp(latest_ts_ms / 1000)

    unit = service_unit or resolve_live_service_unit_name()
    try:
        output = run_command(
            [
                "journalctl",
                "--user",
                "-u",
                unit,
                "--since",
                "12 hours ago",
                "--no-pager",
                "-n",
                "1000",
            ]
        )
        for line in reversed(output.splitlines()):
            if "FILLS_SYNC new_fills=" in line and "new_fills=0" not in line:
                ts = _parse_journal_timestamp(line)
                if ts is not None:
                    return ts
    except Exception as exc:
        print(f"[ERROR] failed to query journal: {exc}")
    return None


def get_recent_errors(*, service_unit: str | None = None) -> list[str]:
    unit = service_unit or resolve_live_service_unit_name()
    errors: list[str] = []
    try:
        output = run_command(
            [
                "journalctl",
                "--user",
                "-u",
                unit,
                "--since",
                "2 hours ago",
                "--no-pager",
                "-n",
                "500",
            ]
        )
        if "borrow_detected" in output:
            count = output.count("borrow_detected")
            errors.append(f"borrow guard triggered {count} times in the last 2 hours")
        if "ABORT" in output:
            count = output.count("ABORT")
            errors.append(f"trade aborted {count} times in the last 2 hours")
        if "RuntimeError" in output:
            count = output.count("RuntimeError")
            errors.append(f"runtime error seen {count} times in the last 2 hours")
    except Exception as exc:
        print(f"[ERROR] failed to inspect recent errors: {exc}")
    return errors


def get_recent_trades_count(*, service_unit: str | None = None) -> tuple[int, int]:
    unit = service_unit or resolve_live_service_unit_name()
    try:
        output = run_command(
            [
                "journalctl",
                "--user",
                "-u",
                unit,
                "--since",
                "6 hours ago",
                "--no-pager",
            ]
        )
    except Exception as exc:
        print(f"[ERROR] failed to inspect trade journal: {exc}")
        return 0, 0

    trade_runs = 0
    total_fills = 0
    for line in output.splitlines():
        if "FILLS_SYNC new_fills=" not in line:
            continue
        trade_runs += 1
        match = FILL_SYNC_RE.search(line)
        if match:
            total_fills += int(match.group(1))
    return trade_runs, total_fills


def _load_telegram_settings(paths: MonitorPaths) -> tuple[str | None, str]:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or TELEGRAM_CHAT_ID

    if not paths.env_path.exists():
        return bot_token, chat_id

    for line in paths.env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key == "TELEGRAM_BOT_TOKEN" and not bot_token:
            bot_token = value
        elif key == "TELEGRAM_CHAT_ID" and not os.getenv("TELEGRAM_CHAT_ID"):
            chat_id = value

    return bot_token, chat_id


def send_telegram_alert(message: str, priority: str = "normal", paths: MonitorPaths = DEFAULT_PATHS) -> bool:
    icon = "[CRITICAL]" if priority == "critical" else "[WARN]" if priority == "warning" else "[INFO]"
    full_message = (
        f"{icon} V5 trade monitor\n\n"
        f"{message}\n\n"
        f"time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )

    bot_token, chat_id = _load_telegram_settings(paths)
    if bot_token:
        try:
            payload = parse.urlencode({"chat_id": chat_id, "text": full_message}).encode("utf-8")
            req = request.Request(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                data=payload,
                method="POST",
            )
            with request.urlopen(req, timeout=10):
                pass
            print("[INFO] alert sent to Telegram")
            return True
        except Exception as exc:
            print(f"[ERROR] failed to send Telegram alert: {exc}")

    try:
        paths.alert_file.parent.mkdir(parents=True, exist_ok=True)
        paths.alert_file.write_text(full_message, encoding="utf-8")
        print("[INFO] alert written to monitor_alert.txt")
        return True
    except Exception as exc:
        print(f"[ERROR] failed to persist alert: {exc}")
        return False


def check_and_alert(paths: MonitorPaths = DEFAULT_PATHS) -> bool:
    alerts: list[str] = []
    priority = "normal"
    service_unit = resolve_live_service_unit_name()

    last_trade = get_last_trade_time(paths, service_unit=service_unit)
    trade_runs, total_fills = get_recent_trades_count(service_unit=service_unit)

    if last_trade is not None:
        hours_since_trade = (datetime.now() - last_trade).total_seconds() / 3600
        if hours_since_trade >= ALERT_THRESHOLDS["no_trade_critical"]:
            alerts.append(f"critical: no trade for {hours_since_trade:.1f} hours")
            priority = "critical"
        elif hours_since_trade >= ALERT_THRESHOLDS["no_trade_hours"]:
            alerts.append(f"warning: no trade for {hours_since_trade:.1f} hours")
            priority = "warning"
    else:
        alerts.append("warning: unable to determine last trade time")

    if trade_runs == 0:
        alerts.append("warning: no trade sync runs in the last 6 hours")
    elif total_fills == 0:
        alerts.append(f"info: {trade_runs} sync runs in the last 6 hours but zero fills")

    errors = get_recent_errors(service_unit=service_unit)
    if errors:
        alerts.extend(errors)
        if any("borrow" in item or "aborted" in item for item in errors):
            priority = "critical"

    regime_file = paths.reports_dir / "regime.json"
    if regime_file.exists():
        try:
            regime = json.loads(regime_file.read_text(encoding="utf-8"))
            state = str(regime.get("state") or "unknown")
            if state == "Risk-Off":
                alerts.append("info: current market regime is Risk-Off")
        except Exception:
            pass

    if alerts:
        summary = "\n".join(alerts)
        summary += f"\n\nstats: last 6 hours trade_runs={trade_runs}, fills={total_fills}"
        send_telegram_alert(summary, priority=priority, paths=paths)
        return True

    print(f"[OK] {datetime.now().strftime('%H:%M')} monitor check passed")
    return False


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    has_alert = check_and_alert()
    if "--silent" in args:
        return 1 if has_alert else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
