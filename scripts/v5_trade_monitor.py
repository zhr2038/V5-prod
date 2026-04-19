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

from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path
from src.execution.fill_store import (
    derive_fill_store_path,
    derive_runtime_auto_risk_eval_path,
    derive_runtime_auto_risk_guard_path,
    derive_runtime_named_artifact_path,
)


TELEGRAM_CHAT_ID = "5065024131"
LIVE_SERVICE_UNITS = ("v5-prod.user.service",)
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


def _load_active_config(*, project_root: Path) -> dict:
    config_path = Path(resolve_runtime_config_path(project_root=project_root))
    try:
        import yaml

        if config_path.exists():
            return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        pass
    return {}


def _resolve_runtime_path(raw_path: object, *, default: str, project_root: Path) -> Path:
    value = str(raw_path or default).strip()
    path = Path(value)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def build_paths(project_root: Path | None = None) -> MonitorPaths:
    root = (project_root or PROJECT_ROOT).resolve()
    cfg = _load_active_config(project_root=root)
    execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
    orders_db_path = _resolve_runtime_path(
        execution_cfg.get("order_store_path"),
        default="reports/orders.sqlite",
        project_root=root,
    )
    fills_db_path = derive_fill_store_path(orders_db_path)
    reports_dir = orders_db_path.parent.resolve()
    logs_dir = root / "logs"
    env_path = Path(resolve_runtime_env_path(project_root=root))
    return MonitorPaths(
        project_root=root,
        reports_dir=reports_dir,
        logs_dir=logs_dir,
        fills_db_path=fills_db_path,
        orders_db_path=orders_db_path,
        env_path=env_path,
        alert_file=derive_runtime_named_artifact_path(orders_db_path, "monitor_alert", ".txt").resolve(),
    )


DEFAULT_PATHS = build_paths()


def _resolve_risk_state_paths(paths: MonitorPaths) -> tuple[Path, Path]:
    return (
        derive_runtime_auto_risk_eval_path(paths.orders_db_path).resolve(),
        derive_runtime_auto_risk_guard_path(paths.orders_db_path).resolve(),
    )


def _load_json_safe(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _coerce_timestamp_epoch(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _risk_state_epoch(payload: object, *, primary_keys: tuple[str, ...]) -> float | None:
    if not isinstance(payload, dict):
        return None
    for key in primary_keys:
        epoch = _coerce_timestamp_epoch(payload.get(key))
        if epoch is not None:
            return epoch
    history = payload.get("history")
    if isinstance(history, list):
        for item in reversed(history):
            if not isinstance(item, dict):
                continue
            epoch = _coerce_timestamp_epoch(item.get("ts"))
            if epoch is not None:
                return epoch
    return None


def get_current_risk_level(paths: MonitorPaths = DEFAULT_PATHS) -> str:
    eval_path, guard_path = _resolve_risk_state_paths(paths)
    eval_state = _load_json_safe(eval_path)
    guard_state = _load_json_safe(guard_path)
    eval_level = str((eval_state or {}).get("current_level", "") or "").strip().upper()
    guard_level = str((guard_state or {}).get("current_level", "") or "").strip().upper()
    eval_epoch = _risk_state_epoch(eval_state, primary_keys=("ts",))
    guard_epoch = _risk_state_epoch(guard_state, primary_keys=("last_update",))
    if eval_level and (not guard_level or guard_epoch is None or (eval_epoch is not None and eval_epoch >= guard_epoch)):
        return eval_level
    if guard_level:
        return guard_level
    return "UNKNOWN"


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
        print(f"[INFO] alert written to {paths.alert_file}")
        return True
    except Exception as exc:
        print(f"[ERROR] failed to persist alert: {exc}")
        return False


def check_and_alert(paths: MonitorPaths = DEFAULT_PATHS) -> bool:
    alerts: list[str] = []
    priority = "normal"
    service_unit = resolve_live_service_unit_name()
    risk_level = get_current_risk_level(paths)

    last_trade = get_last_trade_time(paths, service_unit=service_unit)
    trade_runs, total_fills = get_recent_trades_count(service_unit=service_unit)

    if last_trade is not None:
        hours_since_trade = (datetime.now() - last_trade).total_seconds() / 3600
        if not (risk_level == "PROTECT" and hours_since_trade >= ALERT_THRESHOLDS["no_trade_hours"]):
            if hours_since_trade >= ALERT_THRESHOLDS["no_trade_critical"]:
                alerts.append(f"critical: no trade for {hours_since_trade:.1f} hours")
                priority = "critical"
            elif hours_since_trade >= ALERT_THRESHOLDS["no_trade_hours"]:
                alerts.append(f"warning: no trade for {hours_since_trade:.1f} hours")
                priority = "warning"
    elif risk_level != "PROTECT":
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
