#!/usr/bin/env python3
"""
Operational health check for the V5 workspace.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import (
    load_runtime_config,
    resolve_runtime_config_path,
    resolve_runtime_env_path,
    resolve_runtime_path,
)
from src.execution.fill_store import (
    derive_fill_store_path,
    derive_position_store_path,
    derive_runtime_auto_risk_eval_path,
    derive_runtime_auto_risk_guard_path,
    derive_runtime_named_json_path,
)
from src.risk.auto_risk_guard import extract_risk_level

WORKSPACE = Path(__file__).resolve().parents[1]
REPORTS_DIR = WORKSPACE / "reports"
HEALTH_FILE = REPORTS_DIR / "health_status.json"


def _get_unit_load_state(unit: str) -> str:
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", unit, "--property=LoadState"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return ""

    for line in result.stdout.splitlines():
        if line.startswith("LoadState="):
            return line.split("=", 1)[1].strip()
    return ""


def resolve_live_timer_unit_name() -> str:
    if shutil.which("systemctl") is None:
        return "v5-prod.user.timer"

    current_unit = "v5-prod.user.timer"
    if _get_unit_load_state(current_unit) not in {"", "not-found"}:
        return current_unit

    return "v5-prod.user.timer"


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


def _load_active_runtime_config() -> dict[str, Any]:
    cfg = load_runtime_config(project_root=WORKSPACE)
    config_path = Path(resolve_runtime_config_path(project_root=WORKSPACE)).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"runtime config not found: {config_path}")
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {config_path}")
    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {config_path}")
    return cfg


def _resolve_health_database_paths() -> list[tuple[Path, str]]:
    cfg = _load_active_runtime_config()
    execution_cfg = cfg.get("execution", {})
    orders_db = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=WORKSPACE,
        )
    ).resolve()
    return [
        (orders_db, "orders"),
        (derive_position_store_path(orders_db).resolve(), "positions"),
        (derive_fill_store_path(orders_db).resolve(), "fills"),
    ]


def _resolve_health_risk_paths() -> tuple[Path, Path]:
    cfg = _load_active_runtime_config()
    execution_cfg = cfg.get("execution", {})
    orders_db = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=WORKSPACE,
        )
    ).resolve()
    return (
        derive_runtime_auto_risk_eval_path(orders_db).resolve(),
        derive_runtime_auto_risk_guard_path(orders_db).resolve(),
    )


def _load_json_safe(path: Path) -> dict[str, Any]:
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
        latest_history = max(
            (item for item in history if isinstance(item, dict)),
            key=lambda item: float(_coerce_timestamp_epoch(item.get("ts")) or float("-inf")),
            default=None,
        )
        if isinstance(latest_history, dict):
            epoch = _coerce_timestamp_epoch(latest_history.get("ts"))
            if epoch is not None:
                return epoch
    return None


def _resolve_health_output_path() -> Path:
    cfg = _load_active_runtime_config()
    execution_cfg = cfg.get("execution", {})
    orders_db = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=WORKSPACE,
        )
    ).resolve()
    return derive_runtime_named_json_path(orders_db, "health_status").resolve()


def _resolve_health_env_path() -> Path:
    try:
        return Path(resolve_runtime_env_path(project_root=WORKSPACE)).resolve()
    except Exception:
        return (WORKSPACE / ".env").resolve()


class HealthChecker:
    def __init__(self) -> None:
        self.status = "healthy"
        self.checks: List[Dict[str, Any]] = []

    @staticmethod
    def _parse_timer_show_output(stdout: str) -> tuple[dict[str, str], str | None, int | None]:
        props: dict[str, str] = {}
        last_trigger_text: str | None = None
        last_trigger_monotonic_usec: int | None = None
        for line in stdout.splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            props[key] = value
            if key == "LastTriggerUSec":
                if value and value != "n/a":
                    last_trigger_text = value
            elif key == "LastTriggerUSecMonotonic":
                if value and value != "n/a":
                    try:
                        parsed = int(value)
                    except ValueError:
                        parsed = None
                    if parsed and parsed > 0:
                        last_trigger_monotonic_usec = parsed
        return props, last_trigger_text, last_trigger_monotonic_usec

    def check_timer_health(self) -> Dict[str, Any]:
        if shutil.which("systemctl") is None:
            return {
                "name": "timers",
                "status": "warning",
                "details": "systemctl not available in current environment",
            }

        timers = [
            (resolve_live_timer_unit_name(), 70),
            ("v5-reconcile.timer", 10),
            ("v5-trade-monitor.timer", 70),
        ]
        issues: List[Dict[str, Any]] = []

        for timer_name, max_delay_min in timers:
            try:
                result = subprocess.run(
                    [
                        "systemctl",
                        "--user",
                        "show",
                        timer_name,
                        "--property=LoadState",
                        "--property=ActiveState",
                        "--property=UnitFileState",
                        "--property=LastTriggerUSec",
                        "--property=LastTriggerUSecMonotonic",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                props, last_trigger_text, last_trigger_monotonic_usec = self._parse_timer_show_output(result.stdout)
                load_state = props.get("LoadState", "")
                active_state = props.get("ActiveState", "")
                unit_file_state = props.get("UnitFileState", "")

                if result.returncode != 0 or load_state == "not-found":
                    issues.append({"timer": timer_name, "status": "missing", "detail": "unit not found"})
                    continue
                if unit_file_state not in {"enabled", "static", "alias"}:
                    issues.append(
                        {
                            "timer": timer_name,
                            "status": "disabled",
                            "detail": unit_file_state or "not enabled",
                        }
                    )
                    continue
                if active_state != "active":
                    issues.append(
                        {
                            "timer": timer_name,
                            "status": "inactive",
                            "detail": active_state or "inactive",
                        }
                    )
                    continue

                if last_trigger_monotonic_usec is None:
                    issues.append({"timer": timer_name, "status": "unknown", "detail": "no trigger time"})
                    continue

                delay = max(0.0, (time.monotonic() * 1_000_000 - last_trigger_monotonic_usec) / 60_000_000)
                if delay > max_delay_min:
                    issues.append(
                        {
                            "timer": timer_name,
                            "status": "stalled" if delay > max_delay_min * 2 else "delayed",
                            "last_run": last_trigger_text or "unknown",
                            "delay_min": round(delay, 1),
                        }
                    )
            except Exception as exc:
                issues.append({"timer": timer_name, "status": "error", "detail": str(exc)})

        if any(item["status"] in {"missing", "disabled", "inactive", "stalled", "error"} for item in issues):
            status = "critical"
        elif issues:
            status = "warning"
        else:
            status = "healthy"

        return {"name": "timers", "status": status, "details": issues or "all timers healthy"}

    def check_database_health(self) -> Dict[str, Any]:
        checks: List[Dict[str, Any]] = []
        for db_path, table_name in _resolve_health_database_paths():
            db_name = db_path.name
            if not db_path.exists():
                checks.append({"db": db_name, "status": "warning", "detail": "missing"})
                continue

            try:
                conn = sqlite3.connect(str(db_path))
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                conn.close()
                checks.append(
                    {
                        "db": db_name,
                        "status": "healthy",
                        "records": int(count),
                        "size_mb": round(db_path.stat().st_size / (1024 * 1024), 2),
                    }
                )
            except Exception as exc:
                checks.append({"db": db_name, "status": "critical", "detail": str(exc)})

        if any(item["status"] == "critical" for item in checks):
            status = "critical"
        elif any(item["status"] == "warning" for item in checks):
            status = "warning"
        else:
            status = "healthy"

        return {"name": "database", "status": status, "details": checks}

    def check_okx_api(self) -> Dict[str, Any]:
        load_env_file(_resolve_health_env_path())
        key = os.getenv("EXCHANGE_API_KEY") or os.getenv("OKX_API_KEY")
        secret = os.getenv("EXCHANGE_API_SECRET") or os.getenv("OKX_API_SECRET")
        passphrase = os.getenv("EXCHANGE_PASSPHRASE") or os.getenv("OKX_API_PASSPHRASE")

        if not (key and secret and passphrase):
            return {
                "name": "okx_api",
                "status": "warning",
                "details": f"API credentials missing in runtime env file: {_resolve_health_env_path().name}",
            }

        started = time.time()
        try:
            response = requests.get("https://www.okx.com/api/v5/public/time", timeout=10)
            latency_ms = round((time.time() - started) * 1000, 1)
            if response.status_code != 200:
                return {
                    "name": "okx_api",
                    "status": "critical",
                    "details": {"status_code": response.status_code, "latency_ms": latency_ms},
                }
            return {"name": "okx_api", "status": "healthy", "details": {"latency_ms": latency_ms}}
        except Exception as exc:
            return {"name": "okx_api", "status": "critical", "details": str(exc)}

    def check_disk_space(self) -> Dict[str, Any]:
        try:
            usage = shutil.disk_usage(WORKSPACE)
            used_pct = 0 if usage.total == 0 else round(usage.used / usage.total * 100, 1)
            status = "healthy"
            if used_pct >= 90:
                status = "critical"
            elif used_pct >= 80:
                status = "warning"
            return {
                "name": "disk",
                "status": status,
                "details": {
                    "total_gb": round(usage.total / (1024**3), 2),
                    "used_gb": round(usage.used / (1024**3), 2),
                    "free_gb": round(usage.free / (1024**3), 2),
                    "used_pct": used_pct,
                },
            }
        except Exception as exc:
            return {"name": "disk", "status": "warning", "details": str(exc)}

    def check_risk_guard(self) -> Dict[str, Any]:
        eval_path, guard_path = _resolve_health_risk_paths()
        eval_state = _load_json_safe(eval_path)
        guard_state = _load_json_safe(guard_path)
        eval_level = extract_risk_level(eval_state)
        guard_level = extract_risk_level(guard_state)
        eval_epoch = _risk_state_epoch(eval_state, primary_keys=("ts",))
        guard_epoch = _risk_state_epoch(guard_state, primary_keys=("last_update",))

        source = "missing"
        risk = {}
        if eval_level and (not guard_level or guard_epoch is None or (eval_epoch is not None and eval_epoch >= guard_epoch)):
            risk = eval_state
            source = "eval"
        elif guard_level:
            risk = guard_state
            source = "guard"

        level = extract_risk_level(risk) or "UNKNOWN"
        metrics = risk.get("metrics", {}) if isinstance(risk, dict) else {}
        drawdown = metrics.get("dd_pct", metrics.get("last_dd_pct", 0))
        return {
            "name": "risk_guard",
            "status": "healthy" if level != "UNKNOWN" else "warning",
            "details": {
                "level": level,
                "drawdown": drawdown,
                "source": source,
            },
        }

    def run_all_checks(self) -> Dict[str, Any]:
        self.checks = [
            self.check_timer_health(),
            self.check_database_health(),
            self.check_risk_guard(),
            self.check_okx_api(),
            self.check_disk_space(),
        ]

        if any(item["status"] == "critical" for item in self.checks):
            self.status = "critical"
        elif any(item["status"] == "warning" for item in self.checks):
            self.status = "warning"
        else:
            self.status = "healthy"

        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "workspace": str(WORKSPACE),
            "overall_status": self.status,
            "checks": self.checks,
        }

    def print_report(self) -> Dict[str, Any]:
        result = self.run_all_checks()
        print("=" * 60)
        print("V5 Health Check")
        print("=" * 60)
        print(f"time: {result['timestamp']}")
        print(f"status: {result['overall_status']}")
        print()

        for check in result["checks"]:
            print(f"[{check['status'].upper()}] {check['name']}")
            details = check["details"]
            if isinstance(details, list):
                for item in details:
                    print(f"  - {item}")
            else:
                print(f"  {details}")
            print()

        return result


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    checker = HealthChecker()
    result = checker.print_report()
    output_path = _resolve_health_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return 1 if result["overall_status"] == "critical" else 0


if __name__ == "__main__":
    raise SystemExit(main())
