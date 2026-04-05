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
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import requests

WORKSPACE = Path(__file__).resolve().parents[1]
REPORTS_DIR = WORKSPACE / "reports"
HEALTH_FILE = REPORTS_DIR / "health_status.json"


def resolve_live_timer_unit_name() -> str:
    if shutil.which("systemctl") is None:
        return "v5-prod.user.timer"

    for unit in ("v5-prod.user.timer", "v5-live-20u.user.timer"):
        try:
            result = subprocess.run(
                ["systemctl", "--user", "status", unit],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return unit
        except Exception:
            pass
    return "v5-prod"


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


class HealthChecker:
    def __init__(self) -> None:
        self.status = "healthy"
        self.checks: List[Dict[str, Any]] = []

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
                        "--property=LastTriggerUSec",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                last_trigger_usec = None
                for line in result.stdout.splitlines():
                    if line.startswith("LastTriggerUSec="):
                        value = line.split("=", 1)[1].strip()
                        if value and value != "n/a":
                            try:
                                last_trigger_usec = int(value)
                            except ValueError:
                                pass
                        break

                if last_trigger_usec is None:
                    issues.append({"timer": timer_name, "status": "unknown", "detail": "no trigger time"})
                    continue

                last_trigger = datetime.fromtimestamp(last_trigger_usec / 1_000_000)
                delay = (datetime.now() - last_trigger).total_seconds() / 60
                if delay > max_delay_min:
                    issues.append(
                        {
                            "timer": timer_name,
                            "status": "stalled" if delay > max_delay_min * 2 else "delayed",
                            "last_run": last_trigger.isoformat(timespec="minutes"),
                            "delay_min": round(delay, 1),
                        }
                    )
            except Exception as exc:
                issues.append({"timer": timer_name, "status": "error", "detail": str(exc)})

        if any(item["status"] in {"stalled", "error"} for item in issues):
            status = "critical"
        elif issues:
            status = "warning"
        else:
            status = "healthy"

        return {"name": "timers", "status": status, "details": issues or "all timers healthy"}

    def check_database_health(self) -> Dict[str, Any]:
        checks: List[Dict[str, Any]] = []
        for db_name, table_name in (("orders.sqlite", "orders"), ("positions.sqlite", "positions")):
            db_path = REPORTS_DIR / db_name
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
        load_env_file(WORKSPACE / ".env")
        key = os.getenv("EXCHANGE_API_KEY") or os.getenv("OKX_API_KEY")
        secret = os.getenv("EXCHANGE_API_SECRET") or os.getenv("OKX_API_SECRET")
        passphrase = os.getenv("EXCHANGE_PASSPHRASE") or os.getenv("OKX_API_PASSPHRASE")

        if not (key and secret and passphrase):
            return {
                "name": "okx_api",
                "status": "warning",
                "details": "API credentials missing in root .env",
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

    def run_all_checks(self) -> Dict[str, Any]:
        self.checks = [
            self.check_timer_health(),
            self.check_database_health(),
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
    HEALTH_FILE.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return 1 if result["overall_status"] == "critical" else 0


if __name__ == "__main__":
    raise SystemExit(main())
