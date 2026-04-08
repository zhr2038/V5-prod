#!/usr/bin/env python3
"""
Generate a concise production status report for the V5 workspace.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))

from src.execution.fill_store import derive_fill_store_path

REPORTS_DIR = WORKSPACE / "reports"
RUNS_DIR = REPORTS_DIR / "runs"
FILLS_DB = REPORTS_DIR / "fills.sqlite"
ORDERS_DB = REPORTS_DIR / "orders.sqlite"
LIVE_UNITS = (
    ("v5-prod.user.service", "v5-prod.user.timer"),
    ("v5-live-20u.user.service", "v5-live-20u.user.timer"),
)


def resolve_config_path() -> Path:
    env_cfg = (os.getenv("V5_CONFIG") or "").strip()
    if env_cfg:
        path = Path(env_cfg)
        if not path.is_absolute():
            path = WORKSPACE / path
        return path

    for candidate in ("configs/live_prod.yaml", "configs/live_20u_real.yaml", "configs/config.yaml"):
        path = WORKSPACE / candidate
        if path.exists():
            return path

    return WORKSPACE / "configs/live_prod.yaml"


CONFIG_PATH = resolve_config_path()


@dataclass(frozen=True)
class StatusPaths:
    orders_db: Path
    fills_db: Path
    auto_blacklist_path: Path


def load_config() -> Dict[str, Any]:
    try:
        import yaml

        return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _resolve_runtime_path(raw_path: object, default_path: Path) -> Path:
    raw = str(raw_path or default_path).strip()
    path = Path(raw)
    if not path.is_absolute():
        path = WORKSPACE / path
    return path.resolve()


def _resolve_status_paths(cfg: Optional[Dict[str, Any]] = None) -> StatusPaths:
    config = cfg if isinstance(cfg, dict) else load_config()
    execution_cfg = config.get("execution", {}) if isinstance(config, dict) else {}
    orders_db = _resolve_runtime_path(execution_cfg.get("order_store_path"), ORDERS_DB)
    return StatusPaths(
        orders_db=orders_db,
        fills_db=derive_fill_store_path(orders_db),
        auto_blacklist_path=orders_db.parent / "auto_blacklist.json",
    )


def get_latest_run_data() -> Optional[Dict[str, Any]]:
    if not RUNS_DIR.exists():
        return None

    run_dirs = [path for path in RUNS_DIR.iterdir() if path.is_dir() and (path / "decision_audit.json").exists()]
    if not run_dirs:
        return None

    latest_dir = max(run_dirs, key=lambda path: path.stat().st_mtime)
    try:
        return json.loads((latest_dir / "decision_audit.json").read_text(encoding="utf-8"))
    except Exception:
        return None


def _unit_is_active(unit: str) -> bool:
    result = subprocess.run(
        ["systemctl", "--user", "is-active", unit],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.returncode == 0


def _get_unit_load_state(unit: str) -> str:
    result = subprocess.run(
        ["systemctl", "--user", "show", unit, "--property=LoadState"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    for line in result.stdout.splitlines():
        if line.startswith("LoadState="):
            return line.split("=", 1)[1].strip()
    return ""


def _resolve_live_units() -> tuple[str, str]:
    current_units = LIVE_UNITS[0]
    legacy_units = LIVE_UNITS[1]

    current_exists = any(_get_unit_load_state(unit) not in {"", "not-found"} for unit in current_units)
    if current_exists:
        return current_units

    legacy_exists = any(_get_unit_load_state(unit) not in {"", "not-found"} for unit in legacy_units)
    if legacy_exists:
        return legacy_units

    return current_units


def get_service_status() -> str:
    try:
        service_unit, timer_unit = _resolve_live_units()
        if _unit_is_active(service_unit):
            return "running"
        if _unit_is_active(timer_unit):
            return "scheduled"
        return "stopped"
    except Exception:
        return "unknown"


def check_borrow_status(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = cfg if isinstance(cfg, dict) else load_config()
    paths = _resolve_status_paths(config)
    execution_cfg = config.get("execution", {}) if isinstance(config, dict) else {}

    blacklist_file = paths.auto_blacklist_path
    entries = []
    if blacklist_file.exists():
        try:
            payload = json.loads(blacklist_file.read_text(encoding="utf-8"))
            entries = payload.get("entries", []) if isinstance(payload, dict) else []
        except Exception:
            entries = []

    return {
        "config": {
            "liab_eps": execution_cfg.get("borrow_liab_eps", 0.01),
            "neg_eq_eps": execution_cfg.get("borrow_neg_eq_eps", 0.01),
            "mode": execution_cfg.get("borrow_block_mode", "symbol_only"),
        },
        "blacklist_count": len(entries),
        "blacklist_symbols": [item.get("symbol") for item in entries[:5] if isinstance(item, dict)],
    }


def _format_ts_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(int(ts_ms) / 1000).isoformat(timespec="minutes")


def _get_latest_fill_ts_ms(path: Path) -> Optional[int]:
    try:
        if path.exists():
            conn = sqlite3.connect(str(path))
            try:
                row = conn.execute("SELECT MAX(ts_ms) FROM fills").fetchone()
            finally:
                conn.close()
            ts_ms = row[0] if row else None
            if ts_ms:
                return int(ts_ms)
    except Exception:
        pass
    return None


def _get_latest_filled_order_ts_ms(path: Path) -> Optional[int]:
    if not path.exists():
        return None

    try:
        conn = sqlite3.connect(str(path))
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
                WHERE state='FILLED'
                """
            ).fetchone()
        finally:
            conn.close()
        ts_ms = row[0] if row else None
        return int(ts_ms) if ts_ms else None
    except Exception:
        return None


def get_last_filled_trade_ts(cfg: Optional[Dict[str, Any]] = None) -> Optional[str]:
    paths = _resolve_status_paths(cfg)
    candidates = [
        ts_ms
        for ts_ms in (
            _get_latest_fill_ts_ms(paths.fills_db),
            _get_latest_filled_order_ts_ms(paths.orders_db),
        )
        if ts_ms
    ]
    if not candidates:
        return None
    return _format_ts_ms(max(candidates))


def build_next_run_hint() -> str:
    next_run = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return next_run.strftime("%Y-%m-%d %H:%M")


def generate_report() -> str:
    cfg = load_config()
    run_data = get_latest_run_data() or {}
    borrow = check_borrow_status(cfg)
    service_status = get_service_status()
    last_trade = get_last_filled_trade_ts(cfg) or "n/a"
    budget_cap = cfg.get("budget", {}).get("live_equity_cap_usdt", "n/a") if isinstance(cfg, dict) else "n/a"

    counts = run_data.get("counts", {}) if isinstance(run_data, dict) else {}
    notes = run_data.get("notes", []) if isinstance(run_data, dict) else []
    drawdown_note = next((note for note in notes if isinstance(note, str) and "drawdown" in note.lower()), "n/a")

    return "\n".join(
        [
            "V5 Status Report",
            f"time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"workspace: {WORKSPACE}",
            f"config: {CONFIG_PATH.name}",
            "",
            "System",
            f"- live_service: {service_status}",
            f"- live_equity_cap_usdt: {budget_cap}",
            f"- next_expected_run: {build_next_run_hint()}",
            "",
            "Borrow Guard",
            f"- liab_eps: {borrow['config']['liab_eps']}",
            f"- neg_eq_eps: {borrow['config']['neg_eq_eps']}",
            f"- mode: {borrow['config']['mode']}",
            f"- blacklist_count: {borrow['blacklist_count']}",
            f"- blacklist_symbols: {borrow['blacklist_symbols']}",
            "",
            "Latest Run",
            f"- regime: {run_data.get('regime', 'n/a')}",
            f"- selected: {counts.get('selected', 'n/a')}",
            f"- targets_pre_risk: {counts.get('targets_pre_risk', 'n/a')}",
            f"- orders_rebalance: {counts.get('orders_rebalance', 'n/a')}",
            f"- drawdown_note: {drawdown_note}",
            f"- last_filled_trade: {last_trade}",
        ]
    )


def main() -> int:
    cfg = load_config()
    reports_dir = _resolve_status_paths(cfg).orders_db.parent
    reports_dir.mkdir(parents=True, exist_ok=True)
    report = generate_report()
    print(report)
    output = reports_dir / f"status_report_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
    output.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
