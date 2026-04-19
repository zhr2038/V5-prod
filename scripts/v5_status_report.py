#!/usr/bin/env python3
"""
Generate a concise production status report for the V5 workspace.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))

from configs.runtime_config import resolve_runtime_config_path
from src.execution.fill_store import (
    derive_fill_store_path,
    derive_runtime_auto_risk_eval_path,
    derive_runtime_auto_risk_guard_path,
    derive_runtime_named_artifact_path,
    derive_runtime_named_json_path,
    derive_runtime_runs_dir,
)
from src.risk.auto_risk_guard import extract_risk_level

REPORTS_DIR = WORKSPACE / "reports"
RUNS_DIR = REPORTS_DIR / "runs"
FILLS_DB = REPORTS_DIR / "fills.sqlite"
ORDERS_DB = REPORTS_DIR / "orders.sqlite"
LIVE_UNITS = (
    ("v5-prod.user.service", "v5-prod.user.timer"),
)


def resolve_config_path() -> Path:
    return Path(resolve_runtime_config_path(project_root=WORKSPACE))


CONFIG_PATH = resolve_config_path()


@dataclass(frozen=True)
class StatusPaths:
    orders_db: Path
    fills_db: Path
    auto_blacklist_path: Path
    auto_risk_eval_path: Path
    auto_risk_guard_path: Path
    runs_dir: Path


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"runtime config not found: {CONFIG_PATH}")
    try:
        import yaml

        payload = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise ValueError(f"runtime config is invalid: {CONFIG_PATH}") from exc
    if not isinstance(payload, dict) or not payload:
        raise ValueError(f"runtime config is empty or invalid: {CONFIG_PATH}")
    return payload


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
        auto_blacklist_path=derive_runtime_named_json_path(orders_db, "auto_blacklist"),
        auto_risk_eval_path=derive_runtime_auto_risk_eval_path(orders_db),
        auto_risk_guard_path=derive_runtime_auto_risk_guard_path(orders_db),
        runs_dir=derive_runtime_runs_dir(orders_db),
    )


def _load_json_safe(path: Path) -> Dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _coerce_timestamp_epoch(value: Any) -> float | None:
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


def _risk_state_epoch(payload: Any, *, primary_keys: tuple[str, ...]) -> float | None:
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


def get_current_risk_guard(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    paths = _resolve_status_paths(cfg)
    eval_state = _load_json_safe(paths.auto_risk_eval_path)
    guard_state = _load_json_safe(paths.auto_risk_guard_path)
    eval_level = extract_risk_level(eval_state)
    guard_level = extract_risk_level(guard_state)
    eval_epoch = _risk_state_epoch(eval_state, primary_keys=("ts",))
    guard_epoch = _risk_state_epoch(guard_state, primary_keys=("last_update",))

    source = "missing"
    state: Dict[str, Any] = {}
    if eval_level and (not guard_level or guard_epoch is None or (eval_epoch is not None and eval_epoch >= guard_epoch)):
        source = "eval"
        state = eval_state
    elif guard_level:
        source = "guard"
        state = guard_state

    level = extract_risk_level(state) or "UNKNOWN"
    last_update = (
        str((state or {}).get("ts") or "").strip()
        if source == "eval"
        else str((state or {}).get("last_update") or "").strip()
    )
    if not last_update and isinstance(state.get("history"), list) and state["history"]:
        tail = state["history"][-1]
        if isinstance(tail, dict):
            last_update = str(tail.get("ts") or "").strip()

    return {
        "level": level,
        "source": source,
        "last_update": last_update or "n/a",
    }


def get_latest_run_data(cfg: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    runs_dir = _resolve_status_paths(cfg).runs_dir
    if not runs_dir.exists():
        return None

    run_dirs = [path for path in runs_dir.iterdir() if path.is_dir() and (path / "decision_audit.json").exists()]
    if not run_dirs:
        return None

    latest_dir = max(run_dirs, key=lambda path: (path / "decision_audit.json").stat().st_mtime)
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
    if any(_get_unit_load_state(unit) not in {"", "not-found"} for unit in current_units):
        return current_units
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


def _negative_expectancy_lines(counts: Dict[str, Any]) -> list[str]:
    return [
        f"- negative_expectancy_score_penalty: {counts.get('negative_expectancy_score_penalty', 'n/a')}",
        f"- negative_expectancy_cooldown: {counts.get('negative_expectancy_cooldown', 'n/a')}",
        f"- negative_expectancy_open_block: {counts.get('negative_expectancy_open_block', 'n/a')}",
        f"- negative_expectancy_fast_fail_open_block: {counts.get('negative_expectancy_fast_fail_open_block', 'n/a')}",
    ]


def generate_report() -> str:
    cfg = load_config()
    run_data = get_latest_run_data(cfg) or {}
    borrow = check_borrow_status(cfg)
    risk_guard = get_current_risk_guard(cfg)
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
            f"- risk_guard_level: {risk_guard['level']}",
            f"- risk_guard_source: {risk_guard['source']}",
            f"- risk_guard_last_update: {risk_guard['last_update']}",
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
            *_negative_expectancy_lines(counts),
            f"- drawdown_note: {drawdown_note}",
            f"- last_filled_trade: {last_trade}",
        ]
    )


def _resolve_report_output_path(paths: StatusPaths, timestamp: str) -> Path:
    return derive_runtime_named_artifact_path(
        paths.orders_db,
        f"status_report_{timestamp}",
        ".txt",
    ).resolve()


def main() -> int:
    cfg = load_config()
    paths = _resolve_status_paths(cfg)
    reports_dir = paths.orders_db.parent
    reports_dir.mkdir(parents=True, exist_ok=True)
    report = generate_report()
    print(report)
    output = _resolve_report_output_path(paths, datetime.now().strftime('%Y%m%d_%H%M'))
    output.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
