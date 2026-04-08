"""
V5 health endpoints used by monitoring.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from flask import Blueprint, jsonify

from src.execution.fill_store import derive_fill_store_path, derive_position_store_path

health_bp = Blueprint("health", __name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = PROJECT_ROOT / "reports"
CONFIGS_DIR = PROJECT_ROOT / "configs"


@dataclass(frozen=True)
class HealthPaths:
    orders_db: Path
    fills_db: Path
    positions_db: Path
    kill_switch_path: Path
    reconcile_status_path: Path


def _resolve_active_config_path() -> Path:
    import os

    env_cfg = (os.getenv("V5_CONFIG") or "").strip()
    if env_cfg:
        path = Path(env_cfg)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        return path

    for candidate in ("live_prod.yaml", "live_20u_real.yaml", "config.yaml"):
        path = CONFIGS_DIR / candidate
        if path.exists():
            return path

    return CONFIGS_DIR / "live_prod.yaml"


def _load_json_safe(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _load_active_config() -> dict:
    try:
        import yaml

        path = _resolve_active_config_path()
        if path.exists():
            return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        pass
    return {}


def _resolve_runtime_path(raw_path: object, default_rel_path: str) -> Path:
    raw = str(raw_path or default_rel_path).strip()
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _resolve_health_paths() -> HealthPaths:
    cfg = _load_active_config()
    execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
    orders_db = _resolve_runtime_path(execution_cfg.get("order_store_path"), "reports/orders.sqlite")
    return HealthPaths(
        orders_db=orders_db,
        fills_db=derive_fill_store_path(orders_db),
        positions_db=derive_position_store_path(orders_db),
        kill_switch_path=_resolve_runtime_path(execution_cfg.get("kill_switch_path"), "reports/kill_switch.json"),
        reconcile_status_path=_resolve_runtime_path(execution_cfg.get("reconcile_status_path"), "reports/reconcile_status.json"),
    )


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_kill_switch(data: object) -> dict:
    if isinstance(data, dict):
        if "enabled" in data or "active" in data:
            normalized = dict(data)
            if "enabled" not in normalized:
                normalized["enabled"] = _to_bool(normalized.get("active"))
            return normalized

        nested = data.get("kill_switch")
        if isinstance(nested, dict):
            normalized = dict(nested)
            if "enabled" not in normalized:
                normalized["enabled"] = _to_bool(normalized.get("active"))
            return normalized

        normalized = dict(data)
        normalized["enabled"] = _to_bool(nested)
        return normalized

    if data is None:
        return {"enabled": False}

    return {"enabled": _to_bool(data)}


def _load_latest_fill_ts_ms(fills_db: Path) -> int | None:
    try:
        if fills_db.exists():
            conn = sqlite3.connect(str(fills_db))
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


def _load_latest_filled_order_ts_ms(orders_db: Path) -> int | None:
    if not orders_db.exists():
        return None

    try:
        conn = sqlite3.connect(str(orders_db))
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
        except sqlite3.OperationalError:
            row = conn.execute("SELECT MAX(created_ts) FROM orders WHERE state='FILLED'").fetchone()
        finally:
            conn.close()
        ts_ms = row[0] if row else None
        return int(ts_ms) if ts_ms else None
    except Exception:
        return None


def _load_last_trade_ts_ms() -> int | None:
    paths = _resolve_health_paths()
    fills_ts_ms = _load_latest_fill_ts_ms(paths.fills_db)
    orders_ts_ms = _load_latest_filled_order_ts_ms(paths.orders_db)
    candidates = [ts_ms for ts_ms in (fills_ts_ms, orders_ts_ms) if ts_ms]
    return max(candidates) if candidates else None


def _check_runtime_positions_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
              AND name IN ('positions', 'account_state')
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    if not row:
        raise RuntimeError("missing positions/account_state tables")


@health_bp.route("/health")
def health_check():
    checks = {
        "status": "healthy",
        "timestamp": time.time(),
        "checks": {},
    }
    health_paths = _resolve_health_paths()

    try:
        _check_runtime_positions_db(health_paths.positions_db)
        checks["checks"]["database"] = {"status": "ok", "path": str(health_paths.positions_db)}
    except Exception as exc:
        checks["checks"]["database"] = {
            "status": "error",
            "error": str(exc),
            "path": str(health_paths.positions_db),
        }
        checks["status"] = "unhealthy"

    try:
        kill_switch = _normalize_kill_switch(_load_json_safe(health_paths.kill_switch_path))
        kill_switch_enabled = _to_bool(kill_switch.get("enabled"))
        checks["checks"]["kill_switch"] = {
            "status": "ok",
            "enabled": kill_switch_enabled,
            "trigger": kill_switch.get("trigger", ""),
        }
        if kill_switch_enabled:
            checks["status"] = "degraded"
    except Exception as exc:
        checks["checks"]["kill_switch"] = {"status": "error", "error": str(exc)}

    try:
        reconcile = _load_json_safe(health_paths.reconcile_status_path)
        reconcile_ok = _to_bool(reconcile.get("ok"))
        checks["checks"]["reconcile"] = {
            "status": "ok" if reconcile_ok else "warning",
            "ok": reconcile_ok,
            "reason": reconcile.get("reason", ""),
        }
        if not reconcile_ok:
            checks["status"] = "degraded"
    except Exception as exc:
        checks["checks"]["reconcile"] = {"status": "error", "error": str(exc)}

    try:
        last_trade_ts = _load_last_trade_ts_ms()
        if last_trade_ts:
            age_min = (time.time() * 1000 - last_trade_ts) / 60000
            checks["checks"]["last_trade"] = {
                "status": "ok" if age_min < 120 else "warning",
                "age_minutes": round(age_min, 1),
                "last_ts": last_trade_ts,
            }
            if age_min >= 120:
                checks["status"] = "degraded"
        else:
            checks["checks"]["last_trade"] = {"status": "warning", "message": "No trades yet"}
    except Exception as exc:
        checks["checks"]["last_trade"] = {"status": "error", "error": str(exc)}

    try:
        risk = _load_json_safe(REPORTS_DIR / "auto_risk_eval.json")
        checks["checks"]["risk_guard"] = {
            "status": "ok",
            "level": risk.get("current_level", "UNKNOWN"),
            "drawdown": risk.get("metrics", {}).get("dd_pct", risk.get("metrics", {}).get("last_dd_pct", 0)),
        }
    except Exception as exc:
        checks["checks"]["risk_guard"] = {"status": "error", "error": str(exc)}

    status_code = 200 if checks["status"] == "healthy" else 503
    return jsonify(checks), status_code


@health_bp.route("/ready")
def readiness_check():
    ready = True
    reasons: list[str] = []
    health_paths = _resolve_health_paths()

    try:
        _check_runtime_positions_db(health_paths.positions_db)
    except Exception as exc:
        ready = False
        reasons.append(f"Database unavailable ({health_paths.positions_db}): {exc}")

    config_path = _resolve_active_config_path()
    if not config_path.exists():
        ready = False
        reasons.append(f"Config file missing: {config_path.name}")

    return jsonify({"ready": ready, "reasons": reasons}), 200 if ready else 503


@health_bp.route("/liveness")
def liveness_check():
    return jsonify({"alive": True}), 200
