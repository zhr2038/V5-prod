"""
V5 health endpoints used by monitoring.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from flask import Blueprint, jsonify

health_bp = Blueprint("health", __name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = PROJECT_ROOT / "reports"
CONFIGS_DIR = PROJECT_ROOT / "configs"


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


def _load_latest_fill_ts_ms() -> int | None:
    fills_db = REPORTS_DIR / "fills.sqlite"
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


def _load_latest_filled_order_ts_ms() -> int | None:
    orders_db = REPORTS_DIR / "orders.sqlite"
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
    fills_ts_ms = _load_latest_fill_ts_ms()
    orders_ts_ms = _load_latest_filled_order_ts_ms()
    candidates = [ts_ms for ts_ms in (fills_ts_ms, orders_ts_ms) if ts_ms]
    return max(candidates) if candidates else None


@health_bp.route("/health")
def health_check():
    checks = {
        "status": "healthy",
        "timestamp": time.time(),
        "checks": {},
    }

    try:
        db_path = REPORTS_DIR / "positions.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("SELECT 1")
        conn.close()
        checks["checks"]["database"] = {"status": "ok"}
    except Exception as exc:
        checks["checks"]["database"] = {"status": "error", "error": str(exc)}
        checks["status"] = "unhealthy"

    try:
        kill_switch = _normalize_kill_switch(_load_json_safe(REPORTS_DIR / "kill_switch.json"))
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
        reconcile = _load_json_safe(REPORTS_DIR / "reconcile_status.json")
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

    try:
        db_path = REPORTS_DIR / "positions.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("SELECT 1")
        conn.close()
    except Exception as exc:
        ready = False
        reasons.append(f"Database unavailable: {exc}")

    config_path = _resolve_active_config_path()
    if not config_path.exists():
        ready = False
        reasons.append(f"Config file missing: {config_path.name}")

    return jsonify({"ready": ready, "reasons": reasons}), 200 if ready else 503


@health_bp.route("/liveness")
def liveness_check():
    return jsonify({"alive": True}), 200
