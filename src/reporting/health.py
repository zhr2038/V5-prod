"""
V5 健康检查模块
提供 /health 和 /ready 端点用于监控
"""

import json
import sqlite3
import time
from pathlib import Path
from flask import Blueprint, jsonify

health_bp = Blueprint('health', __name__)

# 路径配置
PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = PROJECT_ROOT / 'reports'
CONFIGS_DIR = PROJECT_ROOT / 'configs'


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
    """安全加载JSON文件"""
    try:
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {}


@health_bp.route("/health")
def health_check():
    """健康检查端点 - 返回详细状态"""
    checks = {
        "status": "healthy",
        "timestamp": time.time(),
        "checks": {}
    }
    
    # 1. 数据库连接检查
    try:
        db_path = REPORTS_DIR / 'positions.sqlite'
        conn = sqlite3.connect(str(db_path))
        conn.execute("SELECT 1")
        conn.close()
        checks["checks"]["database"] = {"status": "ok"}
    except Exception as e:
        checks["checks"]["database"] = {"status": "error", "error": str(e)}
        checks["status"] = "unhealthy"
    
    # 2. Kill Switch状态
    try:
        ks = _load_json_safe(REPORTS_DIR / 'kill_switch.json')
        checks["checks"]["kill_switch"] = {
            "status": "ok",
            "enabled": ks.get("enabled", False),
            "trigger": ks.get("trigger", "")
        }
        if ks.get("enabled"):
            checks["status"] = "degraded"
    except Exception as e:
        checks["checks"]["kill_switch"] = {"status": "error", "error": str(e)}
    
    # 3. Reconcile状态
    try:
        rec = _load_json_safe(REPORTS_DIR / 'reconcile_status.json')
        checks["checks"]["reconcile"] = {
            "status": "ok" if rec.get("ok") else "warning",
            "ok": rec.get("ok", False),
            "reason": rec.get("reason", "")
        }
        if not rec.get("ok"):
            checks["status"] = "degraded"
    except Exception as e:
        checks["checks"]["reconcile"] = {"status": "error", "error": str(e)}
    
    # 4. 最近交易时间
    try:
        db_path = REPORTS_DIR / 'orders.sqlite'
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT MAX(created_ts) FROM orders WHERE state='FILLED'")
        last_order_ts = cursor.fetchone()[0]
        conn.close()
        
        if last_order_ts:
            age_min = (time.time() * 1000 - last_order_ts) / 60000
            checks["checks"]["last_trade"] = {
                "status": "ok" if age_min < 120 else "warning",
                "age_minutes": round(age_min, 1),
                "last_ts": last_order_ts
            }
            if age_min >= 120:
                checks["status"] = "degraded"
        else:
            checks["checks"]["last_trade"] = {"status": "warning", "message": "No trades yet"}
    except Exception as e:
        checks["checks"]["last_trade"] = {"status": "error", "error": str(e)}
    
    # 5. 自动风险档位状态
    try:
        risk = _load_json_safe(REPORTS_DIR / 'auto_risk_eval.json')
        checks["checks"]["risk_guard"] = {
            "status": "ok",
            "level": risk.get("current_level", "UNKNOWN"),
            "drawdown": risk.get("metrics", {}).get("dd_pct", risk.get("metrics", {}).get("last_dd_pct", 0))
        }
    except Exception as e:
        checks["checks"]["risk_guard"] = {"status": "error", "error": str(e)}
    
    status_code = 200 if checks["status"] == "healthy" else 503
    return jsonify(checks), status_code


@health_bp.route("/ready")
def readiness_check():
    """就绪检查 - 用于K8s等编排系统"""
    ready = True
    reasons = []
    
    # 检查数据库可连接
    try:
        db_path = REPORTS_DIR / 'positions.sqlite'
        conn = sqlite3.connect(str(db_path))
        conn.execute("SELECT 1")
        conn.close()
    except Exception as e:
        ready = False
        reasons.append(f"Database unavailable: {e}")
    
    # 检查配置存在
    config_path = _resolve_active_config_path()
    if not config_path.exists():
        ready = False
        reasons.append(f"Config file missing: {config_path.name}")
    
    return jsonify({
        "ready": ready,
        "reasons": reasons
    }), 200 if ready else 503


@health_bp.route("/liveness")
def liveness_check():
    """存活检查 - 最简单的健康检查"""
    return jsonify({"alive": True}), 200
