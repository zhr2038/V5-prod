"""
Smart alert engine for the active V5 workspace.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_path
from src.execution.fill_store import (
    derive_fill_store_path,
    derive_runtime_named_artifact_path,
    derive_runtime_named_json_path,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class SmartAlertPaths:
    reports_dir: Path
    runs_dir: Path
    fills_db: Path
    orders_db: Path
    alerts_state_file: Path
    reconcile_file: Path
    kill_switch_file: Path
    ic_file: Path


def _load_active_config(*, workspace: Path) -> dict[str, Any]:
    config_path = Path(resolve_runtime_config_path(project_root=workspace))
    try:
        import yaml

        if config_path.exists():
            return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        pass
    return {}


def _resolve_runtime_state_path(
    raw_path: object,
    *,
    workspace: Path,
    orders_db: Path,
    base_name: str,
    legacy_default: str,
) -> Path:
    raw = str(raw_path or "").strip()
    if not raw or raw == legacy_default:
        return derive_runtime_named_json_path(orders_db, base_name).resolve()
    return Path(
        resolve_runtime_path(
            raw,
            default=legacy_default,
            project_root=workspace,
        )
    ).resolve()


def _resolve_paths(*, workspace: Path) -> SmartAlertPaths:
    cfg = _load_active_config(workspace=workspace)
    execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
    orders_db = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=workspace,
        )
    )
    reports_dir = orders_db.parent.resolve()
    return SmartAlertPaths(
        reports_dir=reports_dir,
        runs_dir=reports_dir / "runs",
        fills_db=derive_fill_store_path(orders_db),
        orders_db=orders_db,
        alerts_state_file=derive_runtime_named_json_path(orders_db, "alerts_state").resolve(),
        reconcile_file=_resolve_runtime_state_path(
            execution_cfg.get("reconcile_status_path"),
            workspace=workspace,
            orders_db=orders_db,
            base_name="reconcile_status",
            legacy_default="reports/reconcile_status.json",
        ),
        kill_switch_file=_resolve_runtime_state_path(
            execution_cfg.get("kill_switch_path"),
            workspace=workspace,
            orders_db=orders_db,
            base_name="kill_switch",
            legacy_default="reports/kill_switch.json",
        ),
        ic_file=derive_runtime_named_artifact_path(orders_db, "ic_diagnostics_30d_20u", ".json").resolve(),
    )


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _kill_switch_enabled(data: Any) -> bool:
    if not isinstance(data, dict):
        return _to_bool(data)
    if "enabled" in data:
        return _to_bool(data.get("enabled"))
    if "active" in data:
        return _to_bool(data.get("active"))

    nested = data.get("kill_switch")
    if isinstance(nested, dict):
        if "enabled" in nested:
            return _to_bool(nested.get("enabled"))
        if "active" in nested:
            return _to_bool(nested.get("active"))
        return False
    return _to_bool(nested)


class SmartAlertEngine:
    """Emit only actionable anomaly alerts."""

    def __init__(self, workspace: Path = PROJECT_ROOT):
        self.workspace = workspace
        self.paths = _resolve_paths(workspace=workspace)
        self.reports_dir = self.paths.reports_dir
        self.alerts_state_file = self.paths.alerts_state_file
        self._load_state()

    def _load_state(self) -> None:
        if self.alerts_state_file.exists():
            try:
                with self.alerts_state_file.open("r", encoding="utf-8") as f:
                    self.state = json.load(f)
            except Exception:
                self.state = {}
        else:
            self.state = {}

    def _save_state(self) -> None:
        self.alerts_state_file.parent.mkdir(parents=True, exist_ok=True)
        with self.alerts_state_file.open("w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)

    def _should_alert(self, alert_type: str, cooldown_minutes: int = 60) -> bool:
        now = datetime.now().timestamp()
        last_alert = self.state.get(f"last_{alert_type}", 0)
        if now - last_alert > cooldown_minutes * 60:
            self.state[f"last_{alert_type}"] = now
            return True
        return False

    def _load_recent_run_audits(self, limit: int) -> list[dict[str, Any]]:
        runs_dir = self.paths.runs_dir
        if not runs_dir.exists():
            return []

        run_dirs = [
            run_dir
            for run_dir in runs_dir.iterdir()
            if run_dir.is_dir() and (run_dir / "decision_audit.json").exists()
        ]
        run_dirs.sort(key=lambda path: path.stat().st_mtime, reverse=True)

        audits: list[dict[str, Any]] = []
        for run_dir in run_dirs[:limit]:
            try:
                with (run_dir / "decision_audit.json").open("r", encoding="utf-8") as f:
                    audits.append(json.load(f))
            except Exception:
                continue
        return audits

    def _count_recent_buy_fills_from_fill_store(self, cutoff_ts: int) -> int | None:
        fills_db = self.paths.fills_db
        try:
            if fills_db.exists():
                conn = sqlite3.connect(str(fills_db))
                try:
                    row = conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM fills
                        WHERE side = 'buy' AND ts_ms >= ?
                        """,
                        (cutoff_ts,),
                    ).fetchone()
                finally:
                    conn.close()
                return int(row[0] or 0) if row else 0
        except Exception:
            pass
        return None

    def _count_recent_buy_filled_orders(self, cutoff_ts: int) -> int | None:
        orders_db = self.paths.orders_db
        if not orders_db.exists():
            return None

        try:
            conn = sqlite3.connect(str(orders_db))
            try:
                row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM orders
                    WHERE side = 'buy'
                      AND state = 'FILLED'
                      AND COALESCE(NULLIF(updated_ts, 0), created_ts) >= ?
                    """,
                    (cutoff_ts,),
                ).fetchone()
            except sqlite3.OperationalError:
                row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM orders
                    WHERE side = 'buy'
                      AND state = 'FILLED'
                      AND created_ts >= ?
                    """,
                    (cutoff_ts,),
                ).fetchone()
            finally:
                conn.close()
            return int(row[0] or 0) if row else 0
        except Exception:
            return None

    def _count_recent_buy_fills(self, hours: int = 6) -> int:
        cutoff_ts = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)
        fills_count = self._count_recent_buy_fills_from_fill_store(cutoff_ts)
        if fills_count and fills_count > 0:
            return fills_count

        orders_count = self._count_recent_buy_filled_orders(cutoff_ts)
        if orders_count is not None:
            return orders_count
        return fills_count or 0

    def check_signal_no_trade(self) -> Optional[dict[str, Any]]:
        try:
            audits = self._load_recent_run_audits(limit=2)
            if len(audits) < 2:
                return None

            consecutive_no_trade = 0
            for data in audits:
                counts = data.get("counts", {}) or {}
                selected = int(counts.get("selected", 0) or 0)
                rebalance = int(counts.get("orders_rebalance", 0) or 0)
                if selected > 0 and rebalance == 0:
                    consecutive_no_trade += 1

            if consecutive_no_trade >= 2 and self._should_alert("signal_no_trade", cooldown_minutes=120):
                return {
                    "type": "signal_no_trade",
                    "level": "high",
                    "title": "存在信号但无成交",
                    "message": f"连续 {consecutive_no_trade} 轮有策略信号但未执行交易，可能被 deadband 或风控拦截。",
                    "suggestion": "检查决策归因面板，确认是否需要下调 deadband 或放宽执行门槛。",
                }
            return None
        except Exception as exc:
            print(f"[SmartAlert] check_signal_no_trade error: {exc}")
            return None

    def check_no_buy_in_market(self) -> Optional[dict[str, Any]]:
        try:
            audits = self._load_recent_run_audits(limit=6)
            if not audits:
                return None

            in_good_market = False
            for data in audits:
                regime = str(data.get("regime") or "").strip().upper()
                if regime in {"SIDEWAYS", "TRENDING"}:
                    in_good_market = True
                    break

            recent_buy_fills = self._count_recent_buy_fills(hours=6)
            if in_good_market and recent_buy_fills == 0 and self._should_alert("no_buy_in_market", cooldown_minutes=360):
                return {
                    "type": "no_buy_in_market",
                    "level": "medium",
                    "title": "行情正常但无买入",
                    "message": "最近 6 小时处于 Sideways/Trending 状态，但没有任何买入成交。",
                    "suggestion": "检查策略信号强度、deadband 与执行门槛是否过严。",
                }
            return None
        except Exception as exc:
            print(f"[SmartAlert] check_no_buy_in_market error: {exc}")
            return None

    def check_drawdown(self) -> Optional[dict[str, Any]]:
        try:
            reconcile_file = self.paths.reconcile_file
            if not reconcile_file.exists():
                return None

            with reconcile_file.open("r", encoding="utf-8") as f:
                data = json.load(f)

            drawdown_pct = data.get("local_snapshot", {}).get("drawdown_pct", 0)
            if drawdown_pct > 0.10 and self._should_alert("drawdown", cooldown_minutes=180):
                return {
                    "type": "drawdown",
                    "level": "high",
                    "title": "回撤超限警告",
                    "message": f"当前回撤 {drawdown_pct * 100:.1f}%，超过 10% 阈值。",
                    "suggestion": "检查持仓风险，必要时人工干预。",
                }
            return None
        except Exception as exc:
            print(f"[SmartAlert] check_drawdown error: {exc}")
            return None

    def check_ic_degradation(self) -> Optional[dict[str, Any]]:
        try:
            ic_file = self.paths.ic_file
            if not ic_file.exists():
                return None

            with ic_file.open("r", encoding="utf-8") as f:
                data = json.load(f)

            overall_ic = data.get("overall_tradable", {}).get("ic", {}).get("mean", 0)
            if overall_ic is None:
                overall_ic = 0

            if overall_ic < 0 and self._should_alert("ic_degradation", cooldown_minutes=720):
                return {
                    "type": "ic_degradation",
                    "level": "medium",
                    "title": "IC 因子失效",
                    "message": f"整体 IC 为负 ({overall_ic:.4f})，策略可能失效。",
                    "suggestion": "检查因子配置，必要时重新训练模型。",
                }
            return None
        except Exception as exc:
            print(f"[SmartAlert] check_ic_degradation error: {exc}")
            return None

    def check_kill_switch(self) -> Optional[dict[str, Any]]:
        try:
            kill_switch_file = self.paths.kill_switch_file
            if not kill_switch_file.exists():
                return None

            with kill_switch_file.open("r", encoding="utf-8") as f:
                data = json.load(f)

            if _kill_switch_enabled(data) and self._should_alert(
                "kill_switch", cooldown_minutes=30
            ):
                return {
                    "type": "kill_switch",
                    "level": "critical",
                    "title": "Kill Switch 已触发",
                    "message": "系统安全开关已启动，交易暂停。",
                    "suggestion": "立即检查系统状态和日志，确认安全后手动解除。",
                }
            return None
        except Exception as exc:
            print(f"[SmartAlert] check_kill_switch error: {exc}")
            return None

    def run_all_checks(self) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        checks = [
            self.check_signal_no_trade,
            self.check_no_buy_in_market,
            self.check_drawdown,
            self.check_ic_degradation,
            self.check_kill_switch,
        ]

        for check in checks:
            try:
                alert = check()
                if alert:
                    alerts.append(alert)
            except Exception as exc:
                print(f"[SmartAlert] Check error: {exc}")

        if alerts:
            self._save_state()
        return alerts


if __name__ == "__main__":
    engine = SmartAlertEngine()
    alerts = engine.run_all_checks()
    print(f"[SmartAlert] Found {len(alerts)} alerts")
    for alert in alerts:
        print(f"  - {alert['title']}: {alert['message']}")
