#!/usr/bin/env python3
"""
Smart trade auditor V2 for the active V5 workspace.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@dataclass(frozen=True)
class AuditorPaths:
    workspace: Path
    reports_dir: Path
    orders_db: Path
    log_file: Path
    alert_file: Path


def build_paths(workspace: Path | None = None) -> AuditorPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    reports_dir = root / "reports"
    logs_dir = root / "logs"
    return AuditorPaths(
        workspace=root,
        reports_dir=reports_dir,
        orders_db=reports_dir / "orders.sqlite",
        log_file=logs_dir / "trade_audit_v2.log",
        alert_file=logs_dir / "trade_alert_v2.json",
    )


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_kill_switch(data: Any) -> dict[str, Any]:
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


class SmartTradeAuditor:
    def __init__(self, workspace: Path | None = None) -> None:
        self.paths = build_paths(workspace)
        self.issues: list[dict[str, Any]] = []
        self.warnings: list[dict[str, Any]] = []
        self.insights: list[dict[str, Any]] = []

    def log(self, msg: str, level: str = "INFO") -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{level}] {msg}"
        print(line)
        self.paths.log_file.parent.mkdir(parents=True, exist_ok=True)
        with self.paths.log_file.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    def get_orders_in_window(self, minutes: int = 65) -> list[tuple[Any, ...]]:
        if not self.paths.orders_db.exists():
            return []

        conn = sqlite3.connect(str(self.paths.orders_db))
        try:
            now = datetime.now()
            end_ts = int(now.timestamp() * 1000)
            start_ts = int((now - timedelta(minutes=minutes)).timestamp() * 1000)
            try:
                rows = conn.execute(
                    """
                    SELECT
                        cl_ord_id,
                        inst_id,
                        side,
                        state,
                        intent,
                        ord_id,
                        last_error_code,
                        last_error_msg,
                        COALESCE(NULLIF(updated_ts, 0), created_ts) AS event_ts
                    FROM orders
                    WHERE COALESCE(NULLIF(updated_ts, 0), created_ts) BETWEEN ? AND ?
                    ORDER BY event_ts DESC
                    """,
                    (start_ts, end_ts),
                ).fetchall()
            except sqlite3.OperationalError:
                rows = conn.execute(
                    """
                    SELECT cl_ord_id, inst_id, side, state, intent, ord_id,
                           last_error_code, last_error_msg, created_ts
                    FROM orders
                    WHERE created_ts BETWEEN ? AND ?
                    ORDER BY created_ts DESC
                    """,
                    (start_ts, end_ts),
                ).fetchall()
        finally:
            conn.close()
        return rows

    def analyze_orders(self, orders: list[tuple[Any, ...]]) -> dict[str, list[tuple[Any, ...]]]:
        buy_filled: list[tuple[Any, ...]] = []
        sell_filled: list[tuple[Any, ...]] = []
        buy_rejected: list[tuple[Any, ...]] = []
        sell_rejected: list[tuple[Any, ...]] = []

        for order in orders:
            _, _, side, state, *_ = order
            if side == "buy":
                if state == "FILLED":
                    buy_filled.append(order)
                elif state == "REJECTED":
                    buy_rejected.append(order)
            elif side == "sell":
                if state == "FILLED":
                    sell_filled.append(order)
                elif state == "REJECTED":
                    sell_rejected.append(order)

        return {
            "buy_filled": buy_filled,
            "sell_filled": sell_filled,
            "buy_rejected": buy_rejected,
            "sell_rejected": sell_rejected,
        }

    def check_market_regime(self) -> str:
        try:
            runs_dir = self.paths.reports_dir / "runs"
            if runs_dir.exists():
                run_dirs = [
                    d
                    for d in runs_dir.iterdir()
                    if d.is_dir() and (d / "decision_audit.json").exists()
                ]
                run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                if run_dirs:
                    data = json.loads((run_dirs[0] / "decision_audit.json").read_text(encoding="utf-8"))
                    details = data.get("regime_details") or {}
                    regime = details.get("final_state") or data.get("regime")
                    if regime:
                        return str(regime)
        except Exception:
            pass

        possible_paths = [
            self.paths.reports_dir / "regime_state.json",
            self.paths.reports_dir / "regime.json",
        ]
        for regime_file in possible_paths:
            if not regime_file.exists():
                continue
            try:
                data = json.loads(regime_file.read_text(encoding="utf-8"))
                regime = data.get("regime") or data.get("state") or data.get("current_regime")
                if regime:
                    return str(regime)
            except Exception:
                continue

        return "Unknown"

    def validate_logic(self, analysis: dict[str, list[tuple[Any, ...]]], regime: str) -> None:
        buy_filled = analysis["buy_filled"]
        sell_filled = analysis["sell_filled"]
        regime_norm = str(regime or "").upper().replace("-", "_")

        if regime_norm == "RISK_OFF" and buy_filled:
            self.warnings.append(
                {
                    "level": "HIGH",
                    "type": "regime_conflict",
                    "message": f"Risk-Off状态下出现{len(buy_filled)}笔买入",
                    "details": [f"{order[1]} ({order[4]})" for order in buy_filled],
                    "suggestion": "检查是否为 REBALANCE 或运行态配置偏移",
                }
            )

        if len(sell_filled) > 5 and not buy_filled:
            self.insights.append(
                {
                    "level": "INFO",
                    "type": "mass_liquidation",
                    "message": f"纯卖出模式: {len(sell_filled)}笔卖出，0笔买入",
                    "interpretation": "可能触发止损或 Risk-Off 减仓",
                }
            )

        if buy_filled and sell_filled:
            self.insights.append(
                {
                    "level": "INFO",
                    "type": "active_trading",
                    "message": f"双向交易: {len(buy_filled)}笔买入，{len(sell_filled)}笔卖出",
                    "interpretation": "市场震荡，策略在调仓",
                }
            )

        total_rejected = len(analysis["buy_rejected"]) + len(analysis["sell_rejected"])
        if total_rejected >= 15:
            all_rejected = analysis["buy_rejected"] + analysis["sell_rejected"]
            dust_skip_count = sum(
                1
                for order in all_rejected
                if "dust" in str(order[7]).lower() or "51020" in str(order[6])
            )
            if dust_skip_count >= 10:
                self.insights.append(
                    {
                        "level": "INFO",
                        "type": "dust_cleanup",
                        "message": f"dust/minSz 拦截: {dust_skip_count}笔",
                        "interpretation": "多数属于交易所最小下单限制或微量残仓保护",
                    }
                )

    def check_risk_controls(self) -> list[dict[str, str]]:
        issues: list[dict[str, str]] = []
        kill_switch = self.paths.reports_dir / "kill_switch.json"
        if kill_switch.exists():
            try:
                ks = _normalize_kill_switch(json.loads(kill_switch.read_text(encoding="utf-8")))
                if _to_bool(ks.get("enabled")):
                    issues.append(
                        {
                            "level": "CRITICAL",
                            "message": f"Kill Switch 已启用: {ks.get('reason', 'unknown')}",
                        }
                    )
            except Exception:
                pass

        reconcile = self.paths.reports_dir / "reconcile_status.json"
        if reconcile.exists():
            try:
                rc = json.loads(reconcile.read_text(encoding="utf-8"))
                if not _to_bool(rc.get("ok")):
                    issues.append(
                        {
                            "level": "WARNING",
                            "message": f"对账异常: {rc.get('reason', 'unknown')}",
                        }
                    )
            except Exception:
                pass
        return issues

    def generate_report(self, analysis: dict[str, list[tuple[Any, ...]]], regime: str) -> dict[str, Any]:
        return {
            "timestamp": datetime.now().isoformat(),
            "market_regime": regime,
            "summary": {
                "buy_filled": len(analysis["buy_filled"]),
                "sell_filled": len(analysis["sell_filled"]),
                "buy_rejected": len(analysis["buy_rejected"]),
                "sell_rejected": len(analysis["sell_rejected"]),
                "total": sum(len(v) for v in analysis.values()),
            },
            "issues": self.issues,
            "warnings": self.warnings,
            "insights": self.insights,
        }

    def print_report(self, report: dict[str, Any]) -> None:
        self.log("=" * 70)
        self.log("V5 智能交易审计报告 V2")
        self.log("=" * 70)
        self.log(f"\n市场状态: {report['market_regime']}")

        summary = report["summary"]
        self.log("\n交易统计:")
        self.log(f"  买入成交: {summary['buy_filled']} 笔")
        self.log(f"  卖出成交: {summary['sell_filled']} 笔")
        self.log(f"  买入拒绝: {summary['buy_rejected']} 笔")
        self.log(f"  卖出拒绝: {summary['sell_rejected']} 笔")

        if self.insights:
            self.log("\n智能分析:")
            for insight in self.insights:
                self.log(f"  {insight['message']}")
                if "interpretation" in insight:
                    self.log(f"     -> {insight['interpretation']}")

        if self.warnings:
            self.log(f"\n警告 ({len(self.warnings)}):")
            for warning in self.warnings:
                self.log(f"  [{warning['level']}] {warning['message']}")
                if "suggestion" in warning:
                    self.log(f"     建议: {warning['suggestion']}")

        if self.issues:
            self.log(f"\n问题 ({len(self.issues)}):")
            for issue in self.issues:
                self.log(f"  [{issue['level']}] {issue['message']}")

        if not self.warnings and not self.issues:
            self.log("\n审计通过，无异常")
        elif not self.issues:
            self.log("\n审计完成，有警告但无严重问题")
        else:
            self.log("\n审计发现严重问题，需要人工介入")

        self.log("=" * 70)

    def run(self) -> dict[str, Any] | None:
        self.log("智能交易审计启动...")
        orders = self.get_orders_in_window(minutes=65)
        if not orders:
            self.log("时间窗口内无交易记录")
            return None

        self.log(f"分析最近窗口内 {len(orders)} 笔订单")
        analysis = self.analyze_orders(orders)
        regime = self.check_market_regime()
        self.validate_logic(analysis, regime)
        self.issues.extend(self.check_risk_controls())
        report = self.generate_report(analysis, regime)
        self.print_report(report)

        self.paths.alert_file.parent.mkdir(parents=True, exist_ok=True)
        self.paths.alert_file.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return report


def main() -> int:
    SmartTradeAuditor().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
