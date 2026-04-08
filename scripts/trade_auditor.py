#!/usr/bin/env python3
"""
Detailed trade auditor for the active V5 workspace.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@dataclass(frozen=True)
class AuditorPaths:
    workspace: Path
    reports_dir: Path
    runs_dir: Path
    orders_db: Path
    log_file: Path
    alert_file: Path
    kill_switch_file: Path
    reconcile_file: Path


def build_paths(workspace: Path | None = None) -> AuditorPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    reports_dir = root / "reports"
    logs_dir = root / "logs"
    return AuditorPaths(
        workspace=root,
        reports_dir=reports_dir,
        runs_dir=reports_dir / "runs",
        orders_db=reports_dir / "orders.sqlite",
        log_file=logs_dir / "trade_audit.log",
        alert_file=logs_dir / "trade_alert.json",
        kill_switch_file=reports_dir / "kill_switch.json",
        reconcile_file=reports_dir / "reconcile_status.json",
    )


DEFAULT_PATHS = build_paths()


def _normalize_kill_switch(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        if "enabled" in data or "active" in data:
            normalized = dict(data)
            if "enabled" not in normalized:
                normalized["enabled"] = bool(normalized.get("active"))
            return normalized

        nested = data.get("kill_switch")
        if isinstance(nested, dict):
            normalized = dict(nested)
            if "enabled" not in normalized:
                normalized["enabled"] = bool(normalized.get("active"))
            return normalized

        normalized = dict(data)
        normalized["enabled"] = bool(nested)
        return normalized

    if data is None:
        return {"enabled": False}

    return {"enabled": bool(data)}


def log(msg: str, paths: AuditorPaths = DEFAULT_PATHS) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    paths.log_file.parent.mkdir(parents=True, exist_ok=True)
    with paths.log_file.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def get_latest_orders(limit: int = 20, *, paths: AuditorPaths = DEFAULT_PATHS) -> list[tuple[Any, ...]]:
    if not paths.orders_db.exists():
        return []

    conn = sqlite3.connect(str(paths.orders_db))
    try:
        try:
            rows = conn.execute(
                """
                SELECT cl_ord_id, inst_id, side, state, intent, ord_id, last_error_code, last_error_msg
                FROM (
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
                )
                ORDER BY event_ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        except sqlite3.OperationalError:
            rows = conn.execute(
                """
                SELECT cl_ord_id, inst_id, side, state, intent, ord_id, last_error_code, last_error_msg
                FROM orders
                ORDER BY rowid DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    finally:
        conn.close()
    return rows


def analyze_orders(orders: list[tuple[Any, ...]]) -> dict[str, Any]:
    issues: list[str] = []
    buy_orders: list[tuple[Any, ...]] = []
    sell_orders: list[tuple[Any, ...]] = []
    rejected: list[dict[str, str]] = []

    for order in orders:
        _, inst_id, side, state, intent, _, err_code, err_msg = order
        symbol = str(inst_id).replace("-USDT", "/USDT")

        if side == "buy":
            buy_orders.append(order)
        else:
            sell_orders.append(order)

        if state == "REJECTED" or (err_code and str(err_code) != "0"):
            rejected.append(
                {
                    "symbol": symbol,
                    "side": str(side),
                    "intent": str(intent),
                    "error": str(err_msg or f"code:{err_code}"),
                }
            )

        if state not in ["FILLED", "CANCELED", "REJECTED", "LIVE", "OPEN"]:
            issues.append(f"异常状态: {symbol} {side} state={state}")

    return {
        "issues": issues,
        "buy_count": len(buy_orders),
        "sell_count": len(sell_orders),
        "rejected": rejected,
    }


def check_risk_limits(*, paths: AuditorPaths = DEFAULT_PATHS) -> list[str]:
    issues: list[str] = []

    if paths.kill_switch_file.exists():
        try:
            ks = _normalize_kill_switch(json.loads(paths.kill_switch_file.read_text(encoding="utf-8")))
            if ks.get("enabled"):
                issues.append(f"Kill Switch 已启用: {ks.get('reason', 'unknown')}")
        except Exception:
            pass

    if paths.reconcile_file.exists():
        try:
            rc = json.loads(paths.reconcile_file.read_text(encoding="utf-8"))
            if not rc.get("ok"):
                issues.append(f"对账异常: {rc.get('reason', 'unknown')}")
        except Exception:
            pass

    return issues


def run_audit(paths: AuditorPaths = DEFAULT_PATHS) -> dict[str, Any] | None:
    log("=" * 60, paths=paths)
    log("V5 交易审计启动", paths=paths)
    log("=" * 60, paths=paths)

    orders = get_latest_orders(30, paths=paths)
    if not orders:
        log("未找到订单记录", paths=paths)
        return None

    log(f"分析最近 {len(orders)} 笔订单", paths=paths)
    analysis = analyze_orders(orders)
    risk_issues = check_risk_limits(paths=paths)
    all_issues = analysis["issues"] + risk_issues

    log("\n交易统计:", paths=paths)
    log(f"  买入: {analysis['buy_count']} 笔", paths=paths)
    log(f"  卖出: {analysis['sell_count']} 笔", paths=paths)
    log(f"  拒绝: {len(analysis['rejected'])} 笔", paths=paths)

    if analysis["rejected"]:
        log("\n被拒绝订单:", paths=paths)
        for item in analysis["rejected"][:5]:
            log(f"  - {item['symbol']} {item['side']} ({item['intent']}): {item['error']}", paths=paths)

    report = {
        "timestamp": datetime.now().isoformat(),
        "issue_count": len(all_issues),
        "issues": all_issues,
        "rejected_orders": analysis["rejected"],
        "summary": {
            "buy_count": analysis["buy_count"],
            "sell_count": analysis["sell_count"],
            "rejected_count": len(analysis["rejected"]),
        },
    }

    if all_issues:
        log(f"\n发现 {len(all_issues)} 个问题", paths=paths)
        for issue in all_issues:
            log(f"  {issue}", paths=paths)
        paths.alert_file.parent.mkdir(parents=True, exist_ok=True)
        paths.alert_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        log(f"\n告警已保存至: {paths.alert_file}", paths=paths)
    else:
        log("\n审计通过，未发现异常", paths=paths)
        if paths.alert_file.exists():
            paths.alert_file.unlink()

    log("=" * 60, paths=paths)
    log("审计完成", paths=paths)
    log("=" * 60, paths=paths)
    return report


def main() -> int:
    run_audit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
