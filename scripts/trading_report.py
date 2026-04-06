#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ReportPaths:
    workspace: Path
    reports_dir: Path
    runs_dir: Path
    orders_db: Path


def build_paths(workspace: Path | None = None) -> ReportPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    reports_dir = root / "reports"
    return ReportPaths(
        workspace=root,
        reports_dir=reports_dir,
        runs_dir=reports_dir / "runs",
        orders_db=reports_dir / "orders.sqlite",
    )


REPORT_PATHS = build_paths()
REPORTS_DIR = REPORT_PATHS.reports_dir
ORDERS_DB = REPORT_PATHS.orders_db


def _split_inst_id_base_quote(inst_id: str) -> tuple[str, str]:
    inst = str(inst_id or "").upper()
    if "-" in inst:
        return tuple(inst.split("-", 1))
    if "/" in inst:
        return tuple(inst.split("/", 1))
    return inst, "USDT"


def _signed_fee_usdt_from_fee_fields(inst_id: str, px: Any, fee_amount: Any, fee_ccy: Any = None) -> float:
    try:
        fee_val = float(fee_amount or 0.0)
    except Exception:
        return 0.0

    fee_ccy_norm = str(fee_ccy or "").strip().upper()
    if not fee_ccy_norm:
        return fee_val

    base_ccy, quote_ccy = _split_inst_id_base_quote(inst_id)
    if fee_ccy_norm == quote_ccy:
        return fee_val
    if fee_ccy_norm != base_ccy:
        return 0.0

    try:
        px_val = float(px or 0.0)
    except Exception:
        return 0.0
    if px_val <= 0:
        return 0.0
    return fee_val * px_val


def _signed_fee_usdt_from_order_fee(inst_id: str, avg_px: Any, raw_fee: Any) -> float:
    raw = str(raw_fee or "").strip()
    if not raw:
        return 0.0

    try:
        numeric_fee = float(raw)
    except Exception:
        numeric_fee = None
    if numeric_fee is not None:
        return _signed_fee_usdt_from_fee_fields(inst_id, avg_px, numeric_fee)

    try:
        fee_map = json.loads(raw)
    except Exception:
        return 0.0
    if not isinstance(fee_map, dict):
        return 0.0

    total_fee_usdt = 0.0
    for ccy, value in fee_map.items():
        total_fee_usdt += _signed_fee_usdt_from_fee_fields(inst_id, avg_px, value, ccy)
    return total_fee_usdt


def _parse_equity_ts(raw_value: Any) -> datetime | None:
    raw = str(raw_value or "").strip()
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is not None:
        return ts.astimezone().replace(tzinfo=None)
    return ts


class TradingReportGenerator:
    """Generate daily and weekly trading summaries from workspace reports."""

    def __init__(self, paths: ReportPaths | None = None) -> None:
        self.paths = paths or build_paths()

    def log(self, msg: str = "") -> None:
        print(msg)

    def load_equity_data(self, days: int = 7) -> list[dict[str, Any]]:
        points: list[dict[str, Any]] = []
        cutoff = datetime.now() - timedelta(days=days)

        if self.paths.runs_dir.exists():
            for run_dir in self.paths.runs_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                equity_file = run_dir / "equity.jsonl"
                if not equity_file.exists():
                    continue
                try:
                    with equity_file.open(encoding="utf-8") as handle:
                        for line in handle:
                            try:
                                data = json.loads(line)
                            except Exception:
                                continue
                            ts = _parse_equity_ts(data.get("ts"))
                            if ts is None or ts <= cutoff:
                                continue
                            points.append(
                                {
                                    "ts": ts,
                                    "equity": float(data.get("equity", 0) or 0),
                                    "cash": float(data.get("cash", 0) or 0),
                                    "positions_value": float(data.get("positions_value", 0) or 0),
                                }
                            )
                except OSError:
                    continue

        points.sort(key=lambda item: item["ts"])
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for point in points:
            key = point["ts"].strftime("%Y-%m-%d %H:%M")
            if key in seen:
                continue
            seen.add(key)
            unique.append(point)
        return unique

    def load_trade_data(self, days: int = 7) -> list[dict[str, Any]]:
        if not self.paths.orders_db.exists():
            return []

        cutoff_ts = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        with sqlite3.connect(str(self.paths.orders_db)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT inst_id, side, state, notional_usdt, fee, avg_px, created_ts
                FROM orders
                WHERE created_ts > ? AND state = 'FILLED'
                ORDER BY created_ts DESC
                """,
                (cutoff_ts,),
            )
            rows = cursor.fetchall()

        trades: list[dict[str, Any]] = []
        for inst_id, side, state, notional_usdt, fee, avg_px, created_ts in rows:
            trades.append(
                {
                    "symbol": str(inst_id or "").replace("-USDT", "").replace("/USDT", ""),
                    "side": side,
                    "state": state,
                    "notional": float(notional_usdt or 0),
                    "fee": _signed_fee_usdt_from_order_fee(str(inst_id or ""), avg_px, fee),
                    "ts": datetime.fromtimestamp(float(created_ts or 0) / 1000),
                }
            )
        return trades

    def load_regime_history(self, days: int = 7) -> list[dict[str, Any]]:
        regimes: list[dict[str, Any]] = []
        cutoff = datetime.now() - timedelta(days=days)

        if self.paths.runs_dir.exists():
            for run_dir in self.paths.runs_dir.iterdir():
                audit_file = run_dir / "decision_audit.json"
                if not audit_file.exists():
                    continue
                try:
                    modified_at = datetime.fromtimestamp(audit_file.stat().st_mtime)
                    if modified_at <= cutoff:
                        continue
                    data = json.loads(audit_file.read_text(encoding="utf-8"))
                except Exception:
                    continue
                regimes.append(
                    {
                        "ts": modified_at,
                        "regime": data.get("regime", "Unknown"),
                        "multiplier": float(data.get("regime_multiplier", 0.6) or 0.6),
                    }
                )

        regimes.sort(key=lambda item: item["ts"])
        return regimes

    def generate_daily_report(self) -> None:
        self.log("=" * 60)
        self.log("V5 交易日报")
        self.log("=" * 60)
        self.log(f"报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        self.log("报告周期: 最近 24 小时")
        self.log()

        equity_data = self.load_equity_data(days=1)
        if equity_data:
            start_eq = equity_data[0]["equity"]
            end_eq = equity_data[-1]["equity"]
            change = end_eq - start_eq
            change_pct = (change / start_eq * 100) if start_eq > 0 else 0.0

            self.log("权益变化")
            self.log(f"  起始: ${start_eq:.2f}")
            self.log(f"  结束: ${end_eq:.2f}")
            self.log(f"  变化: ${change:+.2f} ({change_pct:+.2f}%)")
            self.log()

        trades = self.load_trade_data(days=1)
        if trades:
            buy_count = sum(1 for trade in trades if trade["side"] == "buy")
            sell_count = sum(1 for trade in trades if trade["side"] == "sell")
            buy_value = sum(trade["notional"] for trade in trades if trade["side"] == "buy")
            sell_value = sum(trade["notional"] for trade in trades if trade["side"] == "sell")
            total_fee = sum(trade["fee"] for trade in trades)

            self.log("交易统计")
            self.log(f"  买入: {buy_count} 笔 ${buy_value:.2f}")
            self.log(f"  卖出: {sell_count} 笔 ${sell_value:.2f}")
            self.log(f"  手续费: ${total_fee:.4f}")
            self.log()

            self.log("最近 5 笔交易:")
            for trade in trades[:5]:
                self.log(
                    f"  {trade['ts'].strftime('%H:%M')} {trade['side']:4} "
                    f"{trade['symbol']:8} ${trade['notional']:.2f}"
                )
            self.log()
        else:
            self.log("今日无成交")
            self.log()

        regimes = self.load_regime_history(days=1)
        if regimes:
            current = regimes[-1]
            self.log("市场状态")
            self.log(f"  当前: {current['regime']}")
            self.log(f"  乘数: {current['multiplier']:.2f}x")
            self.log()

        self.log("=" * 60)

    def generate_weekly_report(self) -> None:
        self.log("=" * 60)
        self.log("V5 交易周报")
        self.log("=" * 60)
        self.log(f"报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        self.log("报告周期: 最近 7 天")
        self.log()

        equity_data = self.load_equity_data(days=7)
        if equity_data:
            start_eq = equity_data[0]["equity"]
            end_eq = equity_data[-1]["equity"]
            peak = max(point["equity"] for point in equity_data)
            trough = min(point["equity"] for point in equity_data)
            change = end_eq - start_eq
            change_pct = (change / start_eq * 100) if start_eq > 0 else 0.0
            max_dd = (trough - peak) / peak if peak > 0 else 0.0

            self.log("权益表现")
            self.log(f"  周初: ${start_eq:.2f}")
            self.log(f"  周末: ${end_eq:.2f}")
            self.log(f"  变化: ${change:+.2f} ({change_pct:+.2f}%)")
            self.log(f"  最高: ${peak:.2f}")
            self.log(f"  最低: ${trough:.2f}")
            self.log(f"  最大回撤: {max_dd:.1%}")
            self.log()

        trades = self.load_trade_data(days=7)
        if trades:
            buy_count = sum(1 for trade in trades if trade["side"] == "buy")
            sell_count = sum(1 for trade in trades if trade["side"] == "sell")
            buy_value = sum(trade["notional"] for trade in trades if trade["side"] == "buy")
            sell_value = sum(trade["notional"] for trade in trades if trade["side"] == "sell")
            total_fee = sum(trade["fee"] for trade in trades)

            symbol_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
            for trade in trades:
                symbol_stats[trade["symbol"]][trade["side"]] += trade["notional"]

            self.log("交易统计")
            self.log(f"  总买入: {buy_count} 笔 ${buy_value:.2f}")
            self.log(f"  总卖出: {sell_count} 笔 ${sell_value:.2f}")
            self.log(f"  总手续费: ${total_fee:.4f}")
            self.log()

            self.log("活跃币种（按交易额）:")
            for symbol, stats in sorted(
                symbol_stats.items(),
                key=lambda item: item[1]["buy"] + item[1]["sell"],
                reverse=True,
            )[:5]:
                self.log(f"  {symbol:8} 买 ${stats['buy']:8.2f} 卖 ${stats['sell']:8.2f}")
            self.log()

        regimes = self.load_regime_history(days=7)
        if regimes:
            regime_counts: dict[str, int] = defaultdict(int)
            for regime in regimes:
                regime_counts[regime["regime"]] += 1

            total = len(regimes)
            self.log(f"市场状态分布（共 {total} 次检测）")
            for regime, count in sorted(regime_counts.items(), key=lambda item: item[1], reverse=True):
                pct = count / total * 100
                self.log(f"  {regime:12} {count:3} 次 ({pct:5.1f}%)")
            self.log()

        self.log("=" * 60)

    def run(self, report_type: str = "daily") -> None:
        if report_type == "daily":
            self.generate_daily_report()
        elif report_type == "weekly":
            self.generate_weekly_report()
        else:
            self.generate_daily_report()
            self.generate_weekly_report()


def main() -> None:
    parser = argparse.ArgumentParser(description="V5 交易报告生成")
    parser.add_argument("--type", choices=["daily", "weekly", "all"], default="daily", help="报告类型")
    args = parser.parse_args()

    generator = TradingReportGenerator()
    generator.run(report_type=args.type)


if __name__ == "__main__":
    main()
