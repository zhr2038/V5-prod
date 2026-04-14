#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_path
from src.execution.fill_store import derive_runtime_named_artifact_path, derive_runtime_reports_dir


@dataclass(frozen=True)
class ConsistencyPaths:
    workspace: Path
    reports_dir: Path
    orders_db: Path


def build_paths(workspace: Path | None = None) -> ConsistencyPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    try:
        cfg = load_runtime_config(project_root=root)
        execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
        orders_db = Path(
            resolve_runtime_path(
                execution_cfg.get("order_store_path") if isinstance(execution_cfg, dict) else None,
                default="reports/orders.sqlite",
                project_root=root,
            )
        ).resolve()
        reports_dir = derive_runtime_reports_dir(orders_db).resolve()
    except Exception:
        reports_dir = (root / "reports").resolve()
        orders_db = (reports_dir / "orders.sqlite").resolve()

    return ConsistencyPaths(
        workspace=root,
        reports_dir=reports_dir,
        orders_db=orders_db,
    )


class BacktestLiveConsistencyChecker:
    """Compare backtest assumptions against recent live trading data."""

    def __init__(self, workspace: Path | None = None):
        self.paths = build_paths(workspace)
        self.results = {
            "slippage_diff": [],
            "fill_rate_diff": [],
            "cost_diff": [],
            "recommendations": [],
        }

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    @staticmethod
    def _order_event_ts_expr() -> str:
        return "COALESCE(NULLIF(updated_ts, 0), created_ts)"

    @staticmethod
    def _split_inst_id_base_quote(inst_id: str) -> tuple[str, str]:
        normalized = str(inst_id or "").replace("/", "-")
        parts = [part.strip().upper() for part in normalized.split("-") if part.strip()]
        if len(parts) >= 2:
            return parts[0], parts[1]
        return normalized.upper(), "USDT"

    @classmethod
    def _signed_fee_usdt_from_fee_fields(cls, inst_id: str, px: Any, fee_amount: Any, fee_ccy: Any = None) -> float:
        try:
            fee_val = float(fee_amount or 0.0)
        except Exception:
            return 0.0

        fee_ccy_norm = str(fee_ccy or "").strip().upper()
        if not fee_ccy_norm:
            return fee_val

        base_ccy, quote_ccy = cls._split_inst_id_base_quote(inst_id)
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

    @classmethod
    def _fee_cost_usdt_from_order_fee(cls, inst_id: str, avg_px: Any, raw_fee: Any) -> float:
        raw = str(raw_fee or "").strip()
        if not raw:
            return 0.0

        try:
            numeric_fee = float(raw)
        except Exception:
            numeric_fee = None
        if numeric_fee is not None:
            return abs(cls._signed_fee_usdt_from_fee_fields(inst_id, avg_px, numeric_fee))

        try:
            fee_map = json.loads(raw)
        except Exception:
            return 0.0
        if not isinstance(fee_map, dict):
            return 0.0

        total_fee_usdt = 0.0
        for ccy, value in fee_map.items():
            total_fee_usdt += cls._signed_fee_usdt_from_fee_fields(inst_id, avg_px, value, ccy)
        return abs(total_fee_usdt)

    def _load_order_state_counts(self, days=7):
        orders_db = self.paths.orders_db
        if not orders_db.exists():
            return {}

        conn = sqlite3.connect(str(orders_db))
        cursor = conn.cursor()
        cutoff = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

        try:
            try:
                cursor.execute(
                    f"""
                    SELECT state, COUNT(*)
                    FROM orders
                    WHERE {self._order_event_ts_expr()} > ?
                    GROUP BY state
                    """,
                    (cutoff,),
                )
            except sqlite3.OperationalError:
                cursor.execute(
                    """
                    SELECT state, COUNT(*)
                    FROM orders
                    WHERE created_ts > ?
                    GROUP BY state
                    """,
                    (cutoff,),
                )
            return dict(cursor.fetchall())
        finally:
            conn.close()

    def load_live_trades(self, days=7):
        """Load recently filled live orders for cost/slippage checks."""
        orders_db = self.paths.orders_db
        if not orders_db.exists():
            return []

        conn = sqlite3.connect(str(orders_db))
        cursor = conn.cursor()
        cutoff = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

        try:
            try:
                cursor.execute(
                    f"""
                    SELECT
                        inst_id,
                        side,
                        px,
                        avg_px,
                        sz,
                        acc_fill_sz,
                        fee,
                        state,
                        {self._order_event_ts_expr()} AS event_ts
                    FROM orders
                    WHERE {self._order_event_ts_expr()} > ? AND state = 'FILLED'
                    """,
                    (cutoff,),
                )
            except sqlite3.OperationalError:
                cursor.execute(
                    """
                    SELECT inst_id, side, px, avg_px, sz, acc_fill_sz, fee, state, created_ts
                    FROM orders
                    WHERE created_ts > ? AND state = 'FILLED'
                    """,
                    (cutoff,),
                )

            trades = []
            for row in cursor.fetchall():
                trades.append(
                    {
                        "symbol": row[0],
                        "side": row[1],
                        "order_px": row[2],
                        "fill_px": row[3],
                        "order_sz": row[4],
                        "fill_sz": row[5],
                        "fee_usdt": self._fee_cost_usdt_from_order_fee(row[0], row[3], row[6]),
                        "ts": datetime.fromtimestamp((row[8] or 0) / 1000),
                    }
                )
            return trades
        finally:
            conn.close()

    def calculate_live_slippage(self, trades):
        slippages = []
        for trade in trades:
            if trade["order_px"] and trade["fill_px"]:
                slippage = abs(float(trade["fill_px"]) - float(trade["order_px"])) / float(trade["order_px"])
                slippages.append(
                    {
                        "symbol": trade["symbol"],
                        "slippage": slippage,
                        "side": trade["side"],
                        "ts": trade["ts"],
                    }
                )
        return slippages

    def load_backtest_config(self):
        cost_dir = derive_runtime_named_artifact_path(self.paths.orders_db, "cost_stats_real", "")
        if not cost_dir.exists():
            cost_dir = derive_runtime_named_artifact_path(self.paths.orders_db, "cost_stats", "")
        cost_files = list(cost_dir.glob("*.json"))
        if not cost_files:
            return None

        latest = max(cost_files, key=lambda x: x.stat().st_mtime)
        with open(latest, encoding="utf-8") as f:
            return json.load(f)

    def compare_cost_models(self, live_trades, backtest_cost):
        print("\n" + "=" * 70)
        print("Cost Model Comparison")
        print("=" * 70)

        if not live_trades:
            print("No live trade data available.")
            return

        if not backtest_cost:
            print("No backtest cost data available.")
            return

        total_fee = sum(float(t.get("fee_usdt", 0) or 0) for t in live_trades)
        total_notional = sum(float(t.get("fill_px", 0) or 0) * float(t.get("fill_sz", 0) or 0) for t in live_trades)
        live_cost_bps = (total_fee / total_notional) * 10000 if total_notional > 0 else 0
        backtest_cost_bps = backtest_cost.get("avg_cost_bps", 0)

        print(f"\nLive avg cost: {live_cost_bps:.2f} bps")
        print(f"Backtest assumed cost: {backtest_cost_bps:.2f} bps")

        diff = live_cost_bps - backtest_cost_bps
        diff_pct = (diff / backtest_cost_bps * 100) if backtest_cost_bps > 0 else 0

        if abs(diff_pct) > 20:
            print(f"Warning: cost gap {diff:+.2f} bps ({diff_pct:+.1f}%)")
            self.results["recommendations"].append(
                f"Adjust backtest cost model from {backtest_cost_bps:.0f}bps toward {live_cost_bps:.0f}bps"
            )
        else:
            print(f"Cost model aligned: {diff:+.2f} bps ({diff_pct:+.1f}%)")

    def analyze_fill_rates(self, days=7):
        print("\n" + "=" * 70)
        print("Fill Rate Analysis")
        print("=" * 70)

        orders_db = self.paths.orders_db
        if not orders_db.exists():
            print("No orders database found.")
            return

        states = self._load_order_state_counts(days=days)
        total = sum(states.values())
        filled = states.get("FILLED", 0)
        rejected = states.get("REJECTED", 0)

        if total > 0:
            fill_rate = filled / total * 100
            reject_rate = rejected / total * 100

            print(f"\nRecent {days}d order stats:")
            print(f"  total: {total}")
            print(f"  filled: {filled} ({fill_rate:.1f}%)")
            print(f"  rejected: {rejected} ({reject_rate:.1f}%)")

            if fill_rate < 50:
                print("Warning: low fill rate, check dust/min-notional settings.")
                self.results["recommendations"].append(
                    f"Fill rate is only {fill_rate:.1f}%; review minimum trade size thresholds"
                )
            else:
                print("Fill rate looks normal.")

    def generate_report(self):
        print("\n" + "=" * 70)
        print("Consistency Report")
        print("=" * 70)

        if self.results["recommendations"]:
            print("\nRecommendations:")
            for i, rec in enumerate(self.results["recommendations"], 1):
                print(f"  {i}. {rec}")
        else:
            print("\nBacktest vs live consistency looks good.")

        report_file = self.paths.reports_dir / f"consistency_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(),
                    "results": self.results,
                },
                f,
                indent=2,
                default=str,
            )

        print(f"\nSaved report: {report_file}")

    def run(self):
        self.log("Starting backtest/live consistency check")
        live_trades = self.load_live_trades(days=7)
        backtest_cost = self.load_backtest_config()

        print(f"Loaded live trades: {len(live_trades)}")
        self.compare_cost_models(live_trades, backtest_cost)
        self.analyze_fill_rates(days=7)
        self.generate_report()

        print("\n" + "=" * 70)
        print("Check complete")
        print("=" * 70)


def main():
    checker = BacktestLiveConsistencyChecker()
    checker.run()


if __name__ == "__main__":
    main()
