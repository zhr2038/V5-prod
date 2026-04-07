#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ConsistencyPaths:
    workspace: Path
    reports_dir: Path


def build_paths(workspace: Path | None = None) -> ConsistencyPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    return ConsistencyPaths(
        workspace=root,
        reports_dir=root / "reports",
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

    def _load_order_state_counts(self, days=7):
        orders_db = self.paths.reports_dir / "orders.sqlite"
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
        orders_db = self.paths.reports_dir / "orders.sqlite"
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
                        "fee": row[6] or 0,
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
        cost_files = list(self.paths.reports_dir.glob("cost_stats_real/*.json"))
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

        total_fee = sum(float(t["fee"] or 0) for t in live_trades)
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

        orders_db = self.paths.reports_dir / "orders.sqlite"
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
