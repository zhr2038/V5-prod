#!/usr/bin/env python3
"""
Batch-mark dust positions in the local position store.

This operator recovery tool tags tiny positions as dust, writes a dust config,
and emits a cleanup report under the repository reports directory.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = PROJECT_ROOT / "reports"
POSITIONS_DB = REPORTS_DIR / "positions.sqlite"
ORDERS_DB = REPORTS_DIR / "orders.sqlite"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_path
from src.execution.fill_store import (
    derive_position_store_path,
    derive_runtime_named_json_path,
    derive_runtime_reports_dir,
)

DUST_CRITERIA = {
    "max_qty": 0.1,
    "max_value_usdt": 0.5,
    "max_price": 0.1,
}

DUST_SYMBOLS = {"PROMPT", "SPACE", "KITE", "WLFI", "MERL", "J", "PEPE", "XAUT"}


@dataclass
class DustCleanerPaths:
    reports_dir: Path
    orders_db: Path
    positions_db: Path
    dust_config_path: Path


def _derive_cleanup_report_path(order_store_path: Path, timestamp: str) -> Path:
    path = Path(order_store_path)
    if path.name == "orders.sqlite":
        return path.with_name(f"dust_cleanup_{timestamp}.json")
    if "orders" in path.stem:
        report_name = path.stem.replace("orders", "dust_cleanup", 1)
        return path.with_name(f"{report_name}_{timestamp}.json")
    return path.with_name(f"dust_cleanup_{timestamp}.json")


def _resolve_default_paths(project_root: Path | None = None) -> DustCleanerPaths:
    root = Path(project_root or PROJECT_ROOT).resolve()
    cfg = load_runtime_config(project_root=root)
    config_path = (root / "configs" / "live_prod.yaml").resolve()
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {config_path}")
    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {config_path}")
    orders_db = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=root,
        )
    ).resolve()
    return DustCleanerPaths(
        reports_dir=derive_runtime_reports_dir(orders_db).resolve(),
        orders_db=orders_db,
        positions_db=derive_position_store_path(orders_db).resolve(),
        dust_config_path=derive_runtime_named_json_path(orders_db, "dust_config").resolve(),
    )


def _resolve_manual_reports_dir_paths(reports_dir: Path) -> DustCleanerPaths:
    resolved_reports_dir = Path(reports_dir).resolve()
    default_orders = (resolved_reports_dir / "orders.sqlite").resolve()
    orders_db = default_orders
    if not default_orders.exists():
        candidates = sorted(
            {
                candidate.resolve()
                for candidate in resolved_reports_dir.glob("*orders*.sqlite")
                if candidate.is_file()
            }
        )
        if len(candidates) == 1:
            orders_db = candidates[0]

    return DustCleanerPaths(
        reports_dir=resolved_reports_dir,
        orders_db=orders_db,
        positions_db=derive_position_store_path(orders_db).resolve(),
        dust_config_path=derive_runtime_named_json_path(orders_db, "dust_config").resolve(),
    )


class DustCleaner:
    def __init__(self, reports_dir: Path | None = None):
        if reports_dir is not None:
            runtime_paths = _resolve_manual_reports_dir_paths(reports_dir)
            self.reports_dir = runtime_paths.reports_dir
            self.positions_db = runtime_paths.positions_db
            self.orders_db = runtime_paths.orders_db
            self.dust_config_path = runtime_paths.dust_config_path
        else:
            runtime_paths = _resolve_default_paths()
            self.reports_dir = runtime_paths.reports_dir
            self.positions_db = runtime_paths.positions_db
            self.orders_db = runtime_paths.orders_db
            self.dust_config_path = runtime_paths.dust_config_path
        self.stats = {"marked": 0, "already_excluded": 0, "errors": 0}
        self.dust_list: list[dict[str, Any]] = []

    def log(self, msg: str) -> None:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def get_positions(self) -> list[dict[str, Any]]:
        if not self.positions_db.exists():
            return []

        conn = sqlite3.connect(str(self.positions_db))
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT symbol, qty, avg_px, last_mark_px FROM positions")
            positions = []
            for row in cursor.fetchall():
                symbol = row[0]
                qty = float(row[1] or 0)
                avg_px = float(row[2] or 0)
                last_px = float(row[3] or 0)
                value = qty * last_px if last_px > 0 else qty * avg_px
                positions.append(
                    {
                        "symbol": symbol,
                        "qty": qty,
                        "avg_px": avg_px,
                        "last_px": last_px,
                        "value": value,
                    }
                )
            return positions
        finally:
            conn.close()

    def is_dust(self, position: dict[str, Any]) -> tuple[bool, str | None]:
        symbol = str(position["symbol"])
        qty = float(position["qty"])
        value = float(position["value"])
        price = float(position["last_px"] or position["avg_px"])

        base_symbol = symbol.split("/")[0] if "/" in symbol else symbol.split("-")[0]
        if base_symbol in DUST_SYMBOLS:
            return True, "listed in dust symbols"
        if value < DUST_CRITERIA["max_value_usdt"]:
            return True, f"value {value:.4f} < ${DUST_CRITERIA['max_value_usdt']}"
        if qty < DUST_CRITERIA["max_qty"] and price < DUST_CRITERIA["max_price"]:
            return True, f"qty {qty:.6f} < {DUST_CRITERIA['max_qty']} and price {price:.4f} < ${DUST_CRITERIA['max_price']}"
        return False, None

    def add_dust_tags(self) -> None:
        if not self.positions_db.exists():
            self.log("positions.sqlite does not exist")
            return

        conn = sqlite3.connect(str(self.positions_db))
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(positions)")
            columns = [row[1] for row in cursor.fetchall()]
            if "tags_json" not in columns:
                self.log("Adding tags_json column")
                cursor.execute("ALTER TABLE positions ADD COLUMN tags_json TEXT DEFAULT '{}'")
                conn.commit()

            positions = self.get_positions()
            self.log("=" * 60)
            self.log("Scanning dust positions...")
            self.log("=" * 60)

            for pos in positions:
                is_dust, reason = self.is_dust(pos)
                if not is_dust:
                    continue

                self.dust_list.append(
                    {
                        "symbol": pos["symbol"],
                        "qty": pos["qty"],
                        "value": pos["value"],
                        "reason": reason,
                    }
                )
                tags = {
                    "dust": True,
                    "dust_reason": reason,
                    "dust_marked_at": datetime.now().isoformat(),
                }
                cursor.execute(
                    "UPDATE positions SET tags_json = ? WHERE symbol = ?",
                    (json.dumps(tags), pos["symbol"]),
                )
                self.stats["marked"] += 1
                self.log(f"MARK {pos['symbol']}: {reason}")

            conn.commit()
        finally:
            conn.close()

        self.log("=" * 60)
        self.log(f"Marked dust positions: {self.stats['marked']}")
        self.log("=" * 60)

    def update_reconcile_config(self) -> Path:
        self.dust_config_path.parent.mkdir(parents=True, exist_ok=True)
        config = {
            "dust_symbols": sorted(DUST_SYMBOLS),
            "dust_criteria": DUST_CRITERIA,
            "excluded_from_equity": True,
            "excluded_from_rebalance": True,
            "excluded_from_borrow_check": True,
            "updated_at": datetime.now().isoformat(),
        }
        self.dust_config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        self.log(f"Dust config saved: {self.dust_config_path}")
        return self.dust_config_path

    def generate_report(self) -> dict[str, Any]:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = _derive_cleanup_report_path(self.orders_db, timestamp)
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report = {
            "timestamp": datetime.now().isoformat(),
            "dust_criteria": DUST_CRITERIA,
            "dust_symbols": sorted(DUST_SYMBOLS),
            "marked_positions": self.dust_list,
            "stats": self.stats,
        }
        report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
        self.log(f"Cleanup report saved: {report_file}")
        return report

    def print_summary(self) -> None:
        print("\n" + "=" * 60)
        print("Dust Cleanup Summary")
        print("=" * 60)
        print("Dust criteria:")
        print(f"  - value < ${DUST_CRITERIA['max_value_usdt']}")
        print(f"  - or qty < {DUST_CRITERIA['max_qty']} and price < ${DUST_CRITERIA['max_price']}")
        print(f"  - or symbol in: {', '.join(sorted(DUST_SYMBOLS))}")
        print()
        print(f"Marked positions: {self.stats['marked']}")
        if self.dust_list:
            print("\nDust list:")
            for item in self.dust_list:
                print(f"  {item['symbol']:12} {item['qty']:12.6f} ${item['value']:8.4f} - {item['reason']}")
        print("=" * 60)

    def run(self) -> None:
        self.log("Dust cleanup started")
        self.add_dust_tags()
        self.update_reconcile_config()
        self.generate_report()
        self.print_summary()
        self.log("Dust cleanup completed")
        self.log("Note: this only tags dust positions; it does not sell them on OKX.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reports-dir", default=None)
    args = parser.parse_args()
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else None
    cleaner = DustCleaner(reports_dir=reports_dir)
    cleaner.run()


if __name__ == "__main__":
    main()
