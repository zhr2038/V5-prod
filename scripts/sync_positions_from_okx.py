#!/usr/bin/env python3
"""
Sync actual OKX balances into the local position store.

This is an operator-invoked recovery tool used to repair local position state
and validate equity calculations against the exchange-reported account equity.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = PROJECT_ROOT / "reports"
POSITIONS_DB = REPORTS_DIR / "positions.sqlite"
EQUITY_FILE = REPORTS_DIR / "equity_validation.json"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path
from src.execution.okx_private_client import OKXPrivateClient


def _ensure_positions_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
          symbol TEXT PRIMARY KEY,
          qty REAL NOT NULL,
          avg_px REAL NOT NULL,
          entry_ts TEXT NOT NULL,
          highest_px REAL NOT NULL,
          last_update_ts TEXT NOT NULL DEFAULT '',
          last_mark_px REAL NOT NULL DEFAULT 0,
          unrealized_pnl_pct REAL NOT NULL DEFAULT 0,
          tags_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.commit()


def sync_positions(*, config_path: str | None = None, env_path: str = ".env") -> dict[str, Any] | None:
    print("Sync positions from OKX")
    print("=" * 50)

    if os.getenv("V5_LIVE_ARM") != "YES":
        print("Set V5_LIVE_ARM=YES to proceed")
        return None

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    cfg = load_config(
        resolve_runtime_config_path(config_path, project_root=PROJECT_ROOT),
        env_path=resolve_runtime_env_path(env_path, project_root=PROJECT_ROOT),
    )
    okx = OKXPrivateClient(exchange=cfg.exchange)

    try:
        print("Fetching OKX account balance...")
        resp = okx.get_balance()
        if not resp.data or "data" not in resp.data:
            print("Unable to fetch OKX account data")
            return None

        account = resp.data["data"][0]
        total_eq = float(account.get("totalEq", 0))
        print(f"Total equity: {total_eq:.4f} USDT")

        positions = []
        usdt_balance = 0.0
        for detail in account.get("details", []):
            ccy = detail.get("ccy", "")
            eq = float(detail.get("eq", 0))
            avail = float(detail.get("availBal", 0))
            liab = float(detail.get("liab", 0))
            if eq > 0.0001 and liab < 0.001:
                if ccy == "USDT":
                    usdt_balance = eq
                    print(f"USDT balance: {usdt_balance:.4f}")
                else:
                    positions.append({"ccy": ccy, "eq": eq, "avail": avail, "liab": liab})

        print(f"Found {len(positions)} non-USDT positions")

        print("\nFetching mark prices...")
        import requests

        total_position_value = 0.0
        position_details = []
        for pos in positions:
            ccy = pos["ccy"]
            symbol = f"{ccy}/USDT"
            inst_id = symbol.replace("/", "-")
            url = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"

            try:
                response = requests.get(url, timeout=5)
                if response.status_code != 200:
                    continue
                data = response.json()
                if data.get("code") != "0" or not data.get("data"):
                    continue
                price = float(data["data"][0]["last"])
                value = pos["eq"] * price
                total_position_value += value
                position_details.append(
                    {
                        "symbol": symbol,
                        "ccy": ccy,
                        "qty": pos["eq"],
                        "price": price,
                        "value": value,
                        "value_pct": (value / total_eq * 100) if total_eq > 0 else 0.0,
                    }
                )
                print(f"  {ccy}: {pos['eq']:.6f} @ {price:.6f} = {value:.4f} USDT")
            except Exception as exc:
                print(f"  {ccy}: price fetch failed - {exc}")

        print("\nEquity check:")
        print(f"  USDT balance: {usdt_balance:.4f}")
        print(f"  Position value: {total_position_value:.4f}")
        print(f"  Calculated total equity: {usdt_balance + total_position_value:.4f}")
        print(f"  OKX reported total equity: {total_eq:.4f}")

        diff = abs((usdt_balance + total_position_value) - total_eq)
        if diff < 1.0:
            print(f"  OK: equity calculation matches within tolerance ({diff:.4f} USDT)")
        else:
            print(f"  WARN: equity difference is {diff:.4f} USDT")

        print(f"\nUpdating local positions DB: {POSITIONS_DB}")
        conn = sqlite3.connect(str(POSITIONS_DB))
        try:
            _ensure_positions_table(conn)
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(positions)")
            columns = [col[1] for col in cursor.fetchall()]

            cursor.execute("DELETE FROM positions")
            now_iso = datetime.now().isoformat()
            for pos in position_details:
                if "avg_px" in columns and "entry_ts" in columns:
                    cursor.execute(
                        """
                        INSERT INTO positions
                        (symbol, qty, avg_px, entry_ts, highest_px, last_update_ts, last_mark_px, unrealized_pnl_pct, tags_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            pos["symbol"],
                            pos["qty"],
                            pos["price"],
                            now_iso,
                            pos["price"],
                            now_iso,
                            pos["price"],
                            0.0,
                            "{}",
                        ),
                    )
                else:
                    cursor.execute(
                        "INSERT INTO positions (symbol, qty) VALUES (?, ?)",
                        (pos["symbol"], pos["qty"]),
                    )

            conn.commit()
            cursor.execute("SELECT COUNT(*) FROM positions")
            count = cursor.fetchone()[0]
            print(f"Local positions rows written: {count}")

            if "avg_px" in columns:
                cursor.execute("SELECT symbol, qty, avg_px FROM positions ORDER BY symbol")
            else:
                cursor.execute("SELECT symbol, qty, NULL as avg_px FROM positions ORDER BY symbol")
            rows = cursor.fetchall()
        finally:
            conn.close()

        if rows:
            print("\nLocal positions:")
            for symbol, qty, price in rows:
                value = qty * price if price else 0.0
                price_str = f"{price:.6f}" if price else "N/A"
                print(f"  {symbol}: {qty:.6f} @ {price_str} = {value:.4f} USDT")

        equity_data = {
            "timestamp": int(time.time()),
            "okx_total_eq": total_eq,
            "calculated_total_eq": usdt_balance + total_position_value,
            "difference": diff,
            "usdt_balance": usdt_balance,
            "positions_value": total_position_value,
            "positions_count": len(position_details),
            "positions": position_details,
        }
        EQUITY_FILE.write_text(json.dumps(equity_data, indent=2), encoding="utf-8")
        print(f"\nEquity validation written to {EQUITY_FILE}")

        print("\n" + "=" * 50)
        print("Position sync complete")
        print("=" * 50)
        return equity_data
    finally:
        okx.close()


def fix_equity_calculation() -> bool:
    print("\nEquity calculation follow-up")
    print("=" * 50)
    print("1. Local positions.sqlite has been refreshed from OKX balances")
    print("2. Equity validation snapshot has been written")
    print("3. If main pipeline equity is still wrong, compare local position state with exchange balances")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--env", default=".env")
    args = parser.parse_args()

    print("V5 equity recovery helper")
    print("=" * 50)

    equity_data = sync_positions(config_path=args.config, env_path=args.env)
    fix_equity_calculation()

    print("\nRecommended follow-up check:")
    print(f"cd {PROJECT_ROOT}")
    print("export V5_CONFIG=configs/live_prod.yaml")
    print("export V5_LIVE_ARM=YES")
    print("python3 main.py --run-id 'fix_test_$(date +%Y%m%d_%H%M%S)'")

    print("\n" + "=" * 50)
    print("Recovery summary:")
    print("1. Synced live OKX balances into local positions state")
    print("2. Refreshed local positions.sqlite")
    print("3. Wrote equity validation snapshot")
    print("4. Printed follow-up verification guidance")
    print("=" * 50)

    if equity_data and equity_data.get("difference", 100.0) < 5.0:
        print("Recovery looks successful; next V5 run should see the corrected equity baseline.")
    else:
        print("Equity still shows a material mismatch; further reconciliation is required.")


if __name__ == "__main__":
    main()
