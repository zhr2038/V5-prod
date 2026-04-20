#!/usr/bin/env python3
"""
Emergency close-all script.

Sell all non-USDT spot balances above the dust threshold and write a report
under the repository reports directory.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import ccxt
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = PROJECT_ROOT / "reports"
REPORT_PATH = REPORTS_DIR / "emergency_close_report.json"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from configs.runtime_config import load_runtime_config, resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
from src.execution.fill_store import derive_runtime_named_json_path


def _build_exchange() -> Any:
    return ccxt.okx(
        {
            "apiKey": os.getenv("EXCHANGE_API_KEY"),
            "secret": os.getenv("EXCHANGE_API_SECRET"),
            "password": os.getenv("EXCHANGE_PASSPHRASE"),
            "enableRateLimit": True,
        }
    )


def _resolve_report_path(config_path: str | None = None) -> Path:
    cfg = load_runtime_config(config_path, project_root=PROJECT_ROOT)
    execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
    orders_db = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path") if isinstance(execution_cfg, dict) else None,
            default="reports/orders.sqlite",
            project_root=PROJECT_ROOT,
        )
    ).resolve()
    return derive_runtime_named_json_path(orders_db, "emergency_close_report").resolve()


def _resolve_active_config_path(config_path: str | None = None) -> Path:
    resolved = Path(resolve_runtime_config_path(config_path, project_root=PROJECT_ROOT)).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"runtime config not found: {resolved}")
    try:
        payload = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise ValueError(f"runtime config is invalid: {resolved}: {exc}") from exc
    if not isinstance(payload, dict) or not payload:
        raise ValueError(f"runtime config is empty or invalid: {resolved}")
    execution = payload.get("execution")
    if not isinstance(execution, dict):
        raise ValueError(f"runtime config missing execution section: {resolved}")
    return resolved


def emergency_close_all(
    *,
    config_path: str | None = None,
    env_path: str = ".env",
    dust_threshold: float = 0.5,
) -> dict[str, Any]:
    resolved_config_path = _resolve_active_config_path(config_path)
    load_config(
        str(resolved_config_path),
        env_path=resolve_runtime_env_path(env_path, project_root=PROJECT_ROOT),
    )
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    exchange = _build_exchange()

    try:
        balance = exchange.fetch_balance()

        print("=" * 60)
        print("EMERGENCY CLOSE ALL")
        print("=" * 60)

        sold = []
        errors = []
        skipped = []

        for coin, amount in balance.get("total", {}).items():
            if coin == "USDT" or amount <= 0:
                continue

            try:
                ticker = exchange.fetch_ticker(f"{coin}/USDT")
                price = float(ticker.get("last", 0) or 0)
                value = float(amount) * price

                if value < dust_threshold:
                    skipped.append({"coin": coin, "amount": amount, "value": value})
                    print(f"[SKIP DUST] {coin}: {amount:.6f} (${value:.2f})")
                    continue

                symbol = f"{coin}/USDT"
                print(f"[SELL] {symbol}: {amount:.6f} value ${value:.2f}")
                order = exchange.create_market_sell_order(symbol, amount)
                sold.append({"coin": coin, "amount": amount, "value": value, "order": order})
                print("  OK")
            except Exception as exc:
                errors.append({"coin": coin, "error": str(exc)})
                print(f"  ERROR: {exc}")

        print()
        print("=" * 60)
        print("Summary")
        print(f"  Sold: {len(sold)}")
        print(f"  Skipped dust: {len(skipped)}")
        print(f"  Errors: {len(errors)}")

        if sold:
            total_sold = sum(float(item["value"]) for item in sold)
            print(f"  Total sold value: ${total_sold:.2f} USDT")

        if skipped:
            total_skipped = sum(float(item["value"]) for item in skipped)
            print(f"  Total skipped dust value: ${total_skipped:.2f} USDT")

        final_balance = exchange.fetch_balance()
        usdt_total = float(final_balance.get("total", {}).get("USDT", 0) or 0)
        print()
        print(f"Final USDT balance: {usdt_total:.2f} USDT")
        print("=" * 60)

        report = {
            "timestamp": datetime.now().isoformat(),
            "sold": sold,
            "skipped_dust": skipped,
            "errors": errors,
            "final_usdt": usdt_total,
        }
        report_path = _resolve_report_path(config_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(f"Report saved to {report_path}")
        return report
    finally:
        close_fn = getattr(exchange, "close", None)
        if callable(close_fn):
            close_fn()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--env", default=".env")
    parser.add_argument("--dust-threshold", type=float, default=0.5)
    args = parser.parse_args()
    emergency_close_all(
        config_path=args.config,
        env_path=args.env,
        dust_threshold=args.dust_threshold,
    )


if __name__ == "__main__":
    main()
