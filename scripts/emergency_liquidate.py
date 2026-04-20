#!/usr/bin/env python3
"""
Emergency liquidation script.

Sell all non-USDT spot balances immediately.
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path
from src.execution.okx_private_client import OKXPrivateClient


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


def emergency_liquidate(*, config_path: str | None = None, env_path: str = ".env") -> None:
    resolved_config_path = _resolve_active_config_path(config_path)
    cfg = load_config(
        str(resolved_config_path),
        env_path=resolve_runtime_env_path(env_path, project_root=PROJECT_ROOT),
    )
    client = OKXPrivateClient(exchange=cfg.exchange)

    try:
        print("=" * 50)
        print("EMERGENCY LIQUIDATION")
        print("=" * 50)

        bal = client.get_balance()
        positions = []

        if hasattr(bal, "data") and bal.data:
            data = bal.data.get("data", [{}])[0]
            details = data.get("details", [])
            for detail in details:
                ccy = detail.get("ccy", "")
                eq = float(detail.get("eq", 0))
                avail = float(detail.get("availBal", 0))
                if ccy != "USDT" and eq > 0.01:
                    positions.append({"ccy": ccy, "eq": eq, "avail": avail})

        if not positions:
            print("No positions to liquidate.")
            return

        print(f"\nPositions to liquidate: {len(positions)}")

        for pos in positions:
            ccy = pos["ccy"]
            avail = pos["avail"]
            inst_id = f"{ccy}-USDT"

            print(f"\n{ccy}: {avail:.6f} available")

            try:
                specs_resp = client.request(
                    "GET",
                    "/api/v5/public/instruments",
                    params={"instType": "SPOT", "instId": inst_id},
                )
                specs = specs_resp.data.get("data", [{}])[0] if specs_resp.data else {}
                lot_sz = float(specs.get("lotSz", 0))
                min_sz = float(specs.get("minSz", 0))

                if lot_sz > 0:
                    lot_dec = Decimal(str(lot_sz))
                    qty_dec = (Decimal(str(avail)) / lot_dec).to_integral_value(rounding=ROUND_DOWN) * lot_dec
                    qty = float(qty_dec)
                else:
                    qty = avail

                if qty < min_sz:
                    print(f"  SKIP: qty {qty} < minSz {min_sz}")
                    continue

                payload = {
                    "instId": inst_id,
                    "tdMode": "cash",
                    "side": "sell",
                    "ordType": "market",
                    "sz": str(qty),
                }

                result = client.place_order(payload)

                if hasattr(result, "data") and result.data:
                    code = result.data.get("code", "")
                    if code == "0":
                        ord_id = result.data.get("data", [{}])[0].get("ordId")
                        print(f"  OK: sell order placed, ordId={ord_id}")
                    else:
                        msg = result.data.get("msg", "unknown")
                        print(f"  FAILED: code={code}, msg={msg}")
                else:
                    print(f"  FAILED: {result}")

            except Exception as exc:
                print(f"  ERROR: {exc}")

        print(f"\n{'=' * 50}")
        print("Check fills in the OKX app or web interface.")
        print("=" * 50)
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--env", default=".env")
    args = parser.parse_args()
    emergency_liquidate(config_path=args.config, env_path=args.env)


if __name__ == "__main__":
    main()
