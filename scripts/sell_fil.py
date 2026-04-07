#!/usr/bin/env python3
"""Sell available FIL balance on OKX spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from configs.runtime_config import resolve_runtime_env_path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class SellFilPaths:
    workspace: Path
    env_path: Path


def build_paths(workspace: Path | None = None, env_path: str | None = None) -> SellFilPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    resolved_env = Path(resolve_runtime_env_path(env_path, project_root=root))
    return SellFilPaths(workspace=root, env_path=resolved_env)


def load_runtime_env(paths: SellFilPaths) -> None:
    load_dotenv(paths.env_path, override=False)


def _require_env(key: str) -> str:
    value = str(os.getenv(key) or "").strip()
    if not value:
        raise RuntimeError(f"missing required environment variable: {key}")
    return value


def sign(timestamp: str, method: str, path: str, body: str = "", *, secret: str | None = None) -> str:
    secret_value = secret if secret is not None else _require_env("EXCHANGE_API_SECRET")
    message = timestamp + method.upper() + path + body
    mac = hmac.new(secret_value.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode("utf-8")


def okx_request(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    session: Any = requests,
    base_url: str = "https://www.okx.com",
) -> dict[str, Any]:
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"
    body_json = json.dumps(body, separators=(",", ":")) if body else ""

    headers = {
        "OK-ACCESS-KEY": _require_env("EXCHANGE_API_KEY"),
        "OK-ACCESS-SIGN": sign(timestamp, method, path, body_json),
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": _require_env("EXCHANGE_PASSPHRASE"),
        "Content-Type": "application/json",
    }

    url = f"{base_url}{path}"
    if method.upper() == "GET":
        response = session.get(url, headers=headers, timeout=10)
    else:
        response = session.post(url, headers=headers, data=body_json, timeout=10)
    return response.json()


def get_available_balance(*, ccy: str = "FIL", session: Any = requests) -> float:
    data = okx_request("GET", "/api/v5/account/balance", session=session)
    if str(data.get("code")) != "0":
        raise RuntimeError(str(data.get("msg") or "failed to fetch OKX balance"))

    for detail in ((data.get("data") or [{}])[0].get("details") or []):
        if str(detail.get("ccy") or "").upper() == str(ccy).upper():
            return float(detail.get("availBal") or 0.0)
    return 0.0


def submit_market_sell(*, inst_id: str, size: float, session: Any = requests) -> dict[str, Any]:
    order = {
        "instId": inst_id,
        "tdMode": "cash",
        "side": "sell",
        "ordType": "market",
        "sz": str(size),
    }
    return okx_request("POST", "/api/v5/trade/order", order, session=session)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sell available FIL balance on OKX spot.")
    parser.add_argument("--env", default=None, help="Path to .env file. Defaults to repo-root .env.")
    parser.add_argument("--ccy", default="FIL", help="Currency balance to sell. Default: FIL.")
    parser.add_argument("--inst-id", default="FIL-USDT", help="OKX instrument id. Default: FIL-USDT.")
    parser.add_argument("--dry-run", action="store_true", help="Print the sell plan without placing an order.")
    args = parser.parse_args(argv)

    paths = build_paths(env_path=args.env)
    load_runtime_env(paths)

    print(f"[1] Querying {args.ccy} balance...")
    available = get_available_balance(ccy=args.ccy)
    print(f"  available {args.ccy}: {available}")

    if available <= 0:
        print(f"[ERROR] no {args.ccy} available to sell")
        return 1

    if args.dry_run:
        print(f"[DRY-RUN] would submit market sell: inst_id={args.inst_id} size={available}")
        return 0

    print(f"[2] Submitting market sell for {available} {args.ccy}...")
    result = submit_market_sell(inst_id=args.inst_id, size=available)
    if str(result.get("code")) == "0":
        ord_id = ((result.get("data") or [{}])[0]).get("ordId")
        print(f"  [OK] order submitted, ord_id={ord_id}")
        return 0

    print(f"  [FAIL] {result.get('msg')}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
