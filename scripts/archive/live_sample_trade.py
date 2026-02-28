from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx

from configs.loader import load_config
from src.core.models import Order
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.live_execution_engine import LiveExecutionEngine


def _public_last(inst_id: str) -> Optional[float]:
    try:
        url = "https://www.okx.com/api/v5/market/ticker"
        r = httpx.get(url, params={"instId": inst_id}, timeout=10)
        j = r.json()
        d = (j or {}).get("data") or []
        if isinstance(d, list) and d:
            last = d[0].get("last")
            if last is not None:
                return float(last)
    except Exception:
        return None
    return None


def main() -> None:
    cfg = load_config("configs/config.yaml", env_path=".env")

    arm_env = str(getattr(cfg.execution, "live_arm_env", "V5_LIVE_ARM"))
    arm_val = str(getattr(cfg.execution, "live_arm_value", "YES"))
    if os.getenv(arm_env) != arm_val:
        raise RuntimeError(f"Refuse to place live order: set {arm_env}={arm_val}")

    symbol = os.getenv("V5_SAMPLE_SYMBOL", "ETH/USDT")
    inst_id = os.getenv("V5_SAMPLE_INST_ID", "ETH-USDT")
    notional = float(os.getenv("V5_SAMPLE_NOTIONAL_USDT", "5"))

    run_id = os.getenv("V5_RUN_ID") or datetime.utcnow().strftime("manual_%Y%m%d_%H%M%S")
    Path(f"reports/runs/{run_id}").mkdir(parents=True, exist_ok=True)

    px = _public_last(inst_id) or 0.0

    okx = OKXPrivateClient(exchange=cfg.exchange, req_exptime_ms=getattr(cfg.execution, "okx_exp_time_ms", None))
    try:
        live = LiveExecutionEngine(cfg.execution, okx=okx, run_id=run_id, exp_time_ms=getattr(cfg.execution, "okx_exp_time_ms", None))

        o = Order(
            symbol=symbol,
            side="buy",
            intent="OPEN_LONG",
            notional_usdt=float(notional),
            signal_price=float(px),
            meta={
                "regime": "Manual",
                "window_start_ts": int(time.time()) - 60,
                "window_end_ts": int(time.time()),
            },
        )

        res = live.place(o)

        # Give OKX a moment to generate fills
        time.sleep(float(os.getenv("V5_SAMPLE_SLEEP_SEC", "2")))
        polled = live.poll_open(limit=200)

        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "placed": {"cl_ord_id": res.cl_ord_id, "state": res.state, "ord_id": res.ord_id},
                    "polled_open": [r.__dict__ for r in polled],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        okx.close()


if __name__ == "__main__":
    main()
