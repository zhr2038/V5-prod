from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

from configs.loader import load_config
from src.core.models import Order
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


def main() -> None:
    cfg = load_config("configs/config.yaml", env_path=".env")

    arm_env = str(getattr(cfg.execution, "live_arm_env", "V5_LIVE_ARM"))
    arm_val = str(getattr(cfg.execution, "live_arm_value", "YES"))
    if os.getenv(arm_env) != arm_val:
        raise RuntimeError(f"Refuse to place live order: set {arm_env}={arm_val}")

    symbol = os.getenv("V5_SELL_SYMBOL", "BNB/USDT")
    notional = float(os.getenv("V5_SELL_NOTIONAL_USDT", "0"))

    run_id = os.getenv("V5_RUN_ID") or datetime.utcnow().strftime("manual_sell_%Y%m%d_%H%M%S")
    Path(f"reports/runs/{run_id}").mkdir(parents=True, exist_ok=True)

    okx = OKXPrivateClient(exchange=cfg.exchange, req_exptime_ms=getattr(cfg.execution, "okx_exp_time_ms", None))
    try:
        ps = PositionStore(path="reports/positions.sqlite")
        os_store = OrderStore(cfg.execution.order_store_path)
        live = LiveExecutionEngine(cfg.execution, okx=okx, order_store=os_store, position_store=ps, run_id=run_id, exp_time_ms=getattr(cfg.execution, "okx_exp_time_ms", None))

        o = Order(
            symbol=symbol,
            side="sell",
            intent="CLOSE_LONG",
            notional_usdt=float(notional),
            signal_price=0.0,
            meta={"regime": "Manual", "ts": int(time.time())},
        )

        res = live.place(o)
        # allow exchange to process
        time.sleep(float(os.getenv("V5_SELL_SLEEP_SEC", "2")))
        live.poll_open(limit=200)
        print(f"SELL submitted: symbol={symbol} clOrdId={res.cl_ord_id} state={res.state} ordId={res.ord_id}")
    finally:
        okx.close()


if __name__ == "__main__":
    main()
