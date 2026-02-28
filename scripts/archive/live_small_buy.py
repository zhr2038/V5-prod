from __future__ import annotations

import os
import json
from datetime import datetime

from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient


def main() -> None:
    cfg = load_config("configs/config.yaml", env_path=".env")

    # Safety: require explicit arm env even for ad-hoc script
    arm_env = str(getattr(cfg.execution, "live_arm_env", "V5_LIVE_ARM"))
    arm_val = str(getattr(cfg.execution, "live_arm_value", "YES"))
    if os.getenv(arm_env) != arm_val:
        raise RuntimeError(f"Refuse to place live order: set {arm_env}={arm_val}")

    inst_id = os.getenv("V5_LIVE_INST_ID", "ETH-USDT")
    quote_sz = float(os.getenv("V5_LIVE_QUOTE_SZ", "5"))

    okx = OKXPrivateClient(exchange=cfg.exchange, req_exptime_ms=getattr(cfg.execution, "okx_exp_time_ms", None))
    try:
        payload = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": "buy",
            "ordType": "market",
            # For spot market buy, OKX supports tgtCcy=quote_ccy to interpret sz in quote.
            "tgtCcy": "quote_ccy",
            "sz": str(quote_sz),
        }
        resp = okx.place_order(payload)
        out = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "request": payload,
            "http_status": resp.http_status,
            "okx_code": resp.okx_code,
            "okx_msg": resp.okx_msg,
            "response": resp.data,
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        okx.close()


if __name__ == "__main__":
    main()
