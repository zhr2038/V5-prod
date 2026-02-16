from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Optional

import httpx

from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient


def _arm_check(cfg) -> None:
    arm_env = str(getattr(cfg.execution, "live_arm_env", "V5_LIVE_ARM"))
    arm_val = str(getattr(cfg.execution, "live_arm_value", "YES"))
    if os.getenv(arm_env) != arm_val:
        raise RuntimeError(f"Refuse to place live order: set {arm_env}={arm_val}")


def _ticker_last(inst_id: str) -> Optional[float]:
    try:
        r = httpx.get("https://www.okx.com/api/v5/market/ticker", params={"instId": inst_id}, timeout=10)
        j = r.json()
        d = (j or {}).get("data") or []
        if isinstance(d, list) and d:
            return float(d[0].get("last"))
    except Exception:
        return None
    return None


def main() -> None:
    cfg_path = os.getenv("V5_CONFIG") or "configs/config.yaml"
    cfg = load_config(cfg_path, env_path=".env")
    _arm_check(cfg)

    inst_id = os.getenv("V5_SMART_INST_ID", "ETH-USDT")
    quote_sz = float(os.getenv("V5_SMART_QUOTE_SZ", "5"))

    # Profit-taking / risk limits (in bps)
    take_profit_bps = float(os.getenv("V5_SMART_TP_BPS", "12"))    # +0.12%
    stop_loss_bps = float(os.getenv("V5_SMART_SL_BPS", "35"))      # -0.35%
    timeout_sec = float(os.getenv("V5_SMART_TIMEOUT_SEC", "600"))  # 10min
    poll_sec = float(os.getenv("V5_SMART_POLL_SEC", "2"))

    okx = OKXPrivateClient(exchange=cfg.exchange, req_exptime_ms=getattr(cfg.execution, "okx_exp_time_ms", None))
    try:
        # 1) BUY market using quote size (USDT)
        buy_payload = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": "buy",
            "ordType": "market",
            "tgtCcy": "quote_ccy",
            "sz": str(quote_sz),
        }
        buy_resp = okx.place_order(buy_payload)
        d = (buy_resp.data or {}).get("data") or []
        buy_ord_id = str((d[0] or {}).get("ordId") or "") if isinstance(d, list) and d else ""
        if not buy_ord_id:
            raise RuntimeError(f"buy place_order failed: {buy_resp.data}")

        # 2) Wait for fills, fetch avgPx + filledSz from order query
        time.sleep(1.5)
        q = okx.get_order(inst_id=inst_id, ord_id=buy_ord_id)
        dd = (q.data or {}).get("data") or []
        od = (dd[0] or {}) if isinstance(dd, list) and dd else {}
        avg_px = float(od.get("avgPx") or 0) if od.get("avgPx") else 0.0
        acc_fill_sz = float(od.get("accFillSz") or 0) if od.get("accFillSz") else 0.0
        if avg_px <= 0 or acc_fill_sz <= 0:
            # fallback: just use public last
            avg_px = _ticker_last(inst_id) or avg_px

        entry_px = float(avg_px)
        qty = float(acc_fill_sz) if acc_fill_sz > 0 else None

        start = time.time()
        exit_reason = "timeout"
        last_px = None
        while True:
            now = time.time()
            if now - start >= timeout_sec:
                break
            last_px = _ticker_last(inst_id)
            if last_px is None or entry_px <= 0:
                time.sleep(poll_sec)
                continue

            ret_bps = (float(last_px) - float(entry_px)) / float(entry_px) * 10_000.0
            if ret_bps >= take_profit_bps:
                exit_reason = "take_profit"
                break
            if ret_bps <= -abs(stop_loss_bps):
                exit_reason = "stop_loss"
                break
            time.sleep(poll_sec)

        # 3) SELL market the filled base qty
        # If qty is missing, query order again
        if qty is None or qty <= 0:
            q2 = okx.get_order(inst_id=inst_id, ord_id=buy_ord_id)
            dd2 = (q2.data or {}).get("data") or []
            od2 = (dd2[0] or {}) if isinstance(dd2, list) and dd2 else {}
            qty = float(od2.get("accFillSz") or 0) if od2.get("accFillSz") else 0.0

        sell_payload = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": "sell",
            "ordType": "market",
            "tgtCcy": "base_ccy",
            "sz": str(qty),
        }
        sell_resp = okx.place_order(sell_payload)

        out = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "instId": inst_id,
            "entry_px": entry_px,
            "qty": qty,
            "exit_reason": exit_reason,
            "last_px": last_px,
            "buy": {"ordId": buy_ord_id, "resp": buy_resp.data},
            "sell": {"resp": sell_resp.data},
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        okx.close()


if __name__ == "__main__":
    main()
