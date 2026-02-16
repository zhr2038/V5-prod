from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime
from typing import Optional, Tuple

from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient


def _arm_check(cfg) -> None:
    arm_env = str(getattr(cfg.execution, "live_arm_env", "V5_LIVE_ARM"))
    arm_val = str(getattr(cfg.execution, "live_arm_value", "YES"))
    if os.getenv(arm_env) != arm_val:
        raise RuntimeError(f"Refuse to place live order: set {arm_env}={arm_val}")


def _latest_fill_for_ord(db_path: str, inst_id: str, ord_id: str) -> Optional[Tuple[float, float]]:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "SELECT fill_sz, fill_px FROM fills WHERE inst_id=? AND ord_id=? ORDER BY ts_ms DESC LIMIT 1",
        (inst_id, ord_id),
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    try:
        return float(row[0]), float(row[1])
    except Exception:
        return None


def main() -> None:
    cfg_path = os.getenv("V5_CONFIG") or "configs/config.yaml"
    cfg = load_config(cfg_path, env_path=".env")
    _arm_check(cfg)

    inst_id = os.getenv("V5_RT_INST_ID", "ETH-USDT")
    quote_sz = float(os.getenv("V5_RT_QUOTE_SZ", "5"))
    sleep_sec = float(os.getenv("V5_RT_SLEEP_SEC", "2"))
    fills_db = os.getenv("V5_RT_FILLS_DB", "reports/fills.sqlite")

    okx = OKXPrivateClient(exchange=cfg.exchange, req_exptime_ms=getattr(cfg.execution, "okx_exp_time_ms", None))
    try:
        # 1) BUY market using quote size
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
        ord_id = str((d[0] or {}).get("ordId") or "") if isinstance(d, list) and d else ""
        if not ord_id:
            raise RuntimeError(f"buy place_order failed: {buy_resp.data}")

        time.sleep(sleep_sec)

        # 2) Pull fills into DB via direct REST (avoid depending on fill_sync CLI here)
        # Use OKX fills endpoint filtered by ordId when possible; fallback to full.
        # We still rely on existing fill_sync elsewhere for full history.
        # Here: best-effort query ordId.
        try:
            okx.request("GET", "/api/v5/trade/fills", params={"instType": "SPOT", "ordId": ord_id, "limit": 100})
        except Exception:
            pass

        # 3) Read fill qty from fills.sqlite (requires fill_sync to have run recently)
        got = _latest_fill_for_ord(fills_db, inst_id, ord_id)
        if got is None:
            raise RuntimeError(f"cannot find fill in {fills_db} for ord_id={ord_id}; run fill_sync and retry")
        fill_sz, fill_px = got

        # 4) SELL back the same base qty (round trip, minimal exposure)
        sell_payload = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": "sell",
            "ordType": "market",
            "tgtCcy": "base_ccy",
            "sz": str(fill_sz),
        }
        sell_resp = okx.place_order(sell_payload)

        out = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "instId": inst_id,
            "buy": {"payload": buy_payload, "resp": buy_resp.data},
            "buy_ord_id": ord_id,
            "buy_fill": {"sz": fill_sz, "px": fill_px},
            "sell": {"payload": sell_payload, "resp": sell_resp.data},
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        okx.close()


if __name__ == "__main__":
    main()
