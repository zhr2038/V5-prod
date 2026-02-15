from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone

from configs.loader import load_config
from src.execution.account_store import AccountStore
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.position_store import Position, PositionStore
from src.reporting.spread_snapshot_store import SpreadSnapshotStore


log = logging.getLogger("bootstrap")


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _mid_px(symbol: str, ts_ms: int) -> float:
    try:
        ss = SpreadSnapshotStore()
        snap = ss.get_latest_before(symbol=symbol, ts_ms=ts_ms)
        if snap is not None and snap.mid and snap.mid > 0:
            return float(snap.mid)
    except Exception:
        pass
    return 0.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/config.yaml")
    ap.add_argument("--env", default=".env")
    ap.add_argument("--positions-db", default="reports/positions.sqlite")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite local stores to match exchange snapshot")
    ap.add_argument("--min_non_usdt", type=float, default=0.0, help="Ignore non-USDT assets whose cashBal is below this")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(args.config, env_path=args.env)

    client = OKXPrivateClient(exchange=cfg.exchange)
    ps = PositionStore(path=args.positions_db)
    ac = AccountStore(path=args.positions_db)

    try:
        r = client.get_balance(ccy=None)
        data = (r.data or {}).get("data") or []
        if not isinstance(data, list) or not data:
            raise RuntimeError("Empty balance response")
        details = (data[0] or {}).get("details") or []
        if not isinstance(details, list):
            details = []

        # Build snapshot ccy->cashBal
        snap = {}
        for d in details:
            if not isinstance(d, dict):
                continue
            ccy = str(d.get("ccy") or "")
            if not ccy:
                continue
            cb = d.get("cashBal")
            if cb is None:
                continue
            try:
                snap[ccy] = float(cb)
            except Exception:
                continue

        now_ms = int(time.time() * 1000)

        # Overwrite local positions if requested
        if args.overwrite:
            for p in ps.list():
                ps.close_long(p.symbol)

        # Set local USDT cash
        usdt = float(snap.get("USDT", 0.0))
        st = ac.get()
        st.cash_usdt = usdt
        ac.set(st)

        created = 0
        for ccy, qty in snap.items():
            if ccy.upper() == "USDT":
                continue
            if float(qty) <= float(args.min_non_usdt):
                continue

            symbol = f"{ccy}/USDT"
            mid = _mid_px(symbol, now_ms)
            now_ts = _iso_utc_now()
            avg_px = mid if mid > 0 else 0.0

            pos = Position(
                symbol=symbol,
                qty=float(qty),
                avg_px=float(avg_px),
                entry_ts=now_ts,
                highest_px=float(avg_px),
                last_update_ts=now_ts,
                last_mark_px=float(avg_px),
                unrealized_pnl_pct=0.0,
                tags_json=json.dumps({"bootstrap": True, "source": "okx_balance"}, ensure_ascii=False),
            )
            ps.upsert_position(pos)
            created += 1

        log.info(json.dumps({"event": "BOOTSTRAP", "usdt": usdt, "non_usdt_positions": created, "overwrite": bool(args.overwrite)}, ensure_ascii=False))

    finally:
        client.close()


if __name__ == "__main__":
    main()
