#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
from src.execution.account_store import AccountStore
from src.execution.fill_store import (
    derive_position_store_path,
    derive_runtime_named_json_path,
    derive_runtime_spread_snapshots_dir,
)
from src.execution.highest_px_tracker import HighestPriceTracker
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.position_store import Position, PositionStore
from src.reporting.spread_snapshot_store import SpreadSnapshotStore


log = logging.getLogger("bootstrap")


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _derive_runtime_artifact_paths(positions_db_path: str | Path) -> tuple[Path, Path]:
    positions_path = Path(positions_db_path).resolve()
    if positions_path.name == "positions.sqlite":
        order_store_path = positions_path.with_name("orders.sqlite")
    elif "positions" in positions_path.stem:
        order_store_path = positions_path.with_name(
            positions_path.name.replace("positions", "orders", 1)
        )
    else:
        order_store_path = positions_path.with_name("orders.sqlite")
    highest_state_path = derive_runtime_named_json_path(order_store_path, "highest_px_state")
    return derive_runtime_spread_snapshots_dir(order_store_path), highest_state_path


def _mid_px(symbol: str, ts_ms: int, *, spread_store: SpreadSnapshotStore) -> float:
    try:
        snap = spread_store.get_latest_before(symbol=symbol, ts_ms=ts_ms)
        if snap is not None and snap.mid and snap.mid > 0:
            return float(snap.mid)
    except Exception:
        pass
    return 0.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--positions-db", default=None)
    ap.add_argument("--overwrite", action="store_true", help="Overwrite local stores to match exchange snapshot")
    ap.add_argument("--min_non_usdt", type=float, default=0.0, help="Ignore non-USDT assets whose cashBal is below this")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(
        resolve_runtime_config_path(args.config),
        env_path=resolve_runtime_env_path(args.env),
    )

    if args.positions_db:
        positions_db_path = resolve_runtime_path(args.positions_db, default="reports/positions.sqlite")
    else:
        order_store_path = resolve_runtime_path(
            getattr(cfg.execution, "order_store_path", None),
            default="reports/orders.sqlite",
        )
        positions_db_path = str(derive_position_store_path(order_store_path))
    spread_snapshots_dir, highest_state_path = _derive_runtime_artifact_paths(positions_db_path)

    client = OKXPrivateClient(exchange=cfg.exchange)
    ps = PositionStore(path=positions_db_path)
    ac = AccountStore(path=positions_db_path)
    spread_store = SpreadSnapshotStore(base_dir=spread_snapshots_dir)
    tracker = HighestPriceTracker(state_path=str(highest_state_path))

    try:
        r = client.get_balance(ccy=None)
        data = (r.data or {}).get("data") or []
        if not isinstance(data, list) or not data:
            raise RuntimeError("Empty balance response")
        details = (data[0] or {}).get("details") or []
        if not isinstance(details, list):
            details = []

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

        if args.overwrite:
            for p in ps.list():
                if float(p.qty) > 0 and float(p.highest_px) > 0:
                    tracker.update(p.symbol, float(p.highest_px), float(p.avg_px), source="bootstrap_backup")
                ps.close_long(p.symbol)

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
            mid = _mid_px(symbol, now_ms, spread_store=spread_store)
            now_ts = _iso_utc_now()
            avg_px = mid if mid > 0 else 0.0
            tracked_high = tracker.get_highest_px(symbol, float(avg_px))
            highest_px = max(float(avg_px), float(tracked_high or 0.0))

            pos = Position(
                symbol=symbol,
                qty=float(qty),
                avg_px=float(avg_px),
                entry_ts=now_ts,
                highest_px=float(highest_px),
                last_update_ts=now_ts,
                last_mark_px=float(avg_px),
                unrealized_pnl_pct=0.0,
                tags_json=json.dumps({"bootstrap": True, "source": "okx_balance"}, ensure_ascii=False),
            )
            ps.upsert_position(pos)
            tracker.update(symbol, float(highest_px), float(avg_px), source="bootstrap_restore")
            created += 1

        log.info(
            json.dumps(
                {
                    "event": "BOOTSTRAP",
                    "usdt": usdt,
                    "non_usdt_positions": created,
                    "overwrite": bool(args.overwrite),
                },
                ensure_ascii=False,
            )
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()


