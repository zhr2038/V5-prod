from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.reporting.fill_trade_exporter import export_fill


def _yyyymmdd_utc_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def _iter_unprocessed_untracked(db_path: str, day: str) -> List[Dict[str, Any]]:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    # We consider "untracked" = no cl_ord_id (manual orders), and not yet in fill_processed.
    # Limit to the specified UTC day.
    day0 = int(datetime(int(day[0:4]), int(day[4:6]), int(day[6:8]), tzinfo=timezone.utc).timestamp() * 1000)
    day1 = day0 + 24 * 3600 * 1000

    cur.execute(
        """
        SELECT f.inst_id,f.trade_id,f.ts_ms,f.side,f.fill_px,f.fill_sz,f.fee,f.fee_ccy,f.ord_id
        FROM fills f
        LEFT JOIN fill_processed p
          ON f.inst_id=p.inst_id AND f.trade_id=p.trade_id
        WHERE p.trade_id IS NULL
          AND (f.cl_ord_id IS NULL OR f.cl_ord_id='')
          AND f.ts_ms>=? AND f.ts_ms<?
        ORDER BY f.ts_ms ASC
        """,
        (day0, day1),
    )
    rows = cur.fetchall()
    con.close()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "inst_id": r[0],
                "trade_id": r[1],
                "ts_ms": int(r[2]),
                "side": r[3],
                "fill_px": r[4],
                "fill_sz": r[5],
                "fee": r[6],
                "fee_ccy": r[7],
                "ord_id": r[8],
            }
        )
    return out


def _mark_processed(db_path: str, inst_id: str, trade_id: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO fill_processed(inst_id, trade_id, processed_ts_ms) VALUES (?,?,?)",
        (str(inst_id), str(trade_id), int(time.time() * 1000)),
    )
    con.commit()
    con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fills_db", default="reports/fills.sqlite")
    ap.add_argument("--day", default=None, help="UTC day YYYYMMDD; default today")
    ap.add_argument("--run_id", default=None, help="run_id to use for exported artifacts")
    args = ap.parse_args()

    day = args.day
    if not day:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")

    run_id = args.run_id or f"untracked_{day}"
    run_dir = f"reports/runs/{run_id}"
    Path(run_dir).mkdir(parents=True, exist_ok=True)

    xs = _iter_unprocessed_untracked(str(args.fills_db), day)
    exported = 0
    for f in xs:
        ts_ms = int(f["ts_ms"])
        # derive a minimal window for cost_event
        window_end = int(ts_ms / 1000)
        window_start = max(0, window_end - 60)

        export_fill(
            fill_ts_ms=ts_ms,
            inst_id=str(f["inst_id"]),
            side=str(f.get("side") or ""),
            fill_px=str(f.get("fill_px") or "0"),
            fill_sz=str(f.get("fill_sz") or "0"),
            fee=f.get("fee"),
            fee_ccy=f.get("fee_ccy"),
            run_id=str(run_id),
            intent="MANUAL",
            window_start_ts=window_start,
            window_end_ts=window_end,
            run_dir=run_dir,
            regime="Manual",
            deadband_pct=None,
            drift=None,
            cl_ord_id=None,
            order_store_path="reports/orders.sqlite",
        )
        _mark_processed(str(args.fills_db), str(f["inst_id"]), str(f["trade_id"]))
        exported += 1

    print(f"export_untracked_fills day={day} run_id={run_id} exported={exported}")


if __name__ == "__main__":
    main()
