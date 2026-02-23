#!/usr/bin/env python3
"""Backfill forward returns in reports/alpha_history.db from market_data_1h.

Why:
- Some backfill scripts wrote fwd_ret_* as 0.0 placeholders, which breaks IC-based analysis.
- This script recomputes fwd_ret_{1,4,12,24,72}h using close prices at ts and ts+h.

Safe:
- Only updates rows whose forward return is NULL or exactly 0.0 (configurable).
- Requires market_data_1h coverage for both timestamps.

Usage:
  cd /home/admin/clawd/v5-trading-bot
  python3 scripts/update_forward_returns_from_market_data.py --lookback-days 30
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path
from typing import Dict, Tuple

DB_PATH = Path("reports/alpha_history.db")

HORIZONS = [1, 4, 12, 24, 72]


def _load_closes(con: sqlite3.Connection, since_ts: int, until_ts: int) -> Dict[Tuple[str, int], float]:
    cur = con.cursor()
    cur.execute(
        "SELECT symbol, timestamp, close FROM market_data_1h WHERE timestamp >= ? AND timestamp <= ?",
        (int(since_ts), int(until_ts)),
    )
    out: Dict[Tuple[str, int], float] = {}
    for sym, ts, close in cur.fetchall():
        try:
            out[(str(sym), int(ts))] = float(close)
        except Exception:
            pass
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--lookback-days", type=int, default=30)
    ap.add_argument("--limit", type=int, default=200000)
    ap.add_argument("--update-zeros", action="store_true", help="Also update rows where fwd_ret_* == 0.0")
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(f"missing db: {db}")
        return 2

    now = int(time.time())
    since_ts = int(now - int(args.lookback_days) * 86400)
    max_h = max(HORIZONS) * 3600
    until_ts = int(now + max_h + 3600)

    con = sqlite3.connect(str(db))

    closes = _load_closes(con, since_ts=since_ts, until_ts=until_ts)
    print(f"CLOSES loaded={len(closes)} since={since_ts} until={until_ts}")

    # Select candidate rows.
    conds = ["fwd_ret_1h IS NULL", "fwd_ret_4h IS NULL", "fwd_ret_12h IS NULL", "fwd_ret_24h IS NULL", "fwd_ret_72h IS NULL"]
    if bool(args.update_zeros):
        conds += [
            "fwd_ret_1h = 0.0",
            "fwd_ret_4h = 0.0",
            "fwd_ret_12h = 0.0",
            "fwd_ret_24h = 0.0",
            "fwd_ret_72h = 0.0",
        ]
    where = " OR ".join(conds)

    cur = con.cursor()
    cur.execute(
        f"SELECT id, symbol, ts, fwd_ret_1h, fwd_ret_4h, fwd_ret_12h, fwd_ret_24h, fwd_ret_72h "
        f"FROM alpha_snapshots WHERE ts >= ? AND ts <= ? AND ({where}) ORDER BY ts ASC LIMIT ?",
        (int(since_ts), int(now), int(args.limit)),
    )
    rows = cur.fetchall()
    print(f"CANDIDATES n={len(rows)}")

    upd = 0
    miss = 0

    for rid, sym, ts, r1, r4, r12, r24, r72 in rows:
        sym = str(sym)
        ts = int(ts)
        c0 = closes.get((sym, ts))
        if c0 is None or float(c0) <= 0:
            miss += 1
            continue

        vals = {}
        ok_any = False
        for h in HORIZONS:
            col = f"fwd_ret_{h}h"
            c1 = closes.get((sym, ts + h * 3600))
            if c1 is None or float(c1) <= 0:
                vals[col] = None
                continue
            vals[col] = float(float(c1) / float(c0) - 1.0)
            ok_any = True

        if not ok_any:
            miss += 1
            continue

        cur.execute(
            "UPDATE alpha_snapshots SET fwd_ret_1h=?, fwd_ret_4h=?, fwd_ret_12h=?, fwd_ret_24h=?, fwd_ret_72h=? WHERE id=?",
            (
                vals.get("fwd_ret_1h"),
                vals.get("fwd_ret_4h"),
                vals.get("fwd_ret_12h"),
                vals.get("fwd_ret_24h"),
                vals.get("fwd_ret_72h"),
                int(rid),
            ),
        )
        upd += 1

    con.commit()
    con.close()

    print(f"UPDATED n={upd} missing_price_pairs={miss}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
