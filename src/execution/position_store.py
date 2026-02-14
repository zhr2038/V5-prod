from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class Position:
    symbol: str
    qty: float
    avg_px: float
    entry_ts: str
    highest_px: float
    tags_json: str = "{}"


class PositionStore:
    """SQLite-backed position store.

    Spot-only, long-only semantics:
      - qty > 0 means holding base asset of symbol (e.g., BTC for BTC/USDT)
      - CLOSE_LONG means reduce qty to 0

    This store is designed to survive restarts.
    """

    def __init__(self, path: str = "reports/positions.sqlite"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
              symbol TEXT PRIMARY KEY,
              qty REAL NOT NULL,
              avg_px REAL NOT NULL,
              entry_ts TEXT NOT NULL,
              highest_px REAL NOT NULL,
              tags_json TEXT NOT NULL
            )
            """
        )
        con.commit()
        con.close()

    def list(self) -> List[Position]:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute("SELECT symbol, qty, avg_px, entry_ts, highest_px, tags_json FROM positions")
        rows = cur.fetchall()
        con.close()
        return [Position(*r) for r in rows]

    def get(self, symbol: str) -> Optional[Position]:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            "SELECT symbol, qty, avg_px, entry_ts, highest_px, tags_json FROM positions WHERE symbol=?",
            (symbol,),
        )
        row = cur.fetchone()
        con.close()
        return Position(*row) if row else None

    def upsert_buy(self, symbol: str, qty: float, px: float) -> Position:
        qty = float(qty)
        px = float(px)
        now = datetime.utcnow().isoformat() + "Z"

        cur_pos = self.get(symbol)
        if not cur_pos or cur_pos.qty <= 0:
            pos = Position(symbol=symbol, qty=qty, avg_px=px, entry_ts=now, highest_px=px)
        else:
            new_qty = cur_pos.qty + qty
            # weighted avg price
            avg = (cur_pos.avg_px * cur_pos.qty + px * qty) / new_qty if new_qty else px
            hi = max(cur_pos.highest_px, px)
            pos = Position(symbol=symbol, qty=new_qty, avg_px=avg, entry_ts=cur_pos.entry_ts, highest_px=hi, tags_json=cur_pos.tags_json)

        con = sqlite3.connect(str(self.path))
        c = con.cursor()
        c.execute(
            "INSERT INTO positions(symbol, qty, avg_px, entry_ts, highest_px, tags_json) VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(symbol) DO UPDATE SET qty=excluded.qty, avg_px=excluded.avg_px, entry_ts=excluded.entry_ts, highest_px=excluded.highest_px, tags_json=excluded.tags_json",
            (pos.symbol, pos.qty, pos.avg_px, pos.entry_ts, pos.highest_px, pos.tags_json),
        )
        con.commit()
        con.close()
        return pos

    def update_highest(self, symbol: str, highest_px: float) -> None:
        con = sqlite3.connect(str(self.path))
        c = con.cursor()
        c.execute("UPDATE positions SET highest_px=? WHERE symbol=?", (float(highest_px), symbol))
        con.commit()
        con.close()

    def upsert_position(self, pos: Position) -> None:
        """Insert/update a full position row (used for migrations/tests)."""
        con = sqlite3.connect(str(self.path))
        c = con.cursor()
        c.execute(
            "INSERT INTO positions(symbol, qty, avg_px, entry_ts, highest_px, tags_json) VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(symbol) DO UPDATE SET qty=excluded.qty, avg_px=excluded.avg_px, entry_ts=excluded.entry_ts, highest_px=excluded.highest_px, tags_json=excluded.tags_json",
            (pos.symbol, float(pos.qty), float(pos.avg_px), str(pos.entry_ts), float(pos.highest_px), str(pos.tags_json)),
        )
        con.commit()
        con.close()

    def close_long(self, symbol: str) -> None:
        con = sqlite3.connect(str(self.path))
        c = con.cursor()
        c.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
        con.commit()
        con.close()
