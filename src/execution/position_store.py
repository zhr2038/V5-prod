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
    last_update_ts: str
    last_mark_px: float
    unrealized_pnl_pct: float
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
              last_update_ts TEXT NOT NULL DEFAULT '',
              last_mark_px REAL NOT NULL DEFAULT 0,
              unrealized_pnl_pct REAL NOT NULL DEFAULT 0,
              tags_json TEXT NOT NULL
            )
            """
        )
        con.commit()
        con.close()
        self._migrate_add_columns()

    def _migrate_add_columns(self) -> None:
        """Add new columns to existing DBs (safe best-effort)."""
        try:
            con = sqlite3.connect(str(self.path))
            cur = con.cursor()
            cur.execute("PRAGMA table_info(positions)")
            cols = {str(r[1]) for r in cur.fetchall()}
            adds = []
            if "last_update_ts" not in cols:
                adds.append("ALTER TABLE positions ADD COLUMN last_update_ts TEXT NOT NULL DEFAULT ''")
            if "last_mark_px" not in cols:
                adds.append("ALTER TABLE positions ADD COLUMN last_mark_px REAL NOT NULL DEFAULT 0")
            if "unrealized_pnl_pct" not in cols:
                adds.append("ALTER TABLE positions ADD COLUMN unrealized_pnl_pct REAL NOT NULL DEFAULT 0")
            for sql in adds:
                cur.execute(sql)
            con.commit()
            con.close()
        except Exception:
            pass

    def list(self) -> List[Position]:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            "SELECT symbol, qty, avg_px, entry_ts, highest_px, last_update_ts, last_mark_px, unrealized_pnl_pct, tags_json FROM positions"
        )
        rows = cur.fetchall()
        con.close()
        return [Position(*r) for r in rows]

    def get(self, symbol: str) -> Optional[Position]:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            "SELECT symbol, qty, avg_px, entry_ts, highest_px, last_update_ts, last_mark_px, unrealized_pnl_pct, tags_json FROM positions WHERE symbol=?",
            (symbol,),
        )
        row = cur.fetchone()
        con.close()
        return Position(*row) if row else None

    def upsert_buy(self, symbol: str, qty: float, px: float, now_ts: Optional[str] = None) -> Position:
        qty = float(qty)
        px = float(px)
        now = now_ts or (datetime.utcnow().isoformat() + "Z")

        cur_pos = self.get(symbol)

        # If existing position is only dust (very small notional), treat as flat.
        # This avoids stale trailing-stop state (e.g. highest_px from an old position)
        # causing immediate exits right after a fresh re-entry.
        dust_reset_notional_usdt = 0.01
        if cur_pos and float(cur_pos.qty) > 0 and float(cur_pos.qty) * float(px) < dust_reset_notional_usdt:
            cur_pos = None

        if not cur_pos or cur_pos.qty <= 0:
            pos = Position(
                symbol=symbol,
                qty=qty,
                avg_px=px,
                entry_ts=now,
                highest_px=px,
                last_update_ts=now,
                last_mark_px=px,
                unrealized_pnl_pct=0.0,
                tags_json="{}",
            )
        else:
            new_qty = cur_pos.qty + qty
            avg = (cur_pos.avg_px * cur_pos.qty + px * qty) / new_qty if new_qty else px
            hi = max(cur_pos.highest_px, px)
            pos = Position(
                symbol=symbol,
                qty=new_qty,
                avg_px=avg,
                entry_ts=cur_pos.entry_ts,
                highest_px=hi,
                last_update_ts=now,
                last_mark_px=px,
                unrealized_pnl_pct=float(cur_pos.unrealized_pnl_pct),
                tags_json=cur_pos.tags_json,
            )

        self.upsert_position(pos)
        return pos

    def mark_position(
        self,
        symbol: str,
        now_ts: str,
        mark_px: float,
        high_px: Optional[float] = None,
    ) -> None:
        """Mark-to-market a position.

        - update last_update_ts
        - update last_mark_px
        - update unrealized_pnl_pct
        - update highest_px = max(existing, high_px or mark_px)
        """
        p = self.get(symbol)
        if not p:
            return
        mp = float(mark_px)
        hp = float(high_px) if high_px is not None else mp
        hi = max(float(p.highest_px), hp)
        pnl = (mp - float(p.avg_px)) / float(p.avg_px) if float(p.avg_px) > 0 else 0.0
        self.upsert_position(
            Position(
                symbol=p.symbol,
                qty=float(p.qty),
                avg_px=float(p.avg_px),
                entry_ts=str(p.entry_ts),
                highest_px=float(hi),
                last_update_ts=str(now_ts),
                last_mark_px=float(mp),
                unrealized_pnl_pct=float(pnl),
                tags_json=str(p.tags_json),
            )
        )

    def update_highest(self, symbol: str, highest_px: float) -> None:
        p = self.get(symbol)
        if not p:
            return
        self.mark_position(symbol=symbol, now_ts=p.last_update_ts or p.entry_ts, mark_px=p.last_mark_px or p.avg_px, high_px=highest_px)

    def upsert_position(self, pos: Position) -> None:
        """Insert/update a full position row (used for migrations/tests)."""
        con = sqlite3.connect(str(self.path))
        c = con.cursor()
        c.execute(
            "INSERT INTO positions(symbol, qty, avg_px, entry_ts, highest_px, last_update_ts, last_mark_px, unrealized_pnl_pct, tags_json) "
            "VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(symbol) DO UPDATE SET qty=excluded.qty, avg_px=excluded.avg_px, entry_ts=excluded.entry_ts, highest_px=excluded.highest_px, "
            "last_update_ts=excluded.last_update_ts, last_mark_px=excluded.last_mark_px, unrealized_pnl_pct=excluded.unrealized_pnl_pct, tags_json=excluded.tags_json",
            (
                pos.symbol,
                float(pos.qty),
                float(pos.avg_px),
                str(pos.entry_ts),
                float(pos.highest_px),
                str(pos.last_update_ts),
                float(pos.last_mark_px),
                float(pos.unrealized_pnl_pct),
                str(pos.tags_json),
            ),
        )
        con.commit()
        con.close()

    def set_qty(self, symbol: str, *, qty: float, now_ts: Optional[str] = None) -> None:
        """Update qty only (avg_px unchanged)."""
        p = self.get(symbol)
        if not p:
            return
        now = now_ts or (datetime.utcnow().isoformat() + "Z")
        self.upsert_position(
            Position(
                symbol=p.symbol,
                qty=float(qty),
                avg_px=float(p.avg_px),
                entry_ts=str(p.entry_ts),
                highest_px=float(p.highest_px),
                last_update_ts=str(now),
                last_mark_px=float(p.last_mark_px or p.avg_px),
                unrealized_pnl_pct=float(p.unrealized_pnl_pct),
                tags_json=str(p.tags_json),
            )
        )

    def close_long(self, symbol: str) -> None:
        con = sqlite3.connect(str(self.path))
        c = con.cursor()
        c.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
        con.commit()
        con.close()
