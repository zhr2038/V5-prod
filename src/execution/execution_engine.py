from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from configs.schema import ExecutionConfig
from src.core.models import ExecutionReport, Order
from src.execution.position_store import PositionStore


class ExecutionEngine:
    def __init__(self, cfg: ExecutionConfig, position_store: Optional[PositionStore] = None):
        self.cfg = cfg
        self.db_path = Path(cfg.slippage_db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self.position_store = position_store

    def _init_db(self) -> None:
        try:
            con = sqlite3.connect(str(self.db_path))
            cur = con.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS slippage (
                    ts TEXT,
                    symbol TEXT,
                    side TEXT,
                    signal_price REAL,
                    execution_price REAL,
                    slippage_bps REAL
                )
                """
            )
            con.commit()
            con.close()
        except Exception:
            pass

    def execute(self, order_batch: List[Order]) -> ExecutionReport:
        ts = datetime.utcnow().isoformat() + "Z"

        # dry-run: assume execution at signal_price
        for o in order_batch or []:
            self._record(o.symbol, o.side, o.signal_price, o.signal_price)

            # update position store (spot long-only semantics)
            if self.position_store and o.intent in {"OPEN_LONG", "REBALANCE"} and o.side == "buy":
                qty = float(o.notional_usdt) / float(o.signal_price) if o.signal_price else 0.0
                if qty > 0:
                    self.position_store.upsert_buy(o.symbol, qty=qty, px=float(o.signal_price))
            elif self.position_store and o.intent in {"CLOSE_LONG", "REBALANCE"} and o.side == "sell":
                # for scaffold: close full position
                self.position_store.close_long(o.symbol)

        return ExecutionReport(timestamp=ts, dry_run=bool(self.cfg.dry_run), orders=list(order_batch or []))

    def _record(self, symbol: str, side: str, signal_price: float, execution_price: float) -> None:
        try:
            sp = float(signal_price)
            ep = float(execution_price)
            bps = ((ep - sp) / sp) * 10_000.0 if sp else 0.0
            con = sqlite3.connect(str(self.db_path))
            cur = con.cursor()
            cur.execute(
                "INSERT INTO slippage(ts, symbol, side, signal_price, execution_price, slippage_bps) VALUES (?,?,?,?,?,?)",
                (datetime.utcnow().isoformat() + "Z", symbol, side, sp, ep, float(bps)),
            )
            con.commit()
            con.close()
        except Exception:
            pass
