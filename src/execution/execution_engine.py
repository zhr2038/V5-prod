from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from configs.schema import ExecutionConfig
from src.core.models import ExecutionReport, Order


class ExecutionEngine:
    def __init__(self, cfg: ExecutionConfig):
        self.cfg = cfg
        self.db_path = Path(cfg.slippage_db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

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
        # dry-run: record slippage as zero (placeholder)
        for o in order_batch or []:
            self._record(o.symbol, o.side, o.signal_price, o.signal_price)
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
