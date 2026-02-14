from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


TRADE_COLUMNS = [
    "ts",
    "run_id",
    "symbol",
    "intent",
    "side",
    "qty",
    "price",
    "notional_usdt",
    "fee_usdt",
    "slippage_usdt",
    "realized_pnl_usdt",
    "realized_pnl_pct",
]


@dataclass
class Fill:
    ts: str
    run_id: str
    symbol: str
    intent: str
    side: str
    qty: float
    price: float
    notional_usdt: float
    fee_usdt: float
    slippage_usdt: float
    realized_pnl_usdt: Optional[float] = None
    realized_pnl_pct: Optional[float] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "run_id": self.run_id,
            "symbol": self.symbol,
            "intent": self.intent,
            "side": self.side,
            "qty": f"{float(self.qty):.12g}",
            "price": f"{float(self.price):.12g}",
            "notional_usdt": f"{float(self.notional_usdt):.12g}",
            "fee_usdt": f"{float(self.fee_usdt):.12g}",
            "slippage_usdt": f"{float(self.slippage_usdt):.12g}",
            "realized_pnl_usdt": "" if self.realized_pnl_usdt is None else f"{float(self.realized_pnl_usdt):.12g}",
            "realized_pnl_pct": "" if self.realized_pnl_pct is None else f"{float(self.realized_pnl_pct):.12g}",
        }


class TradeLogWriter:
    def __init__(self, run_dir: str, filename: str = "trades.csv"):
        self.run_dir = Path(run_dir)
        self.path = self.run_dir / filename
        self.run_dir.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=TRADE_COLUMNS)
                w.writeheader()

    def append_fill(self, fill: Fill) -> None:
        with self.path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=TRADE_COLUMNS)
            w.writerow(fill.to_row())


def iso_utc_now() -> str:
    return datetime.utcnow().isoformat() + "Z"
