from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class MarketSeries:
    symbol: str
    timeframe: str
    ts: List[int]
    open: List[float]
    high: List[float]
    low: List[float]
    close: List[float]
    volume: List[float]


@dataclass
class Order:
    symbol: str
    side: str  # buy|sell
    notional_usdt: float
    signal_price: float
    meta: Dict[str, Any]


@dataclass
class ExecutionReport:
    timestamp: str
    dry_run: bool
    orders: List[Order]
    notes: str = ""


@dataclass
class RiskDecision:
    delever_mult: float
    reason: str


@dataclass
class PositionState:
    equity_usdt: float
    equity_peak_usdt: float
    positions: Dict[str, float]  # symbol -> weight
    entry_prices: Dict[str, float]
    highest_prices: Dict[str, float]
    days_held: Dict[str, int]
