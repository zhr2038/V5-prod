from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class MarketSeries:
    """市场数据序列，包含OHLCV数据"""
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
    """交易订单"""
    symbol: str
    side: str  # buy|sell
    intent: str  # OPEN_LONG|CLOSE_LONG|REBALANCE
    notional_usdt: float
    signal_price: float
    meta: Dict[str, Any]


@dataclass
class ExecutionReport:
    """执行报告"""
    timestamp: str
    dry_run: bool
    orders: List[Order]
    notes: str = ""


@dataclass
class RiskDecision:
    """风险决策结果"""
    delever_mult: float
    reason: str


@dataclass
class PositionState:
    """持仓状态"""
    equity_usdt: float
    equity_peak_usdt: float
    positions: Dict[str, float]  # symbol -> weight
    entry_prices: Dict[str, float]
    highest_prices: Dict[str, float]
    days_held: Dict[str, int]
