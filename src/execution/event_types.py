"""
Event types for event-driven trading system.
"""
from __future__ import print_function

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, Any, Optional
import time


class EventType(Enum):
    """Trading event types ordered by priority."""
    # P0: Risk events (immediate, no cooldown)
    RISK_STOP_LOSS = auto()
    RISK_TAKE_PROFIT = auto()
    RISK_RANK_EXIT = auto()
    RISK_TRAILING_STOP = auto()
    REGIME_RISK_OFF = auto()
    
    # P1: Regime changes (immediate, no cooldown)
    REGIME_CHANGE = auto()
    FUNDING_RATE_EXTREME = auto()
    
    # P2: Signal changes (respect cooldown)
    SIGNAL_DIRECTION_FLIP = auto()
    SIGNAL_SCORE_JUMP = auto()
    SIGNAL_RANK_JUMP = auto()
    SELECTION_CHANGE = auto()
    NEW_ENTRY = auto()
    
    # P2: Breakout events (respect cooldown)
    BREAKOUT_UP = auto()
    BREAKOUT_DOWN = auto()
    BOLLINGER_BREAK = auto()
    
    # P3: Heartbeat (lowest priority)
    HEARTBEAT = auto()


class EventPriority(Enum):
    """Event priority levels."""
    P0_RISK = 0      # Immediate execution, ignore cooldown
    P1_REGIME = 1    # Immediate execution, ignore cooldown
    P2_SIGNAL = 2    # Check cooldown (30 min global)
    P3_HEARTBEAT = 3 # Check cooldown (60 min global)


EVENT_PRIORITY_MAP: Dict[EventType, EventPriority] = {
    # P0
    EventType.RISK_STOP_LOSS: EventPriority.P0_RISK,
    EventType.RISK_TAKE_PROFIT: EventPriority.P0_RISK,
    EventType.RISK_RANK_EXIT: EventPriority.P0_RISK,
    EventType.RISK_TRAILING_STOP: EventPriority.P0_RISK,
    EventType.REGIME_RISK_OFF: EventPriority.P0_RISK,
    # P1
    EventType.REGIME_CHANGE: EventPriority.P1_REGIME,
    EventType.FUNDING_RATE_EXTREME: EventPriority.P1_REGIME,
    # P2
    EventType.SIGNAL_DIRECTION_FLIP: EventPriority.P2_SIGNAL,
    EventType.SIGNAL_SCORE_JUMP: EventPriority.P2_SIGNAL,
    EventType.SIGNAL_RANK_JUMP: EventPriority.P2_SIGNAL,
    EventType.SELECTION_CHANGE: EventPriority.P2_SIGNAL,
    EventType.NEW_ENTRY: EventPriority.P2_SIGNAL,
    EventType.BREAKOUT_UP: EventPriority.P2_SIGNAL,
    EventType.BREAKOUT_DOWN: EventPriority.P2_SIGNAL,
    EventType.BOLLINGER_BREAK: EventPriority.P2_SIGNAL,
    # P3
    EventType.HEARTBEAT: EventPriority.P3_HEARTBEAT,
}


@dataclass
class TradingEvent:
    """A trading event with metadata."""
    type: EventType
    symbol: Optional[str]  # None for global events
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    
    @property
    def priority(self) -> EventPriority:
        return EVENT_PRIORITY_MAP.get(self.type, EventPriority.P3_HEARTBEAT)
    
    @property
    def priority_value(self) -> int:
        return self.priority.value
    
    def is_risk_event(self) -> bool:
        return self.priority == EventPriority.P0_RISK
    
    def ignores_cooldown(self) -> bool:
        return self.priority in (EventPriority.P0_RISK, EventPriority.P1_REGIME)


@dataclass
class SignalState:
    """Represents a symbol's signal state."""
    symbol: str
    direction: str  # 'buy', 'sell', 'hold'
    score: float
    rank: int
    timestamp_ms: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'symbol': self.symbol,
            'direction': self.direction,
            'score': self.score,
            'rank': self.rank,
            'timestamp_ms': self.timestamp_ms,
        }
    
    @classmethod
    def from_dict(cls, d):
        return cls(
            symbol=d['symbol'],
            direction=d['direction'],
            score=d['score'],
            rank=d['rank'],
            timestamp_ms=d['timestamp_ms'],
        )


@dataclass
class MarketState:
    """Current market state snapshot."""
    timestamp_ms: int
    regime: str  # 'TRENDING_UP', 'TRENDING_DOWN', 'SIDEWAYS', 'RISK_OFF'
    prices: Dict[str, float] = field(default_factory=dict)
    positions: Dict[str, Any] = field(default_factory=dict)
    signals: Dict = field(default_factory=dict)
    selected_symbols: list = field(default_factory=list)
    funding_rates: Dict[str, float] = field(default_factory=dict)
