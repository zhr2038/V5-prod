"""
Event-driven trading integration for V5.
Provides a wrapper to integrate event-driven engine with existing V5 system.
"""
from __future__ import print_function

import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass

from src.execution.event_types import MarketState, SignalState, top_selected_symbols
from src.execution.event_monitor import EventMonitor, EventMonitorConfig
from src.execution.cooldown_manager import CooldownManager, CooldownConfig
from src.execution.event_decision_engine import EventDecisionEngine

logger = logging.getLogger(__name__)


@dataclass
class EventDrivenConfig:
    """Configuration for event-driven trading."""
    enabled: bool = True
    check_interval_minutes: int = 15
    monitor_state_path: str = "reports/event_monitor_state.json"
    cooldown_state_path: str = "reports/cooldown_state.json"
    
    # Cooldown settings
    global_cooldown_p2_minutes: int = 30
    symbol_cooldown_minutes: int = 60
    signal_confirmation_periods: int = 2
    
    # Signal thresholds
    score_change_threshold: float = 0.30
    rank_jump_threshold: int = 3
    
    # Breakout
    breakout_enabled: bool = True
    breakout_lookback_hours: int = 24
    breakout_threshold_pct: float = 0.5
    
    # Heartbeat
    heartbeat_interval_hours: int = 4


class EventDrivenTrader:
    """
    High-level wrapper for event-driven trading.
    Integrates with existing V5 components.
    """
    
    def __init__(self, config: Optional[EventDrivenConfig] = None):
        self.config = config or EventDrivenConfig()
        
        if not self.config.enabled:
            logger.info("Event-driven trading is disabled")
            return
        
        # Initialize components
        self.cooldown = CooldownManager(CooldownConfig(
            global_cooldown_p2_seconds=self.config.global_cooldown_p2_minutes * 60,
            symbol_cooldown_seconds=self.config.symbol_cooldown_minutes * 60,
            signal_confirmation_periods=self.config.signal_confirmation_periods,
            state_path=str(self.config.cooldown_state_path),
        ))
        
        self.monitor = EventMonitor(EventMonitorConfig(
            score_change_threshold=self.config.score_change_threshold,
            rank_jump_threshold=self.config.rank_jump_threshold,
            breakout_lookback_hours=self.config.breakout_lookback_hours,
            breakout_threshold_pct=self.config.breakout_threshold_pct,
            heartbeat_interval_hours=self.config.heartbeat_interval_hours,
            state_path=str(self.config.monitor_state_path),
        ))
        
        self.engine = EventDecisionEngine(
            event_monitor=self.monitor,
            cooldown_manager=self.cooldown
        )
        
        logger.info("Event-driven trader initialized")
    
    def should_trade(self, 
                     current_state: Dict[str, Any],
                     last_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Check if trading should occur based on event detection.
        
        Args:
            current_state: Current market state dict
            last_state: Previous market state (optional)
            
        Returns:
            Dict with 'should_trade', 'actions', 'reason', etc.
        """
        if not self.config.enabled:
            return {
                'should_trade': True,
                'reason': 'event_driven_disabled',
                'actions': [],
                'use_default': True
            }
        
        # Convert to MarketState
        market_state = self._build_market_state(current_state)
        
        # Store last state in monitor if provided
        if last_state:
            self.monitor.last_state = self._build_market_state(last_state)
        
        # Run decision engine
        result = self.engine.run(market_state)
        
        return {
            'should_trade': result.should_trade,
            'actions': result.actions,
            'events_processed': result.events_processed,
            'events_blocked': result.events_blocked_by_cooldown,
            'reason': result.reason,
            'use_default': False
        }
    
    def _build_market_state(self, state_dict: Dict[str, Any]) -> MarketState:
        """Build MarketState from dictionary.

        Accept both dict-form signals (from JSON/history) and SignalState objects
        (from in-memory loaders) to avoid dropping signals silently.
        """
        signals = {}
        raw_signals = state_dict.get('signals', {}) or {}

        for sym, sig in raw_signals.items():
            if isinstance(sig, SignalState):
                signals[sym] = sig
            elif isinstance(sig, dict):
                signals[sym] = SignalState(
                    symbol=sig.get('symbol', sym),
                    direction=sig.get('direction', 'hold'),
                    score=float(sig.get('score', 0.0) or 0.0),
                    rank=int(sig.get('rank', 99) or 99),
                    timestamp_ms=int(sig.get('timestamp_ms', 0) or 0)
                )

        selected = top_selected_symbols(
            signals,
            state_dict.get('selected_symbols', []) or None,
            limit=5,
        )

        return MarketState(
            timestamp_ms=int(state_dict.get('timestamp_ms', 0) or 0),
            regime=state_dict.get('regime', 'SIDEWAYS'),
            prices=state_dict.get('prices', {}) or {},
            positions=state_dict.get('positions', {}) or {},
            signals=signals,
            selected_symbols=selected
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Get trader status for monitoring."""
        if not self.config.enabled:
            return {'enabled': False}
        
        return {
            'enabled': True,
            'engine_status': self.engine.get_status(),
            'config': {
                'check_interval_minutes': self.config.check_interval_minutes,
                'cooldown_p2_minutes': self.config.global_cooldown_p2_minutes,
                'symbol_cooldown_minutes': self.config.symbol_cooldown_minutes,
                'score_threshold': self.config.score_change_threshold
            }
        }


def create_event_driven_trader(cfg: Optional[Dict] = None) -> EventDrivenTrader:
    """Factory function to create event-driven trader from config."""
    if cfg is None:
        cfg = {}
    
    config = EventDrivenConfig(
        enabled=cfg.get('enabled', True),
        check_interval_minutes=cfg.get('check_interval_minutes', 15),
        monitor_state_path=cfg.get('monitor_state_path', 'reports/event_monitor_state.json'),
        cooldown_state_path=cfg.get('cooldown_state_path', 'reports/cooldown_state.json'),
        global_cooldown_p2_minutes=cfg.get('global_cooldown_p2_minutes', 30),
        symbol_cooldown_minutes=cfg.get('symbol_cooldown_minutes', 60),
        signal_confirmation_periods=cfg.get('signal_confirmation_periods', 2),
        score_change_threshold=cfg.get('score_change_threshold', 0.30),
        rank_jump_threshold=cfg.get('rank_jump_threshold', 3),
        breakout_enabled=cfg.get('breakout_enabled', True),
        breakout_lookback_hours=cfg.get('breakout_lookback_hours', 24),
        breakout_threshold_pct=cfg.get('breakout_threshold_pct', 0.5),
        heartbeat_interval_hours=cfg.get('heartbeat_interval_hours', 4)
    )
    
    return EventDrivenTrader(config)
