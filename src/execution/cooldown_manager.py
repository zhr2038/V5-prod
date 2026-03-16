"""
Cooldown manager for event-driven trading.
Prevents over-trading by enforcing time-based cooldowns.
"""
from __future__ import print_function

import json
import time
import logging
from pathlib import Path
from typing import Dict, Optional, Set
from dataclasses import dataclass, asdict

from src.execution.event_types import EventPriority, TradingEvent

logger = logging.getLogger(__name__)


@dataclass
class CooldownConfig:
    """Configuration for cooldown manager."""
    global_cooldown_p0_seconds: int = 0      # P0: No cooldown
    global_cooldown_p1_seconds: int = 0      # P1: No cooldown
    global_cooldown_p2_seconds: int = 1800   # P2: 30 minutes
    global_cooldown_p3_seconds: int = 3600   # P3: 60 minutes
    symbol_cooldown_seconds: int = 3600      # Per-symbol: 60 minutes
    signal_confirmation_periods: int = 2     # Confirm for N periods
    state_path: str = "reports/cooldown_state.json"


class CooldownManager:
    """
    Manages trading cooldowns to prevent over-trading.
    
    Rules:
    - P0 (Risk): No cooldown, immediate execution
    - P1 (Regime): No cooldown, immediate execution
    - P2 (Signal): 30 min global, 60 min per-symbol
    - P3 (Heartbeat): 60 min global
    """
    
    def __init__(self, config: Optional[CooldownConfig] = None):
        self.config = config or CooldownConfig()
        self.last_global_trade_ms: int = 0
        self.last_symbol_trade_ms: Dict[str, int] = {}
        self.pending_signals: Dict[str, Dict] = {}  # For confirmation
        self._load_state()
    
    def can_trade(self, symbol: Optional[str] = None, 
                  priority: EventPriority = EventPriority.P2_SIGNAL) -> bool:
        """
        Check if trading is allowed.
        
        Args:
            symbol: Trading pair (None for global check)
            priority: Event priority level
            
        Returns:
            True if trading is allowed
        """
        now_ms = int(time.time() * 1000)
        
        # P0 and P1 ignore cooldown
        if priority in (EventPriority.P0_RISK, EventPriority.P1_REGIME):
            return True
        
        # Check global cooldown
        cooldown_ms = self._get_global_cooldown_ms(priority)
        if cooldown_ms > 0:
            elapsed_ms = now_ms - self.last_global_trade_ms
            if elapsed_ms < cooldown_ms:
                remaining_min = (cooldown_ms - elapsed_ms) / 60000
                logger.debug(f"Global cooldown: {remaining_min:.1f} min remaining")
                return False
        
        # Check symbol cooldown
        if symbol and symbol in self.last_symbol_trade_ms:
            elapsed_ms = now_ms - self.last_symbol_trade_ms[symbol]
            if elapsed_ms < self.config.symbol_cooldown_seconds * 1000:
                remaining_min = (self.config.symbol_cooldown_seconds * 1000 - elapsed_ms) / 60000
                logger.debug(f"{symbol} cooldown: {remaining_min:.1f} min remaining")
                return False
        
        return True
    
    def record_trade(self, symbol: Optional[str] = None,
                     priority: EventPriority = EventPriority.P2_SIGNAL):
        """
        Record a trade for cooldown tracking.
        
        Args:
            symbol: Trading pair (None for global record)
            priority: Event priority (affects cooldown duration)
        """
        now_ms = int(time.time() * 1000)
        
        # Update global trade time for P2/P3
        if priority in (EventPriority.P2_SIGNAL, EventPriority.P3_HEARTBEAT):
            self.last_global_trade_ms = now_ms
        
        # Update symbol-specific trade time
        if symbol:
            self.last_symbol_trade_ms[symbol] = now_ms
        
        self._save_state()
        logger.debug(f"Recorded trade: {symbol} (P{priority.value})")
    
    def check_signal_confirmation(self, symbol: str, new_signal: Dict) -> bool:
        """
        Check if a signal is confirmed (appeared in N consecutive periods).
        
        Args:
            symbol: Trading pair
            new_signal: New signal data
            
        Returns:
            True if signal is confirmed and ready to trade
        """
        if symbol not in self.pending_signals:
            # First occurrence - record and wait
            self.pending_signals[symbol] = {
                'signal': new_signal,
                'count': 1,
                'first_seen_ms': int(time.time() * 1000)
            }
            self._save_state()  # persist across timer runs
            logger.info(f"{symbol}: New signal recorded, waiting confirmation")
            return False
        
        pending = self.pending_signals[symbol]
        
        # Check if same signal
        if self._signals_equal(pending['signal'], new_signal):
            pending['count'] += 1
            if pending['count'] >= self.config.signal_confirmation_periods:
                # Signal confirmed
                del self.pending_signals[symbol]
                self._save_state()  # persist clear
                logger.info(f"{symbol}: Signal confirmed after {pending['count']} periods")
                return True
            else:
                self._save_state()  # persist counter
                logger.info(f"{symbol}: Signal count {pending['count']}/{self.config.signal_confirmation_periods}")
                return False
        else:
            # Signal changed - reset
            logger.info(f"{symbol}: Signal changed, resetting confirmation")
            self.pending_signals[symbol] = {
                'signal': new_signal,
                'count': 1,
                'first_seen_ms': int(time.time() * 1000)
            }
            self._save_state()  # persist reset
            return False
    
    def clear_pending_signal(self, symbol: str):
        """Clear pending signal for a symbol (after trade execution)."""
        if symbol in self.pending_signals:
            del self.pending_signals[symbol]
            self._save_state()
    
    def get_cooldown_status(self) -> Dict:
        """Get current cooldown status for monitoring."""
        now_ms = int(time.time() * 1000)
        
        return {
            'last_global_trade_ms': self.last_global_trade_ms,
            'global_cooldown_remaining_sec': max(0, 
                (self.last_global_trade_ms + self.config.global_cooldown_p2_seconds * 1000 - now_ms) // 1000),
            'symbol_cooldowns': {
                sym: {
                    'last_trade_ms': ts,
                    'remaining_sec': max(0, 
                        (ts + self.config.symbol_cooldown_seconds * 1000 - now_ms) // 1000)
                }
                for sym, ts in self.last_symbol_trade_ms.items()
            },
            'pending_confirmations': {
                sym: {
                    'count': data['count'],
                    'signal': data['signal']
                }
                for sym, data in self.pending_signals.items()
            }
        }
    
    def _get_global_cooldown_ms(self, priority: EventPriority) -> int:
        """Get global cooldown in milliseconds for a priority level."""
        cooldowns = {
            EventPriority.P0_RISK: self.config.global_cooldown_p0_seconds,
            EventPriority.P1_REGIME: self.config.global_cooldown_p1_seconds,
            EventPriority.P2_SIGNAL: self.config.global_cooldown_p2_seconds,
            EventPriority.P3_HEARTBEAT: self.config.global_cooldown_p3_seconds,
        }
        return cooldowns.get(priority, 1800) * 1000
    
    def _signals_equal(self, sig1: Dict, sig2: Dict) -> bool:
        """Check if two signals are equivalent."""
        # Compare direction and approximate score
        if sig1.get('direction') != sig2.get('direction'):
            return False
        
        # Score within 10% tolerance
        score_diff = abs(sig1.get('score', 0) - sig2.get('score', 0))
        return score_diff < 0.1
    
    def _load_state(self):
        """Load cooldown state from disk."""
        try:
            path = Path(self.config.state_path)
            if path.exists() and path.stat().st_size > 0:
                data = json.loads(path.read_text())
                self.last_global_trade_ms = data.get('last_global_trade_ms', 0)
                self.last_symbol_trade_ms = data.get('symbol_cooldowns', {})
                self.pending_signals = data.get('pending_signals', {})
                logger.info(f"Loaded cooldown state: {len(self.last_symbol_trade_ms)} symbols")
            else:
                self.last_global_trade_ms = 0
                self.last_symbol_trade_ms = {}
                self.pending_signals = {}
        except Exception as e:
            logger.warning(f"Failed to load cooldown state: {e}")
            self.last_global_trade_ms = 0
            self.last_symbol_trade_ms = {}
            self.pending_signals = {}
    
    def _save_state(self):
        """Save cooldown state to disk."""
        try:
            path = Path(self.config.state_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                'last_global_trade_ms': self.last_global_trade_ms,
                'symbol_cooldowns': self.last_symbol_trade_ms,
                'pending_signals': self.pending_signals,
                'saved_at_ms': int(time.time() * 1000)
            }
            path.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.warning(f"Failed to save cooldown state: {e}")
