"""
Event monitor for detecting trading events.
"""
from __future__ import print_function

import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

from src.execution.event_types import (
    EventType, TradingEvent, MarketState, SignalState, normalize_signal_rank
)

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class EventMonitorConfig:
    """Configuration for event monitor."""
    # Signal change thresholds - 提高阈值降低噪声
    score_change_threshold: float = 0.60  # 60% change (原0.30)
    rank_jump_threshold: int = 7          # 7 position jump (原3)
    
    # Breakout detection - 提高突破阈值
    breakout_lookback_hours: int = 24
    breakout_threshold_pct: float = 0.8   # 0.8% (原0.5%)
    bollinger_enabled: bool = True
    
    # Heartbeat
    heartbeat_interval_hours: int = 4
    
    # State persistence
    state_path: str = "reports/event_monitor_state.json"


class EventMonitor:
    """
    Monitors market conditions and generates trading events.
    
    Detects:
    - Risk events (stop loss, take profit, rank exit)
    - Regime changes (HMM state transitions)
    - Signal changes (direction flip, score jump, rank jump)
    - Breakout events (price breakout/breakdown)
    - Heartbeat (periodic check)
    """
    
    def __init__(self, config: Optional[EventMonitorConfig] = None):
        self.config = config or EventMonitorConfig()
        self.config.state_path = str(self._resolve_state_path(self.config.state_path))
        self.last_state: Optional[MarketState] = None
        self.price_high_24h: Dict[str, float] = {}
        self.price_low_24h: Dict[str, float] = {}
        self.price_history: Dict[str, List[Dict[str, float]]] = {}
        self.last_trade_time_ms: int = 0
        self._load_state()

    @staticmethod
    def _resolve_state_path(state_path: str | Path) -> Path:
        path = Path(state_path)
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        return path
    
    def collect_events(self, current_state: MarketState) -> List[TradingEvent]:
        """
        Collect all triggered events from current market state.
        
        Args:
            current_state: Current market snapshot
            
        Returns:
            List of trading events sorted by priority
        """
        events = []
        
        # P0: Risk events (highest priority)
        events.extend(self._check_risk_events(current_state))
        
        # P1: Regime changes
        events.extend(self._check_regime_events(current_state))
        
        # P2: Signal changes
        events.extend(self._check_signal_events(current_state))
        
        # P2: Breakout events
        events.extend(self._check_breakout_events(current_state))
        
        # P3: Heartbeat
        events.extend(self._check_heartbeat())
        
        # Sort by priority
        events.sort(key=lambda e: e.priority_value)
        
        # Update state
        self.last_state = current_state
        self._save_state()
        
        return events
    
    def _check_risk_events(self, state: MarketState) -> List[TradingEvent]:
        """Check for risk events (stop loss, take profit, etc)."""
        events = []
        now_ms = int(time.time() * 1000)
        
        for symbol, pos in state.positions.items():
            current_px = state.prices.get(symbol)
            if not current_px:
                continue
            
            entry_px = float(pos.get('entry_price', 0.0) or 0.0)
            stop_px = float(pos.get('current_stop', 0.0) or 0.0)
            stop_source = 'profit_taking'
            if stop_px <= 0:
                stop_px = float(pos.get('fixed_stop_price', 0.0) or 0.0)
                stop_source = 'fixed_stop'
            if stop_px <= 0 and entry_px > 0:
                stop_px = entry_px * 0.95
                stop_source = 'fallback_fixed_pct'

            if stop_px > 0:
                if current_px <= stop_px:
                    events.append(TradingEvent(
                        type=EventType.RISK_STOP_LOSS,
                        symbol=symbol,
                        data={
                            'current_price': current_px,
                            'entry_price': entry_px,
                            'stop_price': stop_px,
                            'stop_source': stop_source,
                            'loss_pct': (current_px - entry_px) / entry_px * 100
                        },
                        timestamp_ms=now_ms
                    ))
                    logger.warning(f"STOP LOSS triggered: {symbol} @ {current_px:.4f}")
                    continue
            
            # Trailing stop (ATR-based)
            highest_px = pos.get('highest_price', entry_px)
            atr = pos.get('atr_14', 0)
            if highest_px > 0 and atr > 0:
                trailing_stop = highest_px - (atr * 2.2)
                if current_px <= trailing_stop:
                    events.append(TradingEvent(
                        type=EventType.RISK_TRAILING_STOP,
                        symbol=symbol,
                        data={
                            'current_price': current_px,
                            'highest_price': highest_px,
                            'trailing_stop': trailing_stop,
                            'atr': atr
                        },
                        timestamp_ms=now_ms
                    ))
                    logger.warning(f"TRAILING STOP: {symbol} @ {current_px:.4f}")
                    continue
            
            # Take profit levels
            if entry_px > 0:
                pnl_pct = (current_px - entry_px) / entry_px * 100
                tp_levels = [5, 10, 15]  # 5%, 10%, 15%
                
                for tp in tp_levels:
                    if pnl_pct >= tp:
                        tp_key = f'tp_{tp}_triggered'
                        if not pos.get(tp_key, False):
                            events.append(TradingEvent(
                                type=EventType.RISK_TAKE_PROFIT,
                                symbol=symbol,
                                data={
                                    'current_price': current_px,
                                    'entry_price': entry_px,
                                    'pnl_pct': pnl_pct,
                                    'tp_level': tp
                                },
                                timestamp_ms=now_ms
                            ))
                            logger.info(f"TAKE PROFIT: {symbol} {pnl_pct:.1f}% (target {tp}%)")
                            break
            
            # Rank exit
            current_rank = state.signals.get(symbol, SignalState(symbol, 'hold', 0, 99, 0)).rank
            max_rank = 3  # Configurable
            if current_rank > max_rank:
                events.append(TradingEvent(
                    type=EventType.RISK_RANK_EXIT,
                    symbol=symbol,
                    data={
                        'current_rank': current_rank,
                        'max_allowed_rank': max_rank
                    },
                    timestamp_ms=now_ms
                ))
                logger.info(f"RANK EXIT: {symbol} rank {current_rank} > {max_rank}")
        
        # RiskOff regime check
        if state.regime == 'RISK_OFF':
            events.append(TradingEvent(
                type=EventType.REGIME_RISK_OFF,
                symbol=None,  # Global event
                data={'regime': state.regime},
                timestamp_ms=now_ms
            ))
            logger.warning("RISK_OFF regime detected - clearing positions")
        
        return events
    
    def _check_regime_events(self, state: MarketState) -> List[TradingEvent]:
        """Check for regime/state changes."""
        events = []
        
        if self.last_state is None:
            return events
        
        last_regime = self.last_state.regime
        current_regime = state.regime
        
        if last_regime != current_regime:
            events.append(TradingEvent(
                type=EventType.REGIME_CHANGE,
                symbol=None,
                data={
                    'from_regime': last_regime,
                    'to_regime': current_regime,
                    'transition': f"{last_regime}->{current_regime}"
                },
                timestamp_ms=int(time.time() * 1000)
            ))
            logger.info(f"REGIME CHANGE: {last_regime} -> {current_regime}")
        
        return events
    
    def _check_signal_events(self, state: MarketState) -> List[TradingEvent]:
        """Check for signal changes."""
        events = []
        
        if self.last_state is None:
            # First run - record signals but don't trade
            return events
        
        last_signals = self.last_state.signatures if hasattr(self.last_state, 'signatures') else {}
        if not last_signals:
            last_signals = self.last_state.signals
        
        current_signals = state.signals
        now_ms = int(time.time() * 1000)
        
        # Get selected symbols
        current_selected = set(state.selected_symbols)
        last_selected = set(self.last_state.selected_symbols) if self.last_state else set()
        
        # Check for new entries
        new_entries = current_selected - last_selected
        for sym in new_entries:
            if sym in current_signals:
                events.append(TradingEvent(
                    type=EventType.NEW_ENTRY,
                    symbol=sym,
                    data={
                        'signal': current_signals[sym].to_dict(),
                        'previous_rank': 99
                    },
                    timestamp_ms=now_ms
                ))
                logger.info(f"NEW ENTRY: {sym}")
        
        # Check for selection changes (exits)
        exits = last_selected - current_selected
        for sym in exits:
            if sym in last_signals:
                events.append(TradingEvent(
                    type=EventType.SELECTION_CHANGE,
                    symbol=sym,
                    data={
                        'change_type': 'exit',
                        'last_signal': last_signals[sym].to_dict() if hasattr(last_signals[sym], 'to_dict') else last_signals[sym]
                    },
                    timestamp_ms=now_ms
                ))
                logger.info(f"EXIT: {sym}")
        
        # Check individual signal changes
        for sym, curr_sig in current_signals.items():
            if sym not in last_signals:
                continue
            
            last_sig = last_signals[sym]
            
            # Handle both SignalState objects and dicts
            if hasattr(last_sig, 'direction'):
                last_dir = last_sig.direction
                last_score = last_sig.score
                last_rank = normalize_signal_rank(last_sig.rank)
            else:
                last_dir = last_sig.get('direction', 'hold')
                last_score = last_sig.get('score', 0)
                last_rank = normalize_signal_rank(last_sig.get('rank', 99))
            
            if hasattr(curr_sig, 'direction'):
                curr_dir = curr_sig.direction
                curr_score = curr_sig.score
                curr_rank = normalize_signal_rank(curr_sig.rank)
            else:
                curr_dir = curr_sig.get('direction', 'hold')
                curr_score = curr_sig.get('score', 0)
                curr_rank = normalize_signal_rank(curr_sig.get('rank', 99))
            
            # Direction flip
            if last_dir != curr_dir:
                events.append(TradingEvent(
                    type=EventType.SIGNAL_DIRECTION_FLIP,
                    symbol=sym,
                    data={
                        'from_direction': last_dir,
                        'to_direction': curr_dir,
                        'score': curr_score
                    },
                    timestamp_ms=now_ms
                ))
                logger.info(f"DIRECTION FLIP: {sym} {last_dir}->{curr_dir}")
                continue
            
            # Score jump
            if last_score > 0:
                score_change = abs(curr_score - last_score)
                if score_change >= self.config.score_change_threshold:
                    events.append(TradingEvent(
                        type=EventType.SIGNAL_SCORE_JUMP,
                        symbol=sym,
                        data={
                            'last_score': last_score,
                            'current_score': curr_score,
                            'change': score_change
                        },
                        timestamp_ms=now_ms
                    ))
                    logger.info(f"SCORE JUMP: {sym} {last_score:.2f}->{curr_score:.2f}")
                    continue
            
            # Rank jump
            rank_change = abs(curr_rank - last_rank)
            if rank_change >= self.config.rank_jump_threshold:
                events.append(TradingEvent(
                    type=EventType.SIGNAL_RANK_JUMP,
                    symbol=sym,
                    data={
                        'last_rank': last_rank,
                        'current_rank': curr_rank,
                        'jump': rank_change
                    },
                    timestamp_ms=now_ms
                ))
                logger.info(f"RANK JUMP: {sym} {last_rank}->{curr_rank}")
        
        return events
    
    def _check_breakout_events(self, state: MarketState) -> List[TradingEvent]:
        """Check for price breakout events."""
        events = []
        now_ms = self._resolve_market_timestamp_ms(state)
        self._trim_price_history(now_ms)

        # Check breakouts against the prior rolling window only.
        threshold = max(0.0, float(self.config.breakout_threshold_pct or 0.0)) / 100.0

        for sym, px in state.prices.items():
            history = self.price_history.get(sym) or []
            if not history:
                continue

            try:
                high = max(float(sample.get('price', 0.0) or 0.0) for sample in history)
                low = min(float(sample.get('price', 0.0) or 0.0) for sample in history)
            except Exception:
                continue

            # Breakout up: price clears the prior rolling high by the configured threshold.
            if high > 0 and px >= high * (1 + threshold):
                events.append(TradingEvent(
                    type=EventType.BREAKOUT_UP,
                    symbol=sym,
                    data={
                        'price': px,
                        'resistance': high,
                        'threshold_pct': self.config.breakout_threshold_pct
                    },
                    timestamp_ms=now_ms
                ))
                logger.info(f"BREAKOUT UP: {sym} @ {px:.4f} (resistance {high:.4f})")

            # Breakdown: price clears the prior rolling low by the configured threshold.
            elif low > 0 and px <= low * (1 - threshold):
                events.append(TradingEvent(
                    type=EventType.BREAKOUT_DOWN,
                    symbol=sym,
                    data={
                        'price': px,
                        'support': low,
                        'threshold_pct': self.config.breakout_threshold_pct
                    },
                    timestamp_ms=now_ms
                ))
                logger.info(f"BREAKOUT DOWN: {sym} @ {px:.4f} (support {low:.4f})")

        self._record_price_snapshot(state, now_ms)
        self._refresh_price_ranges()
        return events

    def _resolve_market_timestamp_ms(self, state: Optional[MarketState]) -> int:
        """Prefer the market snapshot timestamp so rolling windows are deterministic."""
        try:
            ts = int(getattr(state, 'timestamp_ms', 0) or 0)
            if ts > 0:
                return ts
        except Exception:
            pass
        return int(time.time() * 1000)

    def _breakout_lookback_ms(self) -> int:
        hours = max(0, int(self.config.breakout_lookback_hours or 0))
        return hours * 3600 * 1000

    def _trim_price_history(self, now_ms: int) -> None:
        """Keep only samples inside the rolling breakout lookback window."""
        lookback_ms = self._breakout_lookback_ms()
        cutoff_ms = now_ms - lookback_ms
        trimmed: Dict[str, List[Dict[str, float]]] = {}

        for sym, samples in (self.price_history or {}).items():
            kept: List[Dict[str, float]] = []
            for sample in samples or []:
                try:
                    sample_ts = int(sample.get('timestamp_ms', 0) or 0)
                    sample_px = float(sample.get('price', 0.0) or 0.0)
                except Exception:
                    continue
                if sample_px <= 0:
                    continue
                if lookback_ms > 0 and sample_ts < cutoff_ms:
                    continue
                kept.append({'timestamp_ms': sample_ts, 'price': sample_px})
            if kept:
                trimmed[sym] = kept

        self.price_history = trimmed

    def _record_price_snapshot(self, state: MarketState, now_ms: int) -> None:
        for sym, px in (state.prices or {}).items():
            try:
                price = float(px or 0.0)
            except Exception:
                continue
            if price <= 0:
                continue

            history = self.price_history.setdefault(sym, [])
            first_match_index = None
            duplicate_indexes = []
            for idx, sample in enumerate(history):
                if int(sample.get('timestamp_ms', 0) or 0) != now_ms:
                    continue
                if first_match_index is None:
                    first_match_index = idx
                else:
                    duplicate_indexes.append(idx)

            if first_match_index is not None:
                history[first_match_index]['price'] = price
                for idx in reversed(duplicate_indexes):
                    del history[idx]
            else:
                history.append({'timestamp_ms': now_ms, 'price': price})

        self._trim_price_history(now_ms)

    def _refresh_price_ranges(self) -> None:
        self.price_high_24h = {}
        self.price_low_24h = {}
        for sym, samples in (self.price_history or {}).items():
            if not samples:
                continue
            prices = [float(sample.get('price', 0.0) or 0.0) for sample in samples]
            prices = [px for px in prices if px > 0]
            if not prices:
                continue
            self.price_high_24h[sym] = max(prices)
            self.price_low_24h[sym] = min(prices)
    
    def _check_heartbeat(self) -> List[TradingEvent]:
        """Check if heartbeat is needed."""
        now_ms = int(time.time() * 1000)
        
        if self.last_trade_time_ms == 0:
            self.last_trade_time_ms = now_ms
            return []
        
        hours_since_trade = (now_ms - self.last_trade_time_ms) / 3600000

        if hours_since_trade >= self.config.heartbeat_interval_hours:
            return [TradingEvent(
                type=EventType.HEARTBEAT,
                symbol=None,
                data={'hours_since_last_trade': hours_since_trade},
                timestamp_ms=now_ms
            )]
        
        return []
    
    def update_last_trade_time(self):
        """Update last trade time (called after trade execution)."""
        self.last_trade_time_ms = int(time.time() * 1000)
        self._save_state()
    
    def _load_state(self):
        """Load monitor state from disk."""
        try:
            path = Path(self.config.state_path)
            if path.exists() and path.stat().st_size > 0:
                data = json.loads(path.read_text())
                self.price_high_24h = data.get('price_high_24h', {})
                self.price_low_24h = data.get('price_low_24h', {})
                self.price_history = data.get('price_history', {})
                self.last_trade_time_ms = data.get('last_trade_time_ms', 0)
                # Load last_state for signal change detection
                last_state_data = data.get('last_state')
                if last_state_data:
                    from src.execution.event_types import MarketState, SignalState
                    # Reconstruct MarketState
                    signals = {}
                    for sym, sig_data in last_state_data.get('signals', {}).items():
                        if isinstance(sig_data, SignalState):
                            signals[sym] = sig_data
                            continue
                        if not isinstance(sig_data, dict):
                            continue
                        signals[sym] = SignalState(
                            symbol=sig_data.get('symbol', sym),
                            direction=sig_data.get('direction', 'hold'),
                            score=float(sig_data.get('score', 0.0) or 0.0),
                            rank=normalize_signal_rank(sig_data.get('rank', 99)),
                            timestamp_ms=int(sig_data.get('timestamp_ms', 0) or 0)
                        )
                    self.last_state = MarketState(
                        timestamp_ms=last_state_data.get('timestamp_ms', 0),
                        regime=last_state_data.get('regime', 'SIDEWAYS'),
                        prices=last_state_data.get('prices', {}),
                        signals=signals,
                        positions=last_state_data.get('positions', {}),
                        selected_symbols=last_state_data.get('selected_symbols', [])
                    )
                self._refresh_price_ranges()
        except Exception as e:
            logger.warning(f"Failed to load monitor state: {e}")
    
    def _save_state(self):
        """Save monitor state to disk."""
        try:
            path = Path(self.config.state_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            last_state_data = None
            if self.last_state:
                signals_data = {}
                for sym, sig in self.last_state.signals.items():
                    if hasattr(sig, 'to_dict'):
                        signals_data[sym] = sig.to_dict()
                    elif isinstance(sig, dict):
                        signals_data[sym] = sig
                    else:
                        signals_data[sym] = {
                            'symbol': sym,
                            'direction': getattr(sig, 'direction', 'hold'),
                            'score': getattr(sig, 'score', 0.0),
                            'rank': getattr(sig, 'rank', 99),
                            'timestamp_ms': getattr(sig, 'timestamp_ms', 0)
                        }

                last_state_data = {
                    'timestamp_ms': self.last_state.timestamp_ms,
                    'regime': self.last_state.regime,
                    'prices': self.last_state.prices,
                    'signals': signals_data,
                    'positions': self.last_state.positions,
                    'selected_symbols': self.last_state.selected_symbols
                }

            data = {
                'price_high_24h': self.price_high_24h,
                'price_low_24h': self.price_low_24h,
                'price_history': self.price_history,
                'last_trade_time_ms': self.last_trade_time_ms,
                'last_state': last_state_data,
                'saved_at_ms': int(time.time() * 1000)
            }
            path.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.warning(f"Failed to save monitor state: {e}")
