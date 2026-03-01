"""
Event-based decision engine for trading.
"""
from __future__ import print_function

import logging
from typing import Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass

from src.execution.event_types import (
    EventType, EventPriority, TradingEvent, MarketState, SignalState
)
from src.execution.cooldown_manager import CooldownManager
from src.execution.event_monitor import EventMonitor

logger = logging.getLogger(__name__)


@dataclass
class DecisionResult:
    """Result of decision engine execution."""
    should_trade: bool
    actions: List[Dict]
    events_processed: int
    events_blocked_by_cooldown: int
    reason: str


class EventDecisionEngine:
    """
    Event-driven decision engine that processes trading events
    and generates trade actions based on priority and cooldown rules.
    """
    
    def __init__(
        self,
        event_monitor: EventMonitor,
        cooldown_manager: CooldownManager,
        executor: Optional[Callable] = None
    ):
        self.monitor = event_monitor
        self.cooldown = cooldown_manager
        self.executor = executor
        self.last_events: List[TradingEvent] = []
    
    def run(self, state: MarketState) -> DecisionResult:
        """
        Run the decision engine on current market state.
        
        Args:
            state: Current market snapshot
            
        Returns:
            DecisionResult with trade actions
        """
        # 1. Collect all events
        events = self.monitor.collect_events(state)
        self.last_events = events
        
        if not events:
            return DecisionResult(
                should_trade=False,
                actions=[],
                events_processed=0,
                events_blocked_by_cooldown=0,
                reason="no_events"
            )
        
        logger.info(f"Decision engine: {len(events)} events detected")
        for e in events:
            logger.info(f"  [{e.priority.name}] {e.type.name}: {e.symbol}")
        
        # 2. Group by priority
        p0_events = [e for e in events if e.priority == EventPriority.P0_RISK]
        p1_events = [e for e in events if e.priority == EventPriority.P1_REGIME]
        p2_events = [e for e in events if e.priority == EventPriority.P2_SIGNAL]
        p3_events = [e for e in events if e.priority == EventPriority.P3_HEARTBEAT]
        
        actions = []
        blocked_count = 0
        
        # 3. Process P0: Risk events (immediate, no cooldown)
        if p0_events:
            risk_actions = self._process_risk_events(p0_events, state)
            actions.extend(risk_actions)
            logger.info(f"P0 Risk: {len(risk_actions)} actions")
        
        # 4. Process P1: Regime changes (immediate, no cooldown)
        if p1_events and not actions:  # Only if no risk actions (avoid conflict)
            regime_actions = self._process_regime_events(p1_events, state)
            actions.extend(regime_actions)
            logger.info(f"P1 Regime: {len(regime_actions)} actions")
        
        # 5. Process P2: Signal changes (check cooldown)
        if p2_events:
            signal_actions, blocked = self._process_signal_events(p2_events, state)
            actions.extend(signal_actions)
            blocked_count += blocked
            logger.info(f"P2 Signal: {len(signal_actions)} actions, {blocked} blocked by cooldown")
        
        # 6. Process P3: Heartbeat (lowest priority, check cooldown)
        if p3_events and not actions:  # Only if nothing else triggered
            hb_actions, blocked = self._process_heartbeat(p3_events, state)
            actions.extend(hb_actions)
            blocked_count += blocked
            logger.info(f"P3 Heartbeat: {len(hb_actions)} actions")
        
        # Update monitor's last trade time
        if actions:
            self.monitor.update_last_trade_time()
        
        return DecisionResult(
            should_trade=len(actions) > 0,
            actions=actions,
            events_processed=len(events),
            events_blocked_by_cooldown=blocked_count,
            reason="processed" if actions else "no_actionable_events"
        )
    
    def _process_risk_events(self, events: List[TradingEvent], state: MarketState) -> List[Dict]:
        """Process risk events (P0) - immediate execution."""
        actions = []
        
        for event in events:
            if event.type == EventType.REGIME_RISK_OFF:
                # Clear all positions
                for sym in state.positions:
                    actions.append({
                        'symbol': sym,
                        'action': 'close',
                        'reason': 'regime_risk_off',
                        'priority': 0,
                        'event_type': event.type.name
                    })
                logger.warning("RISK_OFF: Closing all positions")
                break  # One risk_off is enough
            
            elif event.symbol and event.symbol in state.positions:
                # Individual risk events
                if event.type == EventType.RISK_STOP_LOSS:
                    actions.append({
                        'symbol': event.symbol,
                        'action': 'close',
                        'reason': 'stop_loss',
                        'priority': 0,
                        'price': event.data.get('current_price'),
                        'stop_price': event.data.get('stop_price'),
                        'event_type': event.type.name
                    })
                
                elif event.type == EventType.RISK_TRAILING_STOP:
                    actions.append({
                        'symbol': event.symbol,
                        'action': 'close',
                        'reason': 'trailing_stop',
                        'priority': 0,
                        'price': event.data.get('current_price'),
                        'trailing_stop': event.data.get('trailing_stop'),
                        'event_type': event.type.name
                    })
                
                elif event.type == EventType.RISK_TAKE_PROFIT:
                    actions.append({
                        'symbol': event.symbol,
                        'action': 'close',
                        'reason': f"take_profit_{event.data.get('tp_level')}%",
                        'priority': 0,
                        'pnl_pct': event.data.get('pnl_pct'),
                        'event_type': event.type.name
                    })
                
                elif event.type == EventType.RISK_RANK_EXIT:
                    actions.append({
                        'symbol': event.symbol,
                        'action': 'close',
                        'reason': f"rank_exit_{event.data.get('current_rank')}",
                        'priority': 0,
                        'event_type': event.type.name
                    })
        
        # Record trades (risk events don't trigger cooldown but we track them)
        for action in actions:
            self.cooldown.record_trade(action['symbol'], EventPriority.P0_RISK)
        
        return actions
    
    def _process_regime_events(self, events: List[TradingEvent], state: MarketState) -> List[Dict]:
        """Process regime change events (P1) - immediate but strategic."""
        actions = []
        
        for event in events:
            if event.type != EventType.REGIME_CHANGE:
                continue
            
            from_regime = event.data.get('from_regime', 'UNKNOWN')
            to_regime = event.data.get('to_regime', 'UNKNOWN')
            transition = f"{from_regime}->{to_regime}"
            
            logger.info(f"Processing regime transition: {transition}")
            
            if to_regime == 'RISK_OFF':
                # Already handled by P0, skip
                continue
            
            elif to_regime == 'TRENDING_UP' and from_regime in ('SIDEWAYS', 'TRENDING_DOWN'):
                # Trend starting - enter top positions
                for sym in state.selected_symbols[:3]:
                    if sym not in state.positions:
                        # Check signal confirmation
                        signal = state.signals.get(sym)
                        if signal:
                            signal_dict = signal.to_dict() if hasattr(signal, 'to_dict') else signal
                            confirmed = self.cooldown.check_signal_confirmation(sym, signal_dict)
                            if confirmed or len(state.positions) == 0:  # Enter if confirmed or no positions
                                actions.append({
                                    'symbol': sym,
                                    'action': 'open',
                                    'reason': f'trend_start_{transition}',
                                    'priority': 1,
                                    'event_type': event.type.name
                                })
                
                # Record trades
                for action in actions:
                    self.cooldown.record_trade(action['symbol'], EventPriority.P1_REGIME)
            
            elif to_regime == 'SIDEWAYS' and from_regime == 'TRENDING_UP':
                # Trend ending - can reduce positions or hold
                logger.info("Trend ending - holding positions (manual review recommended)")
                # Optionally reduce by 50% here
        
        return actions
    
    def _process_signal_events(self, events: List[TradingEvent], state: MarketState) -> Tuple[List[Dict], int]:
        """Process signal change events (P2) - with cooldown check."""
        actions = []
        blocked = 0
        
        # Group by symbol
        symbol_events: Dict[str, List[TradingEvent]] = {}
        for event in events:
            if event.symbol:
                if event.symbol not in symbol_events:
                    symbol_events[event.symbol] = []
                symbol_events[event.symbol].append(event)
        
        for symbol, sym_events in symbol_events.items():
            # Check cooldown
            if not self.cooldown.can_trade(symbol, EventPriority.P2_SIGNAL):
                blocked += len(sym_events)
                logger.info(f"{symbol}: Blocked by cooldown")
                continue
            
            # Get current signal
            signal = state.signals.get(symbol)
            if not signal:
                continue
            
            signal_dict = signal.to_dict() if hasattr(signal, 'to_dict') else signal
            
            # Check signal confirmation (except for exits)
            has_exit = any(e.type in (EventType.SELECTION_CHANGE,) for e in sym_events)
            
            if not has_exit:
                confirmed = self.cooldown.check_signal_confirmation(symbol, signal_dict)
                if not confirmed:
                    logger.info(f"{symbol}: Signal not confirmed yet")
                    continue
            
            # Determine action based on signal direction
            direction = signal_dict.get('direction', 'hold')
            
            if direction == 'buy' and symbol not in state.positions:
                actions.append({
                    'symbol': symbol,
                    'action': 'open',
                    'reason': self._get_primary_reason(sym_events),
                    'priority': 2,
                    'score': signal_dict.get('score'),
                    'event_type': ','.join(e.type.name for e in sym_events)
                })
                self.cooldown.record_trade(symbol, EventPriority.P2_SIGNAL)
                self.cooldown.clear_pending_signal(symbol)
            
            elif direction == 'sell' and symbol in state.positions:
                actions.append({
                    'symbol': symbol,
                    'action': 'close',
                    'reason': self._get_primary_reason(sym_events),
                    'priority': 2,
                    'score': signal_dict.get('score'),
                    'event_type': ','.join(e.type.name for e in sym_events)
                })
                self.cooldown.record_trade(symbol, EventPriority.P2_SIGNAL)
                self.cooldown.clear_pending_signal(symbol)
        
        return actions, blocked
    
    def _process_heartbeat(self, events: List[TradingEvent], state: MarketState) -> Tuple[List[Dict], int]:
        """Process heartbeat events (P3) - lowest priority."""
        actions = []
        blocked = 0
        
        # Only trade if no positions and long time passed
        if state.positions:
            return actions, blocked
        
        # Check global cooldown
        if not self.cooldown.can_trade(None, EventPriority.P3_HEARTBEAT):
            blocked = len(events)
            return actions, blocked
        
        # Enter top 2 positions if in appropriate regime
        if state.regime in ('TRENDING_UP', 'SIDEWAYS'):
            for sym in state.selected_symbols[:2]:
                if sym not in state.positions:
                    signal = state.signals.get(sym)
                    if signal:
                        signal_dict = signal.to_dict() if hasattr(signal, 'to_dict') else signal
                        if signal_dict.get('direction') == 'buy':
                            actions.append({
                                'symbol': sym,
                                'action': 'open',
                                'reason': 'heartbeat_entry',
                                'priority': 3,
                                'score': signal_dict.get('score'),
                                'event_type': 'HEARTBEAT'
                            })
                            self.cooldown.record_trade(sym, EventPriority.P3_HEARTBEAT)
        
        return actions, blocked
    
    def _get_primary_reason(self, events: List[TradingEvent]) -> str:
        """Get primary reason from multiple events."""
        # Priority order
        priority_order = [
            EventType.SIGNAL_DIRECTION_FLIP,
            EventType.SIGNAL_SCORE_JUMP,
            EventType.SIGNAL_RANK_JUMP,
            EventType.NEW_ENTRY,
            EventType.SELECTION_CHANGE,
            EventType.BREAKOUT_UP,
            EventType.BREAKOUT_DOWN,
        ]
        
        for p in priority_order:
            for e in events:
                if e.type == p:
                    return p.name.lower()
        
        return events[0].type.name.lower()
    
    def get_status(self) -> Dict:
        """Get engine status for monitoring."""
        return {
            'last_events': [
                {
                    'type': e.type.name,
                    'symbol': e.symbol,
                    'priority': e.priority.name
                }
                for e in self.last_events[-10:]  # Last 10 events
            ],
            'cooldown_status': self.cooldown.get_cooldown_status()
        }
