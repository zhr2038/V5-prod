from __future__ import annotations

from src.execution.cooldown_manager import CooldownConfig, CooldownManager
from src.execution.event_decision_engine import EventDecisionEngine
from src.execution.event_monitor import EventMonitor, EventMonitorConfig
from src.execution.event_types import EventType, MarketState, SignalState, TradingEvent


def _build_engine(tmp_path) -> EventDecisionEngine:
    monitor = EventMonitor(EventMonitorConfig(state_path=str(tmp_path / "event_monitor_state.json")))
    cooldown = CooldownManager(CooldownConfig(state_path=str(tmp_path / "cooldown_state.json")))
    return EventDecisionEngine(monitor, cooldown)


def test_take_profit_is_deferred_for_active_top_target(tmp_path) -> None:
    engine = _build_engine(tmp_path)
    state = MarketState(
        timestamp_ms=1,
        regime="TRENDING_UP",
        prices={"MON/USDT": 0.0271},
        positions={"MON/USDT": {"entry_price": 0.0248}},
        signals={
            "MON/USDT": SignalState(
                symbol="MON/USDT",
                direction="buy",
                score=0.99,
                rank=1,
                timestamp_ms=1,
            )
        },
        selected_symbols=["MON/USDT"],
    )
    events = [
        TradingEvent(
            type=EventType.RISK_TAKE_PROFIT,
            symbol="MON/USDT",
            data={"tp_level": 5, "pnl_pct": 9.3},
            timestamp_ms=1,
        )
    ]

    actions = engine._process_risk_events(events, state)

    assert actions == []


def test_take_profit_still_closes_when_symbol_is_not_selected(tmp_path) -> None:
    engine = _build_engine(tmp_path)
    state = MarketState(
        timestamp_ms=1,
        regime="TRENDING_UP",
        prices={"MON/USDT": 0.0271},
        positions={"MON/USDT": {"entry_price": 0.0248}},
        signals={
            "MON/USDT": SignalState(
                symbol="MON/USDT",
                direction="sell",
                score=0.4,
                rank=4,
                timestamp_ms=1,
            )
        },
        selected_symbols=["BTC/USDT"],
    )
    events = [
        TradingEvent(
            type=EventType.RISK_TAKE_PROFIT,
            symbol="MON/USDT",
            data={"tp_level": 5, "pnl_pct": 9.3},
            timestamp_ms=1,
        )
    ]

    actions = engine._process_risk_events(events, state)

    assert len(actions) == 1
    assert actions[0]["symbol"] == "MON/USDT"
    assert actions[0]["action"] == "close"
    assert actions[0]["reason"] == "take_profit_5%"
