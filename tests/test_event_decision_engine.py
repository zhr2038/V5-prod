from __future__ import annotations

import json
import logging

from src.execution.cooldown_manager import CooldownConfig, CooldownManager
from src.execution.event_decision_engine import EventDecisionEngine
from src.execution.event_monitor import EventMonitor, EventMonitorConfig
from src.execution.event_types import EventType, MarketState, SignalState, TradingEvent


def _heartbeat_engine(tmp_path):
    monitor = EventMonitor(
        EventMonitorConfig(
            heartbeat_interval_hours=0,
            state_path=str(tmp_path / "event_monitor_state.json"),
        )
    )
    monitor.last_trade_time_ms = 1
    cooldown = CooldownManager(
        CooldownConfig(
            global_cooldown_p3_seconds=3600,
            state_path=str(tmp_path / "cooldown_state.json"),
        )
    )
    return EventDecisionEngine(monitor, cooldown)


def _state() -> MarketState:
    return MarketState(
        timestamp_ms=1000,
        regime="SIDEWAYS",
        prices={"BTC/USDT": 100.0},
        positions={},
        signals={
            "BTC/USDT": SignalState(
                symbol="BTC/USDT",
                direction="buy",
                score=0.2,
                rank=1,
                timestamp_ms=1000,
            )
        },
        selected_symbols=["BTC/USDT"],
    )


def test_deferred_heartbeat_action_does_not_record_cooldown_or_trade_time(tmp_path) -> None:
    engine = _heartbeat_engine(tmp_path)

    result = engine.run(_state(), commit_execution_state=False)

    assert result.should_trade is True
    assert result.actions[0]["reason"] == "heartbeat_entry"
    assert engine.cooldown.last_global_trade_ms == 0
    assert engine.cooldown.last_symbol_trade_ms == {}
    assert engine.monitor.last_trade_time_ms == 1


def test_commit_actions_records_heartbeat_cooldown_and_trade_time(tmp_path) -> None:
    engine = _heartbeat_engine(tmp_path)
    result = engine.run(_state(), commit_execution_state=False)

    engine.commit_actions(result.actions)

    assert engine.cooldown.last_global_trade_ms > 1
    assert engine.cooldown.last_symbol_trade_ms["BTC/USDT"] > 1
    assert engine.monitor.last_trade_time_ms > 1


def test_cooldown_manager_prunes_expired_pending_signals_on_load(tmp_path) -> None:
    state_path = tmp_path / "cooldown_state.json"
    state_path.write_text(
        json.dumps(
            {
                "last_global_trade_ms": 0,
                "symbol_cooldowns": {},
                "pending_signals": {
                    "OLD/USDT": {
                        "signal": {"direction": "buy", "score": 0.2},
                        "count": 1,
                        "first_seen_ms": 1,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    manager = CooldownManager(
        CooldownConfig(
            pending_signal_max_age_seconds=1,
            state_path=str(state_path),
        )
    )

    assert manager.pending_signals == {}
    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["pending_signals"] == {}


def test_expired_pending_signal_does_not_confirm_immediately(tmp_path) -> None:
    manager = CooldownManager(
        CooldownConfig(
            signal_confirmation_periods=2,
            pending_signal_max_age_seconds=1,
            state_path=str(tmp_path / "cooldown_state.json"),
        )
    )
    manager.pending_signals["BTC/USDT"] = {
        "signal": {"direction": "buy", "score": 0.2},
        "count": 1,
        "first_seen_ms": 1,
    }

    confirmed = manager.check_signal_confirmation(
        "BTC/USDT",
        {"direction": "buy", "score": 0.2},
    )

    assert confirmed is False
    assert manager.pending_signals["BTC/USDT"]["count"] == 1


def test_risk_off_without_positions_does_not_warn_closing_positions(tmp_path, caplog) -> None:
    monitor = EventMonitor(EventMonitorConfig(state_path=str(tmp_path / "event_monitor_state.json")))
    cooldown = CooldownManager(CooldownConfig(state_path=str(tmp_path / "cooldown_state.json")))
    engine = EventDecisionEngine(monitor, cooldown)
    state = MarketState(
        timestamp_ms=1_000,
        regime="RISK_OFF",
        prices={},
        positions={},
        signals={},
        selected_symbols=[],
    )
    event = TradingEvent(type=EventType.REGIME_RISK_OFF, symbol=None, data={"regime": "RISK_OFF"})

    caplog.set_level(logging.WARNING)
    actions = engine._process_risk_events([event], state)

    assert actions == []
    assert not any("Closing all positions" in record.getMessage() for record in caplog.records)


def test_risk_off_suppresses_confirmed_signal_open_events(tmp_path) -> None:
    monitor = EventMonitor(EventMonitorConfig(state_path=str(tmp_path / "event_monitor_state.json")))
    monitor.last_state = MarketState(
        timestamp_ms=1_000,
        regime="SIDEWAYS",
        prices={"BTC/USDT": 100.0},
        positions={},
        signals={},
        selected_symbols=[],
    )
    signal = SignalState(
        symbol="BTC/USDT",
        direction="buy",
        score=0.2,
        rank=1,
        timestamp_ms=2_000,
    )
    cooldown = CooldownManager(
        CooldownConfig(
            signal_confirmation_periods=2,
            state_path=str(tmp_path / "cooldown_state.json"),
        )
    )
    cooldown.pending_signals["BTC/USDT"] = {
        "signal": signal.to_dict(),
        "count": 1,
        "first_seen_ms": 1,
    }
    engine = EventDecisionEngine(monitor, cooldown)
    state = MarketState(
        timestamp_ms=2_000,
        regime="RISK_OFF",
        prices={"BTC/USDT": 100.0},
        positions={},
        signals={"BTC/USDT": signal},
        selected_symbols=["BTC/USDT"],
        suppress_entry_events=True,
    )

    result = engine.run(state, commit_execution_state=False)

    assert result.should_trade is False
    assert result.actions == []
    assert result.reason == "no_actionable_events"
    assert any(event.type == EventType.REGIME_RISK_OFF for event in engine.last_events)
    assert not any(event.type == EventType.NEW_ENTRY for event in engine.last_events)
