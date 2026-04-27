from __future__ import annotations

from src.execution.cooldown_manager import CooldownConfig, CooldownManager
from src.execution.event_decision_engine import EventDecisionEngine
from src.execution.event_monitor import EventMonitor, EventMonitorConfig
from src.execution.event_types import MarketState, SignalState


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
