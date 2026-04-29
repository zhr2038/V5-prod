from __future__ import annotations

import logging

from src.execution.event_monitor import EventMonitor, EventMonitorConfig
from src.execution.event_types import EventType, MarketState, SignalState


def test_record_price_snapshot_updates_matching_timestamp_even_when_history_is_unsorted(tmp_path) -> None:
    state_path = tmp_path / "reports" / "event_monitor_state.json"
    monitor = EventMonitor(EventMonitorConfig(state_path=str(state_path)))
    monitor.price_history = {
        "BTC/USDT": [
            {"timestamp_ms": 2_000, "price": 20.0},
            {"timestamp_ms": 1_000, "price": 10.0},
        ]
    }

    state = MarketState(
        timestamp_ms=2_000,
        regime="SIDEWAYS",
        prices={"BTC/USDT": 30.0},
        positions={},
        signals={},
        selected_symbols=[],
    )

    monitor._record_price_snapshot(state, now_ms=2_000)

    samples = monitor.price_history["BTC/USDT"]
    assert len(samples) == 2
    assert samples[0]["timestamp_ms"] == 2_000
    assert samples[0]["price"] == 30.0


def test_record_price_snapshot_deduplicates_matching_timestamp_entries(tmp_path) -> None:
    state_path = tmp_path / "reports" / "event_monitor_state.json"
    monitor = EventMonitor(EventMonitorConfig(state_path=str(state_path)))
    monitor.price_history = {
        "BTC/USDT": [
            {"timestamp_ms": 2_000, "price": 20.0},
            {"timestamp_ms": 1_000, "price": 10.0},
            {"timestamp_ms": 2_000, "price": 5.0},
        ]
    }

    state = MarketState(
        timestamp_ms=2_000,
        regime="SIDEWAYS",
        prices={"BTC/USDT": 30.0},
        positions={},
        signals={},
        selected_symbols=[],
    )

    monitor._record_price_snapshot(state, now_ms=2_000)

    samples = monitor.price_history["BTC/USDT"]
    assert len(samples) == 2
    assert [sample["timestamp_ms"] for sample in samples] == [2_000, 1_000]
    assert samples[0]["price"] == 30.0


def test_zero_based_rank_does_not_emit_repeated_rank_jump(tmp_path) -> None:
    state_path = tmp_path / "reports" / "event_monitor_state.json"
    monitor = EventMonitor(EventMonitorConfig(rank_jump_threshold=3, state_path=str(state_path)))
    monitor.last_state = MarketState(
        timestamp_ms=1_000,
        regime="SIDEWAYS",
        prices={},
        positions={},
        signals={"ETH/USDT": SignalState("ETH/USDT", "sell", 0.12, 0, 1_000)},
        selected_symbols=["ETH/USDT"],
    )
    current = MarketState(
        timestamp_ms=2_000,
        regime="SIDEWAYS",
        prices={},
        positions={},
        signals={"ETH/USDT": SignalState("ETH/USDT", "sell", 0.12, 0, 2_000)},
        selected_symbols=["ETH/USDT"],
    )

    events = monitor._check_signal_events(current)

    assert not any(event.type == EventType.SIGNAL_RANK_JUMP for event in events)


def test_risk_off_without_positions_does_not_warn_clearing_positions(tmp_path, caplog) -> None:
    monitor = EventMonitor(EventMonitorConfig(state_path=str(tmp_path / "event_monitor_state.json")))
    state = MarketState(
        timestamp_ms=1_000,
        regime="RISK_OFF",
        prices={},
        positions={},
        signals={},
        selected_symbols=[],
    )

    caplog.set_level(logging.WARNING)
    events = monitor._check_risk_events(state)

    assert [event.type for event in events] == [EventType.REGIME_RISK_OFF]
    assert not any("clearing positions" in record.getMessage() for record in caplog.records)


def test_risk_off_collect_events_does_not_emit_heartbeat(tmp_path) -> None:
    monitor = EventMonitor(
        EventMonitorConfig(
            heartbeat_interval_hours=0,
            state_path=str(tmp_path / "event_monitor_state.json"),
        )
    )
    monitor.last_trade_time_ms = 1
    state = MarketState(
        timestamp_ms=1_000,
        regime="RISK_OFF",
        prices={},
        positions={},
        signals={},
        selected_symbols=[],
    )

    events = monitor.collect_events(state)

    assert [event.type for event in events] == [EventType.REGIME_RISK_OFF]


def test_risk_off_flat_collect_events_suppresses_signal_and_breakout_noise(tmp_path) -> None:
    monitor = EventMonitor(
        EventMonitorConfig(
            breakout_threshold_pct=0.3,
            state_path=str(tmp_path / "event_monitor_state.json"),
        )
    )
    monitor.last_state = MarketState(
        timestamp_ms=1_000,
        regime="RISK_OFF",
        prices={"BTC/USDT": 100.0},
        positions={},
        signals={},
        selected_symbols=[],
    )
    monitor.price_history = {"ETH/USDT": [{"timestamp_ms": 1_000, "price": 100.0}]}
    state = MarketState(
        timestamp_ms=2_000,
        regime="RISK_OFF",
        prices={"ETH/USDT": 99.0},
        positions={},
        signals={"BTC/USDT": SignalState("BTC/USDT", "buy", 0.5, 1, 2_000)},
        selected_symbols=["BTC/USDT"],
        suppress_entry_events=True,
    )

    events = monitor.collect_events(state)

    assert [event.type for event in events] == [EventType.REGIME_RISK_OFF]
    assert {"timestamp_ms": 2_000, "price": 99.0} in monitor.price_history["ETH/USDT"]


def test_risk_off_flat_without_suppress_flag_keeps_entry_events(tmp_path) -> None:
    monitor = EventMonitor(
        EventMonitorConfig(
            breakout_threshold_pct=0.3,
            state_path=str(tmp_path / "event_monitor_state.json"),
        )
    )
    monitor.last_state = MarketState(
        timestamp_ms=1_000,
        regime="RISK_OFF",
        prices={"BTC/USDT": 100.0},
        positions={},
        signals={},
        selected_symbols=[],
    )
    state = MarketState(
        timestamp_ms=2_000,
        regime="RISK_OFF",
        prices={"BTC/USDT": 101.0},
        positions={},
        signals={"BTC/USDT": SignalState("BTC/USDT", "buy", 0.5, 1, 2_000)},
        selected_symbols=["BTC/USDT"],
        suppress_entry_events=False,
    )

    events = monitor.collect_events(state)

    assert any(event.type == EventType.REGIME_RISK_OFF for event in events)
    assert any(event.type == EventType.NEW_ENTRY for event in events)
