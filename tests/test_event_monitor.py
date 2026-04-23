from __future__ import annotations

from src.execution.event_monitor import EventMonitor, EventMonitorConfig
from src.execution.event_types import MarketState


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
