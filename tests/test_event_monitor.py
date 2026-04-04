from __future__ import annotations

from src.execution.event_monitor import EventMonitor, EventMonitorConfig
from src.execution.event_types import EventType, MarketState


def _build_state(timestamp_ms: int, price: float) -> MarketState:
    return MarketState(
        timestamp_ms=timestamp_ms,
        regime="SIDEWAYS",
        prices={"BTC/USDT": price},
        positions={},
        signals={},
        selected_symbols=[],
    )


def test_breakout_uses_prior_window_high_and_threshold(tmp_path) -> None:
    monitor = EventMonitor(
        EventMonitorConfig(
            breakout_lookback_hours=24,
            breakout_threshold_pct=0.3,
            state_path=str(tmp_path / "event_monitor_state.json"),
        )
    )

    base_ts = 1_710_000_000_000
    assert monitor.collect_events(_build_state(base_ts, 100.0)) == []

    near_high_events = monitor.collect_events(_build_state(base_ts + 15 * 60 * 1000, 100.2))
    assert [event for event in near_high_events if event.type == EventType.BREAKOUT_UP] == []

    breakout_events = monitor.collect_events(_build_state(base_ts + 30 * 60 * 1000, 100.6))
    breakout_up = [event for event in breakout_events if event.type == EventType.BREAKOUT_UP]

    assert len(breakout_up) == 1
    assert breakout_up[0].data["resistance"] == 100.2


def test_breakout_lookback_expires_stale_highs(tmp_path) -> None:
    monitor = EventMonitor(
        EventMonitorConfig(
            breakout_lookback_hours=1,
            breakout_threshold_pct=0.3,
            state_path=str(tmp_path / "event_monitor_state.json"),
        )
    )

    base_ts = 1_710_000_000_000
    assert monitor.collect_events(_build_state(base_ts, 100.0)) == []

    # Two hours later the original 100.0 high must no longer be used.
    stale_window_events = monitor.collect_events(_build_state(base_ts + 2 * 3600 * 1000, 99.0))
    assert [event for event in stale_window_events if event.type == EventType.BREAKOUT_UP] == []

    breakout_events = monitor.collect_events(_build_state(base_ts + 2 * 3600 * 1000 + 15 * 60 * 1000, 99.4))
    breakout_up = [event for event in breakout_events if event.type == EventType.BREAKOUT_UP]

    assert len(breakout_up) == 1
    assert breakout_up[0].data["resistance"] == 99.0
