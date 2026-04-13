from __future__ import annotations

from deploy.prod_release import PRODUCTION_USER_UNIT_MAPPINGS
from src.execution.event_monitor import EventMonitor, EventMonitorConfig
from src.execution.event_types import EventType, MarketState


def test_production_unit_mappings_include_trade_monitor() -> None:
    mappings = dict(PRODUCTION_USER_UNIT_MAPPINGS)

    assert mappings["v5-trade-monitor.service"] == "v5-trade-monitor.service"
    assert mappings["v5-trade-monitor.timer"] == "v5-trade-monitor.timer"


def test_event_monitor_stop_loss_handles_missing_entry_price(tmp_path) -> None:
    monitor = EventMonitor(
        EventMonitorConfig(
            state_path=str(tmp_path / "event_monitor_state.json"),
        )
    )

    state = MarketState(
        timestamp_ms=1_710_000_000_000,
        regime="SIDEWAYS",
        prices={"ENJ/USDT": 1.07},
        positions={
            "ENJ/USDT": {
                "current_stop": 1.08,
            }
        },
        signals={},
        selected_symbols=[],
    )

    events = monitor.collect_events(state)
    stop_events = [event for event in events if event.type == EventType.RISK_STOP_LOSS]

    assert len(stop_events) == 1
    assert stop_events[0].data["entry_price"] == 0.0
    assert stop_events[0].data["loss_pct"] == 0.0
