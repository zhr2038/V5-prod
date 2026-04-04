from __future__ import annotations

from pathlib import Path

from main import _merge_event_close_override_orders
from src.core.models import Order
from src.execution.event_action_bridge import persist_event_actions
from src.execution.position_store import PositionStore


def test_event_close_override_appends_close_long(tmp_path: Path, monkeypatch) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    monkeypatch.chdir(tmp_path)

    db_path = reports_dir / "positions.sqlite"
    store = PositionStore(str(db_path))
    store.upsert_buy("MON/USDT", qty=100.0, px=1.0, now_ts="2026-04-03T15:00:00Z")

    persist_event_actions(
        actions=[{"symbol": "MON/USDT", "action": "close", "reason": "take_profit_5%", "priority": 0}],
        target_run_id="20260403_15",
        path=str(reports_dir / "event_driven_actions.json"),
    )

    merged = _merge_event_close_override_orders(
        orders=[],
        positions=store.list(),
        prices={"MON/USDT": 1.2},
        run_id="20260403_15",
        audit=None,
    )

    assert len(merged) == 1
    order = merged[0]
    assert isinstance(order, Order)
    assert order.symbol == "MON/USDT"
    assert order.side == "sell"
    assert order.intent == "CLOSE_LONG"
    assert order.notional_usdt == 120.0
    assert order.meta["source"] == "event_driven_override"


def test_event_close_override_does_not_duplicate_existing_close(tmp_path: Path, monkeypatch) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    monkeypatch.chdir(tmp_path)

    db_path = reports_dir / "positions.sqlite"
    store = PositionStore(str(db_path))
    store.upsert_buy("MON/USDT", qty=100.0, px=1.0, now_ts="2026-04-03T15:00:00Z")

    persist_event_actions(
        actions=[{"symbol": "MON/USDT", "action": "close", "reason": "take_profit_5%", "priority": 0}],
        target_run_id="20260403_15",
        path=str(reports_dir / "event_driven_actions.json"),
    )

    merged = _merge_event_close_override_orders(
        orders=[
            Order(
                symbol="MON/USDT",
                side="sell",
                intent="CLOSE_LONG",
                notional_usdt=120.0,
                signal_price=1.2,
                meta={},
            )
        ],
        positions=store.list(),
        prices={"MON/USDT": 1.2},
        run_id="20260403_15",
        audit=None,
    )

    assert len(merged) == 1
