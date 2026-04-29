from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

from configs.schema import ExecutionConfig
from src.core.models import Order
from src.execution.event_decision_engine import EventDecisionEngine
from src.execution.event_monitor import EventMonitor, EventMonitorConfig
from src.execution.event_types import EventType, MarketState, SignalState, TradingEvent
from src.execution.fill_reconciler import FillReconciler
from src.execution.fill_store import FillRow, FillStore
from src.execution.highest_px_tracker import HighestPriceTracker
from src.execution import live_execution_engine
from src.execution.live_execution_engine import LiveExecutionEngine, SafetyReject
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore
from src.risk.auto_risk_guard import AutoRiskGuard


class FakeCooldown:
    def __init__(self) -> None:
        self.confirm_checks = 0
        self.recorded: list[str] = []
        self.cleared: list[str] = []

    def can_trade(self, symbol, priority) -> bool:
        return True

    def check_signal_confirmation(self, symbol, signal) -> bool:
        self.confirm_checks += 1
        return False

    def record_trade(self, symbol, priority) -> None:
        self.recorded.append(str(symbol))

    def clear_pending_signal(self, symbol) -> None:
        self.cleared.append(str(symbol))


class FakeOKX:
    place_calls = 0

    def place_order(self, payload, exp_time_ms=None):
        self.place_calls += 1
        return SimpleNamespace(data={"code": "0", "data": [{"ordId": "1", "clOrdId": payload.get("clOrdId")}]})


def _monitor(tmp_path) -> EventMonitor:
    return EventMonitor(EventMonitorConfig(state_path=str(tmp_path / "event_monitor_state.json")))


def test_event_monitor_uses_last_state_signals_even_if_stale_signatures_exists(tmp_path) -> None:
    monitor = _monitor(tmp_path)
    last = MarketState(
        timestamp_ms=1,
        regime="SIDEWAYS",
        signals={"BTC/USDT": SignalState("BTC/USDT", "buy", 0.8, 1, 1)},
        selected_symbols=["BTC/USDT"],
    )
    setattr(last, "signatures", {"BTC/USDT": SignalState("BTC/USDT", "hold", 0.1, 9, 1)})
    monitor.last_state = last

    events = monitor._check_signal_events(
        MarketState(
            timestamp_ms=2,
            regime="SIDEWAYS",
            signals={"BTC/USDT": SignalState("BTC/USDT", "sell", 0.7, 1, 2)},
            selected_symbols=["BTC/USDT"],
        )
    )

    flip = next(event for event in events if event.type == EventType.SIGNAL_DIRECTION_FLIP)
    assert flip.data["from_direction"] == "buy"
    assert flip.data["to_direction"] == "sell"


def test_event_monitor_skips_invalid_entry_stop_loss_and_missing_rank_signal(tmp_path) -> None:
    monitor = _monitor(tmp_path)

    events = monitor._check_risk_events(
        MarketState(
            timestamp_ms=1,
            regime="SIDEWAYS",
            prices={"BTC/USDT": 80.0, "ETH/USDT": 100.0},
            positions={
                "BTC/USDT": {"entry_price": 0.0, "fixed_stop_price": 90.0},
                "ETH/USDT": {"entry_price": 100.0},
            },
            signals={},
        )
    )

    assert all(event.type != EventType.RISK_STOP_LOSS for event in events)
    assert all(event.type != EventType.RISK_RANK_EXIT for event in events)


def test_signal_direction_flip_exit_bypasses_confirmation(tmp_path) -> None:
    cooldown = FakeCooldown()
    engine = EventDecisionEngine(_monitor(tmp_path), cooldown)
    event = TradingEvent(type=EventType.SIGNAL_DIRECTION_FLIP, symbol="BTC/USDT")
    state = MarketState(
        timestamp_ms=1,
        regime="SIDEWAYS",
        positions={"BTC/USDT": {"qty": 1.0}},
        signals={"BTC/USDT": SignalState("BTC/USDT", "sell", 0.9, 1, 1)},
    )

    actions, blocked = engine._process_signal_events([event], state)

    assert blocked == 0
    assert cooldown.confirm_checks == 0
    assert actions == [
        {
            "symbol": "BTC/USDT",
            "action": "close",
            "reason": "signal_direction_flip",
            "priority": 2,
            "score": 0.9,
            "event_type": "SIGNAL_DIRECTION_FLIP",
        }
    ]


def test_auto_risk_guard_accepts_none_conversion_rate_and_persists_losses(tmp_path) -> None:
    guard = AutoRiskGuard(state_path=str(tmp_path / "auto_risk_guard.json"))

    level, _, _ = guard.evaluate(
        dd_pct=0.0,
        conversion_rate=None,
        dust_reject_rate=0.6,
        recent_pnl_trend="flat",
        consecutive_losses=2,
    )

    assert level == "DEFENSE"
    assert guard.metrics["last_conversion_rate"] == 0.0
    assert guard.metrics["consecutive_loss_rounds"] == 2
    assert guard._is_lower_level("TYPO_LEVEL", "NEUTRAL") is True


def test_highest_price_tracker_ignores_legacy_extra_fields(tmp_path) -> None:
    state_path = tmp_path / "highest_px_state.json"
    state_path.write_text(
        json.dumps(
            {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "highest_px": 105.0,
                    "entry_px": 100.0,
                    "updated_at": "2026-04-29T00:00:00",
                    "source": "trade",
                    "legacy_field": "ignored",
                }
            }
        ),
        encoding="utf-8",
    )

    tracker = HighestPriceTracker(state_path=str(state_path))

    assert tracker.get_highest_px("BTC/USDT") == pytest.approx(105.0)


def test_live_execution_internal_value_error_is_not_marked_safety(monkeypatch, tmp_path) -> None:
    okx = FakeOKX()
    store = OrderStore(path=str(tmp_path / "orders.sqlite"))
    pos = PositionStore(path=str(tmp_path / "positions.sqlite"))
    cfg = ExecutionConfig(
        reconcile_status_path=str(tmp_path / "reconcile_status.json"),
        kill_switch_path=str(tmp_path / "kill_switch.json"),
    )
    engine = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")

    monkeypatch.setattr(live_execution_engine, "_public_mid_at_submit", lambda **kwargs: None)

    def raise_internal(self, order, *, inst_id, cl_ord_id):
        raise ValueError("internal bug")

    monkeypatch.setattr(LiveExecutionEngine, "_build_place_payload", raise_internal)

    with pytest.raises(ValueError, match="internal bug"):
        engine.place(
            Order(
                symbol="BTC/USDT",
                side="buy",
                intent="OPEN_LONG",
                notional_usdt=10.0,
                signal_price=100.0,
                meta={"decision_hash": "internal"},
            )
        )
    assert okx.place_calls == 0


def test_live_execution_safety_reject_still_persists_rejected_order(monkeypatch, tmp_path) -> None:
    okx = FakeOKX()
    store = OrderStore(path=str(tmp_path / "orders.sqlite"))
    pos = PositionStore(path=str(tmp_path / "positions.sqlite"))
    cfg = ExecutionConfig(
        reconcile_status_path=str(tmp_path / "reconcile_status.json"),
        kill_switch_path=str(tmp_path / "kill_switch.json"),
    )
    engine = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")

    monkeypatch.setattr(live_execution_engine, "_public_mid_at_submit", lambda **kwargs: None)

    def raise_safety(self, order, *, inst_id, cl_ord_id):
        raise SafetyReject("NO_BORROW_BUY_BLOCK")

    monkeypatch.setattr(LiveExecutionEngine, "_build_place_payload", raise_safety)

    result = engine.place(
        Order(
            symbol="BTC/USDT",
            side="buy",
            intent="OPEN_LONG",
            notional_usdt=10.0,
            signal_price=100.0,
            meta={"decision_hash": "safety"},
        )
    )

    row = store.get(result.cl_ord_id)
    assert result.state == "REJECTED"
    assert row is not None
    assert row.last_error_code == "SAFETY"
    assert okx.place_calls == 0


def test_open_long_cooldown_handles_missing_updated_ts(monkeypatch, tmp_path) -> None:
    okx = FakeOKX()
    store = OrderStore(path=str(tmp_path / "orders.sqlite"))
    pos = PositionStore(path=str(tmp_path / "positions.sqlite"))
    cfg = ExecutionConfig(
        reconcile_status_path=str(tmp_path / "reconcile_status.json"),
        kill_switch_path=str(tmp_path / "kill_switch.json"),
        open_long_cooldown_minutes=10,
    )
    engine = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")

    created_ts = int(time.time() * 1000)
    monkeypatch.setattr(
        store,
        "get_latest_filled",
        lambda **kwargs: SimpleNamespace(
            updated_ts=None,
            created_ts=created_ts,
            cl_ord_id="prev",
            run_id="prev-run",
        ),
    )

    result = engine.place(
        Order(
            symbol="BTC/USDT",
            side="buy",
            intent="OPEN_LONG",
            notional_usdt=10.0,
            signal_price=100.0,
            meta={"decision_hash": "cooldown"},
        )
    )

    row = store.get(result.cl_ord_id)
    assert result.state == "REJECTED"
    assert row is not None
    assert row.last_error_code == "OPEN_LONG_COOLDOWN"
    assert json.loads(row.req_json)["latest_filled_updated_ts"] == 0


def test_fill_reconciler_sell_negative_base_fee_reduces_extra_base(tmp_path) -> None:
    store = OrderStore(path=str(tmp_path / "orders.sqlite"))
    positions = PositionStore(path=str(tmp_path / "positions.sqlite"))
    fills = FillStore(path=str(tmp_path / "fills.sqlite"))
    positions.upsert_buy("BTC/USDT", qty=2.0, px=100.0)

    store.upsert_new(
        cl_ord_id="sell-1",
        run_id="r",
        inst_id="BTC-USDT",
        side="sell",
        intent="REBALANCE",
        decision_hash="h",
        td_mode="cash",
        ord_type="market",
        notional_usdt=40.0,
        req={},
    )
    store.update_state("sell-1", new_state="OPEN", ord_id="1001")
    fills.upsert_many(
        [
            FillRow(
                inst_id="BTC-USDT",
                trade_id="fill-1",
                ts_ms=1,
                ord_id="1001",
                cl_ord_id="sell-1",
                side="sell",
                fill_px="100",
                fill_sz="0.4",
                fee="-0.01",
                fee_ccy="BTC",
            )
        ]
    )

    FillReconciler(fill_store=fills, order_store=store, okx=None, position_store=positions).reconcile()

    position = positions.get("BTC/USDT")
    assert position is not None
    assert position.qty == pytest.approx(1.59)
