from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from configs.schema import AppConfig
from src.core.models import Order
from src.core.pipeline import V5Pipeline
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.position_store import Position


NOW = datetime(2026, 9, 5, 8, tzinfo=timezone.utc)
ENTRY_TS = "2026-09-05T07:00:00Z"


def _position() -> Position:
    return Position(
        symbol="BTC/USDT",
        qty=1.0,
        avg_px=100.0,
        entry_ts=ENTRY_TS,
        highest_px=100.0,
        last_update_ts=ENTRY_TS,
        last_mark_px=100.0,
        unrealized_pnl_pct=0.0,
        tags_json=json.dumps(
            {
                "swing_hold_position": True,
                "swing_entry_ts": ENTRY_TS,
                "swing_min_hold_hours": 24.0,
            }
        ),
    )


def _pipeline() -> V5Pipeline:
    pipe = object.__new__(V5Pipeline)
    pipe.cfg = AppConfig(symbols=["BTC/USDT"])
    pipe._swing_position_meta = lambda position, probe_state: json.loads(position.tags_json)
    return pipe


def _live_engine() -> LiveExecutionEngine:
    engine = object.__new__(LiveExecutionEngine)
    engine.cfg = AppConfig(symbols=["BTC/USDT"]).execution
    engine.position_store = SimpleNamespace(get=lambda _symbol: _position())
    return engine


def _close_order(reason: str, hold_hours: float = 1.0) -> Order:
    return Order(
        symbol="BTC/USDT",
        side="sell",
        intent="CLOSE_LONG",
        notional_usdt=100.0,
        signal_price=100.0,
        meta={"reason": reason, "hold_hours": hold_hours},
    )


@pytest.mark.parametrize(
    "reason", ["zero_target_close", "normal_zero_target_close", "rank_exit_dropped"]
)
def test_ranking_or_zero_target_cannot_override_holding_contract(reason: str) -> None:
    pipe = _pipeline()
    decision = pipe._swing_min_hold_guard_decision(
        position=_position(),
        probe_state={},
        now_utc=NOW,
        blocked_exit_reason=reason,
        target_w=0.0,
    )
    assert decision is not None
    assert decision["exit_priority"] == "soft"
    assert decision["hard_exit_exception_reason"] == ""
    assert decision["blocked_exit_reason"] == reason

    live_decision = _live_engine()._swing_min_hold_live_block_context(_close_order(reason))
    assert live_decision is not None
    assert live_decision["exit_priority"] == "soft"
    assert live_decision["hard_exit_exception_reason"] == ""


@pytest.mark.parametrize(
    "reason",
    [
        "hard_stop_loss",
        "max_loss_hard_stop",
        "fixed_stop_loss",
        "stop_loss",
        "dynamic_stop_loss",
        "risk_off",
        "risk_off_forced_close",
        "risk_off_zero_target_close",
        "manual_close",
        "kill_switch",
        "emergency_close",
        "position_reconcile_force_close",
        "exchange_risk",
    ],
)
def test_risk_and_operator_exits_keep_priority_over_minimum_holding(reason: str) -> None:
    assert _pipeline()._swing_min_hold_guard_decision(
        position=_position(),
        probe_state={},
        now_utc=NOW,
        blocked_exit_reason=reason,
        target_w=0.0,
    ) is None
    assert _live_engine()._swing_min_hold_live_block_context(_close_order(reason)) is None


def test_zero_target_close_is_allowed_after_holding_period() -> None:
    assert _pipeline()._swing_min_hold_guard_decision(
        position=_position(),
        probe_state={},
        now_utc=datetime(2026, 9, 6, 7, tzinfo=timezone.utc),
        blocked_exit_reason="zero_target_close",
        target_w=0.0,
    ) is None
    assert _live_engine()._swing_min_hold_live_block_context(
        _close_order("zero_target_close", hold_hours=24.0)
    ) is None


def test_risk_off_zero_target_does_not_wait_for_minimum_holding() -> None:
    assert _pipeline()._swing_min_hold_guard_decision(
        position=_position(),
        probe_state={},
        now_utc=NOW,
        blocked_exit_reason="zero_target_close",
        target_w=0.0,
        is_risk_off_close_only=True,
    ) is None


@pytest.mark.parametrize("level", ["ATTACK", "NEUTRAL", "DEFENSE", "PROTECT", "NORMAL"])
def test_normal_entry_holding_contract_survives_risk_level_changes(level: str) -> None:
    pipe = _pipeline()
    signal = {
        "side": "buy",
        "score": 0.60,
        "metadata": {
            "raw_factors": {"f4_volume_expansion": 0.60, "f5_rsi_trend_confirm": 0.50}
        },
    }
    meta = pipe._normal_entry_swing_hold_meta(
        symbol="BTC/USDT",
        strategy_signal_lookup={"Alpha6Factor": {"BTC/USDT": signal}},
        current_auto_risk_level=level,
        now_utc=NOW,
    )
    assert meta is not None
    assert meta["swing_hold_position"] is True
    assert meta["current_level"] == level
