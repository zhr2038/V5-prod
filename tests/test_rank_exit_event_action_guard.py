from __future__ import annotations

import time
from pathlib import Path

import pytest

import main as main_module
from configs.schema import AppConfig
from src.execution.event_action_bridge import persist_event_actions
from src.execution.position_store import Position
from src.reporting.decision_audit import DecisionAudit


def _position(symbol: str = "BNB/USDT") -> Position:
    return Position(
        symbol=symbol,
        qty=1.0,
        avg_px=628.4,
        entry_ts="2026-05-04T07:00:00Z",
        highest_px=628.4,
        last_update_ts="2026-05-04T13:00:00Z",
        last_mark_px=622.0,
        unrealized_pnl_pct=-0.01,
    )


def test_stale_rank_exit_event_action_skips_when_current_target_is_positive(tmp_path: Path) -> None:
    order_store_path = tmp_path / "orders.sqlite"
    now_ms = int(time.time() * 1000)
    window_start_ts = int(now_ms / 1000) + 60
    persist_event_actions(
        actions=[{"symbol": "BNB/USDT", "action": "close", "reason": "rank_exit_4", "priority": 0}],
        target_run_id="run-1",
        order_store_path=order_store_path,
        generated_at_ms=now_ms,
    )
    cfg = AppConfig(symbols=["BTC/USDT", "BNB/USDT"])
    cfg.execution.rank_exit_require_zero_target = True
    audit = DecisionAudit(run_id="run-1")
    audit.top_scores = [{"symbol": "BTC/USDT", "rank": 1}, {"symbol": "BNB/USDT", "rank": 2}]

    orders = main_module._merge_event_close_override_orders(
        orders=[],
        positions=[_position()],
        prices={"BNB/USDT": 622.0},
        run_id="run-1",
        order_store_path=order_store_path,
        cfg=cfg,
        targets_post_risk={"BNB/USDT": 0.15},
        window_start_ts=window_start_ts,
        audit=audit,
    )

    assert orders == []
    decision = next(d for d in audit.router_decisions if d.get("symbol") == "BNB/USDT")
    assert decision["action"] == "skip"
    assert decision["reason"] == "rank_exit_target_still_positive"
    assert decision["target_w"] == pytest.approx(0.15)
    assert decision["rank"] == 2
    assert decision["external_rank_exit_action_consumed"] is True
    assert decision["validation_result"] == "blocked_target_still_positive"


def test_external_rank_exit_event_action_requires_current_rank_to_exceed_threshold(tmp_path: Path) -> None:
    order_store_path = tmp_path / "orders.sqlite"
    now_ms = int(time.time() * 1000)
    persist_event_actions(
        actions=[{"symbol": "BNB/USDT", "action": "close", "reason": "rank_exit_4", "priority": 0}],
        target_run_id="run-1",
        order_store_path=order_store_path,
        generated_at_ms=now_ms,
    )
    cfg = AppConfig(symbols=["BTC/USDT", "BNB/USDT"])
    cfg.execution.rank_exit_max_rank = 5
    cfg.execution.rank_exit_buffer_positions = 0
    audit = DecisionAudit(run_id="run-1")
    audit.top_scores = [{"symbol": "BTC/USDT", "rank": 1}, {"symbol": "BNB/USDT", "rank": 4}]

    orders = main_module._merge_event_close_override_orders(
        orders=[],
        positions=[_position()],
        prices={"BNB/USDT": 622.0},
        run_id="run-1",
        order_store_path=order_store_path,
        cfg=cfg,
        targets_post_risk={"BNB/USDT": 0.0},
        window_start_ts=int(now_ms / 1000) - 60,
        audit=audit,
    )

    assert orders == []
    decision = next(d for d in audit.router_decisions if d.get("symbol") == "BNB/USDT")
    assert decision["action"] == "skip"
    assert decision["reason"] == "external_rank_exit_rank_not_exceeded"
    assert decision["validation_result"] == "current_rank_not_rank_exit"


def test_external_rank_exit_event_action_can_be_consumed_after_validation(tmp_path: Path) -> None:
    order_store_path = tmp_path / "orders.sqlite"
    now_ms = int(time.time() * 1000)
    persist_event_actions(
        actions=[{"symbol": "BNB/USDT", "action": "close", "reason": "rank_exit_6", "priority": 0}],
        target_run_id="run-1",
        order_store_path=order_store_path,
        generated_at_ms=now_ms,
    )
    cfg = AppConfig(symbols=["BTC/USDT", "BNB/USDT"])
    cfg.execution.rank_exit_max_rank = 5
    audit = DecisionAudit(run_id="run-1")
    audit.top_scores = [{"symbol": "BTC/USDT", "rank": 1}, {"symbol": "BNB/USDT", "rank": 6}]

    orders = main_module._merge_event_close_override_orders(
        orders=[],
        positions=[_position()],
        prices={"BNB/USDT": 622.0},
        run_id="run-1",
        order_store_path=order_store_path,
        cfg=cfg,
        targets_post_risk={"BNB/USDT": 0.0},
        window_start_ts=int(now_ms / 1000) - 60,
        audit=audit,
    )

    assert len(orders) == 1
    order = orders[0]
    assert order.symbol == "BNB/USDT"
    assert order.side == "sell"
    assert order.intent == "CLOSE_LONG"
    assert order.meta["reason"] == "rank_exit_6"
    assert order.meta["rank_exit_validated_by_router"] is True
    assert order.meta["validation_result"] == "accepted"
    assert any(
        d.get("action") == "create"
        and d.get("source_reason") == "rank_exit_6"
        and d.get("validation_result") == "accepted"
        for d in audit.router_decisions
    )
    assert audit.exit_signals and audit.exit_signals[0]["reason"] == "rank_exit_6"
