from __future__ import annotations

from pathlib import Path

from src.execution.event_action_bridge import consume_event_actions_for_run, persist_event_actions


def test_persist_and_consume_event_actions_for_matching_run(tmp_path: Path) -> None:
    action_path = tmp_path / "event_driven_actions.json"

    saved = persist_event_actions(
        actions=[
            {"symbol": "MON/USDT", "action": "close", "reason": "take_profit_5%", "priority": 0},
            {"symbol": "BTC/USDT", "action": "open", "reason": "signal_rank_jump", "priority": 2},
        ],
        target_run_id="20260403_15",
        path=str(action_path),
        generated_at_ms=1_700_000_000_000,
    )

    assert saved is True
    actions = consume_event_actions_for_run(
        run_id="20260403_15",
        path=str(action_path),
        max_age_minutes=10_000_000,
    )

    assert actions == [
        {
            "symbol": "MON/USDT",
            "action": "close",
            "reason": "take_profit_5%",
            "priority": 0,
            "event_type": "",
        }
    ]
    assert action_path.exists() is False


def test_consume_event_actions_ignores_other_run(tmp_path: Path) -> None:
    action_path = tmp_path / "event_driven_actions.json"
    persist_event_actions(
        actions=[{"symbol": "MON/USDT", "action": "close", "reason": "take_profit_5%", "priority": 0}],
        target_run_id="20260403_15",
        path=str(action_path),
        generated_at_ms=1_700_000_000_000,
    )

    actions = consume_event_actions_for_run(
        run_id="20260403_16",
        path=str(action_path),
        max_age_minutes=10_000_000,
    )

    assert actions == []
    assert action_path.exists() is True


def test_event_actions_use_runtime_path_when_order_store_path_is_provided(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    runtime_action_path = reports_dir / "shadow_event_driven_actions.json"
    root_action_path = reports_dir / "event_driven_actions.json"

    saved = persist_event_actions(
        actions=[{"symbol": "MON/USDT", "action": "close", "reason": "take_profit_5%", "priority": 0}],
        target_run_id="20260403_15",
        order_store_path=reports_dir / "shadow_orders.sqlite",
        generated_at_ms=1_700_000_000_000,
    )

    assert saved is True
    assert runtime_action_path.exists() is True
    assert root_action_path.exists() is False

    actions = consume_event_actions_for_run(
        run_id="20260403_15",
        order_store_path=reports_dir / "shadow_orders.sqlite",
        max_age_minutes=10_000_000,
    )

    assert actions == [
        {
            "symbol": "MON/USDT",
            "action": "close",
            "reason": "take_profit_5%",
            "priority": 0,
            "event_type": "",
        }
    ]
    assert runtime_action_path.exists() is False
