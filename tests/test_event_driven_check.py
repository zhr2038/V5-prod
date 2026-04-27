from __future__ import annotations

from types import SimpleNamespace

import event_driven_check as edc
from event_driven_check import (
    _build_effective_event_log_values,
    _filter_dust_positions,
    _load_fused_signal_states,
    _load_positions_snapshot,
)
from src.execution.event_action_bridge import clear_event_actions, persist_event_actions
from src.execution.event_driven_integration import EventDrivenConfig, EventDrivenTrader
from src.execution.position_store import PositionStore


def _dust_cfg():
    return SimpleNamespace(
        execution=SimpleNamespace(
            dust_value_threshold=0.5,
            dust_usdt_ignore=1.0,
            reconcile_dust_usdt_ignore=1.0,
            min_trade_value_usdt=10.0,
        ),
        budget=SimpleNamespace(),
    )


def test_load_positions_snapshot_reports_empty_position_store(tmp_path) -> None:
    positions_db = tmp_path / "positions.sqlite"
    PositionStore(path=str(positions_db))

    positions, symbols, source = _load_positions_snapshot(
        positions_db_path=positions_db,
        portfolio_path=tmp_path / "portfolio.json",
    )

    assert positions == {}
    assert symbols == set()
    assert source == "position_store_empty"


def test_filter_dust_positions_drops_residual_notional() -> None:
    positions = {
        "BTC/USDT": {
            "entry_price": 78021.7,
            "quantity": 5.43e-9,
        }
    }

    filtered, dust_symbols = _filter_dust_positions(
        positions,
        {"BTC/USDT": 77630.7},
        _dust_cfg(),
    )

    assert filtered == {}
    assert dust_symbols == {"BTC/USDT"}


def test_filter_dust_positions_keeps_executable_position() -> None:
    positions = {
        "BTC/USDT": {
            "entry_price": 78021.7,
            "quantity": 0.001,
        }
    }

    filtered, dust_symbols = _filter_dust_positions(
        positions,
        {"BTC/USDT": 77630.7},
        _dust_cfg(),
    )

    assert filtered == positions
    assert dust_symbols == set()


def test_load_fused_signal_states_normalizes_zero_based_rank() -> None:
    signals = _load_fused_signal_states(
        {
            "fused": {
                "ETH/USDT": {
                    "symbol": "ETH/USDT",
                    "direction": "sell",
                    "score": 0.12,
                    "rank": 0,
                }
            }
        },
        {"ETH/USDT"},
    )

    assert signals["ETH/USDT"].rank == 1


def test_load_fused_signal_states_derives_ranks_for_duplicate_legacy_zero_ranks() -> None:
    signals = _load_fused_signal_states(
        {
            "fused": {
                "ETH/USDT": {
                    "symbol": "ETH/USDT",
                    "direction": "buy",
                    "score": 0.62,
                    "rank": 0,
                },
                "BNB/USDT": {
                    "symbol": "BNB/USDT",
                    "direction": "buy",
                    "score": 0.09,
                    "rank": 0,
                },
            }
        },
        {"ETH/USDT", "BNB/USDT"},
    )

    assert signals["ETH/USDT"].rank == 1
    assert signals["BNB/USDT"].rank == 2


def test_event_driven_history_normalizes_zero_based_rank(tmp_path) -> None:
    trader = EventDrivenTrader(
        EventDrivenConfig(
            monitor_state_path=str(tmp_path / "event_monitor_state.json"),
            cooldown_state_path=str(tmp_path / "cooldown_state.json"),
        )
    )

    state = trader._build_market_state(
        {
            "timestamp_ms": 1,
            "regime": "SIDEWAYS",
            "prices": {},
            "positions": {},
            "signals": {
                "ETH/USDT": {
                    "symbol": "ETH/USDT",
                    "direction": "sell",
                    "score": 0.12,
                    "rank": 0,
                    "timestamp_ms": 1,
                }
            },
        }
    )

    assert state.signals["ETH/USDT"].rank == 1


def test_effective_event_log_excludes_active_throttled_actions() -> None:
    result = {
        "should_trade": True,
        "reason": "processed",
        "actions": [{"symbol": "BTC/USDT", "action": "open"}],
        "events_processed": 1,
        "events_blocked": 0,
    }
    execution = {
        "active_mode": True,
        "live_service_triggered": False,
        "live_service_ok": None,
        "trigger_reason": "active_throttled:same_window_already_ran",
    }

    log_values = _build_effective_event_log_values(result, execution)

    assert log_values["should_trade"] is False
    assert log_values["reason"] == "active_throttled:same_window_already_ran"
    assert log_values["actions"] == []
    assert log_values["events_processed"] == 0
    assert log_values["events_blocked"] == 0
    assert log_values["candidate_should_trade"] is True
    assert log_values["candidate_actions"] == result["actions"]
    assert log_values["candidate_events_processed"] == 1


def test_effective_event_log_keeps_accepted_active_actions() -> None:
    result = {
        "should_trade": True,
        "reason": "processed",
        "actions": [{"symbol": "BTC/USDT", "action": "open"}],
        "events_processed": 1,
        "events_blocked": 0,
    }
    execution = {
        "active_mode": True,
        "live_service_triggered": True,
        "live_service_ok": True,
        "trigger_reason": "event_actions",
    }

    log_values = _build_effective_event_log_values(result, execution)

    assert log_values["should_trade"] is True
    assert log_values["reason"] == "processed"
    assert log_values["actions"] == result["actions"]
    assert log_values["events_processed"] == 1
    assert "candidate_actions" not in log_values


def test_trigger_live_execution_service_rejects_already_running_unit(monkeypatch) -> None:
    class Result:
        returncode = 0
        stdout = "active\n"
        stderr = ""

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return Result()

    monkeypatch.setattr(edc.subprocess, "run", fake_run)

    result = edc.trigger_live_execution_service("v5-prod.user.service")

    assert result["ok"] is False
    assert result["skipped_already_running"] is True
    assert "already active" in result["stderr"]
    assert calls == [["systemctl", "--user", "is-active", "v5-prod.user.service"]]


def test_clear_event_actions_removes_unaccepted_action_file(tmp_path) -> None:
    action_path = tmp_path / "event_driven_actions.json"
    persisted = persist_event_actions(
        actions=[{"symbol": "BTC/USDT", "action": "close", "priority": 0}],
        target_run_id="20260427_18",
        path=str(action_path),
    )

    assert persisted is True
    assert action_path.exists()
    assert clear_event_actions(path=str(action_path)) is True
    assert not action_path.exists()
    assert clear_event_actions(path=str(action_path)) is False
