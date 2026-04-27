from __future__ import annotations

import json
import os
from types import SimpleNamespace

import event_driven_check as edc
from event_driven_check import (
    _build_effective_event_log_values,
    _extract_event_regime,
    _filter_dust_positions,
    _load_fused_signal_states,
    _load_positions_snapshot,
    compute_adaptive_event_cfg,
    filter_event_actions_for_auto_risk,
    load_current_auto_risk_level,
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


def test_find_latest_fused_signals_file_uses_live_run_id_order(tmp_path, monkeypatch) -> None:
    runs_dir = tmp_path / "runs"
    old_live = runs_dir / "20260427_18" / "strategy_signals.json"
    latest_live = runs_dir / "20260427_19" / "strategy_signals.json"
    research_file = runs_dir / "sweep_touch" / "strategy_signals.json"
    for path in (old_live, latest_live, research_file):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"fused": {}}), encoding="utf-8")

    os.utime(old_live, (2000, 2000))
    os.utime(latest_live, (1000, 1000))
    os.utime(research_file, (3000, 3000))
    monkeypatch.setattr(edc.time, "time", lambda: 3600.0)

    selected, meta = edc.find_latest_fused_signals_file(runs_dir, max_age_minutes=1000)

    assert selected == latest_live
    assert meta["count"] == 2
    assert meta["ignored_dirs"] == 1


def test_get_last_live_run_age_sec_uses_live_run_id_order(tmp_path, monkeypatch) -> None:
    runs_dir = tmp_path / "runs"
    old_live = runs_dir / "20260427_18" / "decision_audit.json"
    latest_live = runs_dir / "20260427_19" / "decision_audit.json"
    research_file = runs_dir / "manual_replay" / "decision_audit.json"
    for path in (old_live, latest_live, research_file):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"top_scores": []}), encoding="utf-8")

    os.utime(old_live, (3500, 3500))
    os.utime(latest_live, (1000, 1000))
    os.utime(research_file, (3900, 3900))
    monkeypatch.setattr(edc.time, "time", lambda: 4000.0)

    age_sec, run_id = edc.get_last_live_run_age_sec(runs_dir)

    assert run_id == "20260427_19"
    assert age_sec == 3000.0


def test_extract_event_regime_prefers_regime_state_field() -> None:
    assert _extract_event_regime({"state": "Risk-Off", "regime": "SIDEWAYS"}) == "RISK_OFF"
    assert _extract_event_regime({"state": "Sideways"}) == "SIDEWAYS"
    assert _extract_event_regime({"state": "Trending"}) == "TRENDING"
    assert _extract_event_regime({"state": "unknown", "regime": "RISK_OFF"}) == "RISK_OFF"


def test_event_monitor_detects_risk_off_from_normalized_regime(tmp_path) -> None:
    trader = EventDrivenTrader(
        EventDrivenConfig(
            monitor_state_path=str(tmp_path / "event_monitor_state.json"),
            cooldown_state_path=str(tmp_path / "cooldown_state.json"),
        )
    )
    state = trader._build_market_state(
        {
            "timestamp_ms": 1,
            "regime": _extract_event_regime({"state": "Risk-Off"}),
            "prices": {},
            "positions": {},
            "signals": {},
        }
    )

    events = trader.monitor._check_risk_events(state)

    assert [event.type.name for event in events] == ["REGIME_RISK_OFF"]


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


def test_load_current_auto_risk_level_prefers_newer_eval_snapshot(tmp_path) -> None:
    order_store = tmp_path / "orders.sqlite"
    (tmp_path / "auto_risk_guard.json").write_text(
        json.dumps(
            {
                "current_level": "DEFENSE",
                "last_update": "2026-04-27T10:00:00",
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "auto_risk_eval.json").write_text(
        json.dumps(
            {
                "current_level": "PROTECT",
                "ts": "2026-04-27T10:01:00",
            }
        ),
        encoding="utf-8",
    )

    assert load_current_auto_risk_level(order_store) == "PROTECT"


def test_filter_event_actions_for_auto_risk_blocks_protect_open_actions() -> None:
    result = {
        "should_trade": True,
        "reason": "processed",
        "actions": [{"symbol": "BTC/USDT", "action": "open", "reason": "heartbeat_entry"}],
        "events_processed": 1,
        "events_blocked": 0,
    }

    filtered = filter_event_actions_for_auto_risk(result, "PROTECT")

    assert filtered["should_trade"] is False
    assert filtered["reason"] == "auto_risk_protect_open_block"
    assert filtered["actions"] == []
    assert filtered["events_processed"] == 1
    assert filtered["events_blocked"] == 1
    assert filtered["auto_risk_level"] == "PROTECT"
    assert filtered["auto_risk_blocked_actions"] == result["actions"]


def test_filter_event_actions_for_auto_risk_keeps_protect_close_actions() -> None:
    close_action = {"symbol": "BTC/USDT", "action": "close", "reason": "stop_loss", "priority": 0}
    result = {
        "should_trade": True,
        "reason": "processed",
        "actions": [close_action],
        "events_processed": 1,
        "events_blocked": 0,
    }

    filtered = filter_event_actions_for_auto_risk(result, "PROTECT")

    assert filtered is result
    assert filtered["actions"] == [close_action]
    assert filtered["should_trade"] is True


def test_filter_event_actions_for_auto_risk_keeps_close_when_open_blocked() -> None:
    close_action = {"symbol": "ETH/USDT", "action": "close", "reason": "rank_exit_6", "priority": 0}
    open_action = {"symbol": "BTC/USDT", "action": "open", "reason": "signal_rank_jump", "priority": 2}
    result = {
        "should_trade": True,
        "reason": "processed",
        "actions": [close_action, open_action],
        "events_processed": 2,
        "events_blocked": 0,
    }

    filtered = filter_event_actions_for_auto_risk(result, "PROTECT")

    assert filtered["should_trade"] is True
    assert filtered["reason"] == "auto_risk_protect_open_block_partial"
    assert filtered["actions"] == [close_action]
    assert filtered["auto_risk_blocked_actions"] == [open_action]
    assert filtered["events_blocked"] == 1


def test_adaptive_cooldown_hardens_in_auto_risk_protect_despite_high_block_ratio(tmp_path) -> None:
    log_path = tmp_path / "event_driven_log.jsonl"
    log_path.write_text(
        "\n".join(json.dumps({"events_processed": 1, "events_blocked": 1}) for _ in range(8)) + "\n",
        encoding="utf-8",
    )

    adaptive, meta = compute_adaptive_event_cfg(
        {
            "global_cooldown_p2_minutes": 30,
            "symbol_cooldown_minutes": 60,
            "signal_confirmation_periods": 2,
            "adaptive_cooldown": {
                "enabled": True,
                "lookback_runs": 12,
                "high_block_ratio": 0.75,
                "min_events_for_action": 8,
                "p2_min_minutes": 8,
                "p2_max_minutes": 60,
                "symbol_min_minutes": 15,
                "symbol_max_minutes": 120,
                "confirm_min": 1,
                "confirm_max": 4,
            },
        },
        {"regime": "SIDEWAYS"},
        log_path=log_path,
        adaptive_state_path=tmp_path / "event_adaptive_state.json",
        auto_risk_level="PROTECT",
    )

    assert adaptive["global_cooldown_p2_minutes"] == 36
    assert adaptive["symbol_cooldown_minutes"] == 72
    assert adaptive["signal_confirmation_periods"] == 3
    assert meta["reason"] == "auto_risk_protect_harden"
    assert meta["auto_risk_level"] == "PROTECT"
    assert meta["recent_block_ratio"] == 1.0


def test_adaptive_cooldown_relaxes_high_block_ratio_without_defensive_risk(tmp_path) -> None:
    log_path = tmp_path / "event_driven_log.jsonl"
    log_path.write_text(
        "\n".join(json.dumps({"events_processed": 1, "events_blocked": 1}) for _ in range(8)) + "\n",
        encoding="utf-8",
    )

    adaptive, meta = compute_adaptive_event_cfg(
        {
            "global_cooldown_p2_minutes": 30,
            "symbol_cooldown_minutes": 60,
            "signal_confirmation_periods": 2,
            "adaptive_cooldown": {
                "enabled": True,
                "lookback_runs": 12,
                "high_block_ratio": 0.75,
                "min_events_for_action": 8,
                "p2_min_minutes": 8,
                "p2_max_minutes": 60,
                "symbol_min_minutes": 15,
                "symbol_max_minutes": 120,
                "confirm_min": 1,
                "confirm_max": 4,
            },
        },
        {"regime": "SIDEWAYS"},
        log_path=log_path,
        adaptive_state_path=tmp_path / "event_adaptive_state.json",
        auto_risk_level="NEUTRAL",
    )

    assert adaptive["global_cooldown_p2_minutes"] == 15
    assert adaptive["symbol_cooldown_minutes"] == 30
    assert adaptive["signal_confirmation_periods"] == 1
    assert meta["reason"] == "high_block_ratio_relax"
    assert meta["auto_risk_level"] == "NEUTRAL"


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


def test_repair_unaccepted_event_execution_state_rewinds_matching_state(tmp_path) -> None:
    event_log = tmp_path / "event_driven_log.jsonl"
    cooldown_state = tmp_path / "cooldown_state.json"
    monitor_state = tmp_path / "event_monitor_state.json"
    accepted = {
        "timestamp": "2026-04-27T10:00:00",
        "actions": [{"symbol": "BTC/USDT", "action": "open"}],
        "execution": {"live_service_ok": True},
    }
    unaccepted = {
        "timestamp": "2026-04-27T17:15:04",
        "should_trade": False,
        "actions": [],
        "candidate_actions": [{"symbol": "BTC/USDT", "action": "open"}],
        "execution": {
            "live_service_ok": None,
            "trigger_reason": "active_throttled:same_window_already_ran",
        },
    }
    accepted_ms = edc._event_log_timestamp_ms(accepted)
    unaccepted_ms = edc._event_log_timestamp_ms(unaccepted)
    event_log.write_text(
        "\n".join(json.dumps(item) for item in [accepted, unaccepted]) + "\n",
        encoding="utf-8",
    )
    cooldown_state.write_text(
        json.dumps(
            {
                "last_global_trade_ms": unaccepted_ms,
                "symbol_cooldowns": {
                    "BTC/USDT": unaccepted_ms,
                    "ETH/USDT": 12345,
                },
                "pending_signals": {},
            }
        ),
        encoding="utf-8",
    )
    monitor_state.write_text(
        json.dumps({"last_trade_time_ms": unaccepted_ms, "price_history": {}}),
        encoding="utf-8",
    )

    meta = edc.repair_unaccepted_event_execution_state(
        event_log_path=event_log,
        cooldown_state_path=cooldown_state,
        monitor_state_path=monitor_state,
    )

    cooldown = json.loads(cooldown_state.read_text(encoding="utf-8"))
    monitor = json.loads(monitor_state.read_text(encoding="utf-8"))
    assert meta["changed"] is True
    assert meta["cooldown_repaired"] is True
    assert meta["monitor_repaired"] is True
    assert cooldown["last_global_trade_ms"] == accepted_ms
    assert cooldown["symbol_cooldowns"]["BTC/USDT"] == accepted_ms
    assert cooldown["symbol_cooldowns"]["ETH/USDT"] == 12345
    assert monitor["last_trade_time_ms"] == accepted_ms


def test_repair_unaccepted_event_execution_state_leaves_unmatched_state(tmp_path) -> None:
    event_log = tmp_path / "event_driven_log.jsonl"
    cooldown_state = tmp_path / "cooldown_state.json"
    monitor_state = tmp_path / "event_monitor_state.json"
    unaccepted = {
        "timestamp": "2026-04-27T17:15:04",
        "candidate_actions": [{"symbol": "BTC/USDT", "action": "open"}],
        "execution": {"live_service_ok": None},
    }
    event_log.write_text(json.dumps(unaccepted) + "\n", encoding="utf-8")
    cooldown_state.write_text(
        json.dumps({"last_global_trade_ms": 999, "symbol_cooldowns": {}, "pending_signals": {}}),
        encoding="utf-8",
    )
    monitor_state.write_text(json.dumps({"last_trade_time_ms": 999}), encoding="utf-8")

    meta = edc.repair_unaccepted_event_execution_state(
        event_log_path=event_log,
        cooldown_state_path=cooldown_state,
        monitor_state_path=monitor_state,
    )

    assert meta["changed"] is False
    assert json.loads(cooldown_state.read_text(encoding="utf-8"))["last_global_trade_ms"] == 999
    assert json.loads(monitor_state.read_text(encoding="utf-8"))["last_trade_time_ms"] == 999
