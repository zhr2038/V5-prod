from __future__ import annotations

import json
from pathlib import Path

from configs.schema import ExecutionConfig
from src.execution import account_store, bills_store, bootstrap_patch, cooldown_manager, event_action_bridge, event_monitor, fill_store, highest_px_tracker, ledger_engine, live_execution_engine, live_preflight, multi_level_stop_loss, order_store, position_builder, position_store, reconcile_engine
from src.risk import auto_risk_guard, fixed_stop_loss, negative_expectancy_cooldown, profit_taking
from src.execution.kill_switch_guard import GuardConfig, KillSwitchGuard
import src.execution.order_arbitrator as order_arbitrator


def test_profit_taking_manager_resolves_default_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(profit_taking, "PROJECT_ROOT", tmp_path)
    manager = profit_taking.ProfitTakingManager()
    assert manager.state_file == (tmp_path / "reports" / "profit_taking_state.json").resolve()


def test_fixed_stop_loss_manager_resolves_default_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(fixed_stop_loss, "PROJECT_ROOT", tmp_path)
    manager = fixed_stop_loss.FixedStopLossManager()
    assert manager.state_file == (tmp_path / "reports" / "fixed_stop_loss_state.json").resolve()


def test_multi_level_stop_loss_resolves_default_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(multi_level_stop_loss, "PROJECT_ROOT", tmp_path)
    manager = multi_level_stop_loss.MultiLevelStopLoss()
    assert manager.state_file == (tmp_path / "reports" / "stop_loss_state.json").resolve()


def test_position_builder_resolves_default_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(position_builder, "PROJECT_ROOT", tmp_path)
    builder = position_builder.PositionBuilder()
    assert builder.state_file == (tmp_path / "reports" / "position_builder_state.json").resolve()


def test_highest_price_tracker_resolves_default_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(highest_px_tracker, "PROJECT_ROOT", tmp_path)
    tracker = highest_px_tracker.HighestPriceTracker()
    assert tracker.state_path == (tmp_path / "reports" / "highest_px_state.json").resolve()


def test_get_highest_price_tracker_uses_resolved_singleton_key(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(highest_px_tracker, "PROJECT_ROOT", tmp_path)
    highest_px_tracker._tracker_instances.clear()
    tracker_a = highest_px_tracker.get_highest_price_tracker("reports/highest_px_state.json")
    tracker_b = highest_px_tracker.get_highest_price_tracker(tmp_path / "reports" / "highest_px_state.json")
    assert tracker_a is tracker_b
    highest_px_tracker._tracker_instances.clear()


def test_derive_tracker_state_path_resolves_relative_positions_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(highest_px_tracker, "PROJECT_ROOT", tmp_path)
    assert highest_px_tracker.derive_tracker_state_path("reports/positions.sqlite") == (
        tmp_path / "reports" / "highest_px_state.json"
    ).resolve()


def test_auto_risk_guard_resolves_default_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(auto_risk_guard, "PROJECT_ROOT", tmp_path)
    guard = auto_risk_guard.AutoRiskGuard()
    assert guard.state_path == (tmp_path / "reports" / "auto_risk_guard.json").resolve()


def test_get_auto_risk_guard_resolves_relative_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(auto_risk_guard, "PROJECT_ROOT", tmp_path)
    auto_risk_guard._guard_instances.clear()
    guard = auto_risk_guard.get_auto_risk_guard("reports/custom_guard.json")
    assert guard.state_path == (tmp_path / "reports" / "custom_guard.json").resolve()
    auto_risk_guard._guard_instances.clear()


def test_cooldown_manager_resolves_default_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cooldown_manager, "PROJECT_ROOT", tmp_path)
    manager = cooldown_manager.CooldownManager()
    assert Path(manager.config.state_path) == (tmp_path / "reports" / "cooldown_state.json").resolve()


def test_event_monitor_resolves_default_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(event_monitor, "PROJECT_ROOT", tmp_path)
    monitor = event_monitor.EventMonitor()
    assert Path(monitor.config.state_path) == (tmp_path / "reports" / "event_monitor_state.json").resolve()


def test_negative_expectancy_cooldown_resolves_default_paths_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(negative_expectancy_cooldown, "PROJECT_ROOT", tmp_path)
    cfg = negative_expectancy_cooldown.NegativeExpectancyConfig()
    cooldown = negative_expectancy_cooldown.NegativeExpectancyCooldown(cfg)
    assert Path(cooldown.cfg.state_path) == (tmp_path / "reports" / "negative_expectancy_cooldown.json").resolve()
    assert Path(cooldown.cfg.orders_db_path) == (tmp_path / "reports" / "orders.sqlite").resolve()


def test_ledger_engine_resolves_default_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ledger_engine, "PROJECT_ROOT", tmp_path)
    engine = ledger_engine.LedgerEngine(okx=None, bills_store=None)
    assert Path(engine.state_path) == (tmp_path / "reports" / "ledger_state.json").resolve()


def test_kill_switch_guard_resolves_default_paths_from_project_root(monkeypatch, tmp_path: Path) -> None:
    import src.execution.kill_switch_guard as kill_switch_guard

    monkeypatch.setattr(kill_switch_guard, "PROJECT_ROOT", tmp_path)
    guard = KillSwitchGuard(GuardConfig())
    assert Path(guard.cfg.reconcile_status_path) == (tmp_path / "reports" / "reconcile_status.json").resolve()
    assert Path(guard.cfg.failure_state_path) == (tmp_path / "reports" / "reconcile_failure_state.json").resolve()
    assert Path(guard.cfg.kill_switch_path) == (tmp_path / "reports" / "kill_switch.json").resolve()


def test_bootstrap_patch_resolves_relative_state_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(bootstrap_patch, "PROJECT_ROOT", tmp_path)
    resolved = bootstrap_patch._resolve_path("reports/bootstrap_patch_state.json")
    assert resolved == (tmp_path / "reports" / "bootstrap_patch_state.json").resolve()


def test_bills_store_resolves_default_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(bills_store, "PROJECT_ROOT", tmp_path)
    store = bills_store.BillsStore()
    assert store.path == (tmp_path / "reports" / "bills.sqlite").resolve()


def test_account_store_resolves_default_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(account_store, "PROJECT_ROOT", tmp_path)
    store = account_store.AccountStore()
    assert store.path == (tmp_path / "reports" / "positions.sqlite").resolve()


def test_order_store_resolves_default_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(order_store, "PROJECT_ROOT", tmp_path)
    store = order_store.OrderStore()
    assert store.path == (tmp_path / "reports" / "orders.sqlite").resolve()


def test_position_store_resolves_default_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(position_store, "PROJECT_ROOT", tmp_path)
    store = position_store.PositionStore()
    assert store.path == (tmp_path / "reports" / "positions.sqlite").resolve()


def test_fill_store_resolves_default_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(fill_store, "PROJECT_ROOT", tmp_path)
    store = fill_store.FillStore()
    assert store.path == (tmp_path / "reports" / "fills.sqlite").resolve()


def test_live_execution_engine_resolves_default_cooldown_paths_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(live_execution_engine, "PROJECT_ROOT", tmp_path)
    live_execution_engine._write_rank_exit_cooldown_state({}, "reports/rank_exit_cooldown_state.json")
    live_execution_engine._write_take_profit_cooldown_state({}, "reports/take_profit_cooldown_state.json")
    assert (tmp_path / "reports" / "rank_exit_cooldown_state.json").exists()
    assert (tmp_path / "reports" / "take_profit_cooldown_state.json").exists()
    assert live_execution_engine._derive_highest_tracker_state_path("reports/positions.sqlite") == str(
        (tmp_path / "reports" / "highest_px_state.json").resolve()
    )


def test_order_arbitrator_resolves_default_state_paths_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(order_arbitrator, "PROJECT_ROOT", tmp_path)
    order_arbitrator._save_state("reports/order_state_machine.json", {"version": 1, "symbols": {}})
    tp_path = tmp_path / "reports" / "take_profit_cooldown_state.json"
    tp_path.parent.mkdir(parents=True, exist_ok=True)
    tp_path.write_text(json.dumps({"BTC/USDT": {"last_take_profit_ts_ms": 1}}), encoding="utf-8")
    assert (tmp_path / "reports" / "order_state_machine.json").exists()
    assert order_arbitrator._load_take_profit_cooldown_state("reports/take_profit_cooldown_state.json") == {
        "BTC/USDT": {"last_take_profit_ts_ms": 1}
    }


def test_reconcile_engine_resolves_default_output_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(reconcile_engine, "PROJECT_ROOT", tmp_path)
    reconcile_engine._atomic_write_json("reports/reconcile_status.json", {"ok": True})
    assert (tmp_path / "reports" / "reconcile_status.json").exists()


def test_live_preflight_resolves_runtime_paths_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(live_preflight, "PROJECT_ROOT", tmp_path)
    cfg = ExecutionConfig(order_store_path="reports/orders.sqlite")
    preflight = live_preflight.LivePreflight(
        cfg,
        okx=None,
        position_store=None,
        account_store=None,
    )
    assert preflight.order_store_path == str((tmp_path / "reports" / "orders.sqlite").resolve())
    assert preflight.bills_db_path == str((tmp_path / "reports" / "bills.sqlite").resolve())
    assert preflight.ledger_state_path == str((tmp_path / "reports" / "ledger_state.json").resolve())
    assert preflight.ledger_status_path == str((tmp_path / "reports" / "ledger_status.json").resolve())
    assert preflight.reconcile_status_path == str((tmp_path / "reports" / "reconcile_status.json").resolve())
    assert preflight.reconcile_failure_state_path == str((tmp_path / "reports" / "reconcile_failure_state.json").resolve())
    assert preflight.kill_switch_path == str((tmp_path / "reports" / "kill_switch.json").resolve())


def test_event_action_bridge_resolves_default_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(event_action_bridge, "PROJECT_ROOT", tmp_path)
    resolved = event_action_bridge._resolve_event_actions_path()
    assert resolved == (tmp_path / "reports" / "event_driven_actions.json").resolve()


def test_event_action_bridge_resolves_runtime_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(event_action_bridge, "PROJECT_ROOT", tmp_path)
    resolved = event_action_bridge._resolve_event_actions_path(order_store_path="reports/shadow_orders.sqlite")
    assert resolved == (tmp_path / "reports" / "shadow_event_driven_actions.json").resolve()
