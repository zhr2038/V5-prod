from __future__ import annotations

from pathlib import Path

from src.execution import account_store, bills_store, bootstrap_patch, cooldown_manager, event_monitor, fill_store, highest_px_tracker, ledger_engine, multi_level_stop_loss, order_store, position_builder, position_store
from src.risk import auto_risk_guard, fixed_stop_loss, negative_expectancy_cooldown, profit_taking
from src.execution.kill_switch_guard import GuardConfig, KillSwitchGuard


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
