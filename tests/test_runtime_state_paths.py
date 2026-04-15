from __future__ import annotations

from pathlib import Path

from src.execution import highest_px_tracker, multi_level_stop_loss, position_builder
from src.risk import fixed_stop_loss, profit_taking


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
