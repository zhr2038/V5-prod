from __future__ import annotations

from pathlib import Path

from src.factors import ic_weight_calculator


def test_ic_weight_calculator_resolves_relative_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ic_weight_calculator, "PROJECT_ROOT", tmp_path)

    calculator = ic_weight_calculator.ICBasedWeightCalculator("reports/ic_diagnostics_30d_20u.json")

    assert calculator.ic_file == (tmp_path / "reports" / "ic_diagnostics_30d_20u.json").resolve()


def test_ic_weight_calculator_keeps_absolute_path(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ic_weight_calculator, "PROJECT_ROOT", tmp_path)
    explicit = (tmp_path / "custom" / "ic.json").resolve()

    calculator = ic_weight_calculator.ICBasedWeightCalculator(str(explicit))

    assert calculator.ic_file == explicit
