from __future__ import annotations

import tempfile
from pathlib import Path

from configs.schema import AlphaConfig, RiskConfig
from src.portfolio.portfolio_engine import PortfolioEngine


def test_portfolio_engine_uses_temp_runtime_root_under_pytest(monkeypatch):
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/test_portfolio_engine_runtime_paths.py::test")

    engine = PortfolioEngine(AlphaConfig(), RiskConfig())

    path = engine._resolve_runtime_artifact_path(None, "reports/topk_dropout_state.json")

    assert path == Path(tempfile.gettempdir()) / "v5-test-runtime" / "reports" / "topk_dropout_state.json"


def test_portfolio_engine_keeps_explicit_workspace(monkeypatch, tmp_path):
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/test_portfolio_engine_runtime_paths.py::test")
    monkeypatch.setenv("V5_WORKSPACE", str(tmp_path))

    engine = PortfolioEngine(AlphaConfig(), RiskConfig())

    path = engine._resolve_runtime_artifact_path(None, "reports/topk_dropout_state.json")

    assert path == tmp_path / "reports" / "topk_dropout_state.json"
