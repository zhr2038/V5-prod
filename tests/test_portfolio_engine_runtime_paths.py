from __future__ import annotations

import json
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


def test_portfolio_engine_dynamic_max_positions_falls_back_to_auto_risk_guard(tmp_path, monkeypatch):
    monkeypatch.setenv("V5_WORKSPACE", str(tmp_path))
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "live_prod.yaml").write_text("execution:\n  order_store_path: reports/orders.sqlite\n", encoding="utf-8")
    (reports_dir / "auto_risk_guard.json").write_text(json.dumps({"current_level": "PROTECT"}), encoding="utf-8")

    engine = PortfolioEngine(AlphaConfig(), RiskConfig())

    assert engine._get_dynamic_max_positions() == 1
