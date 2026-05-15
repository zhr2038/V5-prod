from __future__ import annotations

from pathlib import Path

import pytest

import scripts.daily_ml_training as daily_ml_training


def test_runtime_reports_dir_uses_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/shadow_runtime/orders.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(daily_ml_training, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        daily_ml_training,
        "resolve_runtime_config_path",
        lambda project_root=None: str(config_path),
    )
    monkeypatch.setattr(
        daily_ml_training,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    reports_dir = daily_ml_training._runtime_reports_dir()

    assert reports_dir == (tmp_path / "reports" / "shadow_runtime").resolve()


def test_runtime_reports_dir_fails_fast_when_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(daily_ml_training, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        daily_ml_training,
        "resolve_runtime_config_path",
        lambda project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        daily_ml_training._runtime_reports_dir()


def test_daily_ml_training_exits_with_research_dependency_hint(monkeypatch) -> None:
    def missing_deps(_modules):
        raise SystemExit("Missing optional ML/research dependencies: xgboost. Install them with: pip install -r requirements-research.txt")

    monkeypatch.setattr(daily_ml_training, "require_research_dependencies", missing_deps)
    monkeypatch.setattr(
        daily_ml_training,
        "run_ml_training_task",
        lambda **kwargs: pytest.fail("training should not start when research dependencies are missing"),
    )

    with pytest.raises(SystemExit, match="requirements-research.txt"):
        daily_ml_training.main()
