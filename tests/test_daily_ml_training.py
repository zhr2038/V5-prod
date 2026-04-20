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
