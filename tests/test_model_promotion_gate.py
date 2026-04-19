from __future__ import annotations

from pathlib import Path

import pytest

import scripts.model_promotion_gate as promotion_gate


def test_build_paths_uses_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        promotion_gate,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "alpha": {"ml_factor": {}},
        },
    )
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "live_prod.yaml").resolve()),
    )
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = promotion_gate.build_paths(tmp_path)

    assert paths.history_path == (tmp_path / "reports" / "shadow_runtime" / "ml_training_history.json").resolve()
    assert paths.runs_dir == (tmp_path / "reports" / "shadow_runtime" / "runs").resolve()


def test_build_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )
    monkeypatch.setattr(
        promotion_gate,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {},
    )

    with pytest.raises(ValueError, match="live_prod.yaml"):
        promotion_gate.build_paths(tmp_path)
