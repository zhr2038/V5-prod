from __future__ import annotations

from pathlib import Path

import pytest

import scripts.backfill_ml_multihorizon_labels as multihorizon
import scripts.backfill_regime_history_rss as regime_rss


def test_runtime_training_db_path_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(multihorizon, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        multihorizon,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        multihorizon._runtime_training_db_path("configs/missing.yaml")


def test_resolve_main_paths_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(regime_rss, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        regime_rss,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        regime_rss._resolve_main_paths("configs/missing.yaml", None, None)
