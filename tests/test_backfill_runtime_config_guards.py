from __future__ import annotations

from datetime import datetime, timezone
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


def test_rss_backfill_cache_time_parsing_is_utc_aware(tmp_path: Path) -> None:
    cache_file = tmp_path / "rss_MARKET_20260525_04.json"

    assert regime_rss._parse_cache_time(cache_file, {"collected_at": "2026-05-25T04:30:00Z"}) == datetime(
        2026,
        5,
        25,
        4,
        30,
        tzinfo=timezone.utc,
    )
    assert regime_rss._parse_cache_time(cache_file, {}) == datetime(2026, 5, 25, 4, tzinfo=timezone.utc)
