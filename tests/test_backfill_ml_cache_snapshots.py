from __future__ import annotations

from pathlib import Path

import scripts.backfill_ml_cache_snapshots as backfill_ml_cache_snapshots
import pytest


def test_runtime_defaults_use_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/shadow_orders.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(backfill_ml_cache_snapshots, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        backfill_ml_cache_snapshots,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )
    monkeypatch.setattr(
        backfill_ml_cache_snapshots,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    db_path, csv_path, universe_path = backfill_ml_cache_snapshots._runtime_defaults()

    assert db_path == (tmp_path / "reports" / "ml_training_data.db").resolve()
    assert csv_path == (tmp_path / "reports" / "ml_training_data.csv").resolve()
    assert universe_path == (tmp_path / "reports" / "universe_cache.json").resolve()


def test_runtime_defaults_fail_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(backfill_ml_cache_snapshots, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        backfill_ml_cache_snapshots,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        backfill_ml_cache_snapshots._runtime_defaults()
