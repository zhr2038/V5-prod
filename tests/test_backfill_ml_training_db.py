from __future__ import annotations

from pathlib import Path

import pytest

import scripts.backfill_ml_training_db as backfill_ml_training_db


def test_resolve_runtime_training_paths_uses_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/shadow_orders.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(backfill_ml_training_db, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        backfill_ml_training_db,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )
    monkeypatch.setattr(
        backfill_ml_training_db,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    csv_path, db_path = backfill_ml_training_db._resolve_runtime_training_paths()

    assert csv_path == (tmp_path / "reports" / "shadow_ml_training_data.csv").resolve()
    assert db_path == (tmp_path / "reports" / "shadow_ml_training_data.db").resolve()


def test_resolve_runtime_training_paths_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(backfill_ml_training_db, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        backfill_ml_training_db,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        backfill_ml_training_db._resolve_runtime_training_paths()
