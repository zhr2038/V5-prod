from __future__ import annotations

import re
from pathlib import Path

import pytest

import scripts.bills_sync as bills_sync


def test_resolve_active_config_path_accepts_valid_runtime_config(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/orders.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(
        bills_sync,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )
    monkeypatch.setattr(
        bills_sync,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {"execution": {"order_store_path": "reports/orders.sqlite"}},
    )

    resolved = bills_sync._resolve_active_config_path()

    assert resolved == str(config_path)


def test_resolve_active_config_path_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(
        bills_sync,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match=re.escape(str(missing))):
        bills_sync._resolve_active_config_path()
