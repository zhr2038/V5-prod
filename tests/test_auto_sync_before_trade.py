from __future__ import annotations

from pathlib import Path

import pytest

import scripts.auto_sync_before_trade as auto_sync_before_trade


def test_resolve_active_config_path_fails_fast_when_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(auto_sync_before_trade, "WORKSPACE", tmp_path)
    monkeypatch.setattr(
        auto_sync_before_trade,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
        raising=False,
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        auto_sync_before_trade._resolve_active_config_path()
