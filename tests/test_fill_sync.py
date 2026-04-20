from __future__ import annotations

from pathlib import Path

import pytest

import scripts.fill_sync as fill_sync


def test_resolve_active_config_path_fails_fast_when_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(fill_sync, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        fill_sync,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
        raising=False,
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        fill_sync._resolve_active_config_path()
