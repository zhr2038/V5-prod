from __future__ import annotations

from pathlib import Path

import pytest

import scripts.emergency_liquidate as emergency_liquidate


def test_resolve_active_config_path_fails_fast_when_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(emergency_liquidate, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        emergency_liquidate,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        emergency_liquidate._resolve_active_config_path()
