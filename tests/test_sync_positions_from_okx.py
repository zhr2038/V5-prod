from __future__ import annotations

from pathlib import Path

import scripts.sync_positions_from_okx as sync_positions


def test_format_follow_up_config_path_returns_runtime_relative_path(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "shadow_live.yaml").resolve()
    monkeypatch.setattr(sync_positions, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        sync_positions,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(expected),
    )

    path = sync_positions._format_follow_up_config_path(None)

    assert path == "configs/shadow_live.yaml"


def test_format_follow_up_config_path_keeps_absolute_outside_project(monkeypatch, tmp_path: Path) -> None:
    external = (tmp_path.parent / "external_live.yaml").resolve()
    monkeypatch.setattr(sync_positions, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        sync_positions,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(external),
    )

    path = sync_positions._format_follow_up_config_path(None)

    assert path == str(external)
