from __future__ import annotations

from pathlib import Path

import pytest

import scripts.bootstrap_from_okx_balance as bootstrap_from_okx_balance
from scripts.bootstrap_from_okx_balance import _derive_runtime_artifact_paths


def test_derive_runtime_artifact_paths_uses_runtime_dirs(tmp_path: Path) -> None:
    root_positions = tmp_path / "reports" / "positions.sqlite"
    shadow_positions = tmp_path / "reports" / "shadow_positions.sqlite"
    nested_positions = tmp_path / "reports" / "shadow_tuned_xgboost" / "positions.sqlite"

    spread_dir, highest_path = _derive_runtime_artifact_paths(root_positions)
    assert spread_dir == tmp_path / "reports" / "spread_snapshots"
    assert highest_path == tmp_path / "reports" / "highest_px_state.json"

    spread_dir, highest_path = _derive_runtime_artifact_paths(shadow_positions)
    assert spread_dir == tmp_path / "reports" / "shadow_spread_snapshots"
    assert highest_path == tmp_path / "reports" / "shadow_highest_px_state.json"

    spread_dir, highest_path = _derive_runtime_artifact_paths(nested_positions)
    assert spread_dir == tmp_path / "reports" / "shadow_tuned_xgboost" / "spread_snapshots"
    assert highest_path == tmp_path / "reports" / "shadow_tuned_xgboost" / "highest_px_state.json"


def test_resolve_active_config_path_fails_fast_when_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(bootstrap_from_okx_balance, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        bootstrap_from_okx_balance,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        bootstrap_from_okx_balance._resolve_active_config_path()
