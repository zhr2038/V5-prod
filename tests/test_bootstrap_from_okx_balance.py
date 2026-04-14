from __future__ import annotations

from pathlib import Path

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
