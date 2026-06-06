from __future__ import annotations

import os
from pathlib import Path

from src.reporting.bundle_retention import prune_v5_bundle_exports


def _touch(path: Path, mtime: float) -> None:
    path.write_text("x", encoding="utf-8")
    os.utime(path, (mtime, mtime))


def test_prune_v5_bundle_exports_keeps_latest_bundle_pairs(tmp_path: Path) -> None:
    now = 1_800_000_000.0
    bundle_paths: list[Path] = []
    for idx in range(5):
        path = tmp_path / f"v5_live_followup_bundle_20260606T000{idx}00Z.tar.gz"
        _touch(path, now - idx)
        _touch(Path(f"{path}.sha256"), now - idx)
        bundle_paths.append(path)
    orphan_sha = tmp_path / "v5_live_followup_bundle_20260601T000000Z.tar.gz.sha256"
    _touch(orphan_sha, now - 10)

    result = prune_v5_bundle_exports(tmp_path, keep_count=2, max_age_days=None, now=now)

    assert result.kept_bundle_count == 2
    assert result.deleted_bundle_count == 3
    assert result.deleted_sha_count == 4
    assert bundle_paths[0].exists()
    assert Path(f"{bundle_paths[0]}.sha256").exists()
    assert bundle_paths[1].exists()
    assert Path(f"{bundle_paths[1]}.sha256").exists()
    assert not bundle_paths[2].exists()
    assert not Path(f"{bundle_paths[2]}.sha256").exists()
    assert not orphan_sha.exists()


def test_prune_v5_bundle_exports_honors_max_age(tmp_path: Path) -> None:
    now = 1_800_000_000.0
    fresh = tmp_path / "v5_live_followup_bundle_20260606T000000Z.tar.gz"
    old = tmp_path / "v5_live_followup_bundle_20260520T000000Z.tar.gz"
    _touch(fresh, now)
    _touch(Path(f"{fresh}.sha256"), now)
    _touch(old, now - 8 * 86400)
    _touch(Path(f"{old}.sha256"), now - 8 * 86400)

    result = prune_v5_bundle_exports(tmp_path, keep_count=100, max_age_days=7, now=now)

    assert result.kept_bundle_count == 1
    assert result.deleted_bundle_count == 1
    assert result.deleted_sha_count == 1
    assert fresh.exists()
    assert not old.exists()
