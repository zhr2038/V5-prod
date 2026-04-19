from __future__ import annotations

from pathlib import Path

import pytest

import scripts.rollup_last24h as rollup_last24h


def test_resolve_runtime_rollup_paths_uses_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(rollup_last24h, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        rollup_last24h,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(
        rollup_last24h,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = rollup_last24h._resolve_runtime_rollup_paths()

    assert paths.runs_dir == (tmp_path / "reports" / "shadow_runtime" / "runs").resolve()
    assert paths.reports_dir == (tmp_path / "reports" / "shadow_runtime").resolve()


def test_resolve_runtime_rollup_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(rollup_last24h, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(rollup_last24h, "load_runtime_config", lambda project_root=None: {})

    with pytest.raises(ValueError, match="live_prod.yaml"):
        rollup_last24h._resolve_runtime_rollup_paths()
