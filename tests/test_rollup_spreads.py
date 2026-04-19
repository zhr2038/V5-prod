from __future__ import annotations

from pathlib import Path

import pytest

import scripts.rollup_spreads as rollup_spreads


def test_resolve_runtime_dirs_uses_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        rollup_spreads,
        "PROJECT_ROOT",
        tmp_path,
    )
    monkeypatch.setattr(
        rollup_spreads,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "live_prod.yaml").resolve()),
    )
    monkeypatch.setattr(
        rollup_spreads,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        rollup_spreads,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    snapshots_dir, out_dir = rollup_spreads._resolve_runtime_dirs(
        snapshots_dir=None,
        out_dir=None,
        config_path=None,
    )

    assert snapshots_dir == (tmp_path / "reports" / "shadow_spread_snapshots").resolve()
    assert out_dir == (tmp_path / "reports" / "shadow_spread_stats").resolve()


def test_resolve_runtime_dirs_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        rollup_spreads,
        "PROJECT_ROOT",
        tmp_path,
    )
    monkeypatch.setattr(
        rollup_spreads,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "live_prod.yaml").resolve()),
    )
    monkeypatch.setattr(
        rollup_spreads,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {},
    )

    with pytest.raises(ValueError, match="live_prod.yaml"):
        rollup_spreads._resolve_runtime_dirs(
            snapshots_dir=None,
            out_dir=None,
            config_path=None,
        )


def test_resolve_runtime_dirs_allows_explicit_paths_without_runtime_config(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        rollup_spreads,
        "PROJECT_ROOT",
        tmp_path,
    )
    monkeypatch.setattr(
        rollup_spreads,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: (_ for _ in ()).throw(AssertionError("should not load config")),
    )

    snapshots_dir, out_dir = rollup_spreads._resolve_runtime_dirs(
        snapshots_dir="custom/snapshots",
        out_dir="custom/out",
        config_path=None,
    )

    assert snapshots_dir == (tmp_path / "custom" / "snapshots").resolve()
    assert out_dir == (tmp_path / "custom" / "out").resolve()
