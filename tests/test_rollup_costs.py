from __future__ import annotations

from pathlib import Path

import pytest

import scripts.rollup_costs as rollup_costs


def test_resolve_runtime_cost_paths_uses_prefixed_runtime_dirs(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/shadow_orders.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(rollup_costs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        rollup_costs,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    paths = rollup_costs._resolve_runtime_cost_paths(project_root=tmp_path)

    assert paths.events_dir == (tmp_path / "reports" / "shadow_cost_events").resolve()
    assert paths.stats_dir == (tmp_path / "reports" / "shadow_cost_stats").resolve()


def test_resolve_runtime_cost_paths_fails_fast_on_invalid_config(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(rollup_costs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(rollup_costs, "load_runtime_config", lambda project_root=None: {})
    monkeypatch.setattr(
        rollup_costs,
        "resolve_runtime_config_path",
        lambda project_root=None: str(config_path),
    )

    with pytest.raises(ValueError, match="runtime config is empty or invalid"):
        rollup_costs._resolve_runtime_cost_paths(project_root=tmp_path)


def test_resolve_runtime_cost_paths_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(rollup_costs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        rollup_costs,
        "resolve_runtime_config_path",
        lambda project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        rollup_costs._resolve_runtime_cost_paths(project_root=tmp_path)


def test_resolve_runtime_cost_paths_keeps_explicit_dirs_without_runtime_config(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(rollup_costs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(rollup_costs, "load_runtime_config", lambda project_root=None: {})

    paths = rollup_costs._resolve_runtime_cost_paths(
        base_dir="custom/events",
        out_dir="custom/stats",
        project_root=tmp_path,
    )

    assert paths.events_dir == (tmp_path / "custom" / "events").resolve()
    assert paths.stats_dir == (tmp_path / "custom" / "stats").resolve()
