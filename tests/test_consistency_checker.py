from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.consistency_checker import BacktestLiveConsistencyChecker, ConsistencyPaths


@pytest.fixture(autouse=True)
def _runtime_config(monkeypatch, tmp_path: Path) -> Path:
    config_path = tmp_path / "configs" / "live_prod.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "execution:\n  order_store_path: reports/orders.sqlite\n",
        encoding="utf-8",
    )
    return config_path


def test_load_backtest_config_uses_prefixed_runtime_cost_stats_dir(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    root_dir = reports_dir / "cost_stats_real"
    runtime_dir = reports_dir / "shadow_cost_stats_real"
    root_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    (root_dir / "daily_cost_stats_20260407.json").write_text(
        json.dumps({"avg_cost_bps": 99}, ensure_ascii=False),
        encoding="utf-8",
    )
    runtime_file = runtime_dir / "daily_cost_stats_20260408.json"
    runtime_file.write_text(
        json.dumps({"avg_cost_bps": 12.5}, ensure_ascii=False),
        encoding="utf-8",
    )

    checker = BacktestLiveConsistencyChecker(workspace=tmp_path)
    checker.paths = ConsistencyPaths(
        workspace=tmp_path,
        reports_dir=reports_dir,
        orders_db=reports_dir / "shadow_orders.sqlite",
    )

    payload = checker.load_backtest_config()

    assert payload is not None
    assert payload["avg_cost_bps"] == 12.5


def test_load_backtest_config_falls_back_to_suffixed_runtime_cost_stats_dir(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    (reports_dir / "cost_stats_real").mkdir(parents=True, exist_ok=True)
    legacy_runtime_dir = reports_dir / "cost_stats_accelerated"
    legacy_runtime_dir.mkdir(parents=True, exist_ok=True)
    runtime_file = legacy_runtime_dir / "daily_cost_stats_20260409.json"
    runtime_file.write_text(
        json.dumps({"avg_cost_bps": 7.25}, ensure_ascii=False),
        encoding="utf-8",
    )

    checker = BacktestLiveConsistencyChecker(workspace=tmp_path)
    checker.paths = ConsistencyPaths(
        workspace=tmp_path,
        reports_dir=reports_dir,
        orders_db=reports_dir / "orders_accelerated.sqlite",
    )

    payload = checker.load_backtest_config()

    assert payload is not None
    assert payload["avg_cost_bps"] == 7.25


def test_load_backtest_config_prefers_latest_day_over_file_mtime(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    runtime_dir = reports_dir / "shadow_cost_stats_real"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    older_file = runtime_dir / "daily_cost_stats_20260408.json"
    newer_file = runtime_dir / "daily_cost_stats_20260409.json"
    older_file.write_text(json.dumps({"avg_cost_bps": 99}, ensure_ascii=False), encoding="utf-8")
    newer_file.write_text(json.dumps({"avg_cost_bps": 7.25}, ensure_ascii=False), encoding="utf-8")

    import os
    os.utime(older_file, (200, 200))
    os.utime(newer_file, (100, 100))

    checker = BacktestLiveConsistencyChecker(workspace=tmp_path)
    checker.paths = ConsistencyPaths(
        workspace=tmp_path,
        reports_dir=reports_dir,
        orders_db=reports_dir / "shadow_orders.sqlite",
    )

    payload = checker.load_backtest_config()

    assert payload is not None
    assert payload["avg_cost_bps"] == 7.25


def test_generate_report_uses_prefixed_runtime_filename(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    checker = BacktestLiveConsistencyChecker(workspace=tmp_path)
    checker.paths = ConsistencyPaths(
        workspace=tmp_path,
        reports_dir=reports_dir,
        orders_db=reports_dir / "shadow_orders.sqlite",
    )

    checker.generate_report()

    files = list(reports_dir.glob("shadow_consistency_check_*.json"))
    assert len(files) == 1


def test_generate_report_uses_suffixed_runtime_filename(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    checker = BacktestLiveConsistencyChecker(workspace=tmp_path)
    checker.paths = ConsistencyPaths(
        workspace=tmp_path,
        reports_dir=reports_dir,
        orders_db=reports_dir / "orders_accelerated.sqlite",
    )

    checker.generate_report()

    files = list(reports_dir.glob("consistency_check_*_accelerated.json"))
    assert len(files) == 1


def test_build_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    import scripts.consistency_checker as consistency_checker

    monkeypatch.setattr(consistency_checker, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(consistency_checker, "load_runtime_config", lambda project_root=None: {})

    try:
        consistency_checker.build_paths(tmp_path)
    except ValueError as exc:
        assert "live_prod.yaml" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_build_paths_fails_fast_when_runtime_config_is_missing(tmp_path: Path) -> None:
    import scripts.consistency_checker as consistency_checker

    missing_root = tmp_path / "missing-workspace"
    missing_root.mkdir(parents=True, exist_ok=True)

    try:
        consistency_checker.build_paths(missing_root)
    except FileNotFoundError as exc:
        assert "runtime config not found" in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")
