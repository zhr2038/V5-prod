from __future__ import annotations

from pathlib import Path

import pytest

import scripts.equity_anomaly_detector as detector


def test_build_paths_uses_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        detector,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(
        detector,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = detector.build_paths(tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "shadow_runtime" / "orders.sqlite").resolve()
    assert paths.reports_dir == (tmp_path / "reports" / "shadow_runtime").resolve()
    assert paths.runs_dir == (tmp_path / "reports" / "shadow_runtime" / "runs").resolve()


def test_build_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(detector, "load_runtime_config", lambda project_root=None: {})

    with pytest.raises(ValueError, match="live_prod.yaml"):
        detector.build_paths(tmp_path)
