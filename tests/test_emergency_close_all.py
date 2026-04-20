from __future__ import annotations

from pathlib import Path

import pytest

import scripts.emergency_close_all as emergency_close_all


def test_resolve_report_path_uses_prefixed_runtime_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(emergency_close_all, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        emergency_close_all,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        emergency_close_all,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    path = emergency_close_all._resolve_report_path()

    assert path == (tmp_path / "reports" / "shadow_emergency_close_report.json").resolve()


def test_resolve_report_path_uses_suffixed_runtime_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(emergency_close_all, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        emergency_close_all,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        emergency_close_all,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    path = emergency_close_all._resolve_report_path()

    assert path == (tmp_path / "reports" / "emergency_close_report_accelerated.json").resolve()


def test_resolve_active_config_path_fails_fast_when_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(emergency_close_all, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        emergency_close_all,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        emergency_close_all._resolve_active_config_path()
