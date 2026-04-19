from __future__ import annotations

from pathlib import Path

import scripts.alert_manager as alert_manager


def test_alert_manager_uses_prefixed_runtime_state_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        alert_manager,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        alert_manager,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    manager = alert_manager.AlertManager(workspace=tmp_path)

    assert manager.alert_state_file == (tmp_path / "reports" / "shadow_alert_state.json").resolve()


def test_alert_manager_uses_suffixed_runtime_state_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        alert_manager,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        alert_manager,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    manager = alert_manager.AlertManager(workspace=tmp_path)

    assert manager.alert_state_file == (tmp_path / "reports" / "alert_state_accelerated.json").resolve()


def test_alert_manager_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        alert_manager,
        "load_runtime_config",
        lambda project_root=None: {},
    )

    try:
        alert_manager.AlertManager(workspace=tmp_path)
    except ValueError as exc:
        assert "live_prod.yaml" in str(exc)
    else:
        raise AssertionError("expected ValueError")
