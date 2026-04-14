from __future__ import annotations

from pathlib import Path

from src.monitoring import smart_alert as smart_alert_module


def test_resolve_paths_uses_prefixed_runtime_alert_state(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        smart_alert_module,
        "_load_active_config",
        lambda workspace: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        smart_alert_module,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = smart_alert_module._resolve_paths(workspace=tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "shadow_orders.sqlite")
    assert paths.alerts_state_file == (tmp_path / "reports" / "shadow_alerts_state.json").resolve()


def test_resolve_paths_uses_suffixed_runtime_alert_state(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        smart_alert_module,
        "_load_active_config",
        lambda workspace: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        smart_alert_module,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = smart_alert_module._resolve_paths(workspace=tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "orders_accelerated.sqlite")
    assert paths.alerts_state_file == (tmp_path / "reports" / "alerts_state_accelerated.json").resolve()
