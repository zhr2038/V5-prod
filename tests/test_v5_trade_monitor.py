from __future__ import annotations

from pathlib import Path

import scripts.v5_trade_monitor as trade_monitor


def test_build_paths_uses_prefixed_runtime_alert_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_monitor,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        trade_monitor,
        "_resolve_runtime_path",
        lambda raw_path, default, project_root: (project_root / (raw_path or default)).resolve(),
    )
    monkeypatch.setattr(
        trade_monitor,
        "resolve_runtime_env_path",
        lambda project_root=None: str((tmp_path / ".env").resolve()),
    )

    paths = trade_monitor.build_paths(tmp_path)

    assert paths.orders_db_path == (tmp_path / "reports" / "shadow_orders.sqlite").resolve()
    assert paths.alert_file == (tmp_path / "reports" / "shadow_monitor_alert.txt").resolve()


def test_build_paths_uses_suffixed_runtime_alert_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_monitor,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        trade_monitor,
        "_resolve_runtime_path",
        lambda raw_path, default, project_root: (project_root / (raw_path or default)).resolve(),
    )
    monkeypatch.setattr(
        trade_monitor,
        "resolve_runtime_env_path",
        lambda project_root=None: str((tmp_path / ".env").resolve()),
    )

    paths = trade_monitor.build_paths(tmp_path)

    assert paths.orders_db_path == (tmp_path / "reports" / "orders_accelerated.sqlite").resolve()
    assert paths.alert_file == (tmp_path / "reports" / "monitor_alert_accelerated.txt").resolve()
