from __future__ import annotations

from pathlib import Path

import scripts.trade_auditor_v2 as trade_auditor_v2


def test_build_paths_uses_suffixed_runtime_log_and_alert_files(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_auditor_v2,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        trade_auditor_v2,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = trade_auditor_v2.build_paths(tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "orders_accelerated.sqlite")
    assert paths.log_file == (tmp_path / "logs" / "trade_audit_v2_accelerated.log").resolve()
    assert paths.alert_file == (tmp_path / "logs" / "trade_alert_v2_accelerated.json").resolve()


def test_build_paths_uses_nested_runtime_log_and_alert_files(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_auditor_v2,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(
        trade_auditor_v2,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = trade_auditor_v2.build_paths(tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "shadow_runtime" / "orders.sqlite")
    assert paths.log_file == (tmp_path / "logs" / "shadow_runtime_trade_audit_v2.log").resolve()
    assert paths.alert_file == (tmp_path / "logs" / "shadow_runtime_trade_alert_v2.json").resolve()
