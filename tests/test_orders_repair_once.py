from __future__ import annotations

import scripts.orders_repair_once as orders_repair_once


def test_resolve_orders_db_uses_runtime_execution_path(monkeypatch, tmp_path):
    monkeypatch.setattr(orders_repair_once, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        orders_repair_once,
        "load_runtime_config",
        lambda config_path=None, project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    resolved = orders_repair_once.resolve_orders_db(config_path="configs/runtime.yaml")

    assert resolved == (tmp_path / "reports" / "shadow_orders.sqlite").resolve()


def test_main_passes_runtime_orders_db_to_repair(monkeypatch, tmp_path):
    monkeypatch.setattr(orders_repair_once, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        orders_repair_once,
        "load_runtime_config",
        lambda config_path=None, project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        orders_repair_once,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )
    captured = {}
    monkeypatch.setattr(
        orders_repair_once,
        "repair_unknown_orders",
        lambda **kwargs: captured.update(kwargs) or {"ok": True},
    )

    orders_repair_once.main(["--config", "configs/runtime.yaml", "--limit", "12"])

    assert captured["db_path"] == str((tmp_path / "reports" / "shadow_orders.sqlite").resolve())
    assert captured["limit"] == 12
