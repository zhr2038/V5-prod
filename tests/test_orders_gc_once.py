from __future__ import annotations

import scripts.orders_gc_once as orders_gc_once


def test_resolve_orders_db_uses_runtime_execution_path(monkeypatch, tmp_path):
    monkeypatch.setattr(orders_gc_once, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        orders_gc_once,
        "load_runtime_config",
        lambda config_path=None, project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    resolved = orders_gc_once.resolve_orders_db(config_path="configs/runtime.yaml")

    assert resolved == (tmp_path / "reports" / "shadow_orders.sqlite").resolve()


def test_main_passes_runtime_orders_db_to_gc(monkeypatch, tmp_path):
    monkeypatch.setattr(orders_gc_once, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        orders_gc_once,
        "load_runtime_config",
        lambda config_path=None, project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        orders_gc_once,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )
    captured = {}
    monkeypatch.setattr(
        orders_gc_once,
        "gc_unknown_orders",
        lambda **kwargs: captured.update(kwargs) or {"ok": True},
    )

    orders_gc_once.main(["--config", "configs/runtime.yaml", "--ttl-sec", "60", "--limit", "10"])

    assert captured["db_path"] == str((tmp_path / "reports" / "shadow_orders.sqlite").resolve())
    assert captured["ttl_sec"] == 60
    assert captured["limit"] == 10


def test_resolve_orders_db_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(orders_gc_once, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        orders_gc_once,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )
    monkeypatch.setattr(
        orders_gc_once,
        "load_runtime_config",
        lambda config_path=None, project_root=None: {},
    )

    try:
        orders_gc_once.resolve_orders_db(config_path="configs/runtime.yaml")
    except ValueError as exc:
        assert "runtime.yaml" in str(exc)
    else:
        raise AssertionError("expected ValueError")
