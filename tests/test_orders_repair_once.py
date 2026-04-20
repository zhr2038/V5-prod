from __future__ import annotations

import scripts.orders_repair_once as orders_repair_once


def test_resolve_orders_db_uses_runtime_execution_path(monkeypatch, tmp_path):
    monkeypatch.setattr(orders_repair_once, "PROJECT_ROOT", tmp_path)
    config_path = (tmp_path / "configs" / "runtime.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/shadow_orders.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(
        orders_repair_once,
        "load_runtime_config",
        lambda config_path=None, project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    resolved = orders_repair_once.resolve_orders_db(config_path="configs/runtime.yaml")

    assert resolved == (tmp_path / "reports" / "shadow_orders.sqlite").resolve()


def test_main_passes_runtime_orders_db_to_repair(monkeypatch, tmp_path):
    monkeypatch.setattr(orders_repair_once, "PROJECT_ROOT", tmp_path)
    config_path = (tmp_path / "configs" / "runtime.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/shadow_orders.sqlite\n", encoding="utf-8")
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


def test_resolve_orders_db_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(orders_repair_once, "PROJECT_ROOT", tmp_path)
    config_path = (tmp_path / "configs" / "runtime.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        orders_repair_once,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )
    monkeypatch.setattr(
        orders_repair_once,
        "load_runtime_config",
        lambda config_path=None, project_root=None: {},
    )

    try:
        orders_repair_once.resolve_orders_db(config_path="configs/runtime.yaml")
    except ValueError as exc:
        assert "runtime.yaml" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_resolve_orders_db_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(orders_repair_once, "PROJECT_ROOT", tmp_path)
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(
        orders_repair_once,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    try:
        orders_repair_once.resolve_orders_db(config_path="configs/missing.yaml")
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")
