from __future__ import annotations

import sys
from pathlib import Path

import scripts.orders_gc_once as orders_gc_once
import scripts.orders_repair_once as orders_repair_once


def test_orders_gc_once_defaults_to_repo_orders_db(monkeypatch, tmp_path: Path, capsys) -> None:
    fake_root = tmp_path / "repo"
    (fake_root / "reports").mkdir(parents=True, exist_ok=True)
    captured: dict[str, object] = {}

    monkeypatch.setattr(orders_gc_once, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        orders_gc_once,
        "gc_unknown_orders",
        lambda *, db_path, ttl_sec, limit: captured.update(
            {"db_path": Path(db_path).resolve(), "ttl_sec": ttl_sec, "limit": limit}
        )
        or {"stats": {"gc_rejected": 0}},
    )
    monkeypatch.setattr(sys, "argv", ["orders_gc_once.py"])

    orders_gc_once.main()

    assert captured["db_path"] == (fake_root / "reports" / "orders.sqlite").resolve()
    assert captured["ttl_sec"] == 1800
    assert captured["limit"] == 500
    assert '"gc_rejected": 0' in capsys.readouterr().out


def test_orders_repair_once_defaults_to_repo_orders_db(monkeypatch, tmp_path: Path, capsys) -> None:
    fake_root = tmp_path / "repo"
    (fake_root / "reports").mkdir(parents=True, exist_ok=True)
    captured: dict[str, object] = {}

    monkeypatch.setattr(orders_repair_once, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        orders_repair_once,
        "repair_unknown_orders",
        lambda *, db_path, limit: captured.update(
            {"db_path": Path(db_path).resolve(), "limit": limit}
        )
        or {"stats": {"repaired": 0}},
    )
    monkeypatch.setattr(sys, "argv", ["orders_repair_once.py"])

    orders_repair_once.main()

    assert captured["db_path"] == (fake_root / "reports" / "orders.sqlite").resolve()
    assert captured["limit"] == 500
    assert '"repaired": 0' in capsys.readouterr().out
