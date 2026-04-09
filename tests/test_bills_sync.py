from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.bills_sync as bills_sync


def test_bills_sync_uses_runtime_bills_db_from_order_store_path(monkeypatch) -> None:
    captured = {}
    workspace = Path(bills_sync.__file__).resolve().parents[1]

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(order_store_path="reports/shadow_runtime/orders.sqlite"),
    )

    class DummyStore:
        def __init__(self, path: str) -> None:
            captured["store_path"] = Path(path).resolve()

        def count(self) -> int:
            return 0

        def set_state(self, key: str, value: str) -> None:
            captured.setdefault("state_updates", []).append((key, value))

        def get_state(self, key: str):
            return None

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            captured["exchange"] = kwargs.get("exchange")

        def close(self) -> None:
            captured["closed"] = True

    def _fake_sync_once(*, store, client, limit: int, max_pages: int) -> int:
        captured["sync_store_path"] = captured["store_path"]
        captured["limit"] = limit
        captured["max_pages"] = max_pages
        return 0

    monkeypatch.setattr(bills_sync, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(bills_sync, "BillsStore", DummyStore)
    monkeypatch.setattr(bills_sync, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(bills_sync, "sync_once", _fake_sync_once)
    monkeypatch.setattr(sys, "argv", ["bills_sync.py"])

    bills_sync.main()

    assert captured["store_path"] == (workspace / "reports" / "shadow_runtime" / "bills.sqlite").resolve()
    assert captured["sync_store_path"] == captured["store_path"]
    assert captured["limit"] == 100
    assert captured["max_pages"] == 50
    assert captured["closed"] is True


def test_bills_sync_keeps_explicit_db_override(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(order_store_path="reports/shadow_runtime/orders.sqlite"),
    )

    class DummyStore:
        def __init__(self, path: str) -> None:
            captured["store_path"] = Path(path).resolve()

        def count(self) -> int:
            return 0

        def set_state(self, key: str, value: str) -> None:
            pass

        def get_state(self, key: str):
            return None

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setattr(bills_sync, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(bills_sync, "BillsStore", DummyStore)
    monkeypatch.setattr(bills_sync, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(bills_sync, "sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(sys, "argv", ["bills_sync.py", "--db", str(tmp_path / "custom_bills.sqlite")])

    bills_sync.main()

    assert captured["store_path"] == (tmp_path / "custom_bills.sqlite").resolve()
