from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.ledger_once as ledger_once


def test_ledger_once_uses_runtime_bills_and_ledger_paths_from_order_store_path(monkeypatch) -> None:
    captured = {}
    workspace = Path(ledger_once.__file__).resolve().parents[1]

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(order_store_path="reports/shadow_runtime/orders.sqlite"),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            captured["closed"] = True

    class DummyBillsStore:
        def __init__(self, path: str) -> None:
            captured["bills_store_path"] = Path(path).resolve()

    class DummyLedgerEngine:
        def __init__(self, *, okx, bills_store, thresholds, state_path: str) -> None:
            captured["state_path"] = Path(state_path).resolve()
            captured["engine_bills_store_path"] = captured["bills_store_path"]

        def run(self, *, out_path: str):
            captured["out_path"] = Path(out_path).resolve()
            return {"ok": True, "reason": None, "bills_aggregate": {"count": 0}, "current": {"last_bill_id": None}}

    monkeypatch.setattr(ledger_once, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(ledger_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(ledger_once, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(ledger_once, "LedgerEngine", DummyLedgerEngine)
    monkeypatch.setattr(sys, "argv", ["ledger_once.py"])

    ledger_once.main()

    runtime_dir = (workspace / "reports" / "shadow_runtime").resolve()
    assert captured["bills_store_path"] == (runtime_dir / "bills.sqlite").resolve()
    assert captured["engine_bills_store_path"] == (runtime_dir / "bills.sqlite").resolve()
    assert captured["state_path"] == (runtime_dir / "ledger_state.json").resolve()
    assert captured["out_path"] == (runtime_dir / "ledger_status.json").resolve()
    assert captured["closed"] is True


def test_ledger_once_keeps_explicit_path_overrides(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(order_store_path="reports/shadow_runtime/orders.sqlite"),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    class DummyBillsStore:
        def __init__(self, path: str) -> None:
            captured["bills_store_path"] = Path(path).resolve()

    class DummyLedgerEngine:
        def __init__(self, *, okx, bills_store, thresholds, state_path: str) -> None:
            captured["state_path"] = Path(state_path).resolve()

        def run(self, *, out_path: str):
            captured["out_path"] = Path(out_path).resolve()
            return {"ok": True, "reason": None, "bills_aggregate": {"count": 0}, "current": {"last_bill_id": None}}

    monkeypatch.setattr(ledger_once, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(ledger_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(ledger_once, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(ledger_once, "LedgerEngine", DummyLedgerEngine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ledger_once.py",
            "--bills-db",
            str(tmp_path / "custom_bills.sqlite"),
            "--out",
            str(tmp_path / "custom_ledger_status.json"),
        ],
    )

    ledger_once.main()

    assert captured["bills_store_path"] == (tmp_path / "custom_bills.sqlite").resolve()
    assert captured["out_path"] == (tmp_path / "custom_ledger_status.json").resolve()
    assert captured["state_path"] == (Path(ledger_once.__file__).resolve().parents[1] / "reports" / "shadow_runtime" / "ledger_state.json").resolve()
