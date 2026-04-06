from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.bills_sync as bills_sync
import scripts.ledger_once as ledger_once


def test_bills_sync_defaults_db_to_repo_root(monkeypatch) -> None:
    captured = {}
    workspace = Path(bills_sync.__file__).resolve().parents[1]
    expected_cfg = (workspace / "configs" / "live_prod.yaml").resolve()
    expected_env = (workspace / ".env").resolve()

    class DummyStore:
        def __init__(self, path: str) -> None:
            self.path = path
            captured["db_path"] = Path(path).resolve()

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return SimpleNamespace(exchange=SimpleNamespace())

    def _fake_sync_once(*, store, client, limit: int, max_pages: int) -> int:
        captured["limit"] = limit
        captured["max_pages"] = max_pages
        return 0

    monkeypatch.setattr(bills_sync, "load_config", _fake_load_config)
    monkeypatch.setattr(bills_sync, "BillsStore", DummyStore)
    monkeypatch.setattr(bills_sync, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(bills_sync, "sync_once", _fake_sync_once)
    monkeypatch.setattr(sys, "argv", ["bills_sync.py"])

    bills_sync.main()

    assert captured["config_path"] == expected_cfg
    assert captured["env_path"] == expected_env
    assert captured["db_path"] == (workspace / "reports" / "bills.sqlite").resolve()
    assert captured["limit"] == 100
    assert captured["max_pages"] == 50


def test_ledger_once_defaults_runtime_paths_to_repo_root(monkeypatch) -> None:
    captured = {}
    workspace = Path(ledger_once.__file__).resolve().parents[1]
    expected_cfg = (workspace / "configs" / "live_prod.yaml").resolve()
    expected_env = (workspace / ".env").resolve()

    class DummyStore:
        def __init__(self, path: str) -> None:
            self.path = path
            captured["bills_db_path"] = Path(path).resolve()

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    class DummyLedgerEngine:
        def __init__(self, *, okx, bills_store, thresholds, state_path: str) -> None:
            captured["ledger_state_path"] = Path(state_path).resolve()
            captured["engine_bills_db_path"] = Path(bills_store.path).resolve()

        def run(self, *, out_path: str):
            captured["out_path"] = Path(out_path).resolve()
            return {
                "ok": True,
                "reason": None,
                "bills_aggregate": {"count": 0},
                "current": {"last_bill_id": None},
            }

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return SimpleNamespace(exchange=SimpleNamespace())

    monkeypatch.setattr(ledger_once, "load_config", _fake_load_config)
    monkeypatch.setattr(ledger_once, "BillsStore", DummyStore)
    monkeypatch.setattr(ledger_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(ledger_once, "LedgerEngine", DummyLedgerEngine)
    monkeypatch.setattr(sys, "argv", ["ledger_once.py"])

    ledger_once.main()

    assert captured["config_path"] == expected_cfg
    assert captured["env_path"] == expected_env
    assert captured["bills_db_path"] == (workspace / "reports" / "bills.sqlite").resolve()
    assert captured["engine_bills_db_path"] == (workspace / "reports" / "bills.sqlite").resolve()
    assert captured["ledger_state_path"] == (workspace / "reports" / "ledger_state.json").resolve()
    assert captured["out_path"] == (workspace / "reports" / "ledger_status.json").resolve()
