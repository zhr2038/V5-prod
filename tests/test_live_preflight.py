from __future__ import annotations

import json
import tempfile
from types import SimpleNamespace

import src.execution.live_preflight as lp


class DummyOKX:
    pass


class DummyBillsStore:
    def __init__(self, path: str):
        self._count = 0

    def count(self) -> int:
        return self._count


class DummyLedger:
    def __init__(self, **kwargs):
        pass

    def run(self, *, out_path: str):
        obj = {"ts_ms": 1000, "ok": True, "reason": None, "bills_aggregate": {"count": 0}}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(obj, f)
        return obj


class DummyRecon:
    def __init__(self, **kwargs):
        pass

    def reconcile(self, *, out_path: str, **kwargs):
        obj = {"ts_ms": 1000, "ok": True, "reason": None}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(obj, f)
        return obj


def test_preflight_allow(monkeypatch):
    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(lp, "LedgerEngine", DummyLedger)
    monkeypatch.setattr(lp, "ReconcileEngine", DummyRecon)

    class DummyGuard:
        def __init__(self, *a, **k):
            pass

        def apply(self):
            return {"ok": True, "reason": "ok", "category": "OK", "kill_switch": {"enabled": False}}

    monkeypatch.setattr(lp, "KillSwitchGuard", lambda *a, **k: DummyGuard())

    # fresh status
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)

    cfg = SimpleNamespace(
        reconcile_status_path="reconcile.json",
        reconcile_dust_usdt_ignore=1.0,
    )

    with tempfile.TemporaryDirectory() as td:
        pf = lp.LivePreflight(
            cfg,
            okx=DummyOKX(),
            position_store=object(),
            account_store=object(),
            bills_db_path=f"{td}/bills.sqlite",
            ledger_state_path=f"{td}/ledger_state.json",
            ledger_status_path=f"{td}/ledger_status.json",
            reconcile_status_path=f"{td}/reconcile_status.json",
        )
        res = pf.run(max_pages=1, max_status_age_sec=180)
        assert res.decision == "ALLOW"


def test_preflight_abort_on_kill_switch(monkeypatch):
    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(lp, "LedgerEngine", DummyLedger)
    monkeypatch.setattr(lp, "ReconcileEngine", DummyRecon)

    class DummyGuard:
        def __init__(self, *a, **k):
            pass

        def apply(self):
            return {"ok": False, "reason": "x", "category": "HARD", "kill_switch": {"enabled": True, "trigger": "t"}}

    monkeypatch.setattr(lp, "KillSwitchGuard", lambda *a, **k: DummyGuard())
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)

    cfg = SimpleNamespace(reconcile_status_path="reconcile.json", reconcile_dust_usdt_ignore=1.0)
    with tempfile.TemporaryDirectory() as td:
        pf = lp.LivePreflight(
            cfg,
            okx=DummyOKX(),
            position_store=object(),
            account_store=object(),
            bills_db_path=f"{td}/bills.sqlite",
            ledger_state_path=f"{td}/ledger_state.json",
            ledger_status_path=f"{td}/ledger_status.json",
            reconcile_status_path=f"{td}/reconcile_status.json",
        )
        res = pf.run(max_pages=1, max_status_age_sec=180)
        assert res.decision == "ABORT"


def test_preflight_sell_only_if_ledger_not_ok(monkeypatch):
    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)

    class BadLedger(DummyLedger):
        def run(self, *, out_path: str):
            obj = {"ts_ms": 1000, "ok": False, "reason": "ledger_mismatch_base", "bills_aggregate": {"count": 0}}
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(obj, f)
            return obj

    monkeypatch.setattr(lp, "LedgerEngine", BadLedger)
    monkeypatch.setattr(lp, "ReconcileEngine", DummyRecon)

    class DummyGuard:
        def __init__(self, *a, **k):
            pass

        def apply(self):
            return {"ok": True, "reason": "ok", "category": "OK", "kill_switch": {"enabled": False}}

    monkeypatch.setattr(lp, "KillSwitchGuard", lambda *a, **k: DummyGuard())
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)

    cfg = SimpleNamespace(reconcile_status_path="reconcile.json", reconcile_dust_usdt_ignore=1.0)
    with tempfile.TemporaryDirectory() as td:
        pf = lp.LivePreflight(
            cfg,
            okx=DummyOKX(),
            position_store=object(),
            account_store=object(),
            bills_db_path=f"{td}/bills.sqlite",
            ledger_state_path=f"{td}/ledger_state.json",
            ledger_status_path=f"{td}/ledger_status.json",
            reconcile_status_path=f"{td}/reconcile_status.json",
        )
        res = pf.run(max_pages=1, max_status_age_sec=180)
        assert res.decision == "SELL_ONLY"
