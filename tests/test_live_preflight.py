from __future__ import annotations

import json
import tempfile
from types import SimpleNamespace

import src.execution.live_preflight as lp


class DummyOKX:
    def get_balance(self, ccy=None):
        # minimal OKXResponse-like object
        class R:
            def __init__(self):
                self.data = {"data": [{"details": []}]}

        return R()


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


def test_preflight_refreshes_reconcile_before_account_config_error(monkeypatch):
    calls = {"reconcile": 0, "guard": 0, "account_config": 0}
    captured = {}

    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(lp, "LedgerEngine", DummyLedger)

    class CountingRecon(DummyRecon):
        def reconcile(self, *, out_path: str, **kwargs):
            calls["reconcile"] += 1
            return super().reconcile(out_path=out_path, **kwargs)

    monkeypatch.setattr(lp, "ReconcileEngine", CountingRecon)

    class DummyGuard:
        def __init__(self, cfg):
            captured["guard_cfg"] = cfg

        def apply(self):
            calls["guard"] += 1
            return {"ok": True, "reason": "ok", "category": "OK", "kill_switch": {"enabled": False}}

    monkeypatch.setattr(lp, "KillSwitchGuard", DummyGuard)
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)

    class BadAccountConfigOKX(DummyOKX):
        def get_account_config(self):
            calls["account_config"] += 1
            raise RuntimeError("boom")

    with tempfile.TemporaryDirectory() as td:
        kill_path = f"{td}/kill_switch.json"
        cfg = SimpleNamespace(
            reconcile_status_path=f"{td}/reconcile_status.json",
            kill_switch_path=kill_path,
            reconcile_dust_usdt_ignore=1.0,
            enforce_account_config_check=True,
        )
        pf = lp.LivePreflight(
            cfg,
            okx=BadAccountConfigOKX(),
            position_store=object(),
            account_store=object(),
            bills_db_path=f"{td}/bills.sqlite",
            ledger_state_path=f"{td}/ledger_state.json",
            ledger_status_path=f"{td}/ledger_status.json",
            reconcile_status_path=f"{td}/reconcile_status.json",
        )
        res = pf.run(max_pages=1, max_status_age_sec=180)
        assert res.decision == "SELL_ONLY"
        assert res.reason == "account_config_check_error"
        assert calls["reconcile"] == 1
        assert calls["guard"] == 1
        assert calls["account_config"] == 1
        assert captured["guard_cfg"].kill_switch_path == kill_path


def test_preflight_kill_switch_short_circuits_buy_gating_checks(monkeypatch):
    calls = {"balance": 0, "account_config": 0}

    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(lp, "LedgerEngine", DummyLedger)
    monkeypatch.setattr(lp, "ReconcileEngine", DummyRecon)

    class DummyGuard:
        def __init__(self, cfg):
            pass

        def apply(self):
            return {"ok": False, "reason": "base_mismatch", "category": "HARD", "kill_switch": {"enabled": True, "trigger": "t"}}

    monkeypatch.setattr(lp, "KillSwitchGuard", DummyGuard)
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)

    class StrictOKX(DummyOKX):
        def get_balance(self, ccy=None):
            calls["balance"] += 1
            raise AssertionError("borrow check should not run after kill switch")

        def get_account_config(self):
            calls["account_config"] += 1
            raise AssertionError("account config should not run after kill switch")

    with tempfile.TemporaryDirectory() as td:
        cfg = SimpleNamespace(
            reconcile_status_path=f"{td}/reconcile_status.json",
            kill_switch_path=f"{td}/kill_switch.json",
            reconcile_dust_usdt_ignore=1.0,
            enforce_account_config_check=True,
        )
        pf = lp.LivePreflight(
            cfg,
            okx=StrictOKX(),
            position_store=object(),
            account_store=object(),
            bills_db_path=f"{td}/bills.sqlite",
            ledger_state_path=f"{td}/ledger_state.json",
            ledger_status_path=f"{td}/ledger_status.json",
            reconcile_status_path=f"{td}/reconcile_status.json",
        )
        res = pf.run(max_pages=1, max_status_age_sec=180)
        assert res.decision == "ABORT"
        assert res.reason == "kill_switch"
        assert calls["balance"] == 0
        assert calls["account_config"] == 0
