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


def test_preflight_passes_configured_reconcile_mode(monkeypatch):
    captured = {}

    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(lp, "LedgerEngine", DummyLedger)

    class CapturingRecon(DummyRecon):
        def reconcile(self, *, out_path: str, **kwargs):
            captured["kwargs"] = dict(kwargs)
            return super().reconcile(out_path=out_path, **kwargs)

    monkeypatch.setattr(lp, "ReconcileEngine", CapturingRecon)

    class DummyGuard:
        def __init__(self, *a, **k):
            pass

        def apply(self):
            return {"ok": True, "reason": "ok", "category": "OK", "kill_switch": {"enabled": False}}

    monkeypatch.setattr(lp, "KillSwitchGuard", lambda *a, **k: DummyGuard())
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)

    cfg = SimpleNamespace(
        reconcile_status_path="reconcile.json",
        reconcile_dust_usdt_ignore=1.0,
        reconcile_ccy_mode="all",
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
        assert captured["kwargs"]["ccy_mode"] == "all"


def test_preflight_symbol_only_borrow_blocks_only_affected_symbols(monkeypatch):
    recorded = []

    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(lp, "LedgerEngine", DummyLedger)
    monkeypatch.setattr(lp, "ReconcileEngine", DummyRecon)
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)
    monkeypatch.setattr(lp, "auto_blacklist_add", lambda sym, **kwargs: recorded.append(sym))

    class DummyGuard:
        def __init__(self, *a, **k):
            pass

        def apply(self):
            return {"ok": True, "reason": "ok", "category": "OK", "kill_switch": {"enabled": False}}

    monkeypatch.setattr(lp, "KillSwitchGuard", lambda *a, **k: DummyGuard())
    monkeypatch.setattr(
        lp,
        "check_okx_borrows",
        lambda *a, **k: SimpleNamespace(
            ok=False,
            items=[SimpleNamespace(ccy="OKB", eq=-0.2, liab=0.2, cross_liab=0.0, borrow_froz=0.0)],
            reason="borrow_detected",
            raw={},
        ),
    )

    cfg = SimpleNamespace(
        reconcile_status_path="reconcile.json",
        reconcile_dust_usdt_ignore=1.0,
        abort_on_borrow=True,
        borrow_block_mode="symbol_only",
        enforce_account_config_check=False,
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
        assert res.details["borrow_check"]["action"] == "symbol_blacklist_only"
        assert res.details["borrow_check"]["blocked_symbols"] == ["OKB/USDT"]
        assert recorded == ["OKB/USDT"]


def test_preflight_quote_liability_degrades_to_sell_only(monkeypatch):
    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(lp, "LedgerEngine", DummyLedger)
    monkeypatch.setattr(lp, "ReconcileEngine", DummyRecon)
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)

    class DummyGuard:
        def __init__(self, *a, **k):
            pass

        def apply(self):
            return {"ok": True, "reason": "ok", "category": "OK", "kill_switch": {"enabled": False}}

    monkeypatch.setattr(lp, "KillSwitchGuard", lambda *a, **k: DummyGuard())
    monkeypatch.setattr(
        lp,
        "check_okx_borrows",
        lambda *a, **k: SimpleNamespace(
            ok=False,
            items=[SimpleNamespace(ccy="USDT", eq=-3.0, liab=3.0, cross_liab=0.0, borrow_froz=0.0)],
            reason="borrow_detected",
            raw={},
        ),
    )

    cfg = SimpleNamespace(
        reconcile_status_path="reconcile.json",
        reconcile_dust_usdt_ignore=1.0,
        abort_on_borrow=True,
        borrow_block_mode="symbol_only",
        enforce_account_config_check=False,
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
        assert res.decision == "SELL_ONLY"
        assert res.reason == "borrow_detected_quote_liability"
        assert res.details["borrow_check"]["quote_liability_ccys"] == ["USDT"]


def test_preflight_auto_fixes_fee_type_before_allowing_buys(monkeypatch):
    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(lp, "LedgerEngine", DummyLedger)
    monkeypatch.setattr(lp, "ReconcileEngine", DummyRecon)
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)

    class DummyGuard:
        def __init__(self, *a, **k):
            pass

        def apply(self):
            return {"ok": True, "reason": "ok", "category": "OK", "kill_switch": {"enabled": False}}

    monkeypatch.setattr(lp, "KillSwitchGuard", lambda *a, **k: DummyGuard())

    class FeeTypeOKX(DummyOKX):
        def __init__(self):
            self.fee_type = "1"
            self.calls = 0

        def get_account_config(self):
            self.calls += 1
            return SimpleNamespace(
                data={
                    "data": [
                        {
                            "acctLv": "1",
                            "posMode": "net_mode",
                            "autoLoan": False,
                            "enableSpotBorrow": False,
                            "spotBorrowAutoRepay": True,
                            "feeType": self.fee_type,
                        }
                    ]
                }
            )

        def set_fee_type(self, fee_type):
            self.fee_type = str(fee_type)
            return SimpleNamespace(data={"code": "0", "data": []})

    cfg = SimpleNamespace(
        reconcile_status_path="reconcile.json",
        reconcile_dust_usdt_ignore=1.0,
        enforce_account_config_check=True,
        required_acct_lv="1",
        required_pos_mode="net_mode",
        require_auto_loan_false=True,
        require_fee_type_zero=True,
        auto_fix_fee_type_zero=True,
    )

    with tempfile.TemporaryDirectory() as td:
        okx = FeeTypeOKX()
        pf = lp.LivePreflight(
            cfg,
            okx=okx,
            position_store=object(),
            account_store=object(),
            bills_db_path=f"{td}/bills.sqlite",
            ledger_state_path=f"{td}/ledger_state.json",
            ledger_status_path=f"{td}/ledger_status.json",
            reconcile_status_path=f"{td}/reconcile_status.json",
        )
        res = pf.run(max_pages=1, max_status_age_sec=180)
        assert res.decision == "ALLOW"
        assert res.details["account_config"]["feeType"] == "0"
        assert res.details["account_config"]["feeTypeAutoFixed"] is True
        assert okx.calls >= 2


def test_preflight_fee_type_mismatch_degrades_to_sell_only_when_not_fixed(monkeypatch):
    monkeypatch.setattr(lp, "BillsStore", DummyBillsStore)
    monkeypatch.setattr(lp, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(lp, "LedgerEngine", DummyLedger)
    monkeypatch.setattr(lp, "ReconcileEngine", DummyRecon)
    monkeypatch.setattr(lp, "_now_ms", lambda: 1000)

    class DummyGuard:
        def __init__(self, *a, **k):
            pass

        def apply(self):
            return {"ok": True, "reason": "ok", "category": "OK", "kill_switch": {"enabled": False}}

    monkeypatch.setattr(lp, "KillSwitchGuard", lambda *a, **k: DummyGuard())

    class FeeTypeOKX(DummyOKX):
        def get_account_config(self):
            return SimpleNamespace(
                data={
                    "data": [
                        {
                            "acctLv": "1",
                            "posMode": "net_mode",
                            "autoLoan": False,
                            "enableSpotBorrow": False,
                            "spotBorrowAutoRepay": True,
                            "feeType": "1",
                        }
                    ]
                }
            )

    cfg = SimpleNamespace(
        reconcile_status_path="reconcile.json",
        reconcile_dust_usdt_ignore=1.0,
        enforce_account_config_check=True,
        required_acct_lv="1",
        required_pos_mode="net_mode",
        require_auto_loan_false=True,
        require_fee_type_zero=True,
        auto_fix_fee_type_zero=False,
    )

    with tempfile.TemporaryDirectory() as td:
        pf = lp.LivePreflight(
            cfg,
            okx=FeeTypeOKX(),
            position_store=object(),
            account_store=object(),
            bills_db_path=f"{td}/bills.sqlite",
            ledger_state_path=f"{td}/ledger_state.json",
            ledger_status_path=f"{td}/ledger_status.json",
            reconcile_status_path=f"{td}/reconcile_status.json",
        )
        res = pf.run(max_pages=1, max_status_age_sec=180)
        assert res.decision == "SELL_ONLY"
        assert "feeType_mismatch:1!=0" in res.details["account_config"]["violations"]
