from __future__ import annotations

import json
import tempfile
from types import SimpleNamespace

from src.execution.account_store import AccountStore
from src.execution.position_store import PositionStore
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds


class FakeOKX:
    def __init__(self, cash):
        self._cash = cash

    def get_balance(self, ccy=None):
        details = []
        for k, v in self._cash.items():
            if isinstance(v, dict):
                cash_bal = v.get("cashBal", "0")
                eq_usd = v.get("eqUsd")
            else:
                cash_bal = v
                eq_usd = None
            row = {"ccy": k, "cashBal": str(cash_bal), "ordFrozen": "0", "uTime": "1700000000000"}
            if eq_usd is not None:
                row["eqUsd"] = str(eq_usd)
            details.append(row)
        return SimpleNamespace(data={"code": "0", "data": [{"details": details}]})


def test_reconcile_writes_status_and_flags_usdt_mismatch() -> None:
    with tempfile.TemporaryDirectory() as td:
        pos_db = f"{td}/pos.sqlite"
        ps = PositionStore(path=pos_db)
        ac = AccountStore(path=pos_db)
        st = ac.get()
        st.cash_usdt = 100.0
        ac.set(st)

        okx = FakeOKX({"USDT": "102.0"})
        eng = ReconcileEngine(okx=okx, position_store=ps, account_store=ac, thresholds=ReconcileThresholds(abs_usdt_tol=1.0))
        out_path = f"{td}/reconcile_status.json"
        obj = eng.reconcile(out_path=out_path)

        assert obj["ok"] is False
        assert obj["reason"] == "usdt_mismatch"
        disk = json.loads(open(out_path, "r", encoding="utf-8").read())
        assert disk["ok"] is False


def test_reconcile_ignores_exchange_only_dust_using_eq_usd() -> None:
    with tempfile.TemporaryDirectory() as td:
        pos_db = f"{td}/pos.sqlite"
        ps = PositionStore(path=pos_db)
        ac = AccountStore(path=pos_db)
        st = ac.get()
        st.cash_usdt = 100.0
        ac.set(st)

        okx = FakeOKX(
            {
                "USDT": {"cashBal": "100.0", "eqUsd": "100.0"},
                "APT": {"cashBal": "0.0000159", "eqUsd": "0.0000954"},
            }
        )
        eng = ReconcileEngine(
            okx=okx,
            position_store=ps,
            account_store=ac,
            thresholds=ReconcileThresholds(abs_usdt_tol=1.0, abs_base_tol=1e-6, dust_usdt_ignore=1.0),
        )
        obj = eng.reconcile(out_path=f"{td}/reconcile_status.json", ccy_mode="all")

        assert obj["ok"] is True
        assert obj["reason"] is None
        assert int((obj.get("ignored_dust") or {}).get("count") or 0) == 1
        apt = next(d for d in (obj.get("diffs") or []) if d.get("ccy") == "APT")
        assert apt["ignored_as_dust"] is True
        assert float(apt["estimated_delta_usdt"]) < 1.0


def test_reconcile_large_exchange_only_balance_still_fails() -> None:
    with tempfile.TemporaryDirectory() as td:
        pos_db = f"{td}/pos.sqlite"
        ps = PositionStore(path=pos_db)
        ac = AccountStore(path=pos_db)
        st = ac.get()
        st.cash_usdt = 100.0
        ac.set(st)

        okx = FakeOKX(
            {
                "USDT": {"cashBal": "100.0", "eqUsd": "100.0"},
                "APT": {"cashBal": "2.0", "eqUsd": "12.0"},
            }
        )
        eng = ReconcileEngine(
            okx=okx,
            position_store=ps,
            account_store=ac,
            thresholds=ReconcileThresholds(abs_usdt_tol=1.0, abs_base_tol=1e-6, dust_usdt_ignore=1.0),
        )
        obj = eng.reconcile(out_path=f"{td}/reconcile_status.json", ccy_mode="all")

        assert obj["ok"] is False
        assert obj["reason"] == "base_mismatch"
        apt = next(d for d in (obj.get("diffs") or []) if d.get("ccy") == "APT")
        assert apt["ignored_as_dust"] is False
        assert float(apt["estimated_delta_usdt"]) >= 1.0
