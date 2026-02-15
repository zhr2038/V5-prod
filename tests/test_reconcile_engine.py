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
            details.append({"ccy": k, "cashBal": str(v), "ordFrozen": "0", "uTime": "1700000000000"})
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
