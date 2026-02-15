from __future__ import annotations

import json
import tempfile
from types import SimpleNamespace

from src.execution.bills_store import BillRow, BillsStore
from src.execution.ledger_engine import LedgerEngine, LedgerThresholds


class FakeOKX:
    def __init__(self, balances):
        self._balances = balances

    def get_balance(self, ccy=None):
        details = []
        for k, v in self._balances.items():
            details.append({"ccy": k, "cashBal": str(v)})
        return SimpleNamespace(data={"code": "0", "data": [{"details": details}]}, http_status=200, okx_code="0", okx_msg="")


def test_no_baseline_creates_state_and_status() -> None:
    with tempfile.TemporaryDirectory() as td:
        bills_db = f"{td}/bills.sqlite"
        state_path = f"{td}/ledger_state.json"
        out_path = f"{td}/ledger_status.json"
        st = BillsStore(path=bills_db)
        okx = FakeOKX({"USDT": "100"})
        eng = LedgerEngine(okx=okx, bills_store=st, thresholds=LedgerThresholds(), state_path=state_path)
        obj = eng.run(out_path=out_path)
        assert obj["ok"] is False
        assert obj["reason"] == "no_baseline"
        assert json.load(open(state_path, "r", encoding="utf-8"))["balances"]["USDT"] == "100"


def test_delta_ok() -> None:
    with tempfile.TemporaryDirectory() as td:
        bills_db = f"{td}/bills.sqlite"
        state_path = f"{td}/ledger_state.json"
        out_path = f"{td}/ledger_status.json"
        st = BillsStore(path=bills_db)

        # baseline at t=1000 with USDT=100, last_bill_ts_ms=1000
        json.dump(
            {"schema_version": 1, "ts_ms": 1000, "last_bill_id": "1", "last_bill_ts_ms": 1000, "balances": {"USDT": "100"}},
            open(state_path, "w", encoding="utf-8"),
        )

        # add one bill chg -1 at ts=1500
        st.upsert_many([BillRow(bill_id="2", ts_ms=1500, ccy="USDT", bal_chg="-1")])

        okx = FakeOKX({"USDT": "99"})
        eng = LedgerEngine(okx=okx, bills_store=st, thresholds=LedgerThresholds(), state_path=state_path)
        obj = eng.run(out_path=out_path)
        assert obj["ok"] is True


def test_delta_fail_usdt() -> None:
    with tempfile.TemporaryDirectory() as td:
        bills_db = f"{td}/bills.sqlite"
        state_path = f"{td}/ledger_state.json"
        out_path = f"{td}/ledger_status.json"
        st = BillsStore(path=bills_db)

        json.dump(
            {"schema_version": 1, "ts_ms": 1000, "last_bill_id": "1", "last_bill_ts_ms": 1000, "balances": {"USDT": "100"}},
            open(state_path, "w", encoding="utf-8"),
        )
        st.upsert_many([BillRow(bill_id="2", ts_ms=1500, ccy="USDT", bal_chg="-1")])

        # actual says 95 => mismatch
        okx = FakeOKX({"USDT": "95"})
        eng = LedgerEngine(okx=okx, bills_store=st, thresholds=LedgerThresholds(), state_path=state_path)
        obj = eng.run(out_path=out_path)
        assert obj["ok"] is False
        assert obj["reason"] in {"ledger_mismatch_usdt", "ledger_mismatch_base"}
