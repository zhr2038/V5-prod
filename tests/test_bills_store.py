from __future__ import annotations

import tempfile

from src.execution.bills_store import BillRow, BillsStore, parse_okx_bills


def test_bills_store_dedup_by_bill_id() -> None:
    with tempfile.TemporaryDirectory() as td:
        st = BillsStore(path=f"{td}/bills.sqlite")
        a = BillRow(bill_id="1", ts_ms=1, ccy="USDT", bal_chg="-1")
        b = BillRow(bill_id="1", ts_ms=1, ccy="USDT", bal_chg="-1")
        ins, total = st.upsert_many([a, b])
        assert total == 2
        assert ins == 1
        assert st.count() == 1


def test_parse_okx_bills_extracts_keys() -> None:
    resp = {
        "code": "0",
        "data": [
            {
                "billId": "99",
                "ts": "1700000000000",
                "ccy": "USDT",
                "balChg": "-0.01",
                "bal": "100",
                "type": "2",
                "subType": "1",
                "ordId": "123",
                "clOrdId": "ABC",
            }
        ],
    }
    rows = parse_okx_bills(resp)
    assert len(rows) == 1
    assert rows[0].bill_id == "99"
    assert rows[0].ts_ms == 1700000000000
    assert rows[0].ccy == "USDT"
    assert rows[0].bal_chg == "-0.01"
    assert rows[0].ord_id == "123"
    assert rows[0].cl_ord_id == "ABC"
