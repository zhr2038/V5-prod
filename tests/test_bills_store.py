from __future__ import annotations

from src.execution.bills_store import BillRow, BillsStore


def test_last_bill_tie_breaks_same_timestamp_by_highest_bill_id(tmp_path):
    store = BillsStore(path=tmp_path / "bills.sqlite")
    store.upsert_many(
        [
            BillRow(bill_id="3513254028891381760", ts_ms=1777205529198, ccy="USDT"),
            BillRow(bill_id="3513254028891381761", ts_ms=1777205529198, ccy="USDT"),
        ]
    )

    assert store.last_bill() == ("3513254028891381761", 1777205529198)
