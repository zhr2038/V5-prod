from __future__ import annotations

import tempfile

from src.execution.fill_reconciler import FillReconciler
from src.execution.fill_store import FillRow, FillStore
from src.execution.order_store import OrderStore


def test_reconcile_marks_processed_and_is_idempotent() -> None:
    with tempfile.TemporaryDirectory() as td:
        fills = FillStore(path=f"{td}/fills.sqlite")
        orders = OrderStore(path=f"{td}/orders.sqlite")

        clid = "CLID1"
        orders.upsert_new(
            cl_ord_id=clid,
            run_id="r",
            inst_id="BTC-USDT",
            side="buy",
            intent="OPEN_LONG",
            decision_hash="h",
            td_mode="cash",
            ord_type="market",
            notional_usdt=10.0,
            req={"clOrdId": clid},
        )

        fills.upsert_many(
            [FillRow(inst_id="BTC-USDT", trade_id="t1", ts_ms=1, ord_id="OID", cl_ord_id=clid, fill_px="1", fill_sz="1")]
        )

        rec = FillReconciler(fill_store=fills, order_store=orders, okx=None)
        out1 = rec.reconcile()
        out2 = rec.reconcile()

        assert out1["new_fills"] == 1
        assert out2["new_fills"] == 0
