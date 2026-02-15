from __future__ import annotations

import tempfile

from src.execution.fill_reconciler import FillReconciler
from src.execution.fill_store import FillRow, FillStore
from src.execution.order_store import OrderStore


def test_fill_reconciler_pushes_partial_and_agg_fields() -> None:
    with tempfile.TemporaryDirectory() as td:
        fills = FillStore(path=f"{td}/fills.sqlite")
        orders = OrderStore(path=f"{td}/orders.sqlite")

        clid = "CLID123"
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

        # Insert two fills for same order
        fills.upsert_many(
            [
                FillRow(
                    inst_id="BTC-USDT",
                    trade_id="1",
                    ts_ms=1,
                    ord_id="OID",
                    cl_ord_id=clid,
                    fill_px="100",
                    fill_sz="0.01",
                    fee="-0.001",
                    fee_ccy="USDT",
                ),
                FillRow(
                    inst_id="BTC-USDT",
                    trade_id="2",
                    ts_ms=2,
                    ord_id="OID",
                    cl_ord_id=clid,
                    fill_px="110",
                    fill_sz="0.01",
                    fee="-0.001",
                    fee_ccy="USDT",
                ),
            ]
        )

        rec = FillReconciler(fill_store=fills, order_store=orders, okx=None)
        out = rec.reconcile()
        assert out["updated_orders"] == 1

        row = orders.get(clid)
        assert row is not None
        assert row.state in {"PARTIAL", "FILLED", "CANCELED"}
        assert row.acc_fill_sz is not None
        assert float(row.acc_fill_sz) > 0
        assert row.avg_px is not None
