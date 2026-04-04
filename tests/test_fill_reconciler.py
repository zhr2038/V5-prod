from __future__ import annotations

import json
import tempfile

import pytest

from src.execution.fill_reconciler import FillReconciler
from src.execution.fill_store import FillRow, FillStore
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


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


def test_fill_reconciler_partial_buy_updates_position_store() -> None:
    with tempfile.TemporaryDirectory() as td:
        fills = FillStore(path=f"{td}/fills.sqlite")
        orders = OrderStore(path=f"{td}/orders.sqlite")
        positions = PositionStore(path=f"{td}/positions.sqlite")

        clid = "BUY_PARTIAL"
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
            [
                FillRow(
                    inst_id="BTC-USDT",
                    trade_id="b1",
                    ts_ms=1,
                    ord_id="OID1",
                    cl_ord_id=clid,
                    side="buy",
                    fill_px="100",
                    fill_sz="0.01",
                    fee="-0.0001",
                    fee_ccy="BTC",
                ),
                FillRow(
                    inst_id="BTC-USDT",
                    trade_id="b2",
                    ts_ms=2,
                    ord_id="OID1",
                    cl_ord_id=clid,
                    side="buy",
                    fill_px="110",
                    fill_sz="0.01",
                    fee="-0.0001",
                    fee_ccy="BTC",
                ),
            ]
        )

        rec = FillReconciler(fill_store=fills, order_store=orders, okx=None, position_store=positions)
        out = rec.reconcile()
        pos = positions.get("BTC/USDT")

        assert out["updated_orders"] == 1
        assert pos is not None
        assert pos.qty == pytest.approx(0.0198)
        assert pos.avg_px == pytest.approx(105.0)


def test_fill_reconciler_accumulates_order_fields_across_runs() -> None:
    with tempfile.TemporaryDirectory() as td:
        fills = FillStore(path=f"{td}/fills.sqlite")
        orders = OrderStore(path=f"{td}/orders.sqlite")

        clid = "BUY_MULTI_PARTIAL"
        orders.upsert_new(
            cl_ord_id=clid,
            run_id="r",
            inst_id="BTC-USDT",
            side="buy",
            intent="OPEN_LONG",
            decision_hash="h-multi",
            td_mode="cash",
            ord_type="market",
            notional_usdt=10.0,
            req={"clOrdId": clid},
        )

        rec = FillReconciler(fill_store=fills, order_store=orders, okx=None)

        fills.upsert_many(
            [
                FillRow(
                    inst_id="BTC-USDT",
                    trade_id="m1",
                    ts_ms=1,
                    ord_id="OIDM",
                    cl_ord_id=clid,
                    side="buy",
                    fill_px="100",
                    fill_sz="0.4",
                    fee="-0.001",
                    fee_ccy="BTC",
                ),
            ]
        )
        rec.reconcile()

        fills.upsert_many(
            [
                FillRow(
                    inst_id="BTC-USDT",
                    trade_id="m2",
                    ts_ms=2,
                    ord_id="OIDM",
                    cl_ord_id=clid,
                    side="buy",
                    fill_px="110",
                    fill_sz="0.6",
                    fee="-0.002",
                    fee_ccy="BTC",
                ),
            ]
        )
        rec.reconcile()

        row = orders.get(clid)
        assert row is not None
        fee_map = json.loads(str(row.fee))

        assert float(row.acc_fill_sz) == pytest.approx(1.0)
        assert float(row.avg_px) == pytest.approx(106.0)
        assert fee_map == {"BTC": "-0.003"}


def test_fill_reconciler_does_not_double_count_polled_filled_totals() -> None:
    with tempfile.TemporaryDirectory() as td:
        fills = FillStore(path=f"{td}/fills.sqlite")
        orders = OrderStore(path=f"{td}/orders.sqlite")

        clid = "FILLED_ALREADY_POLLED"
        orders.upsert_new(
            cl_ord_id=clid,
            run_id="r",
            inst_id="OKB-USDT",
            side="buy",
            intent="OPEN_LONG",
            decision_hash="h-filled",
            td_mode="cash",
            ord_type="market",
            notional_usdt=10.0,
            req={"clOrdId": clid},
        )
        orders.update_state(
            clid,
            new_state="FILLED",
            ord_id="OIDF",
            acc_fill_sz="0.706776",
            avg_px="97.4",
            event_type="POLL",
        )

        fills.upsert_many(
            [
                FillRow(
                    inst_id="OKB-USDT",
                    trade_id="f1",
                    ts_ms=1,
                    ord_id="OIDF",
                    cl_ord_id=clid,
                    side="buy",
                    fill_px="97.4",
                    fill_sz="0.706776",
                    fee="-0.000706776",
                    fee_ccy="OKB",
                ),
            ]
        )

        rec = FillReconciler(fill_store=fills, order_store=orders, okx=None)
        out = rec.reconcile()
        row = orders.get(clid)

        assert out["updated_orders"] == 1
        assert row is not None
        assert row.state == "FILLED"
        assert float(row.acc_fill_sz) == pytest.approx(0.706776)
        assert float(row.avg_px) == pytest.approx(97.4)
        assert json.loads(str(row.fee)) == {"OKB": "-0.000706776"}


def test_fill_reconciler_partial_sell_updates_position_store_idempotently() -> None:
    with tempfile.TemporaryDirectory() as td:
        fills = FillStore(path=f"{td}/fills.sqlite")
        orders = OrderStore(path=f"{td}/orders.sqlite")
        positions = PositionStore(path=f"{td}/positions.sqlite")

        clid = "SELL_PARTIAL"
        positions.upsert_buy("BTC/USDT", qty=1.0, px=100.0)
        orders.upsert_new(
            cl_ord_id=clid,
            run_id="r",
            inst_id="BTC-USDT",
            side="sell",
            intent="REBALANCE",
            decision_hash="h2",
            td_mode="cash",
            ord_type="market",
            notional_usdt=10.0,
            req={"clOrdId": clid},
        )

        fills.upsert_many(
            [
                FillRow(
                    inst_id="BTC-USDT",
                    trade_id="s1",
                    ts_ms=1,
                    ord_id="OID2",
                    cl_ord_id=clid,
                    side="sell",
                    fill_px="100",
                    fill_sz="0.4",
                    fee="-0.001",
                    fee_ccy="BTC",
                ),
            ]
        )

        rec = FillReconciler(fill_store=fills, order_store=orders, okx=None, position_store=positions)
        out1 = rec.reconcile()
        out2 = rec.reconcile()
        pos = positions.get("BTC/USDT")

        assert out1["updated_orders"] == 1
        assert out2["updated_orders"] == 0
        assert pos is not None
        assert pos.qty == pytest.approx(0.599)
