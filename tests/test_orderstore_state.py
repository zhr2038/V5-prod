from __future__ import annotations

import tempfile

from src.execution.order_store import OrderStore


def test_orderstore_state_monotonic() -> None:
    with tempfile.TemporaryDirectory() as td:
        st = OrderStore(path=f"{td}/orders.sqlite")

        clid = "TESTCLORDID123"
        st.upsert_new(
            cl_ord_id=clid,
            run_id="r",
            inst_id="BTC-USDT",
            side="buy",
            intent="OPEN_LONG",
            decision_hash="h",
            td_mode="cash",
            ord_type="market",
            notional_usdt=10.0,
        )
        st.update_state(clid, new_state="SENT")
        st.update_state(clid, new_state="ACK", ord_id="1")
        st.update_state(clid, new_state="FILLED", avg_px="100", acc_fill_sz="0.1")

        # try to go backward: should be ignored
        st.update_state(clid, new_state="OPEN")
        row = st.get(clid)
        assert row is not None
        assert row.state == "FILLED"


def test_orderstore_upsert_idempotent() -> None:
    with tempfile.TemporaryDirectory() as td:
        st = OrderStore(path=f"{td}/orders.sqlite")
        clid = "IDEMPOTENT1"

        st.upsert_new(
            cl_ord_id=clid,
            run_id="r",
            inst_id="ETH-USDT",
            side="buy",
            intent="REBALANCE",
            decision_hash="h1",
            td_mode="cash",
            ord_type="market",
            notional_usdt=25.0,
        )
        st.upsert_new(
            cl_ord_id=clid,
            run_id="r",
            inst_id="ETH-USDT",
            side="buy",
            intent="REBALANCE",
            decision_hash="h1",
            td_mode="cash",
            ord_type="market",
            notional_usdt=25.0,
        )

        row = st.get(clid)
        assert row is not None
        assert row.cl_ord_id == clid
        assert row.state == "NEW"
