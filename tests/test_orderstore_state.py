from __future__ import annotations

import sqlite3
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


def test_orderstore_unknown_can_recover_to_authoritative_state() -> None:
    with tempfile.TemporaryDirectory() as td:
        st = OrderStore(path=f"{td}/orders.sqlite")
        clid = "RECOVER1"

        st.upsert_new(
            cl_ord_id=clid,
            run_id="r",
            inst_id="BTC-USDT",
            side="buy",
            intent="OPEN_LONG",
            decision_hash="h2",
            td_mode="cash",
            ord_type="market",
            notional_usdt=10.0,
        )
        st.update_state(clid, new_state="SENT")
        st.update_state(clid, new_state="UNKNOWN", last_error_code="QUERY", last_error_msg="timeout")
        st.update_state(clid, new_state="OPEN", ord_id="1001")
        st.update_state(clid, new_state="FILLED", avg_px="100", acc_fill_sz="0.1")

        row = st.get(clid)
        assert row is not None
        assert row.state == "FILLED"
        assert row.ord_id == "1001"


def test_orderstore_fill_updates_do_not_overwrite_original_order_size() -> None:
    with tempfile.TemporaryDirectory() as td:
        st = OrderStore(path=f"{td}/orders.sqlite")
        clid = "KEEP_SZ_1"

        st.upsert_new(
            cl_ord_id=clid,
            run_id="r",
            inst_id="SOL-USDT",
            side="buy",
            intent="OPEN_LONG",
            decision_hash="h3",
            td_mode="cash",
            ord_type="limit",
            notional_usdt=30.0,
            sz="0.300",
        )
        st.update_state(clid, new_state="PARTIAL", acc_fill_sz="0.100", avg_px="100")
        st.update_state(clid, new_state="FILLED", acc_fill_sz="0.300", avg_px="101")

        row = st.get(clid)
        assert row is not None
        assert row.state == "FILLED"
        assert row.sz == "0.300"
        assert row.acc_fill_sz == "0.300"


def test_get_latest_filled_prefers_event_ts_when_updated_ts_missing() -> None:
    with tempfile.TemporaryDirectory() as td:
        st = OrderStore(path=f"{td}/orders.sqlite")
        older = "OLDER1"
        recent = "RECENT1"

        st.upsert_new(
            cl_ord_id=older,
            run_id="r1",
            inst_id="BTC-USDT",
            side="buy",
            intent="OPEN_LONG",
            decision_hash="h1",
            td_mode="cash",
            ord_type="market",
            notional_usdt=10.0,
        )
        st.update_state(older, new_state="FILLED", avg_px="100", acc_fill_sz="0.1")
        st.upsert_new(
            cl_ord_id=recent,
            run_id="r2",
            inst_id="BTC-USDT",
            side="buy",
            intent="OPEN_LONG",
            decision_hash="h2",
            td_mode="cash",
            ord_type="market",
            notional_usdt=20.0,
        )
        st.update_state(recent, new_state="FILLED", avg_px="110", acc_fill_sz="0.2")

        con = sqlite3.connect(str(st.path))
        con.execute("UPDATE orders SET created_ts=?, updated_ts=? WHERE cl_ord_id=?", (100_000, 0, older))
        con.execute("UPDATE orders SET created_ts=?, updated_ts=? WHERE cl_ord_id=?", (950_000, 0, recent))
        con.commit()
        con.close()

        row = st.get_latest_filled(
            inst_id="BTC-USDT",
            side="buy",
            intent="OPEN_LONG",
            since_ts=900_000,
        )
        assert row is not None
        assert row.cl_ord_id == recent
