from __future__ import annotations

import sqlite3
import tempfile

from src.execution.order_gc import gc_unknown_orders
from src.execution.order_store import OrderStore


def _mk_unknown(store: OrderStore, clid: str, *, last_query: dict | None = None, updated_ts_ms: int | None = None) -> None:
    store.upsert_new(
        cl_ord_id=clid,
        run_id="r",
        inst_id="BTC-USDT",
        side="buy",
        intent="OPEN_LONG",
        decision_hash="h",
        td_mode="cash",
        ord_type="market",
        notional_usdt=10.0,
        req={},
    )
    store.update_state(clid, new_state="UNKNOWN", last_query=last_query or {})

    if updated_ts_ms is not None:
        con = sqlite3.connect(str(store.path))
        con.execute("UPDATE orders SET updated_ts=? WHERE cl_ord_id=?", (int(updated_ts_ms), str(clid)))
        con.commit()
        con.close()


def test_gc_unknown_not_found():
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/orders.sqlite"
        store = OrderStore(path=db)
        _mk_unknown(store, "c1", last_query={"code": "51603", "msg": "Order does not exist"}, updated_ts_ms=1)
        out = gc_unknown_orders(db_path=db, ttl_sec=0, limit=10)
        assert out["stats"]["gc_rejected"] == 1
        row = store.get("c1")
        assert row is not None
        assert row.state == "REJECTED"
        assert row.last_error_code == "NOT_FOUND"


def test_gc_unknown_no_ack_no_ordid_expires():
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/orders.sqlite"
        store = OrderStore(path=db)
        _mk_unknown(store, "c2", last_query={}, updated_ts_ms=1)
        out = gc_unknown_orders(db_path=db, ttl_sec=0, limit=10)
        assert out["stats"]["gc_rejected"] == 1
        row = store.get("c2")
        assert row is not None
        assert row.state == "REJECTED"
        assert row.last_error_code == "EXPIRED"
