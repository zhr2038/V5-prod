from __future__ import annotations

import json
import sqlite3
import tempfile

from src.execution.order_repair import repair_unknown_orders
from src.execution.order_store import OrderStore


def _mk_unknown(store: OrderStore, clid: str, ack: dict) -> None:
    store.upsert_new(
        cl_ord_id=clid,
        run_id="r",
        inst_id="BTC-USDT",
        side="sell",
        intent="CLOSE_LONG",
        decision_hash="h",
        td_mode="cash",
        ord_type="market",
        notional_usdt=1.0,
        req={},
    )
    store.update_state(clid, new_state="UNKNOWN", ack=ack)


def test_repair_rejects_scode() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/orders.sqlite"
        store = OrderStore(path=db)
        _mk_unknown(store, "c1", {"code": "1", "msg": "All operations failed", "data": [{"sCode": "51000", "sMsg": "Parameter sz error"}]})

        out = repair_unknown_orders(db_path=db, limit=50)
        assert out["stats"]["repaired"] == 1

        row = store.get("c1")
        assert row is not None
        assert row.state == "REJECTED"
        assert row.last_error_code == "51000"

        # idempotent
        out2 = repair_unknown_orders(db_path=db, limit=50)
        assert out2["stats"]["repaired"] == 0


def test_repair_skips_accepted() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/orders.sqlite"
        store = OrderStore(path=db)
        _mk_unknown(store, "c2", {"code": "0", "msg": "", "data": [{"sCode": "0", "sMsg": "", "ordId": "123"}]})

        out = repair_unknown_orders(db_path=db, limit=50)
        assert out["stats"]["repaired"] == 0
        row = store.get("c2")
        assert row is not None
        assert row.state == "UNKNOWN"
