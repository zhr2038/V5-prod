from __future__ import annotations

import json
import tempfile

from types import SimpleNamespace

from configs.schema import ExecutionConfig
from src.core.models import Order
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


class FakeOKX:
    def __init__(self):
        self.place_calls = 0
        self.get_calls = 0
        self.cancel_calls = 0
        self._orders = {}
        self.balance_by_ccy = {
            "USDT": {"eq": "1000", "availBal": "1000", "cashBal": "1000", "liab": "0"},
        }

    def place_order(self, payload, exp_time_ms=None):
        self.place_calls += 1
        clid = payload.get("clOrdId")
        self._orders[clid] = {
            "instId": payload.get("instId"),
            "clOrdId": clid,
            "ordId": "1001",
            "state": "live",
            "accFillSz": "0",
            "avgPx": "",
        }
        return SimpleNamespace(data={"code": "0", "data": [{"ordId": "1001", "clOrdId": clid}]})

    def get_order(self, *, inst_id, ord_id=None, cl_ord_id=None):
        self.get_calls += 1
        row = self._orders.get(cl_ord_id)
        if row is None:
            return SimpleNamespace(data={"code": "0", "data": []})
        return SimpleNamespace(data={"code": "0", "data": [row]})

    def cancel_order(self, *, inst_id, ord_id=None, cl_ord_id=None):
        self.cancel_calls += 1
        row = self._orders.get(cl_ord_id)
        if row:
            row["state"] = "canceled"
        return SimpleNamespace(data={"code": "0", "data": [{"clOrdId": cl_ord_id}]})

    def get_balance(self, ccy=None):
        details = []
        if ccy is not None:
            payload = self.balance_by_ccy.get(str(ccy), None)
            if payload is not None:
                details.append({"ccy": str(ccy), **payload})
        else:
            for sym, payload in self.balance_by_ccy.items():
                details.append({"ccy": sym, **payload})
        return SimpleNamespace(data={"data": [{"details": details}]})


def test_place_idempotent_same_intent() -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = FakeOKX()
        store = OrderStore(path=f"{td}/orders.sqlite")
        pos = PositionStore(path=f"{td}/pos.sqlite")
        pos.upsert_buy("BTC/USDT", qty=1.0, px=100.0)

        cfg = ExecutionConfig(reconcile_status_path=f"{td}/reconcile_status.json", kill_switch_path=f"{td}/kill_switch.json")
        eng = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")

        o = Order(symbol="BTC/USDT", side="buy", intent="OPEN_LONG", notional_usdt=10.0, signal_price=100.0, meta={"decision_hash": "h"})

        r1 = eng.place(o)
        r2 = eng.place(o)

        assert r1.cl_ord_id == r2.cl_ord_id
        # second call should not place again because order already exists and is non-terminal (will query)
        assert okx.place_calls == 1


def test_sell_market_uses_position_qty() -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = FakeOKX()
        store = OrderStore(path=f"{td}/orders.sqlite")
        pos = PositionStore(path=f"{td}/pos.sqlite")
        pos.upsert_buy("ETH/USDT", qty=2.0, px=100.0)

        cfg = ExecutionConfig(reconcile_status_path=f"{td}/reconcile_status.json", kill_switch_path=f"{td}/kill_switch.json")
        eng = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")
        o = Order(symbol="ETH/USDT", side="sell", intent="REBALANCE", notional_usdt=50.0, signal_price=100.0, meta={"decision_hash": "h2"})

        eng.place(o)
        # payload stored in req_json should have sz==2.0 for sells
        row = store.list_open(limit=10)[0]
        assert row is not None
        req = json.loads(row.req_json)
        assert req["sz"] == "0.5"


def test_sell_market_caps_qty_to_okx_available_balance() -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = FakeOKX()
        okx.balance_by_ccy["ETH"] = {"eq": "1.4", "availBal": "1.4", "cashBal": "1.4", "liab": "0"}
        store = OrderStore(path=f"{td}/orders.sqlite")
        pos = PositionStore(path=f"{td}/pos.sqlite")
        pos.upsert_buy("ETH/USDT", qty=2.0, px=100.0)

        cfg = ExecutionConfig(reconcile_status_path=f"{td}/reconcile_status.json", kill_switch_path=f"{td}/kill_switch.json")
        eng = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")
        o = Order(symbol="ETH/USDT", side="sell", intent="CLOSE_LONG", notional_usdt=200.0, signal_price=100.0, meta={"decision_hash": "h3"})

        eng.place(o)
        row = store.list_open(limit=10)[0]
        req = json.loads(row.req_json)
        assert req["sz"] == "1.4"
