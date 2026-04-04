from __future__ import annotations

import json
import tempfile

from types import SimpleNamespace

import pytest

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
        self.next_ord_id = 1001
        self._orders = {}
        self.fills_by_ord_id = {}
        self.balance_by_ccy = {
            "USDT": {"eq": "1000", "availBal": "1000", "cashBal": "1000", "liab": "0"},
        }

    def place_order(self, payload, exp_time_ms=None):
        self.place_calls += 1
        clid = payload.get("clOrdId")
        ord_id = str(self.next_ord_id)
        self.next_ord_id += 1
        self._orders[clid] = {
            "instId": payload.get("instId"),
            "clOrdId": clid,
            "ordId": ord_id,
            "state": "live",
            "accFillSz": "0",
            "avgPx": "",
        }
        return SimpleNamespace(data={"code": "0", "data": [{"ordId": ord_id, "clOrdId": clid}]})

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

    def get_fills(self, *, inst_type="SPOT", inst_id=None, ord_id=None, after=None, before=None, begin=None, end=None, limit=100):
        rows = list(self.fills_by_ord_id.get(str(ord_id), []))
        return SimpleNamespace(data={"code": "0", "data": rows})


class ImmediateFillOKX(FakeOKX):
    def place_order(self, payload, exp_time_ms=None):
        resp = super().place_order(payload, exp_time_ms=exp_time_ms)
        clid = payload.get("clOrdId")
        row = self._orders[clid]
        row["state"] = "filled"
        row["accFillSz"] = "0.706776"
        row["avgPx"] = "97.4"
        self.fills_by_ord_id[row["ordId"]] = [
            {
                "fillSz": "0.706776",
                "fee": "-0.000706776",
                "feeCcy": "OKB",
            }
        ]
        return resp


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


def test_sell_budget_blocks_second_sell_when_exchange_balance_is_stale() -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = FakeOKX()
        okx.balance_by_ccy["ETH"] = {"eq": "1.4", "availBal": "1.4", "cashBal": "1.4", "liab": "0"}
        store = OrderStore(path=f"{td}/orders.sqlite")
        pos = PositionStore(path=f"{td}/pos.sqlite")
        pos.upsert_buy("ETH/USDT", qty=2.0, px=100.0)

        cfg = ExecutionConfig(reconcile_status_path=f"{td}/reconcile_status.json", kill_switch_path=f"{td}/kill_switch.json")
        eng = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")

        first = Order(symbol="ETH/USDT", side="sell", intent="CLOSE_LONG", notional_usdt=150.0, signal_price=100.0, meta={"decision_hash": "h4"})
        second = Order(symbol="ETH/USDT", side="sell", intent="CLOSE_LONG", notional_usdt=150.0, signal_price=100.0, meta={"decision_hash": "h5"})

        eng.place(first)
        row = store.list_open(limit=10)[0]
        req = json.loads(row.req_json)
        assert req["sz"] == "1.4"

        result = eng.place(second)
        assert result.state == "REJECTED"


def test_dust_skip_does_not_consume_sell_budget(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = FakeOKX()
        okx.balance_by_ccy["ETH"] = {"eq": "2.0", "availBal": "2.0", "cashBal": "2.0", "liab": "0"}
        store = OrderStore(path=f"{td}/orders.sqlite")
        pos = PositionStore(path=f"{td}/pos.sqlite")
        pos.upsert_buy("ETH/USDT", qty=2.0, px=100.0)

        monkeypatch.setattr(
            "src.execution.live_execution_engine.OKXSpotInstrumentsCache.get_spec",
            lambda self, inst_id: SimpleNamespace(lot_sz=0.1, min_sz=1.0),
        )

        cfg = ExecutionConfig(
            reconcile_status_path=f"{td}/reconcile_status.json",
            kill_switch_path=f"{td}/kill_switch.json",
            auto_upgrade_dust_sell_to_close=False,
        )
        eng = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")

        dust_reject = eng.place(
            Order(
                symbol="ETH/USDT",
                side="sell",
                intent="REBALANCE",
                notional_usdt=50.0,
                signal_price=100.0,
                meta={"decision_hash": "dust-skip"},
            )
        )
        close_result = eng.place(
            Order(
                symbol="ETH/USDT",
                side="sell",
                intent="CLOSE_LONG",
                notional_usdt=200.0,
                signal_price=100.0,
                meta={"decision_hash": "close-after-dust"},
            )
        )

        row = store.list_open(limit=10)[0]
        req = json.loads(row.req_json)

        assert dust_reject.state == "REJECTED"
        assert close_result.state == "OPEN"
        assert float(req["sz"]) == pytest.approx(2.0)


def test_dust_auto_upgrade_reserves_full_sell_budget(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = FakeOKX()
        okx.balance_by_ccy["ETH"] = {"eq": "2.0", "availBal": "2.0", "cashBal": "2.0", "liab": "0"}
        store = OrderStore(path=f"{td}/orders.sqlite")
        pos = PositionStore(path=f"{td}/pos.sqlite")
        pos.upsert_buy("ETH/USDT", qty=2.0, px=100.0)

        monkeypatch.setattr(
            "src.execution.live_execution_engine.OKXSpotInstrumentsCache.get_spec",
            lambda self, inst_id: SimpleNamespace(lot_sz=0.1, min_sz=1.0),
        )

        cfg = ExecutionConfig(
            reconcile_status_path=f"{td}/reconcile_status.json",
            kill_switch_path=f"{td}/kill_switch.json",
        )
        eng = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")

        first = eng.place(
            Order(
                symbol="ETH/USDT",
                side="sell",
                intent="REBALANCE",
                notional_usdt=50.0,
                signal_price=100.0,
                meta={"decision_hash": "dust-upgrade"},
            )
        )
        second = eng.place(
            Order(
                symbol="ETH/USDT",
                side="sell",
                intent="CLOSE_LONG",
                notional_usdt=200.0,
                signal_price=100.0,
                meta={"decision_hash": "close-after-upgrade"},
            )
        )

        row = store.list_open(limit=10)[0]
        req = json.loads(row.req_json)

        assert first.state == "OPEN"
        assert float(req["sz"]) == pytest.approx(2.0)
        assert second.state == "REJECTED"


def test_buy_fill_uses_net_base_qty_when_fee_is_charged_in_base() -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = FakeOKX()
        store = OrderStore(path=f"{td}/orders.sqlite")
        pos = PositionStore(path=f"{td}/pos.sqlite")

        cfg = ExecutionConfig(reconcile_status_path=f"{td}/reconcile_status.json", kill_switch_path=f"{td}/kill_switch.json")
        eng = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")
        o = Order(symbol="OKB/USDT", side="buy", intent="OPEN_LONG", notional_usdt=97.4, signal_price=97.4, meta={"decision_hash": "h6"})

        result = eng.place(o)
        row = okx._orders[result.cl_ord_id]
        row["state"] = "filled"
        row["accFillSz"] = "0.706776"
        row["avgPx"] = "97.4"
        okx.fills_by_ord_id[row["ordId"]] = [
            {
                "fillSz": "0.706776",
                "fee": "-0.000706776",
                "feeCcy": "OKB",
            }
        ]

        result = eng.place(o)
        p = pos.get("OKB/USDT")
        assert result.state == "FILLED"
        assert p is not None
        assert p.qty == pytest.approx(0.706069224)


def test_immediate_buy_fill_updates_position_only_once() -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = ImmediateFillOKX()
        store = OrderStore(path=f"{td}/orders.sqlite")
        pos = PositionStore(path=f"{td}/pos.sqlite")

        cfg = ExecutionConfig(reconcile_status_path=f"{td}/reconcile_status.json", kill_switch_path=f"{td}/kill_switch.json")
        eng = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")
        o = Order(symbol="OKB/USDT", side="buy", intent="OPEN_LONG", notional_usdt=97.4, signal_price=97.4, meta={"decision_hash": "h7"})

        result = eng.place(o)
        p = pos.get("OKB/USDT")

        assert result.state == "FILLED"
        assert p is not None
        assert p.qty == pytest.approx(0.706069224)
