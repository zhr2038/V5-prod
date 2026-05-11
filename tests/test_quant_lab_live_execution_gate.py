from __future__ import annotations

from pathlib import Path

from configs.schema import ExecutionConfig
from src.core.models import Order
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


class _OKX:
    def place_order(self, *args, **kwargs):
        raise AssertionError("OKX place_order must not be called")


def _engine(tmp_path: Path) -> LiveExecutionEngine:
    cfg = ExecutionConfig(
        order_store_path=str(tmp_path / "orders.sqlite"),
        kill_switch_path=str(tmp_path / "kill_switch.json"),
        reconcile_status_path=str(tmp_path / "reconcile_status.json"),
        ledger_status_path=str(tmp_path / "ledger_status.json"),
    )
    store = OrderStore(path=str(tmp_path / "orders.sqlite"))
    pos = PositionStore(path=str(tmp_path / "positions.sqlite"))
    return LiveExecutionEngine(cfg, okx=_OKX(), order_store=store, position_store=pos, run_id="r")


def test_live_execution_rejects_quant_lab_abort(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    order = Order("BTC/USDT", "sell", "CLOSE_LONG", 10.0, 100.0, {"quant_lab": {"final_permission": "ABORT", "filter_reason": "quant_lab_abort"}})

    result = engine.place(order)
    row = engine.order_store.get(result.cl_ord_id)

    assert result.state == "REJECTED"
    assert row.last_error_code == "QUANT_LAB_GATE"
    assert row.submit_gate == "ABORT"


def test_live_execution_rejects_quant_lab_sell_only_buy(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"quant_lab": {"final_permission": "SELL_ONLY", "filter_reason": "quant_lab_sell_only"}})

    result = engine.place(order)
    row = engine.order_store.get(result.cl_ord_id)

    assert result.state == "REJECTED"
    assert row.last_error_code == "QUANT_LAB_GATE"
    assert row.submit_gate == "SELL_ONLY"
