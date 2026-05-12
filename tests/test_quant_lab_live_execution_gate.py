from __future__ import annotations

from pathlib import Path

from configs.schema import ExecutionConfig
from src.core.models import Order
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.okx_private_client import OKXResponse
from src.execution.order_store import OrderStore
from src.execution.position_store import Position, PositionStore


class _OKX:
    def place_order(self, *args, **kwargs):
        raise AssertionError("OKX place_order must not be called")


class _AcceptOKX:
    def place_order(self, *args, **kwargs):
        return OKXResponse(data={"code": "0", "data": [{"sCode": "0", "ordId": "okx-1"}]}, http_status=200)


def _engine(tmp_path: Path, *, quant_lab_mode: str | None = None, okx=None) -> LiveExecutionEngine:
    cfg = ExecutionConfig(
        order_store_path=str(tmp_path / "orders.sqlite"),
        kill_switch_path=str(tmp_path / "kill_switch.json"),
        reconcile_status_path=str(tmp_path / "reconcile_status.json"),
        ledger_status_path=str(tmp_path / "ledger_status.json"),
    )
    if quant_lab_mode is not None:
        cfg.quant_lab_effective_enabled = True
        cfg.quant_lab_effective_mode = quant_lab_mode
    store = OrderStore(path=str(tmp_path / "orders.sqlite"))
    pos = PositionStore(path=str(tmp_path / "positions.sqlite"))
    return LiveExecutionEngine(cfg, okx=okx or _OKX(), order_store=store, position_store=pos, run_id="r")


def test_live_execution_rejects_quant_lab_abort(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    order = Order(
        "BTC/USDT",
        "sell",
        "CLOSE_LONG",
        10.0,
        100.0,
        {"quant_lab": {"final_permission": "ABORT", "permission_gate_enforced": True, "filter_reason": "quant_lab_abort"}},
    )

    result = engine.place(order)
    row = engine.order_store.get(result.cl_ord_id)

    assert result.state == "REJECTED"
    assert row.last_error_code == "QUANT_LAB_GATE"
    assert row.submit_gate == "ABORT"


def test_live_execution_rejects_quant_lab_sell_only_buy(tmp_path: Path) -> None:
    engine = _engine(tmp_path, quant_lab_mode="enforce")
    order = Order(
        "BTC/USDT",
        "buy",
        "OPEN_LONG",
        10.0,
        100.0,
        {"quant_lab": {"final_permission": "SELL_ONLY", "permission_gate_enforced": True, "filter_reason": "quant_lab_sell_only"}},
    )

    result = engine.place(order)
    row = engine.order_store.get(result.cl_ord_id)

    assert result.state == "REJECTED"
    assert row.last_error_code == "QUANT_LAB_GATE"
    assert row.submit_gate == "SELL_ONLY"


def test_live_execution_ignores_quant_lab_when_permission_gate_not_enforced(tmp_path: Path) -> None:
    engine = _engine(tmp_path, quant_lab_mode="shadow", okx=_AcceptOKX())
    order = Order(
        "BTC/USDT",
        "buy",
        "OPEN_LONG",
        10.0,
        100.0,
        {"quant_lab": {"final_permission": "SELL_ONLY", "permission_gate_enforced": False, "would_filter_by_permission": True}},
    )

    result = engine.place(order)
    row = engine.order_store.get(result.cl_ord_id)

    assert row is not None
    assert result.state != "REJECTED" or row.last_error_code != "QUANT_LAB_GATE"
    assert row.last_error_code != "QUANT_LAB_GATE"


def test_live_execution_fail_closed_enforce_missing_quant_lab_meta(tmp_path: Path) -> None:
    engine = _engine(tmp_path, quant_lab_mode="enforce")
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {})

    result = engine.place(order)
    row = engine.order_store.get(result.cl_ord_id)

    assert result.state == "REJECTED"
    assert row.last_error_code == "QUANT_LAB_GATE"
    assert row.last_error_msg == "quant_lab_missing_enforced_meta"


def test_live_execution_fail_closed_enforce_meta_not_enforced(tmp_path: Path) -> None:
    engine = _engine(tmp_path, quant_lab_mode="enforce")
    order = Order(
        "BTC/USDT",
        "buy",
        "OPEN_LONG",
        10.0,
        100.0,
        {"quant_lab": {"final_permission": "ALLOW", "permission_gate_enforced": False}},
    )

    result = engine.place(order)
    row = engine.order_store.get(result.cl_ord_id)

    assert result.state == "REJECTED"
    assert row.last_error_code == "QUANT_LAB_GATE"
    assert row.last_error_msg == "quant_lab_permission_meta_not_enforced"


def test_live_execution_sell_close_allowed_under_quant_lab_sell_only(tmp_path: Path) -> None:
    engine = _engine(tmp_path, quant_lab_mode="enforce", okx=_AcceptOKX())
    engine.position_store.upsert_position(
        Position(
            symbol="ETH/USDT",
            qty=1.0,
            avg_px=2000.0,
            entry_ts="2026-05-11T00:00:00Z",
            highest_px=2000.0,
            last_update_ts="2026-05-11T00:00:00Z",
            last_mark_px=2000.0,
            unrealized_pnl_pct=0.0,
        )
    )
    order = Order(
        "ETH/USDT",
        "sell",
        "CLOSE_LONG",
        10.0,
        2000.0,
        {"quant_lab": {"final_permission": "SELL_ONLY", "permission_gate_enforced": True}},
    )

    result = engine.place(order)
    row = engine.order_store.get(result.cl_ord_id)

    assert row is not None
    assert result.state != "REJECTED" or row.last_error_code != "QUANT_LAB_GATE"
    assert row.last_error_code != "QUANT_LAB_GATE"
