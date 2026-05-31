from __future__ import annotations

import csv
from pathlib import Path

from src.core.models import Order
from src.execution.fill_store import FillRow, FillStore
from src.execution.order_store import OrderStore
from src.reporting.order_lifecycle import (
    annotate_orders_with_arrival,
    write_order_lifecycle,
)


def test_order_lifecycle_records_arrival_submit_and_fill(tmp_path: Path) -> None:
    run_id = "run_lifecycle"
    run_dir = tmp_path / "reports" / "runs" / run_id
    reports_dir = tmp_path / "reports"
    order_store_path = tmp_path / "reports" / "orders.sqlite"
    fill_store_path = tmp_path / "reports" / "fills.sqlite"
    order = Order(
        symbol="BNB/USDT",
        side="buy",
        intent="OPEN_LONG",
        notional_usdt=120.0,
        signal_price=600.0,
        meta={},
    )

    count = annotate_orders_with_arrival(
        [order],
        run_id=run_id,
        decision_ts="2026-05-15T01:00:00Z",
        top_of_book={"BNB/USDT": {"bid": 599.0, "ask": 601.0}},
    )

    assert count == 1
    lifecycle = order.meta["order_lifecycle"]
    assert lifecycle["arrival_mid"] == 600.0
    assert lifecycle["spread_bps_at_decision"] > 0

    store = OrderStore(str(order_store_path))
    req = {
        "instId": "BNB-USDT",
        "tdMode": "cash",
        "side": "buy",
        "ordType": "market",
        "clOrdId": "clid-1",
        "_v5_order_lifecycle_submit": {
            "submit_ts": "2026-05-15T01:00:01Z",
            "order_type": "market",
            "order_px": None,
            "cl_ord_id": "clid-1",
        },
        "_v5_order_meta": {"order_lifecycle": lifecycle},
    }
    store.upsert_new(
        cl_ord_id="clid-1",
        run_id=run_id,
        inst_id="BNB-USDT",
        side="buy",
        intent="OPEN_LONG",
        decision_hash="dh",
        td_mode="cash",
        ord_type="market",
        notional_usdt=120.0,
        req=req,
    )
    store.update_state("clid-1", new_state="FILLED", ord_id="okx-1", avg_px="602", acc_fill_sz="0.2")

    fills = FillStore(str(fill_store_path))
    fills.upsert_many(
        [
            FillRow(
                inst_id="BNB-USDT",
                trade_id="trade-1",
                ts_ms=1778806802000,
                ord_id="okx-1",
                cl_ord_id="clid-1",
                side="buy",
                fill_px="602",
                fill_sz="0.2",
                fee="-0.01204",
                fee_ccy="USDT",
            )
        ]
    )

    rows = write_order_lifecycle(
        run_dir=run_dir,
        reports_dir=reports_dir,
        orders=[order],
        order_store_path=order_store_path,
        fill_store_path=fill_store_path,
        append_reports=True,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["normalized_symbol"] == "BNB-USDT"
    assert row["decision_ts"] == "2026-05-15T01:00:00Z"
    assert row["submit_ts"] == "2026-05-15T01:00:01Z"
    assert row["order_type"] == "market"
    assert row["cl_ord_id"] == "clid-1"
    assert row["exchange_order_id"] == "okx-1"
    assert row["avg_fill_px"] == "602"
    assert row["filled_qty"] == "0.2"
    assert row["fee_usdt"] == "0.01204"

    with (reports_dir / "order_lifecycle.csv").open("r", encoding="utf-8", newline="") as fh:
        aggregate_rows = list(csv.DictReader(fh))
    assert len(aggregate_rows) == 1
    assert aggregate_rows[0]["trade_ids"] == "trade-1"


def test_order_lifecycle_projects_close_swing_attribution_meta(tmp_path: Path) -> None:
    run_id = "run_close_lifecycle"
    run_dir = tmp_path / "reports" / "runs" / run_id
    reports_dir = tmp_path / "reports"
    order_store_path = tmp_path / "reports" / "orders.sqlite"
    fill_store_path = tmp_path / "reports" / "fills.sqlite"
    store = OrderStore(str(order_store_path))
    req = {
        "instId": "BNB-USDT",
        "tdMode": "cash",
        "side": "sell",
        "ordType": "market",
        "clOrdId": "clid-close",
        "_v5_order_lifecycle_submit": {
            "submit_ts": "2026-05-29T03:00:54Z",
            "order_type": "market",
            "cl_ord_id": "clid-close",
        },
        "_v5_order_meta": {
            "swing_hold_position": True,
            "swing_entry_ts": "2026-05-28T22:00:59Z",
            "swing_min_hold_hours": 24,
            "hold_hours": 4.9985,
            "exit_reason": "atr_trailing",
            "exit_priority": "soft",
            "exit_allowed_before_min_hold": False,
            "exit_blocked_by_min_hold": False,
            "exited_before_min_hold": True,
            "source_reason": "atr_trailing",
            "max_unrealized_bps": 69.9,
        },
    }
    store.upsert_new(
        cl_ord_id="clid-close",
        run_id=run_id,
        inst_id="BNB-USDT",
        side="sell",
        intent="CLOSE_LONG",
        decision_hash="dh",
        td_mode="cash",
        ord_type="market",
        notional_usdt=12.0,
        req=req,
    )
    store.update_state("clid-close", new_state="FILLED", ord_id="okx-close", avg_px="634.3", acc_fill_sz="0.02")

    rows = write_order_lifecycle(
        run_dir=run_dir,
        reports_dir=reports_dir,
        order_store_path=order_store_path,
        fill_store_path=fill_store_path,
        append_reports=True,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["intent"] == "CLOSE_LONG"
    assert row["swing_hold_position"] == "true"
    assert row["swing_entry_ts"] == "2026-05-28T22:00:59Z"
    assert str(row["swing_min_hold_hours"]) == "24"
    assert str(row["hold_hours"]) == "4.9985"
    assert row["exit_reason"] == "atr_trailing"
    assert row["exit_priority"] == "soft"
    assert row["exited_before_min_hold"] == "true"
    assert str(row["max_unrealized_bps"]) == "69.9"
