from __future__ import annotations

import csv
import json

from src.execution.order_store import OrderStore
from src.reporting.fill_trade_exporter import export_fill


def test_slippage_prefers_mid_at_submit_over_snapshot(tmp_path, monkeypatch):
    # Prepare an order with submit mid meta
    monkeypatch.chdir(tmp_path)
    os = OrderStore(path=str(tmp_path / "orders.sqlite"))
    clid = "c1"
    os.upsert_new(
        cl_ord_id=clid,
        run_id="r",
        inst_id="BTC-USDT",
        side="buy",
        intent="OPEN_LONG",
        decision_hash="h",
        td_mode="cash",
        ord_type="market",
        notional_usdt=10.0,
        req={"_meta": {"mid_px_at_submit": 100.0, "bid": 99.0, "ask": 101.0, "ts_ms": 1}},
    )

    # Export fill: fill at 101, qty=1 -> buy slippage_usdt=1 (vs mid=100)
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Create minimal trades.csv header via exporter
    export_fill(
        fill_ts_ms=2,
        inst_id="BTC-USDT",
        side="buy",
        fill_px="101",
        fill_sz="1",
        fee="0",
        fee_ccy="USDT",
        run_id="r",
        intent="OPEN_LONG",
        window_start_ts=1,
        window_end_ts=2,
        run_dir=str(run_dir),
        cl_ord_id=clid,
        order_store_path=str(tmp_path / "orders.sqlite"),
        spread_store=None,
    )

    # Verify trade log used the submit-time mid instead of falling back to empty slippage.
    csvp = run_dir / "trades.csv"
    with csvp.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["slippage_usdt"] == "1"

    # Verify cost event also records submit mid as the reference source.
    cost_event_files = list((tmp_path / "reports" / "cost_events").glob("*.jsonl"))
    assert len(cost_event_files) == 1
    lines = cost_event_files[0].read_text(encoding="utf-8").strip().splitlines()
    event = json.loads(lines[-1])
    assert event["mid_source"] == "submit"
    assert event["mid_px_at_submit"] == 100.0
