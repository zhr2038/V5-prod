from __future__ import annotations

import tempfile
from pathlib import Path

from src.execution.fill_store import FillRow, FillStore, derive_fill_store_path, parse_okx_fills


def test_fill_store_dedup_by_inst_trade_id() -> None:
    with tempfile.TemporaryDirectory() as td:
        st = FillStore(path=f"{td}/fills.sqlite")
        r1 = FillRow(inst_id="BTC-USDT", trade_id="1", ts_ms=1, ord_id="o", cl_ord_id="c")
        r2 = FillRow(inst_id="BTC-USDT", trade_id="1", ts_ms=1, ord_id="o", cl_ord_id="c")
        ins, total = st.upsert_many([r1, r2])
        assert total == 2
        assert ins == 1
        assert st.count() == 1


def test_parse_okx_fills_extracts_keys() -> None:
    resp = {
        "code": "0",
        "data": [
            {
                "instId": "ETH-USDT",
                "tradeId": "999",
                "ts": "1700000000000",
                "ordId": "123",
                "clOrdId": "ABC",
                "side": "buy",
                "fillPx": "100",
                "fillSz": "0.1",
                "fee": "-0.01",
                "feeCcy": "USDT",
            }
        ],
    }
    rows = parse_okx_fills(resp)
    assert len(rows) == 1
    assert rows[0].inst_id == "ETH-USDT"
    assert rows[0].trade_id == "999"
    assert rows[0].ord_id == "123"
    assert rows[0].cl_ord_id == "ABC"


def test_derive_fill_store_path_tracks_custom_order_store_names() -> None:
    assert derive_fill_store_path("reports/orders.sqlite") == Path("reports/fills.sqlite")
    assert derive_fill_store_path("reports/shadow_orders.sqlite") == Path("reports/shadow_fills.sqlite")
    assert derive_fill_store_path("reports/orders_accelerated.sqlite") == Path("reports/fills_accelerated.sqlite")
    assert derive_fill_store_path("reports/shadow_tuned_xgboost/orders.sqlite") == Path("reports/shadow_tuned_xgboost/fills.sqlite")
