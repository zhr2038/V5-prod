from __future__ import annotations

import tempfile
from pathlib import Path

from src.execution.fill_store import (
    FillRow,
    FillStore,
    derive_fill_store_path,
    derive_runtime_cost_events_dir,
    derive_runtime_named_artifact_path,
    derive_runtime_spread_snapshots_dir,
    derive_runtime_spread_stats_dir,
    derive_position_store_path,
    parse_okx_fills,
)


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


def test_derive_position_store_path_tracks_custom_order_store_names() -> None:
    assert derive_position_store_path("reports/orders.sqlite") == Path("reports/positions.sqlite")
    assert derive_position_store_path("reports/shadow_orders.sqlite") == Path("reports/shadow_positions.sqlite")
    assert derive_position_store_path("reports/orders_accelerated.sqlite") == Path("reports/positions_accelerated.sqlite")
    assert derive_position_store_path("reports/shadow_tuned_xgboost/orders.sqlite") == Path("reports/shadow_tuned_xgboost/positions.sqlite")


def test_derive_runtime_named_artifact_path_tracks_custom_order_store_names() -> None:
    assert derive_runtime_named_artifact_path("reports/orders.sqlite", "model_promotion_decision", ".json") == Path(
        "reports/model_promotion_decision.json"
    )
    assert derive_runtime_named_artifact_path("reports/shadow_orders.sqlite", "ml_runtime_status", ".json") == Path(
        "reports/shadow_ml_runtime_status.json"
    )
    assert derive_runtime_named_artifact_path(
        "reports/orders_accelerated.sqlite",
        "ml_overlay_impact_history",
        ".jsonl",
    ) == Path("reports/ml_overlay_impact_history_accelerated.jsonl")
    assert derive_runtime_named_artifact_path(
        "reports/shadow_tuned_xgboost/orders.sqlite",
        "ml_overlay_impact",
        ".json",
    ) == Path("reports/shadow_tuned_xgboost/ml_overlay_impact.json")


def test_derive_runtime_cost_events_dir_tracks_custom_order_store_names() -> None:
    assert derive_runtime_cost_events_dir("reports/orders.sqlite") == Path("reports/cost_events")
    assert derive_runtime_cost_events_dir("reports/shadow_orders.sqlite") == Path("reports/shadow_cost_events")
    assert derive_runtime_cost_events_dir("reports/orders_accelerated.sqlite") == Path("reports/cost_events_accelerated")
    assert derive_runtime_cost_events_dir("reports/shadow_tuned_xgboost/orders.sqlite") == Path(
        "reports/shadow_tuned_xgboost/cost_events"
    )


def test_derive_runtime_spread_dirs_track_custom_order_store_names() -> None:
    assert derive_runtime_spread_snapshots_dir("reports/orders.sqlite") == Path("reports/spread_snapshots")
    assert derive_runtime_spread_snapshots_dir("reports/shadow_orders.sqlite") == Path("reports/shadow_spread_snapshots")
    assert derive_runtime_spread_snapshots_dir("reports/orders_accelerated.sqlite") == Path(
        "reports/spread_snapshots_accelerated"
    )
    assert derive_runtime_spread_snapshots_dir("reports/shadow_tuned_xgboost/orders.sqlite") == Path(
        "reports/shadow_tuned_xgboost/spread_snapshots"
    )
    assert derive_runtime_spread_stats_dir("reports/orders.sqlite") == Path("reports/spread_stats")
    assert derive_runtime_spread_stats_dir("reports/shadow_orders.sqlite") == Path("reports/shadow_spread_stats")
    assert derive_runtime_spread_stats_dir("reports/orders_accelerated.sqlite") == Path(
        "reports/spread_stats_accelerated"
    )
    assert derive_runtime_spread_stats_dir("reports/shadow_tuned_xgboost/orders.sqlite") == Path(
        "reports/shadow_tuned_xgboost/spread_stats"
    )
