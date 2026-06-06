from src.reporting.bnb_strong_alpha6_bypass_shadow import (
    STRATEGY_ID,
    build_bnb_strong_alpha6_bypass_rows,
    is_bnb_strong_alpha6_bypass_candidate,
)


def _base_row(**overrides):
    row = {
        "run_id": "r1",
        "ts_utc": "2026-05-30T03:00:00Z",
        "symbol": "BNB/USDT",
        "alpha6_side": "buy",
        "alpha6_score": "0.99",
        "expected_edge_bps": "140",
        "required_edge_bps": "30",
        "cost_gate_verified": "true",
        "f4_volume_expansion": "1.1",
        "f3_vol_adj_ret": "8",
        "final_decision": "no_order",
        "block_reason": "negative_expectancy_fast_fail_open_block",
    }
    row.update(overrides)
    return row


def test_bnb_strong_alpha6_filter_is_read_only_shadow_scope() -> None:
    assert is_bnb_strong_alpha6_bypass_candidate(_base_row())
    assert is_bnb_strong_alpha6_bypass_candidate(_base_row(f4_volume_expansion="0.2", f3_vol_adj_ret="10"))
    assert not is_bnb_strong_alpha6_bypass_candidate(_base_row(symbol="ETH/USDT"))
    assert not is_bnb_strong_alpha6_bypass_candidate(_base_row(alpha6_score="0.89"))
    assert not is_bnb_strong_alpha6_bypass_candidate(_base_row(expected_edge_bps="29"))
    assert not is_bnb_strong_alpha6_bypass_candidate(_base_row(f4_volume_expansion="0.2", f3_vol_adj_ret="9.9"))


def test_bnb_strong_alpha6_rows_include_no_live_order_effect() -> None:
    rows = build_bnb_strong_alpha6_bypass_rows(
        [_base_row()],
        future_net_bps={4: 120.0, 8: 80.0, 12: -5.0, 24: 240.0},
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["strategy_id"] == STRATEGY_ID
    assert row["symbol"] == "BNB/USDT"
    assert row["would_bypass"] == "true"
    assert row["negative_expectancy_blocked"] == "true"
    assert row["max_future_net_bps"] == 240.0
    assert row["best_future_horizon_hours"] == 24
    assert row["material_profit_flag"] == "true"
    assert row["label_join_attempted"] == "true"
    assert row["label_join_match_type"] == "none"
    assert row["label_join_failure_reason"] == "no_label_same_run_symbol"
    assert row["outcome"] == "material_profit_shadow"
    assert row["label_4h_status"] == "complete"
    assert row["label_8h_status"] == "complete"
    assert row["label_12h_status"] == "complete"
    assert row["label_24h_status"] == "complete"
    assert row["any_label_complete"] == "true"
    assert row["all_labels_complete"] == "true"
    assert row["label_status"] == "complete"
    assert row["live_order_effect"] == "read_only_no_live_order"


def test_bnb_strong_alpha6_label_status_partial_when_some_horizons_observed() -> None:
    rows = build_bnb_strong_alpha6_bypass_rows(
        [_base_row()],
        future_net_bps={4: 42.0},
    )

    row = rows[0]
    assert row["label_4h_status"] == "complete"
    assert row["label_8h_status"] == "pending"
    assert row["any_label_complete"] == "true"
    assert row["all_labels_complete"] == "false"
    assert row["label_status"] == "partial_complete"


def test_bnb_strong_alpha6_joins_skipped_candidate_label_row() -> None:
    candidate = _base_row(strategy_candidate="f3_dominant_entry")
    label = {
        "run_id": "r1",
        "ts_utc": "2026-05-30T03:00:00Z",
        "symbol": "BNB-USDT",
        "label_4h_net_bps": "64.0",
        "future_12h_net_bps": "120.0",
    }

    rows = build_bnb_strong_alpha6_bypass_rows([candidate, label])

    assert len(rows) == 1
    row = rows[0]
    assert row["future_4h_net_bps"] == "64.0"
    assert row["future_12h_net_bps"] == "120.0"
    assert row["label_4h_status"] == "complete"
    assert row["label_8h_status"] == "pending"
    assert row["label_12h_status"] == "complete"
    assert row["label_status"] == "partial_complete"
    assert row["max_future_net_bps"] == 120.0
    assert row["best_future_horizon_hours"] == 12
    assert row["material_profit_flag"] == "true"
    assert row["outcome"] == "material_profit_shadow"
    assert row["label_join_match_type"] == "exact"
    assert row["label_join_time_skew_sec"] == 0
    assert row["label_join_failure_reason"] == ""


def test_bnb_strong_alpha6_joins_nearby_entry_ts_ms_label_row() -> None:
    candidate = _base_row(ts_utc="2026-05-30T03:00:15Z", strategy_candidate="f3_dominant_entry")
    label = {
        "run_id": "r1",
        "entry_ts_ms": 1_780_110_000_000,
        "symbol": "BNB-USDT",
        "label_4h_after_cost_bps": "51.0",
        "label_8h_net_bps": "88.0",
    }

    rows = build_bnb_strong_alpha6_bypass_rows([candidate, label])

    assert len(rows) == 1
    row = rows[0]
    assert row["future_4h_net_bps"] == "51.0"
    assert row["future_8h_net_bps"] == "88.0"
    assert row["label_4h_status"] == "complete"
    assert row["label_8h_status"] == "complete"
    assert row["label_status"] == "partial_complete"
    assert row["max_future_net_bps"] == 88.0
    assert row["material_profit_flag"] == "true"
    assert row["label_join_match_type"] == "nearest_same_run_symbol"
    assert row["label_join_time_skew_sec"] == 15.0


def test_bnb_strong_alpha6_joins_nearby_label_when_run_id_drifts() -> None:
    candidate = _base_row(run_id="20260521_21", ts_utc="2026-05-21T13:00:50.475143Z")
    label = {
        "run_id": "20260521_22",
        "ts_utc": "2026-05-21T13:00:00Z",
        "symbol": "BNB-USDT",
        "label_8h_net_bps": "90.0",
        "label_12h_net_bps": "83.0",
    }

    rows = build_bnb_strong_alpha6_bypass_rows([candidate, label])

    assert len(rows) == 1
    row = rows[0]
    assert row["future_8h_net_bps"] == "90.0"
    assert row["future_12h_net_bps"] == "83.0"
    assert row["label_status"] == "partial_complete"
    assert row["outcome"] == "material_profit_shadow"
    assert row["label_join_match_type"] == "nearest_symbol_only"
    assert row["label_join_time_skew_sec"] == 50.475


def test_bnb_strong_alpha6_joins_same_run_bar_start_drift() -> None:
    candidate = _base_row(run_id="20260521_21", ts_utc="2026-05-21T13:00:50.475143Z")
    label = {
        "run_id": "20260521_21",
        "ts_utc": "2026-05-21T12:00:00Z",
        "symbol": "BNB-USDT",
        "label_8h_net_bps": "90.0",
        "label_12h_net_bps": "83.0",
    }

    rows = build_bnb_strong_alpha6_bypass_rows([candidate, label])

    assert len(rows) == 1
    row = rows[0]
    assert row["future_8h_net_bps"] == "90.0"
    assert row["future_12h_net_bps"] == "83.0"
    assert row["label_status"] == "partial_complete"
    assert row["label_join_match_type"] == "same_run_symbol_bar_start_drift"
    assert row["label_join_time_skew_sec"] == 3650.475
