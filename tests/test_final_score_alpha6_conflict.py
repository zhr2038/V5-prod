import gzip

from src.reporting.final_score_alpha6_conflict import (
    build_conflict_rows,
    is_final_score_alpha6_conflict_candidate,
    load_report_input_rows,
)


def _base_row(**overrides):
    row = {
        "run_id": "r1",
        "ts_utc": "2026-05-30T03:00:00Z",
        "symbol": "BNB/USDT",
        "alpha6_side": "buy",
        "alpha6_score": "0.994",
        "expected_edge_bps": "140",
        "required_edge_bps": "30",
        "cost_gate_verified": "true",
        "final_score": "-0.17",
        "final_decision": "no_order",
        "block_reason": "negative_expectancy_fast_fail_open_block",
    }
    row.update(overrides)
    return row


def test_final_score_alpha6_conflict_filter_matches_required_conditions() -> None:
    assert is_final_score_alpha6_conflict_candidate(_base_row())
    assert not is_final_score_alpha6_conflict_candidate(_base_row(alpha6_side="sell"))
    assert not is_final_score_alpha6_conflict_candidate(_base_row(alpha6_score="0.89"))
    assert not is_final_score_alpha6_conflict_candidate(_base_row(expected_edge_bps="20"))
    assert not is_final_score_alpha6_conflict_candidate(_base_row(cost_gate_verified="false"))
    assert not is_final_score_alpha6_conflict_candidate(
        _base_row(final_score="0.2", final_decision="open_long")
    )


def test_final_score_alpha6_conflict_rows_keep_negative_expectancy_stats() -> None:
    rows = build_conflict_rows(
        [_base_row(f3_vol_adj_ret="12", f4_volume_expansion="5.82", f5_rsi_trend_confirm="0.832")],
        future_net_bps={4: 120.0, 8: 80.0, 12: -5.0, 24: 240.0},
        negative_expectancy_stats={
            "BNB/USDT": {
                "net_expectancy_bps": -151.83,
                "fast_fail_net_expectancy_bps": -142.89,
            }
        },
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "BNB/USDT"
    assert row["f3_vol_adj_ret"] == "12"
    assert row["f4_volume_expansion"] == "5.82"
    assert row["f5_rsi_trend_confirm"] == "0.832"
    assert row["cost_gate_verified"] == "true"
    assert row["negative_expectancy_net_bps"] == -151.83
    assert row["negative_expectancy_fast_fail_net_bps"] == -142.89
    assert row["future_24h_net_bps"] == 240.0
    assert row["max_future_net_bps"] == 240.0
    assert row["best_future_horizon_hours"] == 24
    assert row["material_profit_flag"] == "true"
    assert row["label_join_attempted"] == "true"
    assert row["label_join_match_type"] == "none"
    assert row["label_join_failure_reason"] == "no_label_same_run_symbol"
    assert row["label_4h_status"] == "complete"
    assert row["label_8h_status"] == "complete"
    assert row["label_12h_status"] == "complete"
    assert row["label_24h_status"] == "complete"
    assert row["any_label_complete"] == "true"
    assert row["all_labels_complete"] == "true"
    assert row["label_status"] == "complete"
    assert row["missed_profit_flag"] == "true"


def test_final_score_alpha6_conflict_label_status_partial_complete() -> None:
    rows = build_conflict_rows(
        [_base_row()],
        future_net_bps={4: 12.0},
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["future_4h_net_bps"] == 12.0
    assert row["label_4h_status"] == "complete"
    assert row["label_8h_status"] == "not_observable"
    assert row["any_label_complete"] == "true"
    assert row["all_labels_complete"] == "false"
    assert row["label_status"] == "partial_complete"
    assert row["max_future_net_bps"] == 12.0
    assert row["best_future_horizon_hours"] == 4
    assert row["material_profit_flag"] == "false"
    assert row["missed_profit_flag"] == "false"


def test_final_score_alpha6_conflict_joins_skipped_candidate_label_row() -> None:
    candidate = _base_row(strategy_candidate="f3_dominant_entry")
    label = {
        "run_id": "r1",
        "ts_utc": "2026-05-30T03:00:00Z",
        "symbol": "BNB-USDT",
        "label_4h_net_bps": "70.5",
        "label_8h_after_cost_bps": "-2.0",
    }

    rows = build_conflict_rows([candidate, label])

    assert len(rows) == 1
    row = rows[0]
    assert row["future_4h_net_bps"] == "70.5"
    assert row["future_8h_net_bps"] == "-2.0"
    assert row["label_4h_status"] == "complete"
    assert row["label_8h_status"] == "complete"
    assert row["label_12h_status"] == "not_observable"
    assert row["label_status"] == "partial_complete"
    assert row["label_join_attempted"] == "true"
    assert row["label_join_match_type"] == "exact"
    assert row["label_join_time_skew_sec"] == 0
    assert row["label_join_failure_reason"] == ""
    assert row["max_future_net_bps"] == 70.5
    assert row["best_future_horizon_hours"] == 4
    assert row["material_profit_flag"] == "true"
    assert row["missed_profit_flag"] == "true"


def test_final_score_alpha6_conflict_joins_entry_ts_ms_label_row() -> None:
    candidate = _base_row(
        ts_utc="2026-05-30T03:00:15Z",
        strategy_candidate="f3_dominant_entry",
    )
    label = {
        "run_id": "r1",
        "entry_ts_ms": 1_780_110_015_000,
        "symbol": "BNB-USDT",
        "label_4h_after_cost_bps": "61.5",
    }

    rows = build_conflict_rows([candidate, label])

    assert len(rows) == 1
    row = rows[0]
    assert row["future_4h_net_bps"] == "61.5"
    assert row["label_4h_status"] == "complete"
    assert row["label_status"] == "partial_complete"
    assert row["label_join_match_type"] == "exact"
    assert row["label_join_time_skew_sec"] == 0


def test_final_score_alpha6_conflict_joins_nearby_label_timestamp() -> None:
    candidate = _base_row(ts_utc="2026-05-30T03:00:15Z")
    label = {
        "run_id": "r1",
        "ts_utc": "2026-05-30T03:00:00Z",
        "symbol": "BNB-USDT",
        "label_8h_net_bps": "93.0",
    }

    rows = build_conflict_rows([candidate, label])

    assert len(rows) == 1
    row = rows[0]
    assert row["future_8h_net_bps"] == "93.0"
    assert row["label_8h_status"] == "complete"
    assert row["label_join_match_type"] == "nearest_same_run_symbol"
    assert row["label_join_time_skew_sec"] == 15.0


def test_final_score_alpha6_conflict_joins_nearby_label_when_run_id_drifts() -> None:
    candidate = _base_row(run_id="20260521_21", ts_utc="2026-05-21T13:00:50.475143Z")
    label = {
        "run_id": "20260521_22",
        "ts_utc": "2026-05-21T13:00:00Z",
        "symbol": "BNB-USDT",
        "label_4h_net_bps": "-6.9",
        "label_24h_net_bps": "143.9",
    }

    rows = build_conflict_rows([candidate, label])

    assert len(rows) == 1
    row = rows[0]
    assert row["future_4h_net_bps"] == "-6.9"
    assert row["future_24h_net_bps"] == "143.9"
    assert row["label_status"] == "partial_complete"
    assert row["material_profit_flag"] == "true"
    assert row["label_join_match_type"] == "nearest_symbol_only"
    assert row["label_join_time_skew_sec"] == 50.475


def test_final_score_alpha6_conflict_joins_same_run_bar_start_drift() -> None:
    candidate = _base_row(run_id="20260521_21", ts_utc="2026-05-21T13:00:50.475143Z")
    label = {
        "run_id": "20260521_21",
        "ts_utc": "2026-05-21T12:00:00Z",
        "symbol": "BNB-USDT",
        "label_4h_net_bps": "-6.9",
        "label_24h_net_bps": "143.9",
    }

    rows = build_conflict_rows([candidate, label])

    assert len(rows) == 1
    row = rows[0]
    assert row["future_4h_net_bps"] == "-6.9"
    assert row["future_24h_net_bps"] == "143.9"
    assert row["label_status"] == "partial_complete"
    assert row["label_join_match_type"] == "same_run_symbol_bar_start_drift"
    assert row["label_join_time_skew_sec"] == 3650.475
    assert row["label_join_failure_reason"] == ""


def test_final_score_alpha6_conflict_label_join_reports_failure_reason() -> None:
    candidate = _base_row(ts_utc="2026-05-30T03:00:00Z")
    far_label = {
        "run_id": "r1",
        "ts_utc": "2026-05-30T05:00:01Z",
        "symbol": "BNB-USDT",
        "label_4h_net_bps": "70.0",
    }

    rows = build_conflict_rows([candidate, far_label])

    assert len(rows) == 1
    row = rows[0]
    assert row["label_join_attempted"] == "true"
    assert row["label_join_match_type"] == "none"
    assert row["label_join_failure_reason"] == "nearest_label_too_far"
    assert row["label_join_time_skew_sec"] == 7201.0
    assert row["label_status"] == "not_observable"


def test_final_score_alpha6_conflict_loads_mature_outcome_summary_labels(tmp_path) -> None:
    candidate_path = tmp_path / "reports" / "candidate_snapshot.csv"
    candidate_path.parent.mkdir(parents=True)
    candidate_path.write_text(
        "\n".join(
            [
                "run_id,ts_utc,symbol,alpha6_side,alpha6_score,expected_edge_bps,required_edge_bps,cost_gate_verified,final_score,final_decision",
                "20260530_03,2026-05-30T03:00:00Z,BNB/USDT,buy,0.994,140,30,true,-0.17,no_order",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    outcome_path = tmp_path / "reports" / "summaries" / "skipped_candidate_outcomes.csv"
    outcome_path.parent.mkdir(parents=True)
    outcome_path.write_text(
        "\n".join(
            [
                "run_id,ts_utc,symbol,label_4h_net_bps,label_8h_net_bps,label_12h_net_bps,label_24h_net_bps,label_status",
                "20260530_03,2026-05-30T03:00:00Z,BNB-USDT,120.0,80.0,40.0,240.0,complete",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    rows = build_conflict_rows(load_report_input_rows(tmp_path))

    assert len(rows) == 1
    row = rows[0]
    assert row["future_4h_net_bps"] == "120.0"
    assert row["future_24h_net_bps"] == "240.0"
    assert row["label_status"] == "complete"
    assert row["label_join_match_type"] == "exact"
    assert row["label_join_failure_reason"] == ""
    assert row["material_profit_flag"] == "true"


def test_final_score_alpha6_conflict_loads_raw_large_gz_outcome_labels(tmp_path) -> None:
    candidate_path = tmp_path / "candidate_snapshot.csv"
    candidate_path.write_text(
        "\n".join(
            [
                "run_id,ts_utc,symbol,alpha6_side,alpha6_score,expected_edge_bps,required_edge_bps,cost_gate_verified,final_score,final_decision",
                "20260530_10,2026-05-30T10:00:00Z,BNB/USDT,buy,0.98,100,30,true,-0.1,no_order",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    outcome_path = tmp_path / "raw" / "large" / "reports" / "summaries" / "skipped_candidate_outcomes.csv.gz"
    outcome_path.parent.mkdir(parents=True)
    with gzip.open(outcome_path, "wt", encoding="utf-8", newline="") as fh:
        fh.write(
            "\n".join(
                [
                    "run_id,ts_utc,symbol,label_4h_net_bps,label_8h_net_bps,label_12h_net_bps,label_24h_net_bps,label_status",
                    "20260530_10,2026-05-30T10:00:00Z,BNB-USDT,51.0,52.0,53.0,54.0,complete",
                ]
            )
            + "\n"
        )

    rows = build_conflict_rows(load_report_input_rows(tmp_path))

    assert len(rows) == 1
    row = rows[0]
    assert row["future_4h_net_bps"] == "51.0"
    assert row["future_24h_net_bps"] == "54.0"
    assert row["label_status"] == "complete"
    assert row["label_join_match_type"] == "exact"
    assert row["label_join_failure_reason"] == ""
    assert row["material_profit_flag"] == "true"
