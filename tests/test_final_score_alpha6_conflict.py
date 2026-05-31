from src.reporting.final_score_alpha6_conflict import (
    build_conflict_rows,
    is_final_score_alpha6_conflict_candidate,
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
    assert row["label_status"] == "shadow_pending"
    assert row["missed_profit_flag"] == "true"
