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
    assert row["outcome"] == "profitable_shadow"
    assert row["label_status"] == "shadow_pending"
    assert row["live_order_effect"] == "read_only_no_live_order"
