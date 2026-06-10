from __future__ import annotations

from typing import Any, Mapping


BTC_PROBE_ENTRY_QUALITY_FIELDS = (
    "run_id",
    "entry_ts",
    "entry_px",
    "final_score",
    "expected_edge_bps",
    "required_edge_bps",
    "btc_trend_score",
    "trend_buy_count",
    "alpha6_score",
    "alpha6_side",
    "bypassed_negative_expectancy_reason",
    "selected_symbol",
    "selection_mode",
    "negative_expectancy_state",
    "same_symbol_reentry_bypass",
    "price_distance_from_recent_low_bps",
    "price_distance_from_recent_high_bps",
    "anti_chase_flag",
    "entry_quality_status",
)


def is_btc_market_impulse_probe_candidate(row: Mapping[str, Any]) -> bool:
    symbol = str(row.get("symbol") or "").upper().replace("-", "/")
    if symbol != "BTC/USDT":
        return False
    text = " ".join(str(value or "").lower() for value in row.values())
    return "market_impulse_probe" in text
