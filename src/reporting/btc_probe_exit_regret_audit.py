from __future__ import annotations

from typing import Any, Mapping


BTC_PROBE_EXIT_REGRET_FIELDS = (
    "entry_ts",
    "exit_ts",
    "entry_px",
    "exit_px",
    "hold_hours",
    "exit_reason",
    "probe_type",
    "net_bps_at_exit",
    "future_1h_after_exit_bps",
    "future_2h_after_exit_bps",
    "future_4h_after_exit_bps",
    "future_8h_after_exit_bps",
    "future_24h_after_exit_bps",
    "regret_bps_4h",
    "regret_bps_8h",
    "diagnosis",
)


def is_btc_market_impulse_probe_roundtrip(row: Mapping[str, Any]) -> bool:
    symbol = str(row.get("symbol") or "").upper().replace("-", "/")
    if symbol != "BTC/USDT":
        return False
    text = " ".join(str(value or "").lower() for value in row.values())
    return "market_impulse_probe" in text

