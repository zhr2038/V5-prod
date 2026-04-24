from __future__ import annotations

import pandas as pd

from src.research.processors import align_cycle_samples


def test_align_cycle_samples_keeps_latest_duplicate_row_for_same_hour_and_symbol() -> None:
    hour_ms = 3600 * 1000
    df = pd.DataFrame(
        [
            {"timestamp": hour_ms + 1, "symbol": "BTC/USDT", "score": 1.0},
            {"timestamp": hour_ms + 2, "symbol": "BTC/USDT", "score": 2.0},
            {"timestamp": hour_ms + 3, "symbol": "ETH/USDT", "score": 3.0},
        ]
    )

    out, meta = align_cycle_samples(df)

    assert meta["duplicates_removed"] == 1
    assert len(out) == 2
    btc_row = out.loc[out["symbol"] == "BTC/USDT"].iloc[0]
    assert btc_row["timestamp"] == hour_ms
    assert btc_row["score"] == 2.0
