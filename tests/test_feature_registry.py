from __future__ import annotations

import pandas as pd

from src.research.feature_registry import build_inference_frame_from_market_data


def test_build_inference_frame_uses_latest_timestamp_for_time_features_when_ts_is_unsorted() -> None:
    newer_ts = 1_710_003_600_000
    older_ts = 1_710_000_000_000
    market_data = {
        "BTC/USDT": {
            "close": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "volume": [10.0, 11.0],
            "ts": [newer_ts, older_ts],
        }
    }

    frame = build_inference_frame_from_market_data(
        market_data,
        feature_names=["hour_of_day", "day_of_week"],
    )

    assert len(frame) == 1
    expected_dt = pd.to_datetime(newer_ts, unit="ms", errors="coerce")
    assert frame.loc[0, "hour_of_day"] == float(expected_dt.hour)
    assert frame.loc[0, "day_of_week"] == float(expected_dt.dayofweek)
