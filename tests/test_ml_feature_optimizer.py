from __future__ import annotations

import numpy as np
import pandas as pd

from src.execution.ml_feature_optimizer import optimize_features_for_training


def test_optimizer_drops_raw_identity_and_low_info_columns() -> None:
    df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3, 4, 5, 6],
            "symbol": ["BTC"] * 6,
            "regime": ["Risk-Off"] * 6,
            "returns_1h": [0.1, 0.2, 0.1, 0.2, 0.1, 0.2],
            "returns_24h": [0.1, 0.3, 0.2, 0.5, 0.4, 0.6],
            "volume_ratio": [1.0, 1.3, 1.1, 1.4, 1.2, 1.5],
            "macd_signal": [0.01, 0.04, 0.02, 0.05, 0.03, 0.06],
        }
    )
    y = pd.Series(np.linspace(-0.2, 0.2, len(df)))

    out = optimize_features_for_training(df, y)

    assert "timestamp" not in out.columns
    assert "symbol" not in out.columns
    assert "regime" not in out.columns
    assert "returns_1h" not in out.columns
    assert len(out.columns) >= 1


def test_optimizer_stable_selector_keeps_deterministic_feature_order() -> None:
    df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3, 4, 5, 6],
            "symbol": ["BTC"] * 6,
            "returns_24h": [0.10, 0.22, 0.18, 0.34, 0.29, 0.41],
            "momentum_5d": [0.52, 0.63, 0.57, 0.66, 0.61, 0.74],
            "volume_ratio": [1.10, 1.28, 1.14, 1.37, 1.22, 1.31],
            "rsi": [40, 47, 53, 56, 51, 61],
            "macd": [0.010, 0.019, 0.015, 0.028, 0.024, 0.033],
            "bb_position": [0.10, 0.14, 0.18, 0.16, 0.22, 0.27],
        }
    )
    y = pd.Series([0.10, 0.18, 0.16, 0.27, 0.24, 0.31])

    out = optimize_features_for_training(df, y, selector="stable", n_features=4)

    stable_order = ["returns_24h", "momentum_5d", "volume_ratio", "rsi", "macd", "bb_position"]
    assert list(out.columns)[0] == "returns_24h"
    assert all(col in stable_order for col in out.columns)
    assert list(out.columns) == [col for col in stable_order if col in out.columns][: len(out.columns)]
