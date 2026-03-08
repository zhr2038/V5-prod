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
