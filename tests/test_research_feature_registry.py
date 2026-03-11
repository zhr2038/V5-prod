from __future__ import annotations

import numpy as np
import pytest

from src.research.dataset_builder import DatasetBuildConfig, ResearchDatasetBuilder
from src.research.feature_registry import resolve_feature_names


def _market_data_with_history() -> dict:
    length = 520
    base_ts = 1_700_000_000_000
    idx = np.arange(length, dtype=float)
    close = 100.0 + idx * 0.05 + np.sin(idx / 8.0)
    high = close * 1.01
    low = close * 0.99
    volume = 1_000.0 + (idx % 24.0) * 10.0
    ts = [int(base_ts + i * 3_600_000) for i in range(length)]
    return {
        "BTC/USDT": {
            "close": close.tolist(),
            "high": high.tolist(),
            "low": low.tolist(),
            "volume": volume.tolist(),
            "ts": ts,
        }
    }


def test_inference_frame_matches_latest_feature_snapshot() -> None:
    config = DatasetBuildConfig(feature_groups=("classic",), include_time_features=True)
    builder = ResearchDatasetBuilder(config)
    market_data = _market_data_with_history()
    feature_names = resolve_feature_names(config.feature_groups, include_time_features=config.include_time_features)

    training_frame = builder.build_feature_frame_from_market_data(market_data)
    latest_training_row = training_frame.dropna(subset=feature_names).iloc[-1]
    inference_frame = builder.build_inference_frame(market_data, feature_names=feature_names)

    assert list(inference_frame.columns) == ["symbol", *feature_names]
    assert len(inference_frame) == 1
    assert inference_frame.loc[0, "symbol"] == "BTC/USDT"
    for feature in feature_names:
        assert inference_frame.loc[0, feature] == pytest.approx(float(latest_training_row[feature]), rel=1e-9, abs=1e-9)
