from __future__ import annotations

import math
import tempfile

import pandas as pd
import pytest

from src.execution.ml_factor_model import MLFactorConfig, MLFactorModel


def test_prepare_target_uses_compounded_future_return() -> None:
    model = MLFactorModel(MLFactorConfig())
    features = pd.DataFrame(
        {
            "symbol": ["BTC/USDT"] * 5,
            "returns_1h": [0.01, 0.02, -0.01, 0.03, 0.04],
        }
    )

    out = model.prepare_target(features, horizon=2)

    assert math.isclose(out.loc[0, "target"], (1.02 * 0.99) - 1.0, rel_tol=1e-9)
    assert math.isclose(out.loc[1, "target"], (0.99 * 1.03) - 1.0, rel_tol=1e-9)
    assert math.isnan(float(out.loc[3, "target"]))


def test_build_training_frame_filters_sparse_symbols_and_removes_timestamp_feature() -> None:
    cfg = MLFactorConfig(
        min_symbol_samples=3,
        min_symbol_target_std=1e-8,
        target_mode="cross_sectional_rank",
    )
    model = MLFactorModel(cfg)
    df = pd.DataFrame(
        {
            "timestamp": [1, 1, 1, 2, 2, 2, 3, 3, 3],
            "symbol": ["A", "B", "C", "A", "B", "C", "A", "B", "C"],
            "returns_24h": [0.1, 0.2, 0.0, 0.2, 0.1, 0.0, 0.3, 0.0, 0.0],
            "momentum_5d": [0.5, 0.1, 0.0, 0.4, 0.2, 0.0, 0.3, 0.3, 0.0],
            "momentum_20d": [0.7, 0.6, 0.0, 0.2, 0.3, 0.0, 0.1, 0.4, 0.0],
            "volatility_24h": [0.2, 0.3, 0.1, 0.1, 0.4, 0.1, 0.5, 0.2, 0.1],
            "volatility_ratio": [1.0, 1.1, 1.0, 0.9, 1.2, 1.0, 1.1, 0.8, 1.0],
            "volume_ratio": [1.5, 1.2, 1.0, 1.4, 1.1, 1.0, 1.3, 1.0, 1.0],
            "obv": [10, 9, 0, 11, 8, 0, 12, 7, 0],
            "rsi": [55, 45, 50, 60, 40, 50, 65, 35, 50],
            "macd": [0.1, 0.0, 0.0, 0.2, -0.1, 0.0, 0.3, -0.2, 0.0],
            "macd_signal": [0.05, 0.0, 0.0, 0.1, -0.05, 0.0, 0.15, -0.1, 0.0],
            "bb_position": [0.2, -0.1, 0.0, 0.3, -0.2, 0.0, 0.4, -0.3, 0.0],
            "price_position": [0.7, 0.3, 0.5, 0.8, 0.2, 0.5, 0.9, 0.1, 0.5],
            "future_return_6h": [0.03, 0.01, 0.02, 0.04, -0.01, 0.02, 0.05, -0.02, 0.02],
        }
    )
    # Symbol C is zero variance on the target and should be dropped.
    df.loc[df["symbol"] == "C", "future_return_6h"] = 0.02

    X, y, meta = model.build_training_frame(df, target_col="future_return_6h")

    assert "C" in meta["dropped_symbols"]
    assert "A" in meta["kept_symbols"]
    assert "timestamp" not in X.columns
    assert "hour_of_day" in X.columns
    assert y.between(-0.5, 0.5).all()


def test_build_training_frame_supports_forward_edge_rank_target_mode() -> None:
    cfg = MLFactorConfig(
        min_symbol_samples=1,
        min_symbol_target_std=1e-8,
        target_mode="forward_edge_rank",
    )
    model = MLFactorModel(cfg)
    df = pd.DataFrame(
        {
            "timestamp": [1, 1, 1, 2, 2, 2],
            "symbol": ["A", "B", "C", "A", "B", "C"],
            "returns_24h": [0.1, 0.2, 0.15, 0.05, 0.08, 0.07],
            "momentum_5d": [0.2, 0.3, 0.25, 0.1, 0.12, 0.11],
            "momentum_20d": [0.3, 0.2, 0.1, 0.15, 0.18, 0.16],
            "volatility_24h": [0.10, 0.20, 0.40, 0.12, 0.18, 0.36],
            "volatility_ratio": [1.0, 1.1, 1.2, 0.9, 1.0, 1.1],
            "volume_ratio": [1.2, 1.1, 0.9, 1.3, 1.0, 0.8],
            "obv": [10, 12, 8, 11, 13, 9],
            "rsi": [50, 55, 45, 52, 58, 48],
            "macd": [0.01, 0.02, -0.01, 0.015, 0.025, -0.005],
            "macd_signal": [0.005, 0.01, -0.005, 0.007, 0.012, -0.002],
            "bb_position": [0.1, 0.2, -0.1, 0.05, 0.15, -0.05],
            "price_position": [0.7, 0.8, 0.4, 0.65, 0.75, 0.45],
            "future_return_6h": [0.03, 0.02, 0.01, 0.02, 0.03, 0.01],
        }
    )

    X, y, _ = model.build_training_frame(df, target_col="future_return_6h")

    assert "timestamp" not in X.columns
    assert len(y) == len(X)
    assert len(y) >= 4
    assert y.abs().max() <= 0.5
    assert y.iloc[0] > y.iloc[1]
    assert y.iloc[2] < y.iloc[3]


@pytest.mark.parametrize("model_type", ["ridge", "hist_gbm"])
def test_model_roundtrip_save_load(model_type: str) -> None:
    cfg = MLFactorConfig(
        model_type=model_type,
        target_mode="raw",
        min_symbol_samples=1,
        max_depth=1,
        hgb_max_iter=20,
        hgb_min_samples_leaf=2,
    )
    model = MLFactorModel(cfg)
    X = pd.DataFrame(
        {
            "returns_24h": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.15, 0.25, 0.35, 0.45],
            "momentum_5d": [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.95, 0.85, 0.75, 0.65],
        }
    )
    y = pd.Series([0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.25, 0.35, 0.45, 0.55])
    model.feature_names = list(X.columns)
    model.train(X.iloc[:7], y.iloc[:7], X.iloc[7:], y.iloc[7:])

    with tempfile.TemporaryDirectory() as td:
        path = f"{td}/ml_factor_model"
        model.save_model(path)

        loaded = MLFactorModel()
        loaded.load_model(path)
        pred1 = model.predict_batch(X)
        pred2 = loaded.predict_batch(X)

    assert pred1.round(12).equals(pred2.round(12))
