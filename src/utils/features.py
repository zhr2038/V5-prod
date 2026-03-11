"""Compatibility wrappers for canonical feature generation."""

from __future__ import annotations

from typing import Dict, Optional, Tuple

import pandas as pd

from src.research.feature_registry import build_snapshot_feature_row


PRICE_FEATURE_NAMES = (
    "returns_1h",
    "returns_6h",
    "returns_24h",
    "momentum_5d",
    "momentum_20d",
)

VOLATILITY_FEATURE_NAMES = (
    "volatility_6h",
    "volatility_24h",
    "volatility_ratio",
)

VOLUME_FEATURE_NAMES = (
    "volume_ratio",
    "obv",
)


def _coerce_series(values: Optional[pd.Series], *, fallback: pd.Series | None = None) -> pd.Series:
    if values is not None:
        return pd.Series(values, dtype=float).reset_index(drop=True)
    if fallback is not None:
        return pd.Series(fallback, dtype=float).reset_index(drop=True)
    return pd.Series(dtype=float)


def calculate_all_features(
    close: pd.Series,
    volume: Optional[pd.Series] = None,
    high: Optional[pd.Series] = None,
    low: Optional[pd.Series] = None,
) -> Dict[str, float]:
    close_s = _coerce_series(close)
    high_s = _coerce_series(high, fallback=close_s)
    low_s = _coerce_series(low, fallback=close_s)
    volume_s = _coerce_series(volume, fallback=pd.Series([0.0] * len(close_s), dtype=float))

    row = build_snapshot_feature_row(
        symbol="__compat__",
        close=close_s.tolist(),
        high=high_s.tolist(),
        low=low_s.tolist(),
        volume=volume_s.tolist(),
        feature_groups=("classic",),
        include_time_features=False,
    )
    row.pop("symbol", None)
    return {str(key): float(value) for key, value in row.items()}


def calculate_price_features(close: pd.Series) -> Dict[str, float]:
    features = calculate_all_features(close)
    return {name: features.get(name, 0.0) for name in PRICE_FEATURE_NAMES}


def calculate_volatility_features(close: pd.Series) -> Dict[str, float]:
    features = calculate_all_features(close)
    return {name: features.get(name, 0.0) for name in VOLATILITY_FEATURE_NAMES}


def calculate_volume_features(close: pd.Series, volume: pd.Series) -> Dict[str, float]:
    features = calculate_all_features(close, volume=volume)
    return {name: features.get(name, 0.0) for name in VOLUME_FEATURE_NAMES}


def calculate_rsi(close: pd.Series, window: int = 14) -> float:
    del window
    return float(calculate_all_features(close).get("rsi", 50.0))


def calculate_macd(close: pd.Series) -> Tuple[float, float]:
    features = calculate_all_features(close)
    return float(features.get("macd", 0.0)), float(features.get("macd_signal", 0.0))


def calculate_bollinger_position(close: pd.Series, window: int = 20) -> float:
    del window
    return float(calculate_all_features(close).get("bb_position", 0.0))


def calculate_price_position(close: pd.Series, high: pd.Series, low: pd.Series, window: int = 20 * 24) -> float:
    del window
    return float(calculate_all_features(close, high=high, low=low).get("price_position", 0.5))
