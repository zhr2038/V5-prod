"""
ML feature optimization utilities.

The main goal is to keep training features free of obvious leakage / identity
columns while retaining a small, stable set of numeric predictors.
"""

from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

try:
    from sklearn.feature_selection import mutual_info_regression

    SKLEARN_AVAILABLE = True
except Exception:
    mutual_info_regression = None
    SKLEARN_AVAILABLE = False


class FeatureEngineeringOptimizer:
    NON_FEATURE_COLUMNS = {
        "timestamp",
        "symbol",
        "regime",
        "future_return_6h",
        "target",
    }

    LOW_INFO_FEATURES = {
        "returns_1h",
        "returns_6h",
        "volatility_6h",
    }

    STABLE_FEATURE_ORDER = [
        "returns_24h",
        "momentum_5d",
        "momentum_20d",
        "volatility_24h",
        "volatility_ratio",
        "volume_ratio",
        "obv",
        "rsi",
        "macd",
        "macd_signal",
        "bb_position",
        "price_position",
        "hour_of_day",
        "day_of_week",
    ]

    @staticmethod
    def remove_high_correlation_features(df: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
        numeric = df.select_dtypes(include=[np.number]).copy()
        if numeric.empty:
            return df.copy()

        corr_matrix = numeric.corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        to_drop = {col for col in upper.columns if any(upper[col] > threshold)}

        out = df.copy()
        if to_drop:
            out = out.drop(columns=list(to_drop), errors="ignore")
        return out

    @staticmethod
    def create_time_aware_features(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "timestamp" in out.columns:
            ts = pd.to_datetime(out["timestamp"], unit="ms", errors="coerce")
            if ts.isna().all():
                ts = pd.to_datetime(out["timestamp"], errors="coerce")
            out["hour_of_day"] = ts.dt.hour.astype(float)
            out["day_of_week"] = ts.dt.dayofweek.astype(float)
        return out

    @staticmethod
    def check_feature_leakage(df: pd.DataFrame, target_col: str = "target") -> List[str]:
        leaky = []
        for col in df.columns:
            if col == target_col:
                continue
            lower = str(col).lower()
            if lower in FeatureEngineeringOptimizer.NON_FEATURE_COLUMNS:
                leaky.append(col)
                continue
            if any(k in lower for k in ("future", "next", "lead", "ahead")):
                leaky.append(col)
        return leaky

    @staticmethod
    def select_features_by_importance(X: pd.DataFrame, y: pd.Series, n_features: int = 10) -> List[str]:
        X = X.drop(columns=[c for c in FeatureEngineeringOptimizer.NON_FEATURE_COLUMNS if c in X.columns], errors="ignore")
        X = X.select_dtypes(include=[np.number]).copy()
        X = X.loc[:, X.nunique(dropna=True) > 1]
        if X.empty:
            return []
        if not SKLEARN_AVAILABLE or mutual_info_regression is None:
            return FeatureEngineeringOptimizer.select_stable_features(X, n_features=n_features)

        X_clean = X.fillna(X.median())
        y_clean = y.fillna(y.median())
        mi_scores = mutual_info_regression(X_clean, y_clean, random_state=42)
        importance_df = pd.DataFrame({"feature": X.columns, "mi_score": mi_scores}).sort_values(
            "mi_score", ascending=False
        )
        return importance_df.head(n_features)["feature"].tolist()

    @staticmethod
    def select_stable_features(X: pd.DataFrame, n_features: int = 12) -> List[str]:
        preferred = [c for c in FeatureEngineeringOptimizer.STABLE_FEATURE_ORDER if c in X.columns]
        extras = sorted(c for c in X.columns if c not in preferred)
        ordered = preferred + extras
        if n_features > 0:
            return ordered[:n_features]
        return ordered


def optimize_features_for_training(
    df: pd.DataFrame,
    y: pd.Series | None = None,
    *,
    selector: str = "stable",
    n_features: int = 12,
) -> pd.DataFrame:
    optimizer = FeatureEngineeringOptimizer()
    out = df.copy()

    leaky = optimizer.check_feature_leakage(out)
    if leaky:
        out = out.drop(columns=leaky, errors="ignore")

    low_info_cols = [c for c in optimizer.LOW_INFO_FEATURES if c in out.columns]
    if low_info_cols:
        out = out.drop(columns=low_info_cols, errors="ignore")

    out = optimizer.create_time_aware_features(out)
    out = out.drop(columns=[c for c in optimizer.NON_FEATURE_COLUMNS if c in out.columns], errors="ignore")
    out = out.select_dtypes(include=[np.number]).copy()
    out = out.loc[:, out.nunique(dropna=True) > 1]
    out = optimizer.remove_high_correlation_features(out, threshold=0.9)

    if y is not None and not out.empty:
        selector_key = str(selector or "stable").strip().lower()
        if selector_key == "mutual_info":
            selected = optimizer.select_features_by_importance(out, y, n_features=n_features)
        else:
            selected = optimizer.select_stable_features(out, n_features=n_features)
        if selected:
            out = out[selected]

    return out
