from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def cross_sectional_rank(values: pd.Series) -> pd.Series:
    series = pd.Series(values)
    if len(series) <= 1:
        return pd.Series(np.zeros(len(series), dtype=float), index=series.index)
    return series.rank(pct=True) - 0.5


def winsorize_series(values: pd.Series, *, lower_q: float = 0.05, upper_q: float = 0.95) -> pd.Series:
    series = pd.Series(values, dtype=float)
    finite = series[np.isfinite(series)]
    if finite.empty:
        return pd.Series(np.zeros(len(series), dtype=float), index=series.index)
    lower = float(finite.quantile(lower_q))
    upper = float(finite.quantile(upper_q))
    return series.clip(lower=lower, upper=upper)


def robust_zscore_series(values: pd.Series, *, winsorize_pct: float = 0.05) -> pd.Series:
    series = winsorize_series(pd.Series(values, dtype=float), lower_q=winsorize_pct, upper_q=1.0 - winsorize_pct)
    finite = series[np.isfinite(series)]
    if finite.empty:
        return pd.Series(np.zeros(len(series), dtype=float), index=series.index)
    median = float(finite.median())
    mad = float((finite - median).abs().median())
    if mad <= 1e-12:
        std = float(finite.std(ddof=0))
        if std <= 1e-12:
            return pd.Series(np.zeros(len(series), dtype=float), index=series.index)
        out = (series - median) / std
    else:
        out = 0.6744897501960817 * (series - median) / mad
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def coerce_group_datetimes(groups: pd.Series) -> pd.Series:
    groups_s = pd.Series(groups).reset_index(drop=True)
    if pd.api.types.is_numeric_dtype(groups_s):
        ts = pd.to_datetime(groups_s, unit="ms", errors="coerce")
        if ts.notna().any():
            return ts
    return pd.to_datetime(groups_s, errors="coerce")


def align_cycle_samples(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    if df.empty or "timestamp" not in df.columns or "symbol" not in df.columns:
        rows = int(len(df))
        return df, {"rows_before": rows, "rows_after": rows, "duplicates_removed": 0}

    out = df.copy()
    ts = pd.to_numeric(out["timestamp"], errors="coerce")
    hour_ms = 3600 * 1000
    out["timestamp"] = ((ts // hour_ms) * hour_ms).astype("Int64")
    out = out.dropna(subset=["timestamp"]).copy()
    out["timestamp"] = out["timestamp"].astype("int64")
    rows_before = int(len(out))
    out = (
        out.sort_values(["timestamp", "symbol"])
        .drop_duplicates(subset=["timestamp", "symbol"], keep="last")
        .reset_index(drop=True)
    )
    rows_after = int(len(out))
    return out, {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "duplicates_removed": rows_before - rows_after,
    }


def apply_rolling_window(
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    *,
    lookback_days: float,
) -> tuple[pd.DataFrame, pd.Series, pd.Series, dict[str, Any]]:
    base_meta: dict[str, Any] = {
        "enabled": bool(lookback_days > 0),
        "lookback_days": float(max(lookback_days, 0.0)),
        "rows_before": int(len(X)),
        "groups_before": int(pd.Series(groups).nunique()),
    }
    if lookback_days <= 0:
        base_meta["rows_after"] = int(len(X))
        base_meta["groups_after"] = int(pd.Series(groups).nunique())
        return (
            X.reset_index(drop=True),
            y.reset_index(drop=True),
            pd.Series(groups).reset_index(drop=True),
            base_meta,
        )

    group_ts = coerce_group_datetimes(groups)
    if group_ts.isna().all():
        base_meta["enabled"] = False
        base_meta["fallback"] = "invalid_group_timestamps"
        base_meta["rows_after"] = int(len(X))
        base_meta["groups_after"] = int(pd.Series(groups).nunique())
        return (
            X.reset_index(drop=True),
            y.reset_index(drop=True),
            pd.Series(groups).reset_index(drop=True),
            base_meta,
        )

    cutoff = group_ts.max() - pd.Timedelta(days=float(lookback_days))
    mask = group_ts >= cutoff
    if int(mask.sum()) == 0 or int(pd.Series(groups).loc[mask].nunique()) < 2:
        base_meta["enabled"] = False
        base_meta["fallback"] = "insufficient_groups_after_window"
        base_meta["rows_after"] = int(len(X))
        base_meta["groups_after"] = int(pd.Series(groups).nunique())
        return (
            X.reset_index(drop=True),
            y.reset_index(drop=True),
            pd.Series(groups).reset_index(drop=True),
            base_meta,
        )

    window_groups = pd.Series(groups).loc[mask].reset_index(drop=True)
    base_meta["cutoff"] = cutoff.isoformat()
    base_meta["rows_after"] = int(mask.sum())
    base_meta["groups_after"] = int(window_groups.nunique())
    return (
        X.loc[mask].reset_index(drop=True),
        y.loc[mask].reset_index(drop=True),
        window_groups,
        base_meta,
    )


def build_recency_sample_weights(
    groups: pd.Series,
    *,
    half_life_days: float,
    max_weight: float,
) -> pd.Series:
    groups_s = pd.Series(groups).reset_index(drop=True)
    if len(groups_s) == 0 or half_life_days <= 0:
        return pd.Series(np.ones(len(groups_s), dtype=float), index=groups_s.index)

    group_ts = coerce_group_datetimes(groups_s)
    if group_ts.isna().all():
        return pd.Series(np.ones(len(groups_s), dtype=float), index=groups_s.index)

    max_ts = group_ts.max()
    age_days = (max_ts - group_ts).dt.total_seconds().fillna(0.0) / 86400.0
    decay = np.power(0.5, age_days / max(float(half_life_days), 1.0 / 24.0))
    weights = pd.Series(decay, index=groups_s.index, dtype=float)
    lower = 1.0 / max(float(max_weight), 1.0)
    weights = weights.clip(lower=lower, upper=max(float(max_weight), 1.0))
    mean_weight = float(weights.mean()) or 1.0
    return weights / mean_weight


def summarize_numeric_series(values: pd.Series) -> dict[str, float]:
    series = pd.Series(values, dtype=float)
    finite = series[np.isfinite(series)]
    if finite.empty:
        return {
            "count": 0,
            "mean": 0.0,
            "std": 0.0,
            "min": 0.0,
            "median": 0.0,
            "max": 0.0,
        }
    return {
        "count": int(len(finite)),
        "mean": float(finite.mean()),
        "std": float(finite.std(ddof=0)),
        "min": float(finite.min()),
        "median": float(finite.median()),
        "max": float(finite.max()),
    }


def safe_json_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return float(default)
        return parsed
    except Exception:
        return float(default)
