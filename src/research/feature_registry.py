from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Sequence

import numpy as np
import pandas as pd

from src.alpha.qlib_factors import (
    _age_since_max_roll,
    _age_since_min_roll,
    _rank_last_roll,
    _rsquare_roll,
    compute_alpha158_style_factors,
)

CLASSIC_FEATURE_NAMES = (
    "returns_1h",
    "returns_6h",
    "returns_24h",
    "momentum_5d",
    "momentum_20d",
    "volatility_6h",
    "volatility_24h",
    "volatility_ratio",
    "volume_ratio",
    "obv",
    "rsi",
    "macd",
    "macd_signal",
    "bb_position",
    "price_position",
)

ALPHA158_FEATURE_NAMES = (
    "f6_corr_pv_10",
    "f7_cord_10",
    "f8_rsqr_10",
    "f9_rank_20",
    "f10_imax_14",
    "f11_imin_14",
    "f12_imxd_14",
)

TIME_FEATURE_NAMES = (
    "hour_of_day",
    "day_of_week",
)

FEATURE_GROUPS = {
    "classic": CLASSIC_FEATURE_NAMES,
    "alpha158": ALPHA158_FEATURE_NAMES,
    "time": TIME_FEATURE_NAMES,
}


def _coerce_numeric_series(
    values: Any,
    *,
    length: int,
    fallback: pd.Series | None = None,
    default: float = 0.0,
) -> pd.Series:
    if values is None:
        if fallback is not None:
            return pd.Series(fallback, dtype=float).reset_index(drop=True)
        return pd.Series(np.full(length, default, dtype=float))

    try:
        arr = np.asarray(list(values), dtype=float)
    except Exception:
        arr = np.asarray([], dtype=float)

    if arr.size != length:
        if fallback is not None:
            return pd.Series(fallback, dtype=float).reset_index(drop=True)
        arr = np.full(length, default, dtype=float)

    out = pd.Series(arr, dtype=float).replace([np.inf, -np.inf], np.nan)
    if fallback is not None:
        fallback_s = pd.Series(fallback, dtype=float).reset_index(drop=True)
        out = out.fillna(fallback_s)
    return out.reset_index(drop=True)


def _coerce_timestamp_series(values: Any, *, length: int) -> pd.Series:
    if values is None:
        return pd.Series([np.nan] * length)
    try:
        arr = list(values)
    except Exception:
        return pd.Series([np.nan] * length)
    if len(arr) != length:
        return pd.Series([np.nan] * length)
    return pd.Series(arr)


def _value_from_data(data: Any, key: str) -> Any:
    if isinstance(data, dict):
        return data.get(key)
    return getattr(data, key, None)


def _normalize_feature_groups(
    feature_groups: Sequence[str] | None = None,
    *,
    include_time_features: bool = False,
    feature_names: Sequence[str] | None = None,
) -> tuple[str, ...]:
    groups = {str(g).strip().lower() for g in (feature_groups or ()) if str(g).strip()}
    if feature_names:
        names = {str(name) for name in feature_names}
        if names & set(CLASSIC_FEATURE_NAMES):
            groups.add("classic")
        if names & set(ALPHA158_FEATURE_NAMES):
            groups.add("alpha158")
        if names & set(TIME_FEATURE_NAMES):
            groups.add("time")
    if not groups:
        groups.add("classic")
    if include_time_features:
        groups.add("time")
    ordered = [name for name in ("classic", "alpha158", "time") if name in groups]
    return tuple(ordered)


def resolve_feature_names(
    feature_groups: Sequence[str] | None = None,
    *,
    include_time_features: bool = False,
    explicit_feature_names: Sequence[str] | None = None,
) -> list[str]:
    if explicit_feature_names:
        names = [str(name) for name in explicit_feature_names]
        seen = set()
        out = []
        for name in names:
            if name in seen:
                continue
            seen.add(name)
            out.append(name)
        return out

    groups = _normalize_feature_groups(feature_groups, include_time_features=include_time_features)
    out: list[str] = []
    for group in groups:
        out.extend(FEATURE_GROUPS.get(group, ()))
    return out


def _build_time_features_from_timestamps(timestamps: pd.Series) -> pd.DataFrame:
    dt = pd.to_datetime(timestamps, unit="ms", errors="coerce")
    if dt.isna().all():
        dt = pd.to_datetime(timestamps, errors="coerce")
    frame = pd.DataFrame(index=timestamps.index)
    frame["hour_of_day"] = dt.dt.hour.astype(float)
    frame["day_of_week"] = dt.dt.dayofweek.astype(float)
    return frame


def _build_classic_feature_frame(
    *,
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
) -> pd.DataFrame:
    returns_1h = close.pct_change(1)
    returns_6h = close.pct_change(6)
    returns_24h = close.pct_change(24)
    volume_sma = volume.rolling(24).mean()
    bb_middle = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    high_20d = high.rolling(20 * 24).max()
    low_20d = low.rolling(20 * 24).min()

    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = (gain / loss).replace([np.inf, -np.inf], np.nan)
    rsi = (100 - (100 / (1 + rs))).fillna(50.0)

    exp1 = close.ewm(span=12).mean()
    exp2 = close.ewm(span=26).mean()
    macd_line = exp1 - exp2
    macd_signal_line = macd_line.ewm(span=9).mean()

    frame = pd.DataFrame(index=close.index)
    frame["returns_1h"] = returns_1h
    frame["returns_6h"] = returns_6h
    frame["returns_24h"] = returns_24h
    frame["momentum_5d"] = (close - close.shift(5 * 24)) / close.shift(5 * 24)
    frame["momentum_20d"] = (close - close.shift(20 * 24)) / close.shift(20 * 24)
    frame["volatility_6h"] = returns_1h.rolling(6).std()
    frame["volatility_24h"] = returns_1h.rolling(24).std()
    frame["volatility_ratio"] = (
        returns_1h.rolling(6).std() / returns_1h.rolling(24).std()
    ).replace([np.inf, -np.inf], np.nan)
    frame["volume_ratio"] = (volume / volume_sma).replace([np.inf, -np.inf], np.nan)
    frame["obv"] = (np.sign(returns_1h.fillna(0.0)) * volume).cumsum()
    frame["rsi"] = rsi
    frame["macd"] = macd_line
    frame["macd_signal"] = macd_signal_line
    frame["bb_position"] = ((close - bb_middle) / (2 * bb_std)).replace([np.inf, -np.inf], np.nan)
    frame["price_position"] = ((close - low_20d) / (high_20d - low_20d)).replace([np.inf, -np.inf], np.nan)
    return frame


def _build_alpha158_feature_frame(
    *,
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
) -> pd.DataFrame:
    lv = np.log(volume + 1.0)
    c_ret = close / close.shift(1)
    v_chg = np.log((volume / volume.shift(1)).replace([np.inf, -np.inf], np.nan) + 1.0)

    frame = pd.DataFrame(index=close.index)
    frame["f6_corr_pv_10"] = close.rolling(10).corr(lv)
    frame["f7_cord_10"] = c_ret.rolling(10).corr(v_chg)
    frame["f8_rsqr_10"] = close.rolling(10).apply(_rsquare_roll, raw=True)
    frame["f9_rank_20"] = close.rolling(20).apply(_rank_last_roll, raw=True)
    frame["f10_imax_14"] = high.rolling(14).apply(_age_since_max_roll, raw=True)
    frame["f11_imin_14"] = low.rolling(14).apply(_age_since_min_roll, raw=True)
    frame["f12_imxd_14"] = frame["f10_imax_14"] - frame["f11_imin_14"]
    return frame


def build_feature_frame_from_market_data(
    market_data: Dict[str, Any],
    *,
    feature_groups: Sequence[str] | None = None,
    include_time_features: bool = False,
    min_bars: int = 2,
) -> pd.DataFrame:
    groups = _normalize_feature_groups(feature_groups, include_time_features=include_time_features)
    frames: list[pd.DataFrame] = []

    for symbol, data in (market_data or {}).items():
        close_raw = _value_from_data(data, "close")
        if close_raw is None:
            continue

        close = pd.Series(list(close_raw), dtype=float).replace([np.inf, -np.inf], np.nan)
        if len(close) < int(min_bars):
            continue

        volume = _coerce_numeric_series(_value_from_data(data, "volume"), length=len(close), default=0.0)
        high = _coerce_numeric_series(_value_from_data(data, "high"), length=len(close), fallback=close)
        low = _coerce_numeric_series(_value_from_data(data, "low"), length=len(close), fallback=close)
        timestamps = _coerce_timestamp_series(_value_from_data(data, "ts"), length=len(close))

        frame = pd.DataFrame({"symbol": [str(symbol)] * len(close), "timestamp": timestamps})
        if "classic" in groups:
            frame = pd.concat(
                [
                    frame,
                    _build_classic_feature_frame(close=close, high=high, low=low, volume=volume).reset_index(drop=True),
                ],
                axis=1,
            )
        if "alpha158" in groups:
            frame = pd.concat(
                [
                    frame,
                    _build_alpha158_feature_frame(close=close, high=high, low=low, volume=volume).reset_index(drop=True),
                ],
                axis=1,
            )
        if "time" in groups:
            frame = pd.concat([frame, _build_time_features_from_timestamps(frame["timestamp"])], axis=1)
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=["symbol", "timestamp", *resolve_feature_names(groups)])
    out = pd.concat(frames, ignore_index=True)
    return out.replace([np.inf, -np.inf], np.nan)


def build_snapshot_feature_row(
    *,
    symbol: str,
    close: Sequence[float],
    high: Sequence[float] | None = None,
    low: Sequence[float] | None = None,
    volume: Sequence[float] | None = None,
    timestamp_ms: int | float | None = None,
    feature_groups: Sequence[str] | None = None,
    include_time_features: bool = False,
) -> dict[str, float | str]:
    groups = _normalize_feature_groups(feature_groups, include_time_features=include_time_features)
    close_s = pd.Series(list(close), dtype=float)
    if close_s.empty:
        return {"symbol": str(symbol)}
    high_s = _coerce_numeric_series(high, length=len(close_s), fallback=close_s)
    low_s = _coerce_numeric_series(low, length=len(close_s), fallback=close_s)
    volume_s = _coerce_numeric_series(volume, length=len(close_s), default=0.0)

    row: dict[str, float | str] = {"symbol": str(symbol)}
    if "classic" in groups:
        latest = _build_classic_feature_frame(close=close_s, high=high_s, low=low_s, volume=volume_s).iloc[-1]
        row.update({str(k): float(v) if pd.notna(v) else 0.0 for k, v in latest.items()})
    if "alpha158" in groups:
        latest = _build_alpha158_feature_frame(close=close_s, high=high_s, low=low_s, volume=volume_s).iloc[-1]
        row.update({str(k): float(v) if pd.notna(v) else 0.0 for k, v in latest.items()})
    if "time" in groups:
        if timestamp_ms is None:
            dt = datetime.now(timezone.utc)
        else:
            dt = pd.to_datetime(timestamp_ms, unit="ms", errors="coerce")
            if pd.isna(dt):
                dt = datetime.now(timezone.utc)
        row["hour_of_day"] = float(dt.hour)
        row["day_of_week"] = float(dt.dayofweek)
    return row


def build_inference_frame_from_market_data(
    market_data: Dict[str, Any],
    *,
    feature_names: Sequence[str],
    feature_groups: Sequence[str] | None = None,
    include_time_features: bool = False,
) -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    groups = _normalize_feature_groups(
        feature_groups,
        include_time_features=include_time_features,
        feature_names=feature_names,
    )

    for symbol, data in (market_data or {}).items():
        close_raw = _value_from_data(data, "close")
        ts_values = _value_from_data(data, "ts")
        if close_raw is None or len(close_raw) < 2:
            continue
        row = build_snapshot_feature_row(
            symbol=str(symbol),
            close=list(close_raw),
            high=_value_from_data(data, "high"),
            low=_value_from_data(data, "low"),
            volume=_value_from_data(data, "volume"),
            timestamp_ms=(list(ts_values)[-1] if ts_values else None),
            feature_groups=groups,
            include_time_features=include_time_features,
        )
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["symbol", *[str(name) for name in feature_names]])

    df = pd.DataFrame(rows)
    for name in feature_names:
        if name not in df.columns:
            df[name] = 0.0
    df = df[["symbol", *[str(name) for name in feature_names]]].replace([np.inf, -np.inf], np.nan)
    valid = df[[str(name) for name in feature_names]].notna().all(axis=1)
    return df.loc[valid].reset_index(drop=True)


def available_feature_groups() -> dict[str, tuple[str, ...]]:
    return dict(FEATURE_GROUPS)


def build_alpha158_snapshot(
    *,
    close: Sequence[float],
    high: Sequence[float] | None = None,
    low: Sequence[float] | None = None,
    volume: Sequence[float] | None = None,
) -> dict[str, float]:
    return compute_alpha158_style_factors(
        list(close),
        list(high or close),
        list(low or close),
        list(volume or [0.0] * len(close)),
    )
