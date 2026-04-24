from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np

from src.core.models import MarketSeries


def _normalize_market_series(series: MarketSeries) -> MarketSeries:
    points = []
    for idx, values in enumerate(
        zip(
            series.ts or [],
            series.open or [],
            series.high or [],
            series.low or [],
            series.close or [],
            series.volume or [],
        )
    ):
        ts_value, open_px, high_px, low_px, close_px, volume = values
        try:
            ts_ms = int(ts_value)
        except Exception:
            continue
        if abs(ts_ms) < 10_000_000_000:
            ts_ms *= 1000
        points.append((ts_ms, idx, open_px, high_px, low_px, close_px, volume))

    if not points:
        return MarketSeries(symbol=series.symbol, timeframe=series.timeframe, ts=[], open=[], high=[], low=[], close=[], volume=[])

    points.sort(key=lambda item: (item[0], item[1]))
    deduped = []
    for point in points:
        if deduped and deduped[-1][0] == point[0]:
            deduped[-1] = point
        else:
            deduped.append(point)

    return MarketSeries(
        symbol=series.symbol,
        timeframe=series.timeframe,
        ts=[int(item[0]) for item in deduped],
        open=[item[2] for item in deduped],
        high=[item[3] for item in deduped],
        low=[item[4] for item in deduped],
        close=[float(item[5]) for item in deduped],
        volume=[item[6] for item in deduped],
    )


def atr(series: MarketSeries, n: int = 14) -> float:
    """计算ATR (Average True Range)

    Args:
        series: 市场数据序列
        n: ATR计算周期

    Returns:
        ATR值
    """
    series = _normalize_market_series(series)
    if len(series.close) < n + 1:
        return 0.0
    h = np.array(series.high[-n:], dtype=float)
    l = np.array(series.low[-n:], dtype=float)
    c_prev = np.array(series.close[-n - 1 : -1], dtype=float)
    tr = np.maximum(h - l, np.maximum(np.abs(h - c_prev), np.abs(l - c_prev)))
    return float(np.mean(tr))


@dataclass
class ATRTrailingState:
    """ATR追踪止损状态"""
    highest_price: float
    stop_price: float


def update_atr_trailing(
    series: MarketSeries,
    state: Optional[ATRTrailingState],
    atr_mult: float = 2.2,
    n: int = 14,
) -> ATRTrailingState:
    """更新ATR追踪止损

    Args:
        series: 市场数据序列
        state: 当前状态
        atr_mult: ATR乘数
        n: ATR计算周期

    Returns:
        更新后的状态
    """
    series = _normalize_market_series(series)
    last = float(series.close[-1]) if series.close else 0.0
    hi = float(state.highest_price) if state else last
    if last > hi:
        hi = last
    a = atr(series, n=n)
    stop = hi - float(atr_mult) * a
    return ATRTrailingState(highest_price=hi, stop_price=float(stop))
