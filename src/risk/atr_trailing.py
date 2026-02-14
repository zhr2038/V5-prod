from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np

from src.core.models import MarketSeries


def atr(series: MarketSeries, n: int = 14) -> float:
    if len(series.close) < n + 1:
        return 0.0
    h = np.array(series.high[-n:], dtype=float)
    l = np.array(series.low[-n:], dtype=float)
    c_prev = np.array(series.close[-n - 1 : -1], dtype=float)
    tr = np.maximum(h - l, np.maximum(np.abs(h - c_prev), np.abs(l - c_prev)))
    return float(np.mean(tr))


@dataclass
class ATRTrailingState:
    highest_price: float
    stop_price: float


def update_atr_trailing(
    series: MarketSeries,
    state: Optional[ATRTrailingState],
    atr_mult: float = 2.2,
    n: int = 14,
) -> ATRTrailingState:
    last = float(series.close[-1]) if series.close else 0.0
    hi = float(state.highest_price) if state else last
    if last > hi:
        hi = last
    a = atr(series, n=n)
    stop = hi - float(atr_mult) * a
    return ATRTrailingState(highest_price=hi, stop_price=float(stop))
