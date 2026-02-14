from __future__ import annotations

from dataclasses import dataclass
from typing import List

import numpy as np

from configs.schema import RegimeConfig, RegimeState
from src.core.models import MarketSeries


def _sma(xs: List[float], n: int) -> float:
    if len(xs) < n:
        return float(np.mean(xs)) if xs else 0.0
    return float(np.mean(np.array(xs[-n:], dtype=float)))


def _atr_pct(series: MarketSeries, n: int = 14) -> float:
    """ATR as percent of close."""
    if len(series.close) < n + 1:
        return 0.0
    h = np.array(series.high[-n:], dtype=float)
    l = np.array(series.low[-n:], dtype=float)
    c_prev = np.array(series.close[-n - 1 : -1], dtype=float)
    tr = np.maximum(h - l, np.maximum(np.abs(h - c_prev), np.abs(l - c_prev)))
    atr = float(np.mean(tr))
    last = float(series.close[-1])
    return atr / last if last else 0.0


@dataclass
class RegimeResult:
    state: RegimeState
    atr_pct: float
    ma20: float
    ma60: float
    multiplier: float


class RegimeEngine:
    def __init__(self, cfg: RegimeConfig):
        self.cfg = cfg

    def detect(self, btc_data: MarketSeries) -> RegimeResult:
        closes = list(btc_data.close)
        ma20 = _sma(closes, 20)
        ma60 = _sma(closes, 60)
        atrp = _atr_pct(btc_data, 14)

        if ma20 > ma60 and atrp > float(self.cfg.atr_threshold):
            st = RegimeState.TRENDING
            mult = float(self.cfg.pos_mult_trending)
        elif atrp < float(self.cfg.atr_very_low):
            st = RegimeState.SIDEWAYS
            mult = float(self.cfg.pos_mult_sideways)
        else:
            st = RegimeState.RISK_OFF
            mult = float(self.cfg.pos_mult_risk_off)

        return RegimeResult(state=st, atr_pct=float(atrp), ma20=float(ma20), ma60=float(ma60), multiplier=float(mult))
