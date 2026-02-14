from __future__ import annotations

import math
import time
from typing import Dict, List

import numpy as np

from src.core.models import MarketSeries
from .market_data_provider import MarketDataProvider


class MockProvider(MarketDataProvider):
    """Deterministic-ish mock OHLCV for dry-run/tests without network."""

    def __init__(self, seed: int = 7):
        self.rng = np.random.default_rng(seed)

    def fetch_ohlcv(self, symbols: List[str], timeframe: str, limit: int) -> Dict[str, MarketSeries]:
        now = int(time.time() * 1000)
        step = 3600_000 if timeframe.endswith("h") else 3600_000
        out: Dict[str, MarketSeries] = {}
        for i, sym in enumerate(symbols):
            base = 100 + 20 * i
            ts = [now - step * (limit - 1 - k) for k in range(limit)]
            drift = 0.0002 * (i + 1)
            vols = self.rng.normal(0, 0.01, size=limit)
            prices = [base]
            for k in range(1, limit):
                prices.append(prices[-1] * (1 + drift + vols[k]))
            close = [float(x) for x in prices]
            open_ = [close[0]] + close[:-1]
            high = [max(o, c) * (1 + abs(float(self.rng.normal(0, 0.002)))) for o, c in zip(open_, close)]
            low = [min(o, c) * (1 - abs(float(self.rng.normal(0, 0.002)))) for o, c in zip(open_, close)]
            volume = [float(1_000_000 + 100_000 * math.sin(k / 5.0) + 50_000 * i) for k in range(limit)]
            out[sym] = MarketSeries(symbol=sym, timeframe=timeframe, ts=ts, open=open_, high=high, low=low, close=close, volume=volume)
        return out
