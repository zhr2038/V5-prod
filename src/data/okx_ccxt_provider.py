from __future__ import annotations

from typing import Dict, List, Optional

import ccxt  # type: ignore

from src.core.models import MarketSeries
from .market_data_provider import MarketDataProvider


class OKXCCXTProvider(MarketDataProvider):
    """OKX spot data provider using ccxt (public endpoints for OHLCV)."""

    def __init__(self, rate_limit: bool = True):
        self.ex = ccxt.okx({"enableRateLimit": bool(rate_limit)})
        try:
            self.ex.load_markets()
        except Exception:
            pass

    def fetch_ohlcv(self, symbols: List[str], timeframe: str, limit: int = 200) -> Dict[str, MarketSeries]:
        out: Dict[str, MarketSeries] = {}
        for s in symbols:
            bars = self.ex.fetch_ohlcv(s, timeframe=timeframe, limit=int(limit))
            ts = [int(b[0]) for b in bars]
            o = [float(b[1]) for b in bars]
            h = [float(b[2]) for b in bars]
            l = [float(b[3]) for b in bars]
            c = [float(b[4]) for b in bars]
            v = [float(b[5]) for b in bars]
            out[s] = MarketSeries(symbol=s, timeframe=timeframe, ts=ts, open=o, high=h, low=l, close=c, volume=v)
        return out
