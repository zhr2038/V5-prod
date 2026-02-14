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

    def fetch_ohlcv(
        self,
        symbols: List[str],
        timeframe: str,
        limit: int = 200,
        end_ts_ms: int | None = None,
    ) -> Dict[str, MarketSeries]:
        out: Dict[str, MarketSeries] = {}
        
        # 辅助函数：切片MarketSeries
        def _slice_series(series: MarketSeries, idxs: List[int]) -> MarketSeries:
            return MarketSeries(
                symbol=series.symbol,
                timeframe=series.timeframe,
                ts=[series.ts[i] for i in idxs],
                open=[series.open[i] for i in idxs],
                high=[series.high[i] for i in idxs],
                low=[series.low[i] for i in idxs],
                close=[series.close[i] for i in idxs],
                volume=[series.volume[i] for i in idxs],
            )
        
        for s in symbols:
            bars = self.ex.fetch_ohlcv(s, timeframe=timeframe, limit=int(limit))
            ts = [int(b[0]) for b in bars]
            o = [float(b[1]) for b in bars]
            h = [float(b[2]) for b in bars]
            l = [float(b[3]) for b in bars]
            c = [float(b[4]) for b in bars]
            v = [float(b[5]) for b in bars]
            
            series = MarketSeries(symbol=s, timeframe=timeframe, ts=ts, open=o, high=h, low=l, close=c, volume=v)
            
            # 如果指定了end_ts_ms，过滤掉ts >= end_ts_ms的bar（排除未收盘bar）
            if end_ts_ms is not None:
                idxs = [i for i, t in enumerate(series.ts) if t < end_ts_ms]
                if idxs:  # 只有有数据时才切片
                    series = _slice_series(series, idxs)
                else:
                    # 如果没有符合条件的bar，创建一个空的series
                    series = MarketSeries(
                        symbol=s, timeframe=timeframe,
                        ts=[], open=[], high=[], low=[], close=[], volume=[]
                    )
            
            out[s] = series
        return out
