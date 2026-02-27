from __future__ import annotations

from typing import Dict, List, Optional

import ccxt  # type: ignore

from src.core.models import MarketSeries
from .market_data_provider import MarketDataProvider


class OKXCCXTProvider(MarketDataProvider):
    """OKX spot data provider using ccxt (public endpoints for OHLCV + top-of-book)."""

    def __init__(self, rate_limit: bool = True):
        self.ex = ccxt.okx({"enableRateLimit": bool(rate_limit)})
        try:
            self.ex.load_markets()
        except Exception:
            pass

    def fetch_ohlcv(
        """Fetch ohlcv"""
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
            try:
                bars = self.ex.fetch_ohlcv(s, timeframe=timeframe, limit=int(limit))
                if not bars or len(bars) == 0:
                    # 没有数据，记录警告但继续处理其他symbol
                    print(f"[OKXCCXT] Warning: No OHLCV data for {s}")
                    continue
                    
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
            except Exception as e:
                # 单个symbol失败不中断全量，记录错误并继续
                print(f"[OKXCCXT] Error fetching OHLCV for {s}: {e}")
                continue
        return out

    def fetch_top_of_book(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        """Return {symbol: {bid, ask}} using ccxt tickers.

        Best-effort: if bid/ask missing, symbol omitted.
        """
        out: Dict[str, Dict[str, float]] = {}
        try:
            tickers = None
            # fetch_tickers is more efficient if available
            if hasattr(self.ex, "fetch_tickers"):
                try:
                    tickers = self.ex.fetch_tickers(symbols)
                except Exception:
                    tickers = None
            if tickers is None:
                tickers = {s: self.ex.fetch_ticker(s) for s in symbols}

            for s in symbols:
                t = (tickers or {}).get(s) or {}
                bid = t.get("bid")
                ask = t.get("ask")
                if bid is None or ask is None:
                    continue
                bid_f = float(bid)
                ask_f = float(ask)
                if bid_f <= 0 or ask_f <= 0:
                    continue
                out[s] = {"bid": bid_f, "ask": ask_f}
        except Exception:
            return out
        return out
