from __future__ import annotations

import time
from typing import Dict, List, Optional

import ccxt  # type: ignore
import requests

from src.core.models import MarketSeries
from .market_data_provider import MarketDataProvider


class OKXCCXTProvider(MarketDataProvider):
    """OKX spot data provider.

    OHLCV uses OKX public REST directly so we can page historical candles
    reliably beyond the exchange's single-request cap. Top-of-book still uses
    ccxt tickers.
    """

    def __init__(
        self,
        rate_limit: bool = True,
        *,
        base_url: str = "https://www.okx.com",
        timeout_sec: float = 10.0,
    ):
        self.ex = ccxt.okx({"enableRateLimit": bool(rate_limit)})
        self.base_url = str(base_url).rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.max_ohlcv_batch = 300
        try:
            self.ex.load_markets()
        except Exception:
            pass

    @staticmethod
    def _timeframe_to_okx_bar(timeframe: str) -> str:
        tf = str(timeframe or "").strip().lower()
        if len(tf) < 2:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        unit = tf[-1]
        value = int(tf[:-1])
        if value <= 0:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        if unit == "m":
            return f"{value}m"
        if unit == "h":
            return f"{value}H"
        if unit == "d":
            return f"{value}D"
        if unit == "w":
            return f"{value}W"
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    @staticmethod
    def _timeframe_ms(timeframe: str) -> int:
        tf = str(timeframe or "").strip().lower()
        if len(tf) < 2:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        unit = tf[-1]
        value = int(tf[:-1])
        mult = {
            "m": 60_000,
            "h": 3_600_000,
            "d": 86_400_000,
            "w": 604_800_000,
        }.get(unit)
        if mult is None or value <= 0:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        return int(value * mult)

    def _symbol_to_inst_id(self, symbol: str) -> str:
        try:
            market = self.ex.market(symbol)
            market_id = str((market or {}).get("id") or "").strip()
            if market_id:
                return market_id
        except Exception:
            pass
        return str(symbol or "").replace("/", "-").strip()

    def _fetch_history_candles(
        self,
        inst_id: str,
        timeframe: str,
        *,
        after_ms: int,
        limit: int,
    ) -> List[List[float]]:
        url = f"{self.base_url}/api/v5/market/history-candles"
        params = {
            "instId": str(inst_id),
            "bar": self._timeframe_to_okx_bar(timeframe),
            "after": str(int(after_ms)),
            "limit": str(min(int(limit), int(self.max_ohlcv_batch))),
        }
        r = requests.get(url, params=params, timeout=self.timeout_sec)
        r.raise_for_status()
        obj = r.json()
        if str(obj.get("code", "0")) != "0":
            raise RuntimeError(f"OKX history-candles error: code={obj.get('code')} msg={obj.get('msg')}")

        rows = obj.get("data") or []
        out: List[List[float]] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            try:
                out.append(
                    [
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5]),
                    ]
                )
            except Exception:
                continue
        return out

    def _fetch_symbol_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        *,
        limit: int,
        end_ts_ms: Optional[int],
    ) -> MarketSeries:
        requested = max(int(limit or 0), 0)
        if requested <= 0:
            return MarketSeries(symbol=symbol, timeframe=timeframe, ts=[], open=[], high=[], low=[], close=[], volume=[])

        cursor_ms = int(end_ts_ms) if end_ts_ms is not None else int(time.time() * 1000) + self._timeframe_ms(timeframe)
        inst_id = self._symbol_to_inst_id(symbol)

        all_rows: List[List[float]] = []
        seen_ts = set()

        while len(all_rows) < requested:
            chunk = min(int(self.max_ohlcv_batch), requested - len(all_rows))
            page = self._fetch_history_candles(inst_id, timeframe, after_ms=cursor_ms, limit=chunk)
            if not page:
                break

            oldest_ts = cursor_ms
            for bar in page:
                ts = int(bar[0])
                oldest_ts = min(oldest_ts, ts)
                if end_ts_ms is not None and ts >= int(end_ts_ms):
                    continue
                if ts in seen_ts:
                    continue
                seen_ts.add(ts)
                all_rows.append(bar)

            if oldest_ts >= cursor_ms:
                break
            cursor_ms = oldest_ts
            if len(page) < chunk:
                break

        all_rows.sort(key=lambda x: int(x[0]))
        if len(all_rows) > requested:
            all_rows = all_rows[-requested:]

        return MarketSeries(
            symbol=symbol,
            timeframe=timeframe,
            ts=[int(b[0]) for b in all_rows],
            open=[float(b[1]) for b in all_rows],
            high=[float(b[2]) for b in all_rows],
            low=[float(b[3]) for b in all_rows],
            close=[float(b[4]) for b in all_rows],
            volume=[float(b[5]) for b in all_rows],
        )

    def fetch_ohlcv(
        self,
        symbols: List[str],
        timeframe: str,
        limit: int = 200,
        end_ts_ms: int | None = None,
    ) -> Dict[str, MarketSeries]:
        """Fetch OHLCV for multiple symbols."""
        out: Dict[str, MarketSeries] = {}
        for symbol in symbols:
            try:
                series = self._fetch_symbol_ohlcv(
                    symbol,
                    timeframe,
                    limit=int(limit),
                    end_ts_ms=end_ts_ms,
                )
                if not series.ts:
                    print(f"[OKXCCXT] Warning: No OHLCV data for {symbol}")
                    continue
                out[symbol] = series
            except Exception as e:
                print(f"[OKXCCXT] Error fetching OHLCV for {symbol}: {e}")
                continue
        return out

    def fetch_top_of_book(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        """Return {symbol: {bid, ask}} using ccxt tickers."""
        out: Dict[str, Dict[str, float]] = {}
        try:
            tickers = None
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
