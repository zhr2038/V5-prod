"""Bounded, public, GET-only chart data for the dashboard, isolated from trading."""
from __future__ import annotations

import copy
import math
import threading
import time

import requests


SYMBOLS = ("BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT")
TIMEFRAMES = {
    "1m": ("1m", 60), "5m": ("5m", 300), "15m": ("15m", 900),
    "30m": ("30m", 1800), "1h": ("1H", 3600), "4h": ("4H", 14400),
    "1d": ("1Dutc", 86400),
}
_KEYS = [(symbol, timeframe) for symbol in SYMBOLS for timeframe in TIMEFRAMES]
_LOCKS = {key: threading.Lock() for key in _KEYS}
_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}


class MarketChartError(ValueError):
    """A safe, fixed error code; upstream bodies and credentials never escape."""


def validate_selection(symbol: str, timeframe: str) -> tuple[str, str]:
    symbol = symbol.strip().upper().replace("/", "-")
    if symbol not in SYMBOLS or timeframe not in TIMEFRAMES:
        raise ValueError("unsupported_market_selection")
    return symbol, timeframe


def _number(value, *, positive=False) -> float:
    try:
        if isinstance(value, bool) or value is None or value == "":
            raise ValueError
        number = float(value)
        if not math.isfinite(number) or number < 0 or (positive and number == 0):
            raise ValueError
        return number
    except (TypeError, ValueError, OverflowError) as exc:
        raise MarketChartError("invalid_market_number") from exc


def _timestamp(value, now_ms: int) -> int:
    number = _number(value, positive=True)
    if number != int(number) or number < 1_000_000_000_000 or number > now_ms + 5000:
        raise MarketChartError("invalid_market_timestamp")
    return int(number)


def normalize_candles(rows, timeframe: str, now_ms: int) -> list[dict]:
    if not isinstance(rows, list) or not 1 <= len(rows) <= 300:
        raise MarketChartError("missing_or_oversized_candles")
    candles = []
    step = TIMEFRAMES[timeframe][1] * 1000
    for row in rows:
        if not isinstance(row, list) or len(row) != 9 or row[8] not in ("0", "1"):
            raise MarketChartError("invalid_candle_shape")
        ts = _timestamp(row[0], now_ms)
        o, h, low, c = [_number(value, positive=True) for value in row[1:5]]
        if ts % step or low > min(o, c) or h < max(o, c):
            raise MarketChartError("invalid_candle_range")
        candles.append({"ts": ts, "open": o, "high": h, "low": low, "close": c,
                        "volume": _number(row[5]), "quote_volume": _number(row[7]),
                        "closed": row[8] == "1"})
    candles.sort(key=lambda candle: candle["ts"])
    if any(right["ts"] - left["ts"] != step for left, right in zip(candles, candles[1:])):
        raise MarketChartError("duplicate_or_missing_candle")
    return candles


def normalize_ticker(rows, symbol: str, now_ms: int) -> dict:
    if (not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict)
            or rows[0].get("instId") != symbol or rows[0].get("instType") != "SPOT"):
        raise MarketChartError("ticker_identity_mismatch")
    row = rows[0]
    return {"ts": _timestamp(row.get("ts"), now_ms),
            "last": _number(row.get("last"), positive=True),
            "open_24h": _number(row.get("open24h"), positive=True),
            "high_24h": _number(row.get("high24h"), positive=True),
            "low_24h": _number(row.get("low24h"), positive=True),
            "volume_24h": _number(row.get("vol24h")),
            "quote_volume_24h": _number(row.get("volCcy24h"))}


def _public_get(endpoint: str, params: dict) -> list:
    # No account client, API keys, trading provider, redirects, retries or disk writes.
    try:
        response = requests.get("https://www.okx.com/api/v5/market/" + endpoint,
                                params=params, timeout=(2, 3), allow_redirects=False)
        if response.status_code != 200:
            raise MarketChartError("market_http_unavailable")
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("code") != "0":
            raise MarketChartError("market_api_unavailable")
        return payload.get("data")
    except (requests.RequestException, ValueError) as exc:
        if isinstance(exc, MarketChartError):
            raise
        raise MarketChartError("market_transport_unavailable") from exc


def get_market_chart(symbol: str, timeframe: str) -> dict:
    key = validate_selection(symbol, timeframe)
    symbol, timeframe = key
    # Exactly 28 possible cache/lock entries, regardless of arbitrary URL query args.
    with _LOCKS[key]:
        cached = _CACHE.get(key)
        if cached and cached[0] > time.monotonic():
            return copy.deepcopy(cached[1])
        rows = _public_get("candles", {"instId": symbol, "bar": TIMEFRAMES[timeframe][0], "limit": "300"})
        candle_received_at_ms = int(time.time() * 1000)
        candles = normalize_candles(rows, timeframe, candle_received_at_ms)
        ticker, ticker_error = None, None
        try:
            ticker_rows = _public_get("ticker", {"instId": symbol})
            ticker = normalize_ticker(ticker_rows, symbol, int(time.time() * 1000))
        except MarketChartError as exc:
            ticker_error = str(exc)
        payload = {
            "schema_version": "v5.market_chart.v1", "read_only": True,
            "source": "OKX public REST", "symbol": symbol, "timeframe": timeframe,
            "bar_seconds": TIMEFRAMES[timeframe][1], "day_boundary": "UTC",
            "candle_received_at_ms": candle_received_at_ms,
            "fetched_at_ms": int(time.time() * 1000), "candles": candles,
            "ticker": ticker, "ticker_error": ticker_error,
        }
        _CACHE[key] = (time.monotonic() + 8, payload)
        return copy.deepcopy(payload)
