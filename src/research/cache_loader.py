from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pandas as pd

from src.core.models import MarketSeries


def _cache_timeframe_token(timeframe: str) -> str:
    value = str(timeframe or "").strip().lower()
    if len(value) < 2:
        raise ValueError(f"invalid timeframe: {timeframe}")
    unit = value[-1]
    amount = int(value[:-1])
    if amount <= 0:
        raise ValueError(f"invalid timeframe: {timeframe}")
    token = {"m": "M", "h": "H", "d": "D", "w": "W"}.get(unit)
    if token is None:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    return f"{amount}{token}"


def _load_symbol_cache_frame(cache_dir: Path, symbol: str, timeframe: str) -> pd.DataFrame:
    prefix = f"{str(symbol).replace('/', '_')}_{_cache_timeframe_token(timeframe)}_"
    paths = sorted(cache_dir.glob(f"{prefix}*.csv"))
    if not paths:
        raise FileNotFoundError(f"no cache files found for {symbol} in {cache_dir}")

    frames = []
    for path in paths:
        df = pd.read_csv(path)
        required = {"timestamp", "open", "high", "low", "close", "volume"}
        cols = {str(c).strip().lower(): c for c in df.columns}
        if not required.issubset(cols.keys()):
            raise ValueError(f"cache file missing OHLCV columns: {path}")
        normalized = df[[cols["timestamp"], cols["open"], cols["high"], cols["low"], cols["close"], cols["volume"]]].copy()
        normalized.columns = ["timestamp", "open", "high", "low", "close", "volume"]
        normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True)
        frames.append(normalized)

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
    merged = merged.set_index("timestamp")[["open", "high", "low", "close", "volume"]]
    if merged.empty:
        raise ValueError(f"cache data is empty after merge for {symbol}")
    return merged


def _index_to_epoch_ms(index: pd.Index) -> list[int]:
    raw = index.asi8.astype("int64")
    if len(raw) == 0:
        return []

    sample = int(abs(raw[0]))
    if sample >= 10**17:
        scale = 1_000_000  # ns -> ms
    elif sample >= 10**14:
        scale = 1_000  # us -> ms
    elif sample >= 10**11:
        scale = 1  # already ms
    else:
        scale = -1  # seconds -> ms

    if scale == -1:
        return (raw * 1_000).astype(int).tolist()
    return (raw // scale).astype(int).tolist()


def load_cached_market_data(
    cache_dir: str | Path,
    symbols: list[str],
    timeframe: str,
    *,
    limit: int | None = None,
) -> Dict[str, MarketSeries]:
    root = Path(cache_dir)
    if not root.exists():
        raise FileNotFoundError(f"cache directory not found: {root}")

    aligned_frames: dict[str, pd.DataFrame] = {}
    common_index = None
    for symbol in symbols:
        frame = _load_symbol_cache_frame(root, symbol, timeframe)
        aligned_frames[str(symbol)] = frame
        common_index = frame.index if common_index is None else common_index.intersection(frame.index)

    if common_index is None or len(common_index) == 0:
        raise ValueError("no common timestamps across cached symbols")

    common_index = common_index.sort_values()
    if limit is not None:
        limited = int(limit)
        if limited > 0:
            common_index = common_index[-limited:]

    market_data: Dict[str, MarketSeries] = {}
    for symbol, frame in aligned_frames.items():
        sliced = frame.loc[common_index]
        timestamps = _index_to_epoch_ms(sliced.index)
        market_data[symbol] = MarketSeries(
            symbol=symbol,
            timeframe=timeframe,
            ts=timestamps,
            open=sliced["open"].astype(float).tolist(),
            high=sliced["high"].astype(float).tolist(),
            low=sliced["low"].astype(float).tolist(),
            close=sliced["close"].astype(float).tolist(),
            volume=sliced["volume"].astype(float).tolist(),
        )

    return market_data


def summarize_market_data(
    market_data: Dict[str, MarketSeries],
    *,
    source: str,
    source_path: str | None = None,
) -> dict[str, object]:
    if not market_data:
        return {
            "source": source,
            "source_path": source_path,
            "symbol_count": 0,
            "symbols": [],
            "bars": 0,
        }

    first = next(iter(market_data.values()))
    bars = min(len(series.close) for series in market_data.values())
    min_ts = min(min(series.ts) for series in market_data.values() if series.ts)
    max_ts = max(max(series.ts) for series in market_data.values() if series.ts)
    return {
        "source": source,
        "source_path": source_path,
        "symbol_count": len(market_data),
        "symbols": list(market_data.keys()),
        "timeframe": str(first.timeframe),
        "bars": int(bars),
        "time_range": {
            "start_ts": int(min_ts),
            "end_ts": int(max_ts),
            "start_iso": datetime.fromtimestamp(int(min_ts) / 1000.0, tz=timezone.utc).isoformat(),
            "end_iso": datetime.fromtimestamp(int(max_ts) / 1000.0, tz=timezone.utc).isoformat(),
        },
    }
