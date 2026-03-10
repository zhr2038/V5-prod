from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from scripts.backfill_ml_multihorizon_labels import backfill_multihorizon_labels
from src.core.models import MarketSeries
from src.execution.ml_data_collector import MLDataCollector


class FakeProvider:
    def __init__(self, frame: pd.DataFrame):
        self.frame = frame.copy()
        self.calls = []

    def fetch_ohlcv(self, symbols, timeframe, limit, end_ts_ms=None):
        self.calls.append(
            {
                "symbols": list(symbols),
                "timeframe": timeframe,
                "limit": int(limit),
                "end_ts_ms": end_ts_ms,
            }
        )
        out = {}
        for symbol in symbols:
            df = self.frame[self.frame["symbol"] == symbol].copy()
            if end_ts_ms is not None:
                df = df[df["timestamp_ms"] < int(end_ts_ms)]
            if int(limit) > 0:
                df = df.sort_values("timestamp_ms").tail(int(limit))
            out[symbol] = MarketSeries(
                symbol=symbol,
                timeframe=timeframe,
                ts=df["timestamp_ms"].astype(int).tolist(),
                open=df["close"].astype(float).tolist(),
                high=df["close"].astype(float).tolist(),
                low=df["close"].astype(float).tolist(),
                close=df["close"].astype(float).tolist(),
                volume=[1.0] * len(df),
            )
        return out


def _market_data() -> dict:
    close = [100.0 + i for i in range(30)]
    return {
        "close": close,
        "high": [x + 1.0 for x in close],
        "low": [x - 1.0 for x in close],
        "volume": [10.0 + i for i in range(30)],
    }


def _write_cache(cache_dir: Path, symbol: str, rows: list[tuple[int, float]]) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    prefix = symbol.replace("/", "_")
    df = pd.DataFrame(
        {
            "timestamp": [pd.to_datetime(ts, unit="ms").strftime("%Y-%m-%d %H:%M:%S") for ts, _ in rows],
            "close": [px for _, px in rows],
        }
    )
    df.to_csv(cache_dir / f"{prefix}_1H_2026-01-01_2026-01-02.csv", index=False)


def test_backfill_multihorizon_labels_merges_cache_and_api(tmp_path: Path) -> None:
    db_path = tmp_path / "ml_training_data.db"
    cache_dir = tmp_path / "cache"
    collector = MLDataCollector(db_path=str(db_path))
    ts = 1_700_000_000_000
    collector.collect_features(timestamp=ts, symbol="BTC/USDT", market_data=_market_data(), regime="Trending")

    _write_cache(
        cache_dir,
        "BTC/USDT",
        [
            (ts, 100.0),
            (ts + 6 * 3600 * 1000, 106.0),
            (ts + 12 * 3600 * 1000, 112.0),
        ],
    )
    provider = FakeProvider(
        pd.DataFrame(
            {
                "symbol": ["BTC/USDT"] * 4,
                "timestamp_ms": [
                    ts,
                    ts + 6 * 3600 * 1000,
                    ts + 12 * 3600 * 1000,
                    ts + 24 * 3600 * 1000,
                ],
                "close": [100.0, 106.0, 112.0, 124.0],
            }
        )
    )

    result = backfill_multihorizon_labels(
        db_path=db_path,
        cache_dir=cache_dir,
        as_of_ms=ts + 25 * 3600 * 1000,
        provider=provider,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT future_return_6h, future_return_12h, future_return_24h, label_filled "
            "FROM feature_snapshots WHERE symbol = 'BTC/USDT'"
        ).fetchone()
    finally:
        conn.close()

    assert result["rows_filled"] == 1
    assert result["cache_symbols"] == 1
    assert result["api_symbols"] == 1
    assert provider.calls
    assert row == (0.06, 0.12, 0.24, 1)


def test_backfill_multihorizon_labels_skips_rows_younger_than_24h(tmp_path: Path) -> None:
    db_path = tmp_path / "ml_training_data.db"
    cache_dir = tmp_path / "cache"
    collector = MLDataCollector(db_path=str(db_path))
    ts = 1_700_000_000_000
    collector.collect_features(timestamp=ts, symbol="ETH/USDT", market_data=_market_data(), regime="Trending")

    _write_cache(
        cache_dir,
        "ETH/USDT",
        [
            (ts, 200.0),
            (ts + 6 * 3600 * 1000, 212.0),
            (ts + 12 * 3600 * 1000, 224.0),
            (ts + 24 * 3600 * 1000, 248.0),
        ],
    )

    result = backfill_multihorizon_labels(
        db_path=db_path,
        cache_dir=cache_dir,
        as_of_ms=ts + 23 * 3600 * 1000,
        provider=None,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT future_return_6h, future_return_12h, future_return_24h, label_filled "
            "FROM feature_snapshots WHERE symbol = 'ETH/USDT'"
        ).fetchone()
    finally:
        conn.close()

    assert result["rows_pending"] == 0
    assert result["rows_filled"] == 0
    assert row == (None, None, None, 0)
