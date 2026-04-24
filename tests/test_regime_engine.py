from __future__ import annotations

import os

from configs.schema import RegimeConfig
from src.core.models import MarketSeries
from src.regime.regime_engine import RegimeEngine


def test_load_market_sentiment_prefers_filename_timestamp_over_mtime(tmp_path) -> None:
    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    older = cache_dir / "funding_BTC-USDT_20260419_22.json"
    newer = cache_dir / "funding_BTC-USDT_20260419_23.json"
    older.write_text('{"f6_sentiment": -0.4}', encoding="utf-8")
    newer.write_text('{"f6_sentiment": 0.35}', encoding="utf-8")
    os.utime(older, (2_000_000_000, 2_000_000_000))
    os.utime(newer, (1_000_000_000, 1_000_000_000))

    engine = RegimeEngine(RegimeConfig())
    engine.sentiment_cache_dir = cache_dir

    assert engine._load_market_sentiment() == 0.35


def test_detect_normalizes_unsorted_market_series_before_ma_atr() -> None:
    cfg = RegimeConfig(sentiment_regime_override_enabled=False)
    engine = RegimeEngine(cfg)

    base_ts = 1_710_000_000_000
    ts = [base_ts + i * 3_600_000 for i in range(80)]
    closes = [100.0 + i for i in range(80)]
    sorted_series = MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=ts,
        open=closes,
        high=[value + 2.0 for value in closes],
        low=[value - 2.0 for value in closes],
        close=closes,
        volume=[1.0 for _ in closes],
    )
    unsorted_ts = list(ts)
    unsorted_close = list(closes)
    unsorted_open = list(closes)
    unsorted_high = [value + 2.0 for value in closes]
    unsorted_low = [value - 2.0 for value in closes]
    unsorted_volume = [1.0 for _ in closes]
    for values in (unsorted_ts, unsorted_close, unsorted_open, unsorted_high, unsorted_low, unsorted_volume):
        latest = values.pop()
        values.insert(0, latest)
    unsorted_series = MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=unsorted_ts,
        open=unsorted_open,
        high=unsorted_high,
        low=unsorted_low,
        close=unsorted_close,
        volume=unsorted_volume,
    )

    sorted_result = engine.detect(sorted_series)
    unsorted_result = engine.detect(unsorted_series)

    assert unsorted_result.state == sorted_result.state
    assert unsorted_result.ma20 == sorted_result.ma20
    assert unsorted_result.ma60 == sorted_result.ma60
    assert unsorted_result.atr_pct == sorted_result.atr_pct
    assert unsorted_result.multiplier == sorted_result.multiplier
