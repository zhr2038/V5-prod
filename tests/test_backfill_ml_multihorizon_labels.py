from __future__ import annotations

from pathlib import Path

import pandas as pd

import scripts.backfill_ml_multihorizon_labels as multihorizon


def test_load_cache_candles_prefers_logically_newer_file_for_duplicate_timestamp(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    (cache_dir / "BTC_USDT_1H_20260101.csv").write_text(
        "\n".join(
            [
                "timestamp,close",
                "2026-01-01 00:00:00,100",
                "2026-01-01 01:00:00,101",
            ]
        ),
        encoding="utf-8",
    )
    (cache_dir / "BTC_USDT_1H_2026-01-01_2026-01-02.csv").write_text(
        "\n".join(
            [
                "timestamp,close",
                "2026-01-01 01:00:00,999",
                "2026-01-01 02:00:00,103",
            ]
        ),
        encoding="utf-8",
    )

    candles = multihorizon.load_cache_candles(cache_dir, "BTC/USDT")

    assert list(candles["close"]) == [100.0, 999.0, 103.0]


def test_compute_future_returns_sorts_unsorted_candles_before_searchsorted() -> None:
    rows = pd.DataFrame(
        [
            {
                "id": 1,
                "timestamp": 1_710_000_000_000,
            }
        ]
    )
    candles = pd.DataFrame(
        [
            {"timestamp_ms": 1_710_086_400_000, "close": 130.0},  # +24h
            {"timestamp_ms": 1_710_021_600_000, "close": 110.0},  # +6h
            {"timestamp_ms": 1_710_000_000_000, "close": 100.0},  # start
            {"timestamp_ms": 1_710_043_200_000, "close": 120.0},  # +12h
        ]
    )

    out = multihorizon.compute_future_returns(rows, candles)

    assert out.loc[0, "future_return_6h"] == 0.10
    assert out.loc[0, "future_return_12h"] == 0.20
    assert out.loc[0, "future_return_24h"] == 0.30
