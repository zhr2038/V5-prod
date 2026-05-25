from __future__ import annotations

import sqlite3
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


def test_load_pending_rows_binds_symbol_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "ml.sqlite"
    as_of_ms = 1_710_086_400_000
    mature_ts_ms = as_of_ms - 25 * multihorizon.ONE_HOUR_MS
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE feature_snapshots (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                label_filled INTEGER DEFAULT 0,
                future_return_6h REAL,
                future_return_12h REAL,
                future_return_24h REAL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO feature_snapshots (
                id, timestamp, symbol, label_filled, future_return_6h, future_return_12h, future_return_24h
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, mature_ts_ms, "BTC/USDT", 0, None, None, None),
                (2, mature_ts_ms, "ETH/USDT", 0, None, None, None),
            ],
        )

        rows = multihorizon._load_pending_rows(conn, as_of_ms=as_of_ms, symbol="BTC/USDT' OR 1=1 --")
        assert rows.empty

        rows = multihorizon._load_pending_rows(conn, as_of_ms=as_of_ms, symbol="BTC/USDT")
        assert rows["id"].tolist() == [1]
