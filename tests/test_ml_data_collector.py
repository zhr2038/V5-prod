from __future__ import annotations

import pandas as pd
import sqlite3
from pathlib import Path

from src.execution import ml_data_collector as collector_mod


def test_ml_data_collector_resolves_relative_db_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(collector_mod, "PROJECT_ROOT", tmp_path)

    collector = collector_mod.MLDataCollector(db_path="reports/ml_training_data.db")

    assert collector.db_path == str((tmp_path / "reports" / "ml_training_data.db").resolve())
    collector._close_connection()


def test_export_training_data_resolves_relative_output_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(collector_mod, "PROJECT_ROOT", tmp_path)
    collector = collector_mod.MLDataCollector(db_path="reports/ml_training_data.db")

    conn = sqlite3.connect(collector.db_path)
    try:
        conn.execute(
            """
            INSERT INTO feature_snapshots (
                timestamp, symbol,
                returns_1h, returns_6h, returns_24h,
                momentum_5d, momentum_20d,
                volatility_6h, volatility_24h, volatility_ratio,
                volume_ratio, obv, rsi, macd, macd_signal,
                bb_position, price_position, regime,
                future_return_6h, future_return_12h, future_return_24h,
                label_filled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1_700_000_000_000,
                "BTC/USDT",
                0.1,
                0.2,
                0.3,
                1.0,
                2.0,
                0.1,
                0.2,
                0.5,
                1.1,
                2.2,
                50.0,
                0.3,
                0.2,
                0.6,
                0.7,
                "TRENDING",
                0.01,
                0.02,
                0.03,
                1,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    assert collector.export_training_data("reports/ml_training_data.csv", min_samples=1) is True
    assert (tmp_path / "reports" / "ml_training_data.csv").exists()
    collector._close_connection()


def test_ml_data_collector_cache_ohlcv_prefers_logically_newer_file_for_duplicate_timestamp(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    (cache_dir / "BTC_USDT_1H_20260101.csv").write_text(
        "\n".join(
            [
                "timestamp,open,high,low,close,volume",
                "2026-01-01 00:00:00,100,101,99,100,10",
                "2026-01-01 01:00:00,101,102,100,101,11",
            ]
        ),
        encoding="utf-8",
    )
    (cache_dir / "BTC_USDT_1H_2026-01-01_2026-01-02.csv").write_text(
        "\n".join(
            [
                "timestamp,open,high,low,close,volume",
                "2026-01-01 01:00:00,101,102,100,999,11",
                "2026-01-01 02:00:00,102,103,101,103,12",
            ]
        ),
        encoding="utf-8",
    )

    ohlcv = collector_mod.MLDataCollector._load_cache_ohlcv(cache_dir, "BTC/USDT")

    assert list(ohlcv["timestamp_ms"]) == [
        collector_mod.MLDataCollector._parse_cache_timestamp_ms(pd.Series(["2026-01-01 00:00:00"])).iloc[0],
        collector_mod.MLDataCollector._parse_cache_timestamp_ms(pd.Series(["2026-01-01 01:00:00"])).iloc[0],
        collector_mod.MLDataCollector._parse_cache_timestamp_ms(pd.Series(["2026-01-01 02:00:00"])).iloc[0],
    ]
    assert list(ohlcv["close"]) == [100.0, 999.0, 103.0]


def test_align_export_cycles_keeps_latest_duplicate_row_for_same_hour_and_symbol() -> None:
    df = pd.DataFrame(
        [
            {"timestamp": 3_600_000 + 1, "symbol": "BTC/USDT", "score": 1.0},
            {"timestamp": 3_600_000 + 2, "symbol": "BTC/USDT", "score": 2.0},
            {"timestamp": 3_600_000 + 3, "symbol": "ETH/USDT", "score": 3.0},
        ]
    )

    out, meta = collector_mod.MLDataCollector._align_export_cycles(df)

    assert meta["duplicates_removed"] == 1
    assert len(out) == 2
    btc_row = out.loc[out["symbol"] == "BTC/USDT"].iloc[0]
    assert btc_row["timestamp"] == 3_600_000
    assert btc_row["score"] == 2.0


def test_compute_future_return_from_candles_sorts_unsorted_rows_before_searchsorted() -> None:
    candles = pd.DataFrame(
        [
            {"timestamp_ms": 1_710_021_600_000, "close": 110.0},  # +6h
            {"timestamp_ms": 1_710_000_000_000, "close": 100.0},  # start
            {"timestamp_ms": 1_710_043_200_000, "close": 120.0},  # +12h
        ]
    )

    future_return = collector_mod.MLDataCollector._compute_future_return_from_candles(
        candles,
        start_timestamp=1_710_000_000_000,
        hours=6,
    )

    assert future_return == 0.10
