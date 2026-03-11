from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from scripts.backfill_ml_training_db import backfill_from_csv
from src.execution.ml_data_collector import MLDataCollector


def _sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": [1000, 1000, 2000],
            "symbol": ["BTC/USDT", "ETH/USDT", "BTC/USDT"],
            "returns_1h": [0.1, 0.2, 0.3],
            "returns_6h": [0.2, 0.1, 0.4],
            "returns_24h": [0.5, 0.6, 0.7],
            "momentum_5d": [1.0, 1.1, 1.2],
            "momentum_20d": [2.0, 2.1, 2.2],
            "volatility_6h": [0.01, 0.02, 0.03],
            "volatility_24h": [0.04, 0.05, 0.06],
            "volatility_ratio": [0.2, 0.3, 0.4],
            "volume_ratio": [1.5, 1.6, 1.7],
            "obv": [10.0, 11.0, 12.0],
            "rsi": [45.0, 55.0, 65.0],
            "macd": [0.1, 0.2, 0.3],
            "macd_signal": [0.05, 0.06, 0.07],
            "bb_position": [0.1, 0.2, 0.3],
            "price_position": [0.5, 0.6, 0.7],
            "regime": ["Risk-Off", "Risk-Off", "Trending"],
            "future_return_6h": [0.01, -0.02, 0.03],
            "future_return_12h": [0.02, -0.01, 0.04],
            "future_return_24h": [0.03, 0.00, 0.05],
        }
    )


def test_backfill_from_csv_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "ml_training_data.db"
    MLDataCollector(db_path=str(db_path))
    df = _sample_frame()

    conn = sqlite3.connect(str(db_path))
    try:
        first = backfill_from_csv(conn, df)
        second = backfill_from_csv(conn, df)
        total = int(conn.execute("SELECT COUNT(*) FROM feature_snapshots").fetchone()[0])
        labeled = int(conn.execute("SELECT COUNT(*) FROM feature_snapshots WHERE label_filled = 1").fetchone()[0])
    finally:
        conn.close()

    assert first == {"inserted": 3, "updated": 0}
    assert second == {"inserted": 0, "updated": 3}
    assert total == 3
    assert labeled == 3


def test_backfill_from_csv_relabels_existing_pending_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "ml_training_data.db"
    collector = MLDataCollector(db_path=str(db_path))
    df = _sample_frame()

    collector.collect_features(
        timestamp=1000,
        symbol="BTC/USDT",
        market_data={
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0,
                      110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 117.0, 118.0, 119.0,
                      120.0, 121.0, 122.0, 123.0, 124.0, 125.0, 126.0, 127.0, 128.0, 129.0],
            "high": [100.0 + i for i in range(30)],
            "low": [99.0 + i for i in range(30)],
            "volume": [10.0 + i for i in range(30)],
        },
        regime="Risk-Off",
    )

    conn = sqlite3.connect(str(db_path))
    try:
        result = backfill_from_csv(conn, df)
        row = conn.execute(
            "SELECT future_return_6h, future_return_12h, future_return_24h, label_filled "
            "FROM feature_snapshots WHERE timestamp = 1000 AND symbol = 'BTC/USDT'"
        ).fetchone()
    finally:
        conn.close()

    assert result["updated"] >= 1
    assert row == (0.01, 0.02, 0.03, 1)


def test_collect_features_is_idempotent_for_same_timestamp_symbol(tmp_path: Path) -> None:
    db_path = tmp_path / "ml_training_data.db"
    collector = MLDataCollector(db_path=str(db_path))
    market_data = {
        "close": [100.0 + i for i in range(30)],
        "high": [101.0 + i for i in range(30)],
        "low": [99.0 + i for i in range(30)],
        "volume": [10.0 + i for i in range(30)],
    }

    assert collector.collect_features(timestamp=1000, symbol="BTC/USDT", market_data=market_data, regime="SIDEWAYS")
    assert collector.collect_features(timestamp=1000, symbol="BTC/USDT", market_data=market_data, regime="TRENDING")

    conn = sqlite3.connect(str(db_path))
    try:
        count = int(
            conn.execute(
                "SELECT COUNT(*) FROM feature_snapshots WHERE timestamp = 1000 AND symbol = 'BTC/USDT'"
            ).fetchone()[0]
        )
        regime = conn.execute(
            "SELECT regime FROM feature_snapshots WHERE timestamp = 1000 AND symbol = 'BTC/USDT'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert count == 1
    assert regime == "TRENDING"


def test_export_training_data_dedupes_legacy_same_hour_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "ml_training_data.db"
    collector = MLDataCollector(db_path=str(db_path))
    hour_ms = 3600 * 1000
    base_ts = 1_700_000_000_000

    conn = sqlite3.connect(str(db_path))
    try:
        conn.executemany(
            """
            INSERT INTO feature_snapshots (
                timestamp, symbol, returns_1h, returns_6h, returns_24h,
                momentum_5d, momentum_20d, volatility_6h, volatility_24h,
                volatility_ratio, volume_ratio, obv, rsi, macd, macd_signal,
                bb_position, price_position, regime,
                future_return_6h, future_return_12h, future_return_24h, label_filled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            [
                (
                    base_ts + 5 * 60 * 1000,
                    "BTC/USDT",
                    0.1,
                    0.2,
                    0.3,
                    1.0,
                    2.0,
                    0.01,
                    0.02,
                    0.5,
                    1.5,
                    10.0,
                    50.0,
                    0.1,
                    0.05,
                    0.2,
                    0.6,
                    "SIDEWAYS",
                    0.01,
                    0.02,
                    0.03,
                ),
                (
                    base_ts + 35 * 60 * 1000,
                    "BTC/USDT",
                    0.11,
                    0.21,
                    0.31,
                    1.1,
                    2.1,
                    0.011,
                    0.021,
                    0.51,
                    1.6,
                    11.0,
                    51.0,
                    0.11,
                    0.051,
                    0.21,
                    0.61,
                    "TRENDING",
                    0.011,
                    0.021,
                    0.031,
                ),
                (
                    base_ts + hour_ms + 5 * 60 * 1000,
                    "BTC/USDT",
                    0.12,
                    0.22,
                    0.32,
                    1.2,
                    2.2,
                    0.012,
                    0.022,
                    0.52,
                    1.7,
                    12.0,
                    52.0,
                    0.12,
                    0.052,
                    0.22,
                    0.62,
                    "TRENDING",
                    0.012,
                    0.022,
                    0.032,
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    output_path = tmp_path / "ml_training_data.csv"
    assert collector.export_training_data(str(output_path), min_samples=2) is True

    df = pd.read_csv(output_path)
    assert len(df) == 2
    assert df["timestamp"].tolist() == [
        (base_ts // hour_ms) * hour_ms,
        ((base_ts + hour_ms) // hour_ms) * hour_ms,
    ]
    assert df.iloc[0]["regime"] == "TRENDING"


def test_fill_labels_waits_for_24h_before_marking_row_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "ml_training_data.db"
    collector = MLDataCollector(db_path=str(db_path))
    ts = 1_700_000_000_000
    collector.collect_features(
        timestamp=ts,
        symbol="BTC/USDT",
        market_data={
            "close": [100.0 + i for i in range(30)],
            "high": [101.0 + i for i in range(30)],
            "low": [99.0 + i for i in range(30)],
            "volume": [10.0 + i for i in range(30)],
        },
        regime="Risk-Off",
    )

    def fake_future_return(symbol: str, start_timestamp: int, hours: int) -> float:
        assert symbol == "BTC/USDT"
        assert start_timestamp == ts
        return {6: 0.06, 12: 0.12, 24: 0.24}[hours]

    monkeypatch.setattr(collector, "_calculate_future_return", fake_future_return)

    assert collector.fill_labels(ts + 13 * 3600 * 1000) == 0

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT future_return_6h, future_return_12h, future_return_24h, label_filled "
            "FROM feature_snapshots WHERE timestamp = ? AND symbol = 'BTC/USDT'",
            (ts,),
        ).fetchone()
    finally:
        conn.close()

    assert row == (0.06, 0.12, None, 0)

    assert collector.fill_labels(ts + 25 * 3600 * 1000) == 1

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT future_return_6h, future_return_12h, future_return_24h, label_filled "
            "FROM feature_snapshots WHERE timestamp = ? AND symbol = 'BTC/USDT'",
            (ts,),
        ).fetchone()
    finally:
        conn.close()

    assert row == (0.06, 0.12, 0.24, 1)


def test_fetch_future_return_from_cache_merges_overlapping_cache_files(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    db_path = reports_dir / "ml_training_data.db"
    collector = MLDataCollector(db_path=str(db_path))

    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True)
    ts = 1_700_000_000_000

    pd.DataFrame(
        {
            "timestamp": [
                pd.to_datetime(ts, unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
                pd.to_datetime(ts + 6 * 3600 * 1000, unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
                pd.to_datetime(ts + 12 * 3600 * 1000, unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
            ],
            "close": [100.0, 106.0, 112.0],
        }
    ).to_csv(cache_dir / "BTC_USDT_1H_2026-02-01_2026-02-01.csv", index=False)
    pd.DataFrame(
        {
            "timestamp": [
                pd.to_datetime(ts + 12 * 3600 * 1000, unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
                pd.to_datetime(ts + 24 * 3600 * 1000, unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
            ],
            "close": [112.0, 124.0],
        }
    ).to_csv(cache_dir / "BTC_USDT_1H_2026-02-01_2026-02-02.csv", index=False)

    out = collector._fetch_future_return_from_cache("BTC/USDT", ts, 24)

    assert out == pytest.approx(0.24)


def test_backfill_feature_snapshots_from_cache_exports_labeled_rows(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    db_path = reports_dir / "ml_training_data.db"
    csv_path = reports_dir / "ml_training_data.csv"
    collector = MLDataCollector(db_path=str(db_path))

    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True)
    base_ts = 1_740_614_400_000
    bars = 40
    frame = pd.DataFrame(
        {
            "timestamp": [
                pd.to_datetime(base_ts + idx * 3600 * 1000, unit="ms").strftime("%Y-%m-%d %H:%M:%S")
                for idx in range(bars)
            ],
            "open": [100.0 + idx for idx in range(bars)],
            "high": [101.0 + idx for idx in range(bars)],
            "low": [99.0 + idx for idx in range(bars)],
            "close": [100.5 + idx for idx in range(bars)],
            "volume": [10.0 + idx for idx in range(bars)],
        }
    )
    frame.to_csv(cache_dir / "BTC_USDT_1H_2026-02-27_2026-02-28.csv", index=False)

    start_ts = base_ts + 10 * 3600 * 1000
    end_ts = base_ts + 11 * 3600 * 1000

    stats = collector.backfill_feature_snapshots_from_cache(
        symbols=["BTC/USDT"],
        start_timestamp=start_ts,
        end_timestamp=end_ts,
        lookback_bars=24,
        overwrite_existing=False,
        regime="SIDEWAYS",
    )
    filled = collector.fill_labels(base_ts + 39 * 3600 * 1000)
    exported = collector.export_training_data(str(csv_path), min_samples=1)

    assert stats["symbols_loaded"] == 1
    assert stats["inserted"] == 2
    assert stats["updated"] == 0
    assert stats["failed"] == 0
    assert stats["missing_cache_symbols"] == []
    assert filled == 2
    assert exported is True

    df = pd.read_csv(csv_path)
    assert len(df) == 2
    assert df["symbol"].tolist() == ["BTC/USDT", "BTC/USDT"]
    assert df["timestamp"].tolist() == [start_ts, end_ts]
    assert df["future_return_24h"].notna().all()


def test_fill_all_labels_runs_multiple_batches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "ml_training_data.db"
    collector = MLDataCollector(db_path=str(db_path))
    conn = sqlite3.connect(str(db_path))
    try:
        rows = []
        for idx in range(1005):
            ts = 1_700_000_000_000 + idx * 3600 * 1000
            rows.append(
                (
                    ts,
                    "BTC/USDT",
                    0.1,
                    0.2,
                    0.3,
                    1.0,
                    2.0,
                    0.01,
                    0.02,
                    0.5,
                    1.5,
                    10.0,
                    50.0,
                    0.1,
                    0.05,
                    0.2,
                    0.6,
                    "SIDEWAYS",
                )
            )
        conn.executemany(
            """
            INSERT INTO feature_snapshots (
                timestamp, symbol, returns_1h, returns_6h, returns_24h,
                momentum_5d, momentum_20d, volatility_6h, volatility_24h,
                volatility_ratio, volume_ratio, obv, rsi, macd, macd_signal,
                bb_position, price_position, regime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(collector, "_calculate_future_return", lambda symbol, start_timestamp, hours: {6: 0.06, 12: 0.12, 24: 0.24}[hours])

    result = collector.fill_all_labels(1_700_000_000_000 + 2000 * 3600 * 1000, max_batches=5)

    conn = sqlite3.connect(str(db_path))
    try:
        labeled = int(conn.execute("SELECT COUNT(*) FROM feature_snapshots WHERE label_filled = 1").fetchone()[0])
    finally:
        conn.close()

    assert result == {"filled": 1005, "batches": 2}
    assert labeled == 1005
