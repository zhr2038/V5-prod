from __future__ import annotations

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
