from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import scripts.trading_report as trading_report


def test_build_paths_anchors_reports_to_workspace(tmp_path: Path) -> None:
    paths = trading_report.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == tmp_path / "reports"
    assert paths.runs_dir == tmp_path / "reports" / "runs"
    assert paths.orders_db == tmp_path / "reports" / "orders.sqlite"


def test_load_trade_data_converts_json_fee_map_to_signed_usdt(tmp_path: Path) -> None:
    paths = trading_report.build_paths(tmp_path)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(paths.orders_db))
    conn.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            state TEXT,
            notional_usdt REAL,
            fee TEXT,
            avg_px TEXT,
            created_ts INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(inst_id, side, state, notional_usdt, fee, avg_px, created_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "BTC-USDT",
            "buy",
            "FILLED",
            100.0,
            '{"BTC":"-0.001"}',
            "50000",
            int(datetime.now().timestamp() * 1000),
        ),
    )
    conn.commit()
    conn.close()

    trades = trading_report.TradingReportGenerator(paths=paths).load_trade_data(days=7)

    assert len(trades) == 1
    assert trades[0]["symbol"] == "BTC"
    assert trades[0]["fee"] == -50.0


def test_generate_daily_report_uses_converted_fee_values(tmp_path: Path) -> None:
    paths = trading_report.build_paths(tmp_path)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(paths.orders_db))
    conn.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            state TEXT,
            notional_usdt REAL,
            fee TEXT,
            avg_px TEXT,
            created_ts INTEGER
        )
        """
    )
    rows = [
        ("BTC-USDT", "buy", "FILLED", 100.0, '{"BTC":"-0.001"}', "50000"),
        ("BTC-USDT", "sell", "FILLED", 120.0, "-0.2", "50000"),
    ]
    now_ms = int(datetime.now().timestamp() * 1000)
    conn.executemany(
        """
        INSERT INTO orders(inst_id, side, state, notional_usdt, fee, avg_px, created_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [(*row, now_ms) for row in rows],
    )
    conn.commit()
    conn.close()

    generator = trading_report.TradingReportGenerator(paths=paths)
    messages: list[str] = []
    generator.log = lambda message="": messages.append(message)

    generator.generate_daily_report()

    assert any("手续费: $-50.2000" in message for message in messages)
