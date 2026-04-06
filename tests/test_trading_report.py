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
    assert paths.fills_db == tmp_path / "reports" / "fills.sqlite"


def test_load_trade_data_prefers_fill_timestamps_over_order_created_ts(tmp_path: Path) -> None:
    paths = trading_report.build_paths(tmp_path)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)

    now_ms = int(datetime.now().timestamp() * 1000)
    stale_order_ms = now_ms - 8 * 24 * 3600 * 1000
    recent_fill_ms = now_ms - 2 * 3600 * 1000

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
        ("BTC-USDT", "buy", "FILLED", 100.0, "-0.2", "50000", stale_order_ms),
    )
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(paths.fills_db))
    conn.execute(
        """
        CREATE TABLE fills (
            inst_id TEXT,
            trade_id TEXT,
            ts_ms INTEGER,
            side TEXT,
            fill_notional TEXT,
            fill_px TEXT,
            fill_sz TEXT,
            fee TEXT,
            fee_ccy TEXT,
            created_ts_ms INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO fills(inst_id, trade_id, ts_ms, side, fill_notional, fill_px, fill_sz, fee, fee_ccy, created_ts_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC-USDT", "trade-1", recent_fill_ms, "buy", "120.0", "60000", "0.002", "-0.1", "USDT", recent_fill_ms),
    )
    conn.commit()
    conn.close()

    trades = trading_report.TradingReportGenerator(paths=paths).load_trade_data(days=7)

    assert len(trades) == 1
    assert trades[0]["symbol"] == "BTC"
    assert trades[0]["notional"] == 120.0
    assert int(trades[0]["ts"].timestamp() * 1000) == recent_fill_ms


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


def test_load_trade_data_falls_back_to_order_updated_ts(tmp_path: Path) -> None:
    paths = trading_report.build_paths(tmp_path)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)

    now_ms = int(datetime.now().timestamp() * 1000)
    stale_created_ms = now_ms - 8 * 24 * 3600 * 1000
    recent_updated_ms = now_ms - 30 * 60 * 1000

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
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(inst_id, side, state, notional_usdt, fee, avg_px, created_ts, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("ETH-USDT", "sell", "FILLED", 80.0, "-0.1", "4000", stale_created_ms, recent_updated_ms),
    )
    conn.commit()
    conn.close()

    trades = trading_report.TradingReportGenerator(paths=paths).load_trade_data(days=7)

    assert len(trades) == 1
    assert trades[0]["symbol"] == "ETH"
    assert int(trades[0]["ts"].timestamp() * 1000) == recent_updated_ms


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
