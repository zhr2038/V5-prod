from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.execution.reflection_agent import ReflectionAgentV2


def test_reflection_agent_load_recent_trades_uses_event_ts_and_fee_map(tmp_path: Path) -> None:
    db_path = tmp_path / "orders.sqlite"
    now_ms = int(datetime.now().timestamp() * 1000)
    stale_created_ts = int((datetime.now() - timedelta(days=10)).timestamp() * 1000)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            state TEXT,
            notional_usdt REAL,
            fee TEXT,
            acc_fill_sz REAL,
            avg_px REAL,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(inst_id, side, state, notional_usdt, fee, acc_fill_sz, avg_px, created_ts, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC-USDT", "buy", "FILLED", 100.0, '{"BTC":"-0.001"}', 5.0, 20.0, stale_created_ts, now_ms),
    )
    conn.commit()
    conn.close()

    agent = ReflectionAgentV2(
        db_path=str(db_path),
        report_dir=str(tmp_path / "reflection"),
        bills_db=str(tmp_path / "bills.sqlite"),
    )
    trades = agent._load_recent_trades(days=7)

    assert len(trades) == 1
    row = trades.iloc[0]
    assert int(row["event_ts"]) == now_ms
    assert float(row["fee_usdt"]) == 0.02
    assert float(row["fee"]) == 0.02


def test_reflection_agent_infers_fill_px_from_notional_and_fill_size_when_avg_px_missing(tmp_path: Path) -> None:
    agent = ReflectionAgentV2(
        db_path=str(tmp_path / "orders.sqlite"),
        report_dir=str(tmp_path / "reflection"),
        bills_db=str(tmp_path / "bills.sqlite"),
    )

    fee_usdt = agent._fee_cost_usdt_from_order_fee(
        "BTC-USDT",
        agent._infer_fill_px(None, 100.0, 5.0),
        '{"BTC":"-0.001"}',
    )

    assert fee_usdt == 0.02


def test_reflection_agent_pnl_attribution_prefers_event_ts_and_fee_usdt(tmp_path: Path) -> None:
    agent = ReflectionAgentV2(
        db_path=str(tmp_path / "orders.sqlite"),
        report_dir=str(tmp_path / "reflection"),
        bills_db=str(tmp_path / "bills.sqlite"),
    )

    trades = pd.DataFrame(
        [
            {
                "inst_id": "BTC-USDT",
                "side": "sell",
                "notional_usdt": 110.0,
                "created_ts": 100,
                "event_ts": 200,
                "fee_usdt": 1.0,
            },
            {
                "inst_id": "BTC-USDT",
                "side": "buy",
                "notional_usdt": 100.0,
                "created_ts": 200,
                "event_ts": 100,
                "fee_usdt": 1.0,
            },
        ]
    )

    attribution = agent._analyze_pnl_attribution(trades)

    assert attribution["total_realized_pnl"] == 8.0
    assert attribution["winning_symbols"] == 1
    assert attribution["losing_symbols"] == 0


def test_reflection_agent_execution_quality_filters_fills_to_trade_window(tmp_path: Path) -> None:
    agent = ReflectionAgentV2(
        db_path=str(tmp_path / "orders.sqlite"),
        report_dir=str(tmp_path / "reflection"),
        bills_db=str(tmp_path / "bills.sqlite"),
    )

    fills_db = tmp_path / "fills.sqlite"
    conn = sqlite3.connect(str(fills_db))
    conn.execute(
        """
        CREATE TABLE fills (
            ts_ms INTEGER,
            ord_id TEXT,
            cl_ord_id TEXT,
            slippage_bps REAL,
            fee REAL,
            notional_usdt REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO fills(ts_ms, ord_id, cl_ord_id, slippage_bps, fee, notional_usdt) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1_000, "old-1", "old-1", 99.0, 9.9, 100.0),
            (2_000, "new-1", "new-1", 5.0, 0.1, 100.0),
        ],
    )
    conn.commit()
    conn.close()

    trades = pd.DataFrame(
        [
            {
                "inst_id": "BTC-USDT",
                "side": "buy",
                "notional_usdt": 100.0,
                "event_ts": 2_000,
            }
        ]
    )

    quality = agent._analyze_execution_quality(trades)

    assert quality.avg_slippage_bps == 5.0
    assert quality.avg_fee_bps == 10.0


def test_reflection_agent_execution_quality_uses_unique_orders_for_fill_rate(tmp_path: Path) -> None:
    agent = ReflectionAgentV2(
        db_path=str(tmp_path / "orders.sqlite"),
        report_dir=str(tmp_path / "reflection"),
        bills_db=str(tmp_path / "bills.sqlite"),
    )

    fills_db = tmp_path / "fills.sqlite"
    conn = sqlite3.connect(str(fills_db))
    conn.execute(
        """
        CREATE TABLE fills (
            ts_ms INTEGER,
            ord_id TEXT,
            cl_ord_id TEXT,
            slippage_bps REAL,
            fee REAL,
            notional_usdt REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO fills(ts_ms, ord_id, cl_ord_id, slippage_bps, fee, notional_usdt) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (2_000, "ord-1", "cl-1", 5.0, 0.1, 50.0),
            (2_001, "ord-1", "cl-1", 6.0, 0.1, 50.0),
        ],
    )
    conn.commit()
    conn.close()

    trades = pd.DataFrame(
        [
            {
                "inst_id": "BTC-USDT",
                "side": "buy",
                "notional_usdt": 100.0,
                "event_ts": 2_001,
            }
        ]
    )

    quality = agent._analyze_execution_quality(trades)

    assert quality.fill_rate == 1.0
