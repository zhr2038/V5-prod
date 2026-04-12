from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.execution import reflection_agent as reflection_module
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




def test_reflection_agent_runtime_db_derivation_does_not_rewrite_parent_directory_names(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "orders_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    orders_db = runtime_dir / "shadow_orders.sqlite"
    fills_db = runtime_dir / "shadow_fills.sqlite"
    positions_db = runtime_dir / "shadow_positions.sqlite"

    agent = ReflectionAgentV2(
        db_path=str(orders_db),
        report_dir=str(tmp_path / "reflection"),
        bills_db=str(tmp_path / "bills.sqlite"),
    )

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
    conn.execute(
        "INSERT INTO fills(ts_ms, ord_id, cl_ord_id, slippage_bps, fee, notional_usdt) VALUES (?, ?, ?, ?, ?, ?)",
        (2_000, "ord-1", "ord-1", 7.0, 0.2, 100.0),
    )
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(positions_db))
    conn.execute("CREATE TABLE positions (value_usdt REAL)")
    conn.execute("INSERT INTO positions(value_usdt) VALUES (60.0)")
    conn.execute("INSERT INTO positions(value_usdt) VALUES (40.0)")
    conn.commit()
    conn.close()

    trades = pd.DataFrame([{"inst_id": "BTC-USDT", "side": "buy", "notional_usdt": 100.0, "event_ts": 2_000}])

    quality = agent._analyze_execution_quality(trades)
    risk = agent._analyze_risk(trades)

    assert quality.avg_slippage_bps == 7.0
    assert quality.avg_fee_bps == 20.0
    assert risk.max_position_pct == 0.6
    assert risk.concentration_score == 0.52

def test_reflection_agent_uses_active_runtime_paths_for_defaults(monkeypatch, tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    (reports_dir / "ic_diagnostics_30d_20u.json").write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "f1_mom_5d": {"mean": -0.05},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (runtime_dir / "ic_diagnostics_30d_20u.json").write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "f1_mom_5d": {"mean": 0.06},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(reflection_module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(reflection_module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        reflection_module,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(
        reflection_module,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    agent = reflection_module.ReflectionAgentV2()

    assert agent.db_path == str((runtime_dir / "orders.sqlite").resolve())
    assert agent.report_dir == (runtime_dir / "reflection").resolve()
    assert agent.ic_file == (runtime_dir / "ic_diagnostics_30d_20u.json").resolve()
    assert not (reports_dir / "reflection").exists()

    factors = agent._analyze_factor_effectiveness()
    assert factors[0].ic == 0.06
    assert factors[0].status == "effective"
