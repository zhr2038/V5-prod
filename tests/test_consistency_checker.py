from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import scripts.consistency_checker as consistency_checker


def test_consistency_checker_build_paths_anchor_to_workspace(tmp_path) -> None:
    paths = consistency_checker.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == (tmp_path / "reports").resolve()


def test_consistency_checker_reads_and_writes_under_workspace_reports(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    cost_dir = reports_dir / "cost_stats_real"
    cost_dir.mkdir(parents=True, exist_ok=True)
    latest_cost = cost_dir / "latest.json"
    latest_cost.write_text(json.dumps({"avg_cost_bps": 12.5}), encoding="utf-8")

    checker = consistency_checker.BacktestLiveConsistencyChecker(workspace=tmp_path)

    assert checker.load_backtest_config()["avg_cost_bps"] == 12.5

    checker.generate_report()

    reports = list(reports_dir.glob("consistency_check_*.json"))
    assert len(reports) == 1
    payload = json.loads(reports[0].read_text(encoding="utf-8"))
    assert payload["results"] == checker.results


def test_consistency_checker_load_live_trades_uses_updated_ts_for_recent_fills(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    db_path = reports_dir / "orders.sqlite"
    now_ms = int(datetime.now().timestamp() * 1000)
    stale_created_ts = int((datetime.now() - timedelta(days=10)).timestamp() * 1000)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            px REAL,
            avg_px REAL,
            sz REAL,
            acc_fill_sz REAL,
            fee REAL,
            state TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(inst_id, side, px, avg_px, sz, acc_fill_sz, fee, state, created_ts, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC-USDT", "buy", 100.0, 101.0, 1.0, 1.0, 0.1, "FILLED", stale_created_ts, now_ms),
    )
    conn.commit()
    conn.close()

    checker = consistency_checker.BacktestLiveConsistencyChecker(workspace=tmp_path)
    trades = checker.load_live_trades(days=7)

    assert len(trades) == 1
    assert trades[0]["symbol"] == "BTC-USDT"
    assert int(trades[0]["ts"].timestamp() * 1000) == now_ms


def test_consistency_checker_fill_rate_counts_use_updated_ts_for_recent_events(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    db_path = reports_dir / "orders.sqlite"
    now_ms = int(datetime.now().timestamp() * 1000)
    stale_created_ts = int((datetime.now() - timedelta(days=10)).timestamp() * 1000)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE orders (
            state TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO orders(state, created_ts, updated_ts)
        VALUES (?, ?, ?)
        """,
        [
            ("FILLED", stale_created_ts, now_ms),
            ("REJECTED", stale_created_ts, now_ms),
        ],
    )
    conn.commit()
    conn.close()

    checker = consistency_checker.BacktestLiveConsistencyChecker(workspace=tmp_path)
    states = checker._load_order_state_counts(days=7)

    assert states == {"FILLED": 1, "REJECTED": 1}
