from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import scripts.consistency_checker as consistency_checker


def test_consistency_checker_build_paths_anchor_to_workspace(tmp_path) -> None:
    paths = consistency_checker.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == (tmp_path / "reports").resolve()
    assert paths.orders_db == (tmp_path / "reports" / "orders.sqlite").resolve()


def test_consistency_checker_uses_active_runtime_orders_and_reports_dir(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    configs_dir = tmp_path / "configs"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)
    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
                "",
            ]
        ),
        encoding="utf-8",
    )

    root_orders = reports_dir / "orders.sqlite"
    runtime_orders = runtime_dir / "orders.sqlite"
    now_ms = int(datetime.now().timestamp() * 1000)

    for db_path, inst_id in ((root_orders, "ROOT-USDT"), (runtime_orders, "RUNTIME-USDT")):
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
            (inst_id, "buy", 100.0, 101.0, 1.0, 1.0, 0.1, "FILLED", now_ms, now_ms),
        )
        conn.commit()
        conn.close()

    (reports_dir / "cost_stats_real").mkdir(parents=True, exist_ok=True)
    (runtime_dir / "cost_stats_real").mkdir(parents=True, exist_ok=True)
    (reports_dir / "cost_stats_real" / "root.json").write_text(json.dumps({"avg_cost_bps": 99.0}), encoding="utf-8")
    (runtime_dir / "cost_stats_real" / "runtime.json").write_text(json.dumps({"avg_cost_bps": 12.5}), encoding="utf-8")

    checker = consistency_checker.BacktestLiveConsistencyChecker(workspace=tmp_path)

    assert checker.paths.reports_dir == runtime_dir.resolve()
    assert checker.paths.orders_db == runtime_orders.resolve()
    trades = checker.load_live_trades(days=7)
    assert [trade["symbol"] for trade in trades] == ["RUNTIME-USDT"]
    assert checker.load_backtest_config()["avg_cost_bps"] == 12.5

    checker.generate_report()

    assert not list(reports_dir.glob("consistency_check_*.json"))
    runtime_reports = list(runtime_dir.glob("consistency_check_*.json"))
    assert len(runtime_reports) == 1


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


def test_consistency_checker_load_live_trades_converts_json_fee_map_to_usdt_cost(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    db_path = reports_dir / "orders.sqlite"
    now_ms = int(datetime.now().timestamp() * 1000)

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
            fee TEXT,
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
        ("BTC-USDT", "buy", 100.0, 100.0, 1.0, 1.0, '{"BTC":"-0.001"}', "FILLED", now_ms, now_ms),
    )
    conn.commit()
    conn.close()

    checker = consistency_checker.BacktestLiveConsistencyChecker(workspace=tmp_path)
    trades = checker.load_live_trades(days=7)

    assert len(trades) == 1
    assert trades[0]["fee_usdt"] == 0.1


def test_consistency_checker_compare_cost_models_uses_positive_fee_cost(tmp_path) -> None:
    checker = consistency_checker.BacktestLiveConsistencyChecker(workspace=tmp_path)
    live_trades = [
        {
            "symbol": "BTC-USDT",
            "side": "buy",
            "order_px": 100.0,
            "fill_px": 100.0,
            "order_sz": 1.0,
            "fill_sz": 1.0,
            "fee_usdt": 0.1,
            "ts": datetime.now(),
        }
    ]

    checker.compare_cost_models(live_trades, {"avg_cost_bps": 10.0})

    assert checker.results["recommendations"] == []
