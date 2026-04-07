from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from src.monitoring.smart_alert import SmartAlertEngine


def _write_audit(run_dir: Path, *, regime: str, orders_rebalance: int) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": regime,
                "counts": {
                    "selected": 1,
                    "orders_rebalance": orders_rebalance,
                    "orders_exit": 0,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _seed_good_market_runs(workspace: Path) -> None:
    runs_dir = workspace / "reports" / "runs"
    for idx in range(6):
        run_dir = runs_dir / f"20260406_{idx:02d}"
        _write_audit(run_dir, regime="TRENDING", orders_rebalance=1)


def test_check_no_buy_in_market_alerts_when_only_sell_rebalances_exist(tmp_path: Path) -> None:
    _seed_good_market_runs(tmp_path)

    engine = SmartAlertEngine(workspace=tmp_path)
    alert = engine.check_no_buy_in_market()

    assert alert is not None
    assert alert["type"] == "no_buy_in_market"
    assert alert["level"] == "medium"


def test_check_no_buy_in_market_uses_recent_buy_fill_to_suppress_alert(tmp_path: Path) -> None:
    _seed_good_market_runs(tmp_path)

    fills_db = tmp_path / "reports" / "fills.sqlite"
    conn = sqlite3.connect(str(fills_db))
    conn.execute("CREATE TABLE fills (side TEXT, ts_ms INTEGER)")
    conn.execute(
        "INSERT INTO fills(side, ts_ms) VALUES (?, ?)",
        ("buy", int(datetime.now().timestamp() * 1000)),
    )
    conn.commit()
    conn.close()

    engine = SmartAlertEngine(workspace=tmp_path)
    alert = engine.check_no_buy_in_market()

    assert alert is None


def test_check_no_buy_in_market_falls_back_to_order_updated_ts(tmp_path: Path) -> None:
    _seed_good_market_runs(tmp_path)

    orders_db = tmp_path / "reports" / "orders.sqlite"
    conn = sqlite3.connect(str(orders_db))
    conn.execute(
        """
        CREATE TABLE orders (
            side TEXT,
            state TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    now_ts = int(datetime.now().timestamp() * 1000)
    conn.execute(
        "INSERT INTO orders(side, state, created_ts, updated_ts) VALUES (?, ?, ?, ?)",
        ("buy", "FILLED", 1_000, now_ts),
    )
    conn.commit()
    conn.close()

    engine = SmartAlertEngine(workspace=tmp_path)
    alert = engine.check_no_buy_in_market()

    assert alert is None


def test_check_no_buy_in_market_uses_orders_when_fill_store_lags(tmp_path: Path) -> None:
    _seed_good_market_runs(tmp_path)

    fills_db = tmp_path / "reports" / "fills.sqlite"
    conn = sqlite3.connect(str(fills_db))
    conn.execute("CREATE TABLE fills (side TEXT, ts_ms INTEGER)")
    conn.execute(
        "INSERT INTO fills(side, ts_ms) VALUES (?, ?)",
        ("buy", 1_000),
    )
    conn.commit()
    conn.close()

    orders_db = tmp_path / "reports" / "orders.sqlite"
    conn = sqlite3.connect(str(orders_db))
    conn.execute(
        """
        CREATE TABLE orders (
            side TEXT,
            state TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    now_ts = int(datetime.now().timestamp() * 1000)
    conn.execute(
        "INSERT INTO orders(side, state, created_ts, updated_ts) VALUES (?, ?, ?, ?)",
        ("buy", "FILLED", 1_000, now_ts),
    )
    conn.commit()
    conn.close()

    engine = SmartAlertEngine(workspace=tmp_path)
    alert = engine.check_no_buy_in_market()

    assert alert is None


def test_check_kill_switch_ignores_nested_disabled_kill_switch_dict(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "kill_switch.json").write_text(
        json.dumps({"kill_switch": {"enabled": False}}),
        encoding="utf-8",
    )

    engine = SmartAlertEngine(workspace=tmp_path)

    assert engine.check_kill_switch() is None


def test_check_kill_switch_accepts_nested_enabled_kill_switch_dict(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "kill_switch.json").write_text(
        json.dumps({"kill_switch": {"enabled": True}}),
        encoding="utf-8",
    )

    engine = SmartAlertEngine(workspace=tmp_path)
    alert = engine.check_kill_switch()

    assert alert is not None
    assert alert["type"] == "kill_switch"
    assert alert["level"] == "critical"
