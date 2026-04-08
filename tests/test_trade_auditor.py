from __future__ import annotations

import json
import sqlite3

import scripts.trade_auditor as trade_auditor


def test_build_paths_anchor_trade_auditor_to_repo_root(tmp_path) -> None:
    paths = trade_auditor.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == tmp_path / "reports"
    assert paths.runs_dir == tmp_path / "reports" / "runs"
    assert paths.orders_db == tmp_path / "reports" / "orders.sqlite"
    assert paths.log_file == tmp_path / "logs" / "trade_audit.log"
    assert paths.alert_file == tmp_path / "logs" / "trade_alert.json"
    assert paths.kill_switch_file == tmp_path / "reports" / "kill_switch.json"
    assert paths.reconcile_file == tmp_path / "reports" / "reconcile_status.json"


def test_get_latest_orders_reads_workspace_orders_db(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    db_path = reports_dir / "orders.sqlite"

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE orders (
            cl_ord_id TEXT,
            inst_id TEXT,
            side TEXT,
            state TEXT,
            intent TEXT,
            ord_id TEXT,
            last_error_code TEXT,
            last_error_msg TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(cl_ord_id, inst_id, side, state, intent, ord_id, last_error_code, last_error_msg)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("cid-1", "BTC-USDT", "buy", "FILLED", "OPEN_LONG", "oid-1", None, None),
    )
    conn.commit()
    conn.close()

    rows = trade_auditor.get_latest_orders(10, paths=trade_auditor.build_paths(tmp_path))

    assert rows == [("cid-1", "BTC-USDT", "buy", "FILLED", "OPEN_LONG", "oid-1", None, None)]


def test_get_latest_orders_prefers_recent_updated_event_ts(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    db_path = reports_dir / "orders.sqlite"

    now_ms = 1_710_000_000_000
    stale_created_ts = now_ms - 2 * 60 * 60 * 1000
    older_event_ts = now_ms - 30 * 60 * 1000

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE orders (
            cl_ord_id TEXT,
            inst_id TEXT,
            side TEXT,
            state TEXT,
            intent TEXT,
            ord_id TEXT,
            last_error_code TEXT,
            last_error_msg TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO orders(
            cl_ord_id, inst_id, side, state, intent, ord_id,
            last_error_code, last_error_msg, created_ts, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("cid-old", "BTC-USDT", "buy", "FILLED", "OPEN_LONG", "oid-old", None, None, stale_created_ts, now_ms),
            ("cid-newer-row", "ETH-USDT", "sell", "FILLED", "CLOSE_LONG", "oid-new", None, None, now_ms - 60_000, older_event_ts),
        ],
    )
    conn.commit()
    conn.close()

    rows = trade_auditor.get_latest_orders(1, paths=trade_auditor.build_paths(tmp_path))

    assert rows == [("cid-old", "BTC-USDT", "buy", "FILLED", "OPEN_LONG", "oid-old", None, None)]


def test_check_risk_limits_uses_workspace_reports_dir(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "kill_switch.json").write_text(
        json.dumps({"enabled": True, "reason": "manual-stop"}),
        encoding="utf-8",
    )
    (reports_dir / "reconcile_status.json").write_text(
        json.dumps({"ok": False, "reason": "drift"}),
        encoding="utf-8",
    )

    issues = trade_auditor.check_risk_limits(paths=trade_auditor.build_paths(tmp_path))

    assert len(issues) == 2
    assert any("manual-stop" in issue for issue in issues)
    assert any("drift" in issue for issue in issues)


def test_check_risk_limits_accepts_nested_enabled_kill_switch(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "kill_switch.json").write_text(
        json.dumps({"kill_switch": {"enabled": True, "reason": "manual-stop"}}),
        encoding="utf-8",
    )

    issues = trade_auditor.check_risk_limits(paths=trade_auditor.build_paths(tmp_path))

    assert len(issues) == 1
    assert "manual-stop" in issues[0]


def test_run_audit_writes_alert_and_log_to_workspace(tmp_path) -> None:
    paths = trade_auditor.build_paths(tmp_path)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(paths.orders_db))
    conn.execute(
        """
        CREATE TABLE orders (
            cl_ord_id TEXT,
            inst_id TEXT,
            side TEXT,
            state TEXT,
            intent TEXT,
            ord_id TEXT,
            last_error_code TEXT,
            last_error_msg TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(cl_ord_id, inst_id, side, state, intent, ord_id, last_error_code, last_error_msg)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("cid-1", "BTC-USDT", "buy", "PENDING", "OPEN_LONG", "oid-1", None, None),
    )
    conn.commit()
    conn.close()

    report = trade_auditor.run_audit(paths)

    assert report is not None
    assert paths.alert_file.exists()
    assert paths.log_file.exists()
    assert "state=PENDING" in paths.alert_file.read_text(encoding="utf-8")
