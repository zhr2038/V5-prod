from __future__ import annotations

import json
import sqlite3
from pathlib import Path

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

    assert issues == [
        "Kill Switch 已启用: manual-stop",
        "对账异常: drift",
    ]


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
    assert "异常状态" in paths.alert_file.read_text(encoding="utf-8")
