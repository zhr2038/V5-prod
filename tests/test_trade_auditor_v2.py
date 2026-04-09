from __future__ import annotations

import json
import sqlite3
from datetime import datetime

import scripts.trade_auditor_v2 as trade_auditor_v2


def test_build_paths_anchor_trade_auditor_v2_to_repo_root(tmp_path) -> None:
    paths = trade_auditor_v2.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == tmp_path / "reports"
    assert paths.runs_dir == tmp_path / "reports" / "runs"
    assert paths.orders_db == tmp_path / "reports" / "orders.sqlite"
    assert paths.log_file == tmp_path / "logs" / "trade_audit_v2.log"
    assert paths.alert_file == tmp_path / "logs" / "trade_alert_v2.json"
    assert paths.kill_switch_file == tmp_path / "reports" / "kill_switch.json"
    assert paths.reconcile_file == tmp_path / "reports" / "reconcile_status.json"


def test_build_paths_follow_active_config_runtime_paths(tmp_path) -> None:
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
                "  kill_switch_path: reports/shadow_runtime/kill_switch_shadow.json",
                "  reconcile_status_path: reports/shadow_runtime/reconcile_shadow.json",
                "",
            ]
        ),
        encoding="utf-8",
    )

    paths = trade_auditor_v2.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == tmp_path / "reports" / "shadow_runtime"
    assert paths.runs_dir == tmp_path / "reports" / "shadow_runtime" / "runs"
    assert paths.orders_db == tmp_path / "reports" / "shadow_runtime" / "orders.sqlite"
    assert paths.log_file == tmp_path / "logs" / "trade_audit_v2.log"
    assert paths.alert_file == tmp_path / "logs" / "trade_alert_v2.json"
    assert paths.kill_switch_file == tmp_path / "reports" / "shadow_runtime" / "kill_switch_shadow.json"
    assert paths.reconcile_file == tmp_path / "reports" / "shadow_runtime" / "reconcile_shadow.json"


def test_build_paths_derives_runtime_default_state_files_from_order_store(tmp_path) -> None:
    configs_dir = tmp_path / "configs"
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

    paths = trade_auditor_v2.build_paths(tmp_path)

    assert paths.orders_db == tmp_path / "reports" / "shadow_runtime" / "orders.sqlite"
    assert paths.kill_switch_file == tmp_path / "reports" / "shadow_runtime" / "kill_switch.json"
    assert paths.reconcile_file == tmp_path / "reports" / "shadow_runtime" / "reconcile_status.json"


def test_get_orders_in_window_reads_workspace_orders_db(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    db_path = reports_dir / "orders.sqlite"
    now_ms = int(datetime.now().timestamp() * 1000)

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
            created_ts INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(cl_ord_id, inst_id, side, state, intent, ord_id, last_error_code, last_error_msg, created_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("cid-1", "BTC-USDT", "buy", "FILLED", "OPEN_LONG", "oid-1", None, None, now_ms),
    )
    conn.commit()
    conn.close()

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)

    assert auditor.get_orders_in_window(minutes=65) == [
        ("cid-1", "BTC-USDT", "buy", "FILLED", "OPEN_LONG", "oid-1", None, None, now_ms)
    ]


def test_get_orders_in_window_uses_updated_ts_for_recent_events(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    db_path = reports_dir / "orders.sqlite"
    now_ms = int(datetime.now().timestamp() * 1000)
    stale_created_ts = now_ms - 2 * 60 * 60 * 1000

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
    conn.execute(
        """
        INSERT INTO orders(
            cl_ord_id, inst_id, side, state, intent, ord_id,
            last_error_code, last_error_msg, created_ts, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("cid-1", "BTC-USDT", "buy", "FILLED", "OPEN_LONG", "oid-1", None, None, stale_created_ts, now_ms),
    )
    conn.commit()
    conn.close()

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)

    assert auditor.get_orders_in_window(minutes=65) == [
        ("cid-1", "BTC-USDT", "buy", "FILLED", "OPEN_LONG", "oid-1", None, None, now_ms)
    ]


def test_check_market_regime_uses_workspace_runs_dir(tmp_path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260406_000000"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "decision_audit.json").write_text(
        json.dumps({"regime": "Sideways", "regime_details": {"final_state": "Risk-Off"}}),
        encoding="utf-8",
    )

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)

    assert auditor.check_market_regime() == "Risk-Off"


def test_check_market_regime_falls_back_to_workspace_regime_file(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "regime.json").write_text(
        json.dumps({"state": "TrendingUp"}),
        encoding="utf-8",
    )

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)

    assert auditor.check_market_regime() == "TrendingUp"


def test_check_market_regime_uses_active_config_runtime_runs_dir(tmp_path) -> None:
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    shadow_runs_dir = tmp_path / "reports" / "shadow_runtime" / "runs" / "20260406_010000"
    shadow_runs_dir.mkdir(parents=True, exist_ok=True)
    (configs_dir / "live_prod.yaml").write_text(
        "execution:\n  order_store_path: reports/shadow_runtime/orders.sqlite\n",
        encoding="utf-8",
    )
    (shadow_runs_dir / "decision_audit.json").write_text(
        json.dumps({"regime": "Sideways", "regime_details": {"final_state": "Risk-Off"}}),
        encoding="utf-8",
    )
    (tmp_path / "reports").mkdir(parents=True, exist_ok=True)
    (tmp_path / "reports" / "regime.json").write_text(
        json.dumps({"state": "TrendingUp"}),
        encoding="utf-8",
    )

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)

    assert auditor.check_market_regime() == "Risk-Off"


def test_check_risk_controls_uses_workspace_reports_dir(tmp_path) -> None:
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

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)
    issues = auditor.check_risk_controls()

    assert len(issues) == 2
    assert any(issue["level"] == "CRITICAL" and "manual-stop" in issue["message"] for issue in issues)
    assert any(issue["level"] == "WARNING" and "drift" in issue["message"] for issue in issues)


def test_check_risk_controls_accepts_nested_enabled_kill_switch(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "kill_switch.json").write_text(
        json.dumps({"kill_switch": {"enabled": True, "reason": "manual-stop"}}),
        encoding="utf-8",
    )

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)
    issues = auditor.check_risk_controls()

    assert len(issues) == 1
    assert issues[0]["level"] == "CRITICAL"
    assert "manual-stop" in issues[0]["message"]


def test_check_risk_controls_treats_string_false_reconcile_as_failure(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "reconcile_status.json").write_text(
        json.dumps({"ok": "false", "reason": "drift"}),
        encoding="utf-8",
    )

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)
    issues = auditor.check_risk_controls()

    assert len(issues) == 1
    assert issues[0]["level"] == "WARNING"
    assert "drift" in issues[0]["message"]


def test_check_risk_controls_uses_active_config_runtime_state_files(tmp_path) -> None:
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir = tmp_path / "reports" / "shadow_runtime"
    shadow_dir.mkdir(parents=True, exist_ok=True)
    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
                "  kill_switch_path: reports/shadow_runtime/kill_switch_shadow.json",
                "  reconcile_status_path: reports/shadow_runtime/reconcile_shadow.json",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "reports").mkdir(parents=True, exist_ok=True)
    (tmp_path / "reports" / "kill_switch.json").write_text(
        json.dumps({"enabled": False}),
        encoding="utf-8",
    )
    (tmp_path / "reports" / "reconcile_status.json").write_text(
        json.dumps({"ok": True}),
        encoding="utf-8",
    )
    (shadow_dir / "kill_switch_shadow.json").write_text(
        json.dumps({"enabled": True, "reason": "shadow-stop"}),
        encoding="utf-8",
    )
    (shadow_dir / "reconcile_shadow.json").write_text(
        json.dumps({"ok": False, "reason": "shadow-drift"}),
        encoding="utf-8",
    )

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)
    issues = auditor.check_risk_controls()

    assert len(issues) == 2
    assert any(issue["level"] == "CRITICAL" and "shadow-stop" in issue["message"] for issue in issues)
    assert any(issue["level"] == "WARNING" and "shadow-drift" in issue["message"] for issue in issues)
