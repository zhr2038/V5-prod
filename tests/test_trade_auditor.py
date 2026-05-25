from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

import scripts.trade_auditor as trade_auditor


@pytest.fixture(autouse=True)
def _runtime_config(monkeypatch, tmp_path: Path) -> Path:
    config_path = tmp_path / "configs" / "live_prod.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "execution:\n  order_store_path: reports/orders.sqlite\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        trade_auditor,
        "resolve_runtime_config_path",
        lambda project_root=None: str(config_path),
    )
    return config_path


def test_build_paths_uses_prefixed_runtime_log_and_alert_files(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_auditor,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        trade_auditor,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = trade_auditor.build_paths(tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "shadow_orders.sqlite")
    assert paths.log_file == (tmp_path / "logs" / "shadow_trade_audit.log").resolve()
    assert paths.alert_file == (tmp_path / "logs" / "shadow_trade_alert.json").resolve()


def test_build_paths_uses_nested_runtime_log_and_alert_files(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_auditor,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(
        trade_auditor,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = trade_auditor.build_paths(tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "shadow_runtime" / "orders.sqlite")
    assert paths.log_file == (tmp_path / "logs" / "shadow_runtime_trade_audit.log").resolve()
    assert paths.alert_file == (tmp_path / "logs" / "shadow_runtime_trade_alert.json").resolve()


def test_load_active_config_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = tmp_path / "configs" / "missing.yaml"
    monkeypatch.setattr(
        trade_auditor,
        "resolve_runtime_config_path",
        lambda project_root=None: str(missing),
    )

    try:
        trade_auditor._load_active_config(project_root=tmp_path)
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_run_audit_writes_utc_report_timestamp(monkeypatch, tmp_path: Path) -> None:
    orders_db = tmp_path / "reports" / "orders.sqlite"
    orders_db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(orders_db)) as conn:
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
            INSERT INTO orders (
                cl_ord_id, inst_id, side, state, intent, ord_id, last_error_code, last_error_msg, created_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("cid-1", "BTC-USDT", "buy", "FILLED", "OPEN_LONG", "oid-1", "0", "", 1_700_000_000_000),
        )

    monkeypatch.setattr(
        trade_auditor,
        "_utc_now",
        lambda: datetime(2026, 5, 25, 4, 0, 1, tzinfo=timezone.utc),
    )
    paths = trade_auditor.AuditorPaths(
        workspace=tmp_path,
        reports_dir=orders_db.parent,
        runs_dir=orders_db.parent / "runs",
        orders_db=orders_db,
        log_file=tmp_path / "logs" / "trade_audit.log",
        alert_file=tmp_path / "logs" / "trade_alert.json",
        kill_switch_file=tmp_path / "reports" / "kill_switch.json",
        reconcile_file=tmp_path / "reports" / "reconcile_status.json",
    )

    report = trade_auditor.run_audit(paths=paths)

    assert report is not None
    assert report["timestamp"] == "2026-05-25T04:00:01Z"
