from __future__ import annotations

import json
import sqlite3
import subprocess

import scripts.v5_status_report as v5_status_report


def _completed(returncode: int) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout="", stderr="")


def _show_completed(load_state: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[],
        returncode=0 if load_state != "not-found" else 1,
        stdout=f"LoadState={load_state}\n",
        stderr="",
    )


def test_get_service_status_reports_running_when_service_active(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return _show_completed("loaded")
        unit = cmd[-1]
        if unit == "v5-prod.user.service":
            return _completed(0)
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "running"


def test_get_service_status_reports_scheduled_when_timer_active(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return _show_completed("loaded")
        unit = cmd[-1]
        if unit == "v5-prod.user.timer":
            return _completed(0)
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "scheduled"


def test_get_service_status_reports_stopped_when_units_inactive(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return _show_completed("loaded")
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "stopped"


def test_get_service_status_does_not_fall_back_to_legacy_units_when_prod_exists(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return _show_completed("loaded")
        unit = cmd[-1]
        if unit in {"v5-live-20u.user.service", "v5-live-20u.user.timer"}:
            return _completed(0)
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "stopped"


def test_get_last_filled_trade_ts_prefers_fill_store_timestamp(tmp_path, monkeypatch) -> None:
    fills_db = tmp_path / "fills.sqlite"
    orders_db = tmp_path / "orders.sqlite"
    config_path = tmp_path / "empty.yaml"

    config_path.write_text("{}", encoding="utf-8")

    conn = sqlite3.connect(str(fills_db))
    conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (1_710_000_300_000,))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(orders_db))
    conn.execute("CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)")
    conn.execute("INSERT INTO orders(state, created_ts, updated_ts) VALUES ('FILLED', ?, ?)", (1_710_000_000_000, 1_710_000_100_000))
    conn.commit()
    conn.close()

    monkeypatch.setattr(v5_status_report, "FILLS_DB", fills_db)
    monkeypatch.setattr(v5_status_report, "ORDERS_DB", orders_db)
    monkeypatch.setattr(v5_status_report, "CONFIG_PATH", config_path)

    assert v5_status_report.get_last_filled_trade_ts() == v5_status_report._format_ts_ms(1_710_000_300_000)


def test_get_last_filled_trade_ts_falls_back_to_order_updated_ts(tmp_path, monkeypatch) -> None:
    fills_db = tmp_path / "fills.sqlite"
    orders_db = tmp_path / "orders.sqlite"
    config_path = tmp_path / "empty.yaml"

    config_path.write_text("{}", encoding="utf-8")

    conn = sqlite3.connect(str(orders_db))
    conn.execute("CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)")
    conn.execute("INSERT INTO orders(state, created_ts, updated_ts) VALUES ('FILLED', ?, ?)", (1_710_000_000_000, 1_710_000_600_000))
    conn.commit()
    conn.close()

    monkeypatch.setattr(v5_status_report, "FILLS_DB", fills_db)
    monkeypatch.setattr(v5_status_report, "ORDERS_DB", orders_db)
    monkeypatch.setattr(v5_status_report, "CONFIG_PATH", config_path)

    assert v5_status_report.get_last_filled_trade_ts() == v5_status_report._format_ts_ms(1_710_000_600_000)


def test_get_last_filled_trade_ts_uses_newer_order_event_when_fill_store_lags(tmp_path, monkeypatch) -> None:
    fills_db = tmp_path / "fills.sqlite"
    orders_db = tmp_path / "orders.sqlite"
    config_path = tmp_path / "empty.yaml"

    config_path.write_text("{}", encoding="utf-8")

    conn = sqlite3.connect(str(fills_db))
    conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (1_710_000_300_000,))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(orders_db))
    conn.execute("CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)")
    conn.execute(
        "INSERT INTO orders(state, created_ts, updated_ts) VALUES ('FILLED', ?, ?)",
        (1_710_000_000_000, 1_710_000_900_000),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(v5_status_report, "FILLS_DB", fills_db)
    monkeypatch.setattr(v5_status_report, "ORDERS_DB", orders_db)
    monkeypatch.setattr(v5_status_report, "CONFIG_PATH", config_path)

    assert v5_status_report.get_last_filled_trade_ts() == v5_status_report._format_ts_ms(1_710_000_900_000)


def test_get_last_filled_trade_ts_follows_active_config_order_store_path(tmp_path, monkeypatch) -> None:
    root_fills_db = tmp_path / "fills.sqlite"
    root_orders_db = tmp_path / "orders.sqlite"
    shadow_orders_db = tmp_path / "shadow_orders.sqlite"
    shadow_fills_db = tmp_path / "shadow_fills.sqlite"
    config_path = tmp_path / "live_prod.yaml"

    conn = sqlite3.connect(str(root_fills_db))
    conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (1_710_000_300_000,))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(root_orders_db))
    conn.execute("CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)")
    conn.execute(
        "INSERT INTO orders(state, created_ts, updated_ts) VALUES ('FILLED', ?, ?)",
        (1_710_000_000_000, 1_710_000_100_000),
    )
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(shadow_fills_db))
    conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (1_710_000_800_000,))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(shadow_orders_db))
    conn.execute("CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)")
    conn.execute(
        "INSERT INTO orders(state, created_ts, updated_ts) VALUES ('FILLED', ?, ?)",
        (1_710_000_000_000, 1_710_000_900_000),
    )
    conn.commit()
    conn.close()

    config_path.write_text(
        f"execution:\n  order_store_path: {json.dumps(str(shadow_orders_db))}\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(v5_status_report, "CONFIG_PATH", config_path)
    monkeypatch.setattr(v5_status_report, "ORDERS_DB", root_orders_db)
    monkeypatch.setattr(v5_status_report, "FILLS_DB", root_fills_db)

    assert v5_status_report.get_last_filled_trade_ts() == v5_status_report._format_ts_ms(1_710_000_900_000)


def test_check_borrow_status_follows_active_config_runtime_reports_dir(tmp_path, monkeypatch) -> None:
    root_orders_db = tmp_path / "orders.sqlite"
    shadow_dir = tmp_path / "shadow_runtime"
    shadow_orders_db = shadow_dir / "orders.sqlite"
    config_path = tmp_path / "live_prod.yaml"

    root_orders_db.write_text("", encoding="utf-8")
    shadow_dir.mkdir(parents=True, exist_ok=True)
    shadow_orders_db.write_text("", encoding="utf-8")
    (tmp_path / "auto_blacklist.json").write_text(
        json.dumps({"entries": [{"symbol": "ROOT-USDT-SWAP"}]}),
        encoding="utf-8",
    )
    (shadow_dir / "auto_blacklist.json").write_text(
        json.dumps({"entries": [{"symbol": "SHADOW-USDT-SWAP"}, {"symbol": "ALT-USDT-SWAP"}]}),
        encoding="utf-8",
    )

    config_path.write_text(
        "\n".join(
            [
                "execution:",
                f"  order_store_path: {json.dumps(str(shadow_orders_db))}",
                "  borrow_liab_eps: 0.0",
                "  borrow_neg_eq_eps: 0.0",
                '  borrow_block_mode: "symbol_only"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(v5_status_report, "CONFIG_PATH", config_path)
    monkeypatch.setattr(v5_status_report, "ORDERS_DB", root_orders_db)

    borrow = v5_status_report.check_borrow_status()

    assert borrow["blacklist_count"] == 2
    assert borrow["blacklist_symbols"] == ["SHADOW-USDT-SWAP", "ALT-USDT-SWAP"]


def test_main_writes_status_report_to_active_runtime_reports_dir(tmp_path, monkeypatch) -> None:
    root_reports_dir = tmp_path / "reports"
    shadow_dir = root_reports_dir / "shadow_runtime"
    root_orders_db = root_reports_dir / "orders.sqlite"
    shadow_orders_db = shadow_dir / "orders.sqlite"
    config_path = tmp_path / "live_prod.yaml"

    root_reports_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir.mkdir(parents=True, exist_ok=True)
    root_orders_db.write_text("", encoding="utf-8")
    shadow_orders_db.write_text("", encoding="utf-8")
    config_path.write_text(
        f"execution:\n  order_store_path: {json.dumps(str(shadow_orders_db))}\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(v5_status_report, "CONFIG_PATH", config_path)
    monkeypatch.setattr(v5_status_report, "REPORTS_DIR", root_reports_dir)
    monkeypatch.setattr(v5_status_report, "ORDERS_DB", root_orders_db)
    monkeypatch.setattr(v5_status_report, "generate_report", lambda: "shadow report")

    assert v5_status_report.main() == 0
    assert list(shadow_dir.glob("status_report_*.txt"))
    assert not list(root_reports_dir.glob("status_report_*.txt"))


def test_get_latest_run_data_follows_active_config_runtime_runs_dir(tmp_path, monkeypatch) -> None:
    root_reports_dir = tmp_path / "reports"
    shadow_dir = root_reports_dir / "shadow_runtime"
    root_orders_db = root_reports_dir / "orders.sqlite"
    shadow_orders_db = shadow_dir / "orders.sqlite"
    config_path = tmp_path / "live_prod.yaml"

    root_reports_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir.mkdir(parents=True, exist_ok=True)
    root_orders_db.write_text("", encoding="utf-8")
    shadow_orders_db.write_text("", encoding="utf-8")

    root_run = root_reports_dir / "runs" / "20260409_00"
    shadow_run = shadow_dir / "runs" / "20260409_01"
    root_run.mkdir(parents=True, exist_ok=True)
    shadow_run.mkdir(parents=True, exist_ok=True)
    (root_run / "decision_audit.json").write_text(
        json.dumps({"regime": "ROOT", "counts": {"orders_rebalance": 1}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (shadow_run / "decision_audit.json").write_text(
        json.dumps({"regime": "SHADOW", "counts": {"orders_rebalance": 2}}, ensure_ascii=False),
        encoding="utf-8",
    )

    config_path.write_text(
        f"execution:\n  order_store_path: {json.dumps(str(shadow_orders_db))}\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(v5_status_report, "CONFIG_PATH", config_path)
    monkeypatch.setattr(v5_status_report, "REPORTS_DIR", root_reports_dir)
    monkeypatch.setattr(v5_status_report, "ORDERS_DB", root_orders_db)

    run_data = v5_status_report.get_latest_run_data()

    assert run_data is not None
    assert run_data["regime"] == "SHADOW"
    assert run_data["counts"]["orders_rebalance"] == 2
