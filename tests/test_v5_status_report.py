from __future__ import annotations

import json
import os
from pathlib import Path

import scripts.v5_status_report as status_report


def test_resolve_status_paths_uses_prefixed_runtime_auto_blacklist(tmp_path: Path) -> None:
    cfg = {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}}

    original_workspace = status_report.WORKSPACE
    original_reports = status_report.REPORTS_DIR
    original_orders = status_report.ORDERS_DB
    try:
        status_report.WORKSPACE = tmp_path
        status_report.REPORTS_DIR = tmp_path / "reports"
        status_report.ORDERS_DB = status_report.REPORTS_DIR / "orders.sqlite"

        paths = status_report._resolve_status_paths(cfg)

        assert paths.orders_db == (status_report.REPORTS_DIR / "shadow_orders.sqlite").resolve()
        assert paths.auto_blacklist_path == (status_report.REPORTS_DIR / "shadow_auto_blacklist.json").resolve()
    finally:
        status_report.WORKSPACE = original_workspace
        status_report.REPORTS_DIR = original_reports
        status_report.ORDERS_DB = original_orders


def test_resolve_config_path_uses_runtime_config_helper(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "custom_live.yaml").resolve()
    monkeypatch.setattr(
        status_report,
        "resolve_runtime_config_path",
        lambda project_root=None: str(expected),
    )
    monkeypatch.setattr(status_report, "WORKSPACE", tmp_path)

    path = status_report.resolve_config_path()

    assert path == expected


def test_resolve_live_units_ignores_retired_live_20u(monkeypatch) -> None:
    monkeypatch.setattr(
        status_report,
        "_get_unit_load_state",
        lambda unit: "loaded" if unit == "v5-live-20u.user.service" else "not-found",
    )

    service_unit, timer_unit = status_report._resolve_live_units()

    assert service_unit == "v5-prod.user.service"
    assert timer_unit == "v5-prod.user.timer"


def test_load_config_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(status_report, "CONFIG_PATH", missing)

    try:
        status_report.load_config()
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_resolve_status_paths_uses_suffixed_runtime_auto_blacklist(tmp_path: Path) -> None:
    cfg = {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}}

    original_workspace = status_report.WORKSPACE
    original_reports = status_report.REPORTS_DIR
    original_orders = status_report.ORDERS_DB
    try:
        status_report.WORKSPACE = tmp_path
        status_report.REPORTS_DIR = tmp_path / "reports"
        status_report.ORDERS_DB = status_report.REPORTS_DIR / "orders.sqlite"

        paths = status_report._resolve_status_paths(cfg)

        assert paths.orders_db == (status_report.REPORTS_DIR / "orders_accelerated.sqlite").resolve()
        assert paths.auto_blacklist_path == (status_report.REPORTS_DIR / "auto_blacklist_accelerated.json").resolve()
    finally:
        status_report.WORKSPACE = original_workspace
        status_report.REPORTS_DIR = original_reports
        status_report.ORDERS_DB = original_orders


def test_resolve_report_output_path_uses_prefixed_runtime_name(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    paths = status_report.StatusPaths(
        orders_db=(reports_dir / "shadow_orders.sqlite").resolve(),
        fills_db=(reports_dir / "shadow_fills.sqlite").resolve(),
        auto_blacklist_path=(reports_dir / "shadow_auto_blacklist.json").resolve(),
        auto_risk_eval_path=(reports_dir / "shadow_auto_risk_eval.json").resolve(),
        auto_risk_guard_path=(reports_dir / "shadow_auto_risk_guard.json").resolve(),
        runs_dir=(reports_dir / "runs").resolve(),
    )

    path = status_report._resolve_report_output_path(paths, "20260414_1930")

    assert path == (reports_dir / "shadow_status_report_20260414_1930.txt").resolve()


def test_resolve_report_output_path_uses_suffixed_runtime_name(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    paths = status_report.StatusPaths(
        orders_db=(reports_dir / "orders_accelerated.sqlite").resolve(),
        fills_db=(reports_dir / "fills_accelerated.sqlite").resolve(),
        auto_blacklist_path=(reports_dir / "auto_blacklist_accelerated.json").resolve(),
        auto_risk_eval_path=(reports_dir / "auto_risk_eval_accelerated.json").resolve(),
        auto_risk_guard_path=(reports_dir / "auto_risk_guard_accelerated.json").resolve(),
        runs_dir=(reports_dir / "runs").resolve(),
    )

    path = status_report._resolve_report_output_path(paths, "20260414_1930")

    assert path == (reports_dir / "status_report_20260414_1930_accelerated.txt").resolve()


def test_generate_report_includes_negative_expectancy_counts(monkeypatch) -> None:
    monkeypatch.setattr(status_report, "load_config", lambda: {"budget": {"live_equity_cap_usdt": 20}})
    monkeypatch.setattr(
        status_report,
        "get_latest_run_data",
        lambda cfg=None: {
            "regime": "TRENDING",
            "counts": {
                "selected": 3,
                "targets_pre_risk": 2,
                "orders_rebalance": 1,
                "risk_off_suppressed_count": 1,
                "target_zero_after_regime_count": 2,
                "target_zero_after_dd_throttle_count": 3,
                "protect_entry_block_count": 4,
                "protect_entry_trend_only_block_count": 5,
                "protect_entry_alpha6_rsi_block_count": 6,
                "negative_expectancy_score_penalty": 4,
                "negative_expectancy_cooldown": 5,
                "negative_expectancy_open_block": 6,
                "negative_expectancy_fast_fail_open_block": 7,
            },
            "notes": [],
        },
    )
    monkeypatch.setattr(
        status_report,
        "check_borrow_status",
        lambda cfg=None: {
            "config": {"liab_eps": 0.01, "neg_eq_eps": 0.01, "mode": "symbol_only"},
            "blacklist_count": 0,
            "blacklist_symbols": [],
        },
    )
    monkeypatch.setattr(status_report, "get_service_status", lambda: "running")
    monkeypatch.setattr(
        status_report,
        "get_current_risk_guard",
        lambda cfg=None: {"level": "DEFENSE", "source": "guard", "last_update": "2026-04-17T11:55:00"},
    )
    monkeypatch.setattr(status_report, "get_last_filled_trade_ts", lambda cfg=None: "2026-04-17T12:00")
    monkeypatch.setattr(status_report, "build_next_run_hint", lambda: "2026-04-17 13:00")

    report = status_report.generate_report()

    assert "- risk_off_suppressed_count: 1" in report
    assert "- target_zero_after_regime_count: 2" in report
    assert "- target_zero_after_dd_throttle_count: 3" in report
    assert "- protect_entry_block_count: 4" in report
    assert "- protect_entry_trend_only_block_count: 5" in report
    assert "- protect_entry_alpha6_rsi_block_count: 6" in report
    assert "- negative_expectancy_score_penalty: 4" in report
    assert "- negative_expectancy_cooldown: 5" in report
    assert "- negative_expectancy_open_block: 6" in report
    assert "- negative_expectancy_fast_fail_open_block: 7" in report
    assert "- risk_guard_level: DEFENSE" in report
    assert "- risk_guard_source: guard" in report
    assert "- risk_guard_last_update: 2026-04-17T11:55:00" in report


def test_get_current_risk_guard_prefers_newer_guard_state_over_eval(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    cfg = {"execution": {"order_store_path": str((reports_dir / "shadow_orders.sqlite").resolve())}}
    (reports_dir / "shadow_auto_risk_eval.json").write_text(
        json.dumps({"ts": "2026-04-19T13:00:00", "current_level": "PROTECT"}),
        encoding="utf-8",
    )
    (reports_dir / "shadow_auto_risk_guard.json").write_text(
        json.dumps({"current_level": "DEFENSE", "last_update": "2026-04-19T14:05:00"}),
        encoding="utf-8",
    )

    state = status_report.get_current_risk_guard(cfg)

    assert state["level"] == "DEFENSE"
    assert state["source"] == "guard"
    assert state["last_update"] == "2026-04-19T14:05:00"


def test_get_current_risk_guard_accepts_legacy_guard_level_field(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    cfg = {"execution": {"order_store_path": str((reports_dir / "shadow_orders.sqlite").resolve())}}
    (reports_dir / "shadow_auto_risk_guard.json").write_text(
        json.dumps({"level": "DEFENSE", "last_update": "2026-04-19T14:05:00"}),
        encoding="utf-8",
    )

    state = status_report.get_current_risk_guard(cfg)

    assert state["level"] == "DEFENSE"
    assert state["source"] == "guard"


def test_get_latest_run_data_prefers_decision_audit_mtime(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    stale_run = runs_dir / "stale"
    fresh_run = runs_dir / "fresh"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)

    stale_audit = stale_run / "decision_audit.json"
    fresh_audit = fresh_run / "decision_audit.json"
    stale_audit.write_text(json.dumps({"run_id": "stale"}), encoding="utf-8")
    fresh_audit.write_text(json.dumps({"run_id": "fresh"}), encoding="utf-8")

    stale_audit_ts = 1_710_000_000
    fresh_audit_ts = 1_710_000_100
    os.utime(stale_audit, (stale_audit_ts, stale_audit_ts))
    os.utime(fresh_audit, (fresh_audit_ts, fresh_audit_ts))
    os.utime(stale_run, (fresh_audit_ts + 500, fresh_audit_ts + 500))
    os.utime(fresh_run, (stale_audit_ts, stale_audit_ts))

    cfg = {"execution": {"order_store_path": str((reports_dir / "orders.sqlite").resolve())}}
    data = status_report.get_latest_run_data(cfg)

    assert data is not None
    assert data["run_id"] == "fresh"
