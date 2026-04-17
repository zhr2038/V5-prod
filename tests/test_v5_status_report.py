from __future__ import annotations

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
    monkeypatch.setattr(status_report, "get_last_filled_trade_ts", lambda cfg=None: "2026-04-17T12:00")
    monkeypatch.setattr(status_report, "build_next_run_hint", lambda: "2026-04-17 13:00")

    report = status_report.generate_report()

    assert "- negative_expectancy_score_penalty: 4" in report
    assert "- negative_expectancy_cooldown: 5" in report
    assert "- negative_expectancy_open_block: 6" in report
    assert "- negative_expectancy_fast_fail_open_block: 7" in report
