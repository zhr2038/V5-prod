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
