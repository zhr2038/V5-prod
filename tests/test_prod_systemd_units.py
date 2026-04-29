from __future__ import annotations

from pathlib import Path

from deploy.prod_release import PRODUCTION_USER_UNIT_MAPPINGS


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROD_SYSTEMD_SERVICE_UNITS = (
    "v5-auto-risk-eval.service",
    "v5-cost-rollup-real.user.service",
    "v5-daily-ml-training.service",
    "v5-event-driven.service",
    "v5-ledger.service",
    "v5-model-promotion-gate.service",
    "v5-prod.user.service",
    "v5-reconcile.service",
    "v5-sentiment-collect.service",
    "v5-spread-rollup.service",
    "v5-trade-monitor.service",
    "v5-web-dashboard.service",
)
PROD_SYSTEMD_UNITS = tuple(
    sorted(set(PROD_SYSTEMD_SERVICE_UNITS).union(*(set(pair) for pair in PRODUCTION_USER_UNIT_MAPPINGS)))
)


def test_prod_systemd_units_match_ubuntu_workspace_path() -> None:
    for unit in PROD_SYSTEMD_SERVICE_UNITS:
        path = PROJECT_ROOT / "deploy" / "systemd" / unit
        text = path.read_text(encoding="utf-8")

        assert "/home/ubuntu/clawd/v5-prod" in text, unit
        assert "/home/admin" not in text, unit
        assert "v5-trading-bot" not in text, unit
        assert "\nUser=admin" not in text, unit
        assert "\nGroup=admin" not in text, unit


def test_prod_systemd_mapping_sources_are_safe_for_ubuntu_install() -> None:
    for unit in PROD_SYSTEMD_UNITS:
        path = PROJECT_ROOT / "deploy" / "systemd" / unit
        raw = path.read_bytes()
        text = raw.decode("utf-8")

        assert b"\r\n" not in raw, unit
        assert "/home/admin" not in text, unit
        assert "v5-trading-bot" not in text, unit
        assert "\nUser=admin" not in text, unit
        assert "\nGroup=admin" not in text, unit
        if "WorkingDirectory=" in text:
            assert "/home/ubuntu/clawd/v5-prod" in text, unit


def test_trade_monitor_timer_runs_after_hourly_live_window() -> None:
    timer = (PROJECT_ROOT / "deploy" / "systemd" / "v5-trade-monitor.timer").read_text(encoding="utf-8")

    assert "OnCalendar=*-*-* *:07:00" in timer
    assert "Unit=v5-trade-monitor.service" in timer


def test_live_prod_service_fails_when_pre_trade_auto_sync_fails() -> None:
    service = (PROJECT_ROOT / "deploy" / "systemd" / "v5-prod.user.service").read_text(encoding="utf-8")

    assert "scripts/auto_sync_before_trade.py" in service
    assert "ExecStartPre=-" not in service
    assert "ExecStartPre=/bin/bash" in service


def test_event_driven_timer_is_offset_from_hourly_live_and_auto_risk_writes() -> None:
    timer = (PROJECT_ROOT / "deploy" / "systemd" / "v5-event-driven.timer").read_text(encoding="utf-8")

    assert "OnCalendar=*:02/15:00" in timer
    assert "OnCalendar=*:0/15:00" not in timer
    assert "AccuracySec=30s" in timer
