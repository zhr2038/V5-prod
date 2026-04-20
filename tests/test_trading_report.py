from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import scripts.trading_report as trading_report


def test_build_paths_uses_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/shadow_orders.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(
        trading_report,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        trading_report,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = trading_report.build_paths(tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "shadow_orders.sqlite").resolve()
    assert paths.fills_db == (tmp_path / "reports" / "shadow_fills.sqlite").resolve()
    assert paths.runs_dir == (tmp_path / "reports" / "runs").resolve()


def test_build_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(trading_report, "load_runtime_config", lambda project_root=None: {})

    with pytest.raises(ValueError, match="live_prod.yaml"):
        trading_report.build_paths(tmp_path)


def test_build_paths_fails_fast_when_runtime_config_is_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        trading_report.build_paths(tmp_path)


def test_load_regime_history_prefers_audit_timestamp_over_file_mtime(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    stale_run = runs_dir / "20260401_01"
    fresh_run = runs_dir / "20260420_10"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    stale_ts = (now - timedelta(days=8)).timestamp()
    fresh_ts = (now - timedelta(hours=1)).timestamp()

    (stale_run / "decision_audit.json").write_text(
        json.dumps({"run_id": "20260401_01", "timestamp": stale_ts, "regime": "SIDEWAYS", "regime_multiplier": 0.5}),
        encoding="utf-8",
    )
    (fresh_run / "decision_audit.json").write_text(
        json.dumps({"run_id": "20260420_10", "timestamp": fresh_ts, "regime": "TRENDING", "regime_multiplier": 0.9}),
        encoding="utf-8",
    )

    stale_audit = stale_run / "decision_audit.json"
    fresh_audit = fresh_run / "decision_audit.json"
    stale_audit.touch()
    fresh_audit.touch()
    stale_now = now.timestamp()
    old_mtime = (now - timedelta(days=30)).timestamp()
    stale_audit.chmod(0o644)
    fresh_audit.chmod(0o644)
    import os
    os.utime(stale_audit, (stale_now, stale_now))
    os.utime(fresh_audit, (old_mtime, old_mtime))

    generator = trading_report.TradingReportGenerator(
        paths=trading_report.ReportPaths(
            workspace=tmp_path,
            reports_dir=reports_dir,
            runs_dir=runs_dir,
            orders_db=reports_dir / "orders.sqlite",
            fills_db=reports_dir / "fills.sqlite",
        )
    )

    regimes = generator.load_regime_history(days=7)

    assert len(regimes) == 1
    assert regimes[0]["regime"] == "TRENDING"
    assert regimes[0]["multiplier"] == 0.9
