from __future__ import annotations

import json
from pathlib import Path

import scripts.trade_auditor_v2 as trade_auditor_v2


def test_build_paths_uses_suffixed_runtime_log_and_alert_files(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_auditor_v2,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        trade_auditor_v2,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = trade_auditor_v2.build_paths(tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "orders_accelerated.sqlite")
    assert paths.log_file == (tmp_path / "logs" / "trade_audit_v2_accelerated.log").resolve()
    assert paths.alert_file == (tmp_path / "logs" / "trade_alert_v2_accelerated.json").resolve()


def test_build_paths_uses_nested_runtime_log_and_alert_files(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_auditor_v2,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(
        trade_auditor_v2,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = trade_auditor_v2.build_paths(tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "shadow_runtime" / "orders.sqlite")
    assert paths.log_file == (tmp_path / "logs" / "shadow_runtime_trade_audit_v2.log").resolve()
    assert paths.alert_file == (tmp_path / "logs" / "shadow_runtime_trade_alert_v2.json").resolve()


def test_generate_report_includes_negative_expectancy_counts(tmp_path: Path) -> None:
    runs_dir = tmp_path / "reports" / "runs" / "20260417_01"
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "TRENDING",
                "counts": {
                    "negative_expectancy_score_penalty": 2,
                    "negative_expectancy_cooldown": 3,
                    "negative_expectancy_open_block": 4,
                    "negative_expectancy_fast_fail_open_block": 5,
                },
            }
        ),
        encoding="utf-8",
    )

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)
    report = auditor.generate_report(
        {
            "buy_filled": [],
            "sell_filled": [],
            "buy_rejected": [],
            "sell_rejected": [],
        },
        "TRENDING",
    )

    assert report["summary"]["negative_expectancy_penalty_count"] == 2
    assert report["summary"]["negative_expectancy_cooldown_count"] == 3
    assert report["summary"]["negative_expectancy_open_block_count"] == 4
    assert report["summary"]["negative_expectancy_fast_fail_open_block_count"] == 5


def test_load_latest_decision_audit_prefers_audit_file_mtime(tmp_path: Path) -> None:
    stale_run = tmp_path / "reports" / "runs" / "stale"
    fresh_run = tmp_path / "reports" / "runs" / "fresh"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)
    stale_audit = stale_run / "decision_audit.json"
    fresh_audit = fresh_run / "decision_audit.json"
    stale_audit.write_text(json.dumps({"run_id": "stale"}), encoding="utf-8")
    fresh_audit.write_text(json.dumps({"run_id": "fresh"}), encoding="utf-8")

    import os
    stale_audit_ts = 1_710_000_000
    fresh_audit_ts = 1_710_000_100
    os.utime(stale_audit, (stale_audit_ts, stale_audit_ts))
    os.utime(fresh_audit, (fresh_audit_ts, fresh_audit_ts))
    os.utime(stale_run, (fresh_audit_ts + 500, fresh_audit_ts + 500))
    os.utime(fresh_run, (stale_audit_ts, stale_audit_ts))

    auditor = trade_auditor_v2.SmartTradeAuditor(workspace=tmp_path)

    assert auditor._load_latest_decision_audit()["run_id"] == "fresh"
