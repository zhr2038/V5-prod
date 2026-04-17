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
