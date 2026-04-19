from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import scripts.risk_auto_recovery as risk_auto_recovery
from src.risk.auto_risk_guard import AutoRiskGuard


def test_execute_recovery_writes_guard_schema_compatible_state(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    reports_dir = workspace / "reports" / "shadow_runtime"
    reports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        risk_auto_recovery,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
    manager.risk_state_file.write_text(
        json.dumps(
            {
                "level": "PROTECT",
                "since": "2026-04-19T10:00:00",
                "metrics": {"last_dd_pct": 0.25},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    success, _ = manager.execute_recovery("DEFENSE")

    assert success is True
    state = json.loads(manager.risk_state_file.read_text(encoding="utf-8"))
    assert state["current_level"] == "DEFENSE"
    assert state["current_config"]["max_positions"] == AutoRiskGuard.LEVELS["DEFENSE"].max_positions
    assert state["metrics"]["last_dd_pct"] == 0.25
    assert state["history"][-1]["from"] == "PROTECT"
    assert state["history"][-1]["to"] == "DEFENSE"
    assert state["history"][-1]["reason"] == "[AUTO] recovery"
    assert state["level"] == "DEFENSE"

    guard = AutoRiskGuard(state_path=manager.risk_state_file)
    assert guard.current_level == "DEFENSE"


def test_get_drawdown_history_reads_runtime_runs_equity_jsonl(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    runtime_runs_dir = workspace / "reports" / "shadow_runtime" / "runs" / "20260419_1300"
    runtime_runs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        risk_auto_recovery,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
    (runtime_runs_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": "2026-04-19T13:00:00Z", "equity": 100.0, "drawdown": 0.22}),
                json.dumps({"ts": "2026-04-19T14:00:00Z", "equity": 105.0, "drawdown": 0.12}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    points = manager.get_drawdown_history(hours=24)

    assert len(points) == 2
    assert [point["drawdown"] for point in points] == [0.22, 0.12]


def test_get_drawdown_history_accepts_current_dd_field_from_runtime_equity(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    runtime_runs_dir = workspace / "reports" / "shadow_runtime" / "runs" / "20260419_1300"
    runtime_runs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        risk_auto_recovery,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
    (runtime_runs_dir / "equity.jsonl").write_text(
        json.dumps({"ts": "2026-04-19T13:00:00Z", "equity": 100.0, "dd": 0.31}) + "\n",
        encoding="utf-8",
    )

    points = manager.get_drawdown_history(hours=24)

    assert len(points) == 1
    assert points[0]["drawdown"] == 0.31


def test_time_in_current_level_accepts_zulu_timestamp(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    monkeypatch.setattr(
        risk_auto_recovery,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)

    hours = manager.time_in_current_level({"since": "2026-04-19T10:00:00Z"})

    assert hours != 999


def test_parse_state_datetime_treats_naive_timestamp_as_local_time(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    monkeypatch.setattr(
        risk_auto_recovery,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    previous_tz = os.environ.get("TZ")
    monkeypatch.setenv("TZ", "Asia/Shanghai")
    if hasattr(time, "tzset"):
        time.tzset()

    try:
        manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
        parsed = manager._parse_state_datetime("2026-04-19T10:00:00")
        assert parsed == datetime(2026, 4, 19, 2, 0, 0, tzinfo=timezone.utc)
    finally:
        if previous_tz is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", previous_tz)
        if hasattr(time, "tzset"):
            time.tzset()
