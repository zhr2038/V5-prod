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
        risk_auto_recovery.RiskAutoRecovery,
        "_load_active_runtime_config",
        lambda self: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
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
    now = datetime.now(timezone.utc)
    run_name = (now - risk_auto_recovery.timedelta(hours=1)).strftime("%Y%m%d_%H")
    runtime_runs_dir = workspace / "reports" / "shadow_runtime" / "runs" / run_name
    runtime_runs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        risk_auto_recovery.RiskAutoRecovery,
        "_load_active_runtime_config",
        lambda self: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
    (runtime_runs_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": (now - risk_auto_recovery.timedelta(hours=1)).isoformat().replace("+00:00", "Z"), "equity": 100.0, "drawdown": 0.22}),
                json.dumps({"ts": (now - risk_auto_recovery.timedelta(minutes=30)).isoformat().replace("+00:00", "Z"), "equity": 105.0, "drawdown": 0.12}),
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
    now = datetime.now(timezone.utc)
    run_name = (now - risk_auto_recovery.timedelta(hours=1)).strftime("%Y%m%d_%H")
    runtime_runs_dir = workspace / "reports" / "shadow_runtime" / "runs" / run_name
    runtime_runs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        risk_auto_recovery.RiskAutoRecovery,
        "_load_active_runtime_config",
        lambda self: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
    (runtime_runs_dir / "equity.jsonl").write_text(
        json.dumps({"ts": (now - risk_auto_recovery.timedelta(hours=1)).isoformat().replace("+00:00", "Z"), "equity": 100.0, "dd": 0.31}) + "\n",
        encoding="utf-8",
    )

    points = manager.get_drawdown_history(hours=24)

    assert len(points) == 1
    assert points[0]["drawdown"] == 0.31


def test_get_drawdown_history_limits_recent_equity_file_reads_before_parsing(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    runtime_runs_dir = workspace / "reports" / "shadow_runtime" / "runs"
    runtime_runs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        risk_auto_recovery.RiskAutoRecovery,
        "_load_active_runtime_config",
        lambda self: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    now = datetime.now(timezone.utc)
    recent_hours = {19, 18, 17, 16}
    for hour in range(20):
        day_offset = 0 if hour in recent_hours else 10
        run_dt = now - risk_auto_recovery.timedelta(days=day_offset, hours=19 - hour)
        run_name = run_dt.strftime("%Y%m%d_%H")
        run_dir = runtime_runs_dir / run_name
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "equity.jsonl").write_text(
            json.dumps(
                {
                    "ts": (run_dt + risk_auto_recovery.timedelta(minutes=30)).isoformat().replace("+00:00", "Z"),
                    "equity": 100.0 + hour,
                    "drawdown": 0.1,
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )

    original_open = Path.open
    reads = {"equity": 0}

    def counting_open(self: Path, *args, **kwargs):
        if self.name == "equity.jsonl":
            reads["equity"] += 1
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting_open)

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
    points = manager.get_drawdown_history(hours=24)

    assert len(points) == 4
    assert reads["equity"] <= 4


def test_get_drawdown_history_prefers_newer_run_for_duplicate_timestamp(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    now = datetime.now(timezone.utc)
    recent_run_name = (now - risk_auto_recovery.timedelta(hours=1)).strftime("%Y%m%d_%H")
    older_run_name = (now - risk_auto_recovery.timedelta(hours=2)).strftime("%Y%m%d_%H")
    runtime_runs_dir = workspace / "reports" / "shadow_runtime" / "runs"
    recent_run_dir = runtime_runs_dir / recent_run_name
    older_run_dir = runtime_runs_dir / older_run_name
    recent_run_dir.mkdir(parents=True, exist_ok=True)
    older_run_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        risk_auto_recovery.RiskAutoRecovery,
        "_load_active_runtime_config",
        lambda self: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    duplicate_ts = (now - risk_auto_recovery.timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
    (recent_run_dir / "equity.jsonl").write_text(
        json.dumps({"ts": duplicate_ts, "equity": 120.0, "drawdown": 0.05}) + "\n",
        encoding="utf-8",
    )
    (older_run_dir / "equity.jsonl").write_text(
        json.dumps({"ts": duplicate_ts, "equity": 80.0, "drawdown": 0.25}) + "\n",
        encoding="utf-8",
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
    points = manager.get_drawdown_history(hours=24)

    assert len(points) == 1
    assert points[0]["equity"] == 120.0
    assert points[0]["drawdown"] == 0.05


def test_time_in_current_level_accepts_zulu_timestamp(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    monkeypatch.setattr(
        risk_auto_recovery.RiskAutoRecovery,
        "_load_active_runtime_config",
        lambda self: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)

    hours = manager.time_in_current_level({"since": "2026-04-19T10:00:00Z"})

    assert hours != 999


def test_parse_state_datetime_treats_naive_timestamp_as_local_time(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    monkeypatch.setattr(
        risk_auto_recovery.RiskAutoRecovery,
        "_load_active_runtime_config",
        lambda self: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
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


def test_auto_risk_guard_loads_legacy_level_field(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    reports_dir = workspace / "reports" / "shadow_runtime"
    reports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        risk_auto_recovery.RiskAutoRecovery,
        "_load_active_runtime_config",
        lambda self: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
    manager.risk_state_file.write_text(
        json.dumps({"level": "PROTECT", "last_update": "2026-04-19T14:05:00"}),
        encoding="utf-8",
    )

    guard = AutoRiskGuard(state_path=manager.risk_state_file)

    assert guard.current_level == "PROTECT"


def test_get_current_risk_state_prefers_latest_history_ts_when_history_is_unsorted(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path
    reports_dir = workspace / "reports" / "shadow_runtime"
    reports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        risk_auto_recovery.RiskAutoRecovery,
        "_load_active_runtime_config",
        lambda self: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=workspace)
    manager.risk_state_file.write_text(
        json.dumps(
            {
                "level": "PROTECT",
                "history": [
                    {"ts": "2026-04-19T14:00:00Z", "to": "PROTECT"},
                    {"ts": "2026-04-19T13:00:00Z", "to": "DEFENSE"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    state = manager.get_current_risk_state()

    assert state["current_level"] == "PROTECT"
    assert state["since"] == "2026-04-19T14:00:00Z"


def test_risk_auto_recovery_fails_fast_when_runtime_config_is_missing(tmp_path: Path) -> None:
    try:
        risk_auto_recovery.RiskAutoRecovery(workspace=tmp_path)
    except FileNotFoundError as exc:
        assert "runtime config not found" in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")
