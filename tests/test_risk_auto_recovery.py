from __future__ import annotations

import json
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
