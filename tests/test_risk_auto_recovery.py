from __future__ import annotations

import json

import scripts.risk_auto_recovery as risk_auto_recovery


def test_risk_auto_recovery_loads_config_from_workspace_reports(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    config_path = reports_dir / "risk_recovery_config.json"
    config_path.write_text(
        json.dumps({"enabled": False, "cooldown_hours": 12}),
        encoding="utf-8",
    )

    manager = risk_auto_recovery.RiskAutoRecovery(workspace=tmp_path)

    assert manager.reports_dir == reports_dir
    assert manager.config_file == config_path
    assert manager.config["enabled"] is False
    assert manager.config["cooldown_hours"] == 12


def test_risk_auto_recovery_creates_workspace_reports_when_saving_state(tmp_path) -> None:
    manager = risk_auto_recovery.RiskAutoRecovery(workspace=tmp_path)

    manager.config["enabled"] = False
    manager.save_config()
    success, _ = manager.execute_recovery("DEFENSE")

    assert success is True
    assert manager.config_file.exists()
    assert manager.risk_state_file.exists()
    assert json.loads(manager.config_file.read_text(encoding="utf-8"))["enabled"] is False
    state = json.loads(manager.risk_state_file.read_text(encoding="utf-8"))
    assert state["level"] == "DEFENSE"
    assert state["recovery_reason"] == "auto"
