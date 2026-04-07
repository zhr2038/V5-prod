from __future__ import annotations

import asyncio
import json

from scripts.alert_manager import AlertManager


def test_alert_manager_anchors_state_to_workspace_reports(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    state_file = reports_dir / "alert_state.json"
    state_file.write_text(
        json.dumps({"last_alerts": {"MEDIUM:no_trades": "2026-04-07T00:00:00"}}),
        encoding="utf-8",
    )

    manager = AlertManager(workspace=tmp_path)

    assert manager.alert_state_file == state_file
    assert manager.state["last_alerts"]["MEDIUM:no_trades"] == "2026-04-07T00:00:00"


def test_alert_manager_creates_workspace_reports_dir_when_recording_alert(tmp_path) -> None:
    manager = AlertManager(workspace=tmp_path)

    assert not manager.alert_state_file.exists()

    result = asyncio.run(manager.send_alert("CRITICAL", "title", "message", alert_type="kill_switch"))

    assert result is True
    assert manager.alert_state_file.exists()
    payload = json.loads(manager.alert_state_file.read_text(encoding="utf-8"))
    assert "CRITICAL:kill_switch" in payload["last_alerts"]
