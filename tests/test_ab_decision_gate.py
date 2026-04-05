from __future__ import annotations

import json
import sys
from pathlib import Path

import scripts.ab_decision_gate as ab_decision_gate


def test_ab_decision_gate_defaults_to_repo_reports_dir(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    fake_reports = fake_root / "reports"
    fake_runs = fake_reports / "runs"
    fake_run = fake_runs / "run-1"
    fake_run.mkdir(parents=True)
    (fake_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "counts": {"selected": 10, "orders_rebalance": 2, "orders_exit": 1},
                "router_decisions": [{"reason": "deadband", "drift": 0.035}],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(ab_decision_gate, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["ab_decision_gate.py"])

    ab_decision_gate.main()

    out_path = fake_reports / "ab_gate_status.json"
    assert out_path.exists()
    assert not (tmp_path / "reports" / "ab_gate_status.json").exists()

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["window_runs"] == 1
    assert payload["current"]["selected"] == 10
    assert payload["current"]["rebalance"] == 2
    assert payload["candidate"]["estimated_opened_from_deadband"] == 1
