from __future__ import annotations

import json

from src.risk.auto_risk_guard import AutoRiskGuard


def test_evaluate_persists_metrics_when_level_does_not_change(tmp_path) -> None:
    state_path = tmp_path / "auto_risk_guard.json"
    state_path.write_text(
        json.dumps(
            {
                "current_level": "PROTECT",
                "metrics": {
                    "last_dd_pct": 0.10,
                    "last_conversion_rate": 0.50,
                },
                "history": [
                    {
                        "ts": "2026-04-26T01:00:00",
                        "from": "DEFENSE",
                        "to": "PROTECT",
                        "reason": "existing transition",
                    }
                ],
                "last_update": "2026-04-26T01:00:00",
            }
        ),
        encoding="utf-8",
    )

    guard = AutoRiskGuard(state_path=str(state_path))
    level, _, _ = guard.evaluate(
        dd_pct=0.19,
        conversion_rate=0.0,
        dust_reject_rate=0.0,
        recent_pnl_trend="flat",
        consecutive_losses=0,
    )

    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert level == "PROTECT"
    assert saved["current_level"] == "PROTECT"
    assert saved["metrics"]["last_dd_pct"] == 0.19
    assert saved["metrics"]["last_conversion_rate"] == 0.0
    assert saved["last_update"] != "2026-04-26T01:00:00"
    assert len(saved["history"]) == 1
