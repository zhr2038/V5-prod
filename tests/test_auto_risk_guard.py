from __future__ import annotations

import json
import pytest

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
    assert saved["last_update"].endswith("Z")
    assert len(saved["history"]) == 1


def test_force_level_persists_utc_history_timestamp(tmp_path) -> None:
    state_path = tmp_path / "auto_risk_guard.json"
    guard = AutoRiskGuard(state_path=str(state_path))

    guard.force_level("DEFENSE", reason="test")

    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["last_update"].endswith("Z")
    assert saved["history"][-1]["ts"].endswith("Z")


@pytest.mark.parametrize("drawdown", [0.19, 0.12, 0.119, 0.08, 0.079, 0.05])
def test_protect_has_no_drawdown_band_shortcut(tmp_path, drawdown):
    guard = AutoRiskGuard(str(tmp_path / "guard.json"))
    guard.current_level = "PROTECT"
    level, _, _ = guard.evaluate(drawdown, 0.8, 0.0, "up")
    assert level == "PROTECT"


@pytest.mark.parametrize("old_level,new_level", [("PROTECT", "DEFENSE"), ("DEFENSE", "NEUTRAL")])
def test_no_opportunities_can_recover_only_below_original_drawdown_threshold(tmp_path, old_level, new_level):
    guard = AutoRiskGuard(str(tmp_path / "guard.json"))
    guard.current_level = old_level
    level, _, _ = guard.evaluate(0.049, None, 0.0, "flat", no_trade_opportunities=True)
    assert level == new_level


@pytest.mark.parametrize("rate,no_opportunities,evidence", [(0.0, False, True), (None, False, True), (None, True, False), (0.8, False, False)])
def test_failed_or_unknown_execution_does_not_authorize_recovery(tmp_path, rate, no_opportunities, evidence):
    guard = AutoRiskGuard(str(tmp_path / "guard.json"))
    guard.current_level = "PROTECT"
    level, _, _ = guard.evaluate(0.049, rate, 0.0, "flat", no_trade_opportunities=no_opportunities, recovery_evidence_ok=evidence)
    assert level == "PROTECT"


def test_missing_recovery_evidence_still_allows_protect_downgrade(tmp_path):
    guard = AutoRiskGuard(str(tmp_path / "guard.json"))
    level, _, _ = guard.evaluate(0.19, None, 0.0, "flat", recovery_evidence_ok=False)
    assert level == "PROTECT"


def test_displayed_position_caps_match_effective_runtime_caps():
    assert {name: level.max_positions for name, level in AutoRiskGuard.LEVELS.items()} == {
        "ATTACK": 8, "NEUTRAL": 5, "DEFENSE": 3, "PROTECT": 1,
    }
