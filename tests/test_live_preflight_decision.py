from __future__ import annotations

from src.execution.live_preflight import LivePreflightResult


def test_preflight_result_shape() -> None:
    r = LivePreflightResult(decision="ALLOW", reconcile_ok=True, ledger_ok=True, kill_switch_enabled=False)
    assert r.decision in {"ALLOW", "SELL_ONLY", "ABORT"}
