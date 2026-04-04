from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from src.execution.order_arbitrator import arbitrate_orders


def test_exit_pending_expiry_keeps_still_held_symbol_out_of_cooldown(tmp_path: Path) -> None:
    state_path = tmp_path / "order_state_machine.json"
    state_path.write_text(
        json.dumps(
            {
                "version": 1,
                "symbols": {
                    "MON/USDT": {
                        "state": "EXIT_PENDING",
                        "cooldown_until_ms": 9_999_999_999_999,
                        "exit_pending_until_ms": 1,
                        "last_run_id": "20260404_13",
                        "last_reason": "close_dominates",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    positions = [SimpleNamespace(symbol="MON/USDT", qty=12.0)]
    selected, decisions = arbitrate_orders(
        orders=[],
        positions=positions,
        run_id="20260404_14",
        cooldown_minutes=10,
        state_path=str(state_path),
    )

    assert selected == []
    assert decisions == []

    state = json.loads(state_path.read_text(encoding="utf-8"))
    mon = state["symbols"]["MON/USDT"]
    assert mon["state"] == "LONG"
    assert mon["cooldown_until_ms"] == 0
    assert mon["exit_pending_until_ms"] == 0
    assert mon["last_reason"] == "exit_pending_expired_still_held"
