from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src.execution.position_store import PositionStore


def test_close_long_clears_runtime_risk_state_files() -> None:
    with tempfile.TemporaryDirectory() as td:
        store = PositionStore(path=f"{td}/shadow_positions.sqlite")
        store.upsert_buy("BTC/USDT", qty=1.0, px=100.0)

        runtime_files = [
            Path(td) / "shadow_stop_loss_state.json",
            Path(td) / "shadow_fixed_stop_loss_state.json",
            Path(td) / "shadow_profit_taking_state.json",
            Path(td) / "shadow_highest_px_state.json",
        ]
        for path in runtime_files:
            path.write_text(
                json.dumps({"BTC/USDT": {"source": "runtime"}, "ETH/USDT": {"source": "keep"}}),
                encoding="utf-8",
            )

        assert store.close_long("BTC/USDT") is True

        for path in runtime_files:
            payload = json.loads(path.read_text(encoding="utf-8"))
            assert "BTC/USDT" not in payload
            assert "ETH/USDT" in payload
