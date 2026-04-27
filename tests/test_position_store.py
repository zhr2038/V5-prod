from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src.execution.fill_store import derive_runtime_named_json_path
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


def test_prune_orphan_risk_state_keeps_held_symbols_only() -> None:
    with tempfile.TemporaryDirectory() as td:
        store = PositionStore(path=f"{td}/shadow_positions.sqlite")
        store.upsert_buy("ETH/USDT", qty=1.0, px=100.0)

        runtime_files = [
            Path(td) / "shadow_stop_loss_state.json",
            Path(td) / "shadow_fixed_stop_loss_state.json",
            Path(td) / "shadow_profit_taking_state.json",
            Path(td) / "shadow_highest_px_state.json",
        ]
        for path in runtime_files:
            path.write_text(
                json.dumps(
                    {
                        "BTC/USDT": {"source": "stale"},
                        "ETH/USDT": {"source": "held"},
                    }
                ),
                encoding="utf-8",
            )

        summary = store.prune_orphan_risk_state()

        assert summary
        for path in runtime_files:
            payload = json.loads(path.read_text(encoding="utf-8"))
            assert "BTC/USDT" not in payload
            assert payload["ETH/USDT"]["source"] == "held"


def test_close_long_preserves_market_impulse_cooldown_without_active_marker() -> None:
    with tempfile.TemporaryDirectory() as td:
        store = PositionStore(path=f"{td}/shadow_positions.sqlite")
        store.upsert_buy("BTC/USDT", qty=1.0, px=100.0)

        state_path = derive_runtime_named_json_path(Path(td) / "shadow_orders.sqlite", "market_impulse_probe_state")
        state_path.write_text(
            json.dumps(
                {
                    "BTC/USDT": {
                        "symbol": "BTC/USDT",
                        "entry_ts_ms": 1_700_000_000_000,
                        "entry_ts": "2023-11-14T22:13:20Z",
                        "cooldown_until_ms": 1_700_028_800_000,
                        "cooldown_until": "2023-11-15T06:13:20Z",
                        "time_stop_hours": 4,
                    }
                }
            ),
            encoding="utf-8",
        )

        assert store.close_long("BTC/USDT") is True

        payload = json.loads(state_path.read_text(encoding="utf-8"))["BTC/USDT"]
        assert payload["cooldown_until_ms"] == 1_700_028_800_000
        assert payload["active_position"] is False
        assert "entry_ts_ms" not in payload
        assert "time_stop_hours" not in payload
