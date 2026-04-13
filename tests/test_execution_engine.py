from __future__ import annotations

import json
import tempfile
from pathlib import Path

from configs.schema import ExecutionConfig
from src.core.models import Order
from src.execution.account_store import AccountStore
from src.execution.execution_engine import ExecutionEngine
from src.execution.position_store import PositionStore


def test_dry_run_profit_partial_records_take_profit_cooldown() -> None:
    with tempfile.TemporaryDirectory() as td:
        cfg = ExecutionConfig(
            order_store_path=f"{td}/shadow_orders.sqlite",
            slippage_db_path=f"{td}/shadow_slippage.sqlite",
        )
        pos = PositionStore(path=f"{td}/shadow_positions.sqlite")
        pos.upsert_buy("BTC/USDT", qty=2.0, px=100.0)
        acc = AccountStore(path=f"{td}/shadow_positions.sqlite")
        eng = ExecutionEngine(cfg, position_store=pos, account_store=acc, run_id="r")

        eng.execute(
            [
                Order(
                    symbol="BTC/USDT",
                    side="sell",
                    intent="REBALANCE",
                    notional_usdt=50.0,
                    signal_price=100.0,
                    meta={"reason": "profit_partial_peak_drawdown_8pct_retrace_2_5pct"},
                )
            ]
        )

        state_path = Path(td) / "shadow_take_profit_cooldown_state.json"
        assert state_path.exists()
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        assert "BTC/USDT" in payload
        assert payload["BTC/USDT"]["reason"] == "profit_partial_peak_drawdown_8pct_retrace_2_5pct"


def test_dry_run_full_close_clears_runtime_risk_state_files() -> None:
    with tempfile.TemporaryDirectory() as td:
        cfg = ExecutionConfig(
            order_store_path=f"{td}/shadow_orders.sqlite",
            slippage_db_path=f"{td}/shadow_slippage.sqlite",
        )
        pos = PositionStore(path=f"{td}/shadow_positions.sqlite")
        pos.upsert_buy("BTC/USDT", qty=1.0, px=100.0)
        acc = AccountStore(path=f"{td}/shadow_positions.sqlite")
        eng = ExecutionEngine(cfg, position_store=pos, account_store=acc, run_id="r")

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

        eng.execute(
            [
                Order(
                    symbol="BTC/USDT",
                    side="sell",
                    intent="CLOSE_LONG",
                    notional_usdt=100.0,
                    signal_price=100.0,
                    meta={"reason": "profit_taking_take_profit_10pct"},
                )
            ]
        )

        for path in runtime_files:
            payload = json.loads(path.read_text(encoding="utf-8"))
            assert "BTC/USDT" not in payload
            assert "ETH/USDT" in payload
