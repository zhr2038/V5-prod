from __future__ import annotations

import json
from pathlib import Path

from src.core.models import Order
from src.execution.order_arbitrator import arbitrate_orders


class _Pos:
    def __init__(self, symbol: str, qty: float):
        self.symbol = symbol
        self.qty = qty


def test_take_profit_cooldown_blocks_rebalance_buy(tmp_path, monkeypatch):
    state_path = tmp_path / "order_state_machine.json"
    tp_path = tmp_path / "take_profit_cooldown_state.json"
    tp_path.write_text(
        json.dumps({"BTC/USDT": {"last_take_profit_ts_ms": 950_000, "reason": "profit_partial"}}),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.execution.order_arbitrator._now_ms", lambda: 1_000_000)

    selected, decisions = arbitrate_orders(
        orders=[
            Order(
                symbol="BTC/USDT",
                side="buy",
                intent="REBALANCE",
                notional_usdt=10.0,
                signal_price=100.0,
                meta={"reason": "rebalance_topup"},
            )
        ],
        positions=[_Pos("BTC/USDT", 1.0)],
        run_id="r",
        cooldown_minutes=10,
        state_path=str(state_path),
        take_profit_cooldown_minutes=10,
        take_profit_cooldown_state_path=str(tp_path),
    )

    assert selected == []
    assert decisions
    assert decisions[0]["code"] == "ARB_BLOCKED_BY_TAKE_PROFIT_COOLDOWN"


def test_take_profit_cooldown_does_not_block_after_expiry(tmp_path, monkeypatch):
    state_path = tmp_path / "order_state_machine.json"
    tp_path = tmp_path / "take_profit_cooldown_state.json"
    tp_path.write_text(
        json.dumps({"BTC/USDT": {"last_take_profit_ts_ms": 1, "reason": "profit_partial"}}),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.execution.order_arbitrator._now_ms", lambda: 1_000_000)

    selected, decisions = arbitrate_orders(
        orders=[
            Order(
                symbol="BTC/USDT",
                side="buy",
                intent="REBALANCE",
                notional_usdt=10.0,
                signal_price=100.0,
                meta={"reason": "rebalance_topup"},
            )
        ],
        positions=[_Pos("BTC/USDT", 1.0)],
        run_id="r",
        cooldown_minutes=10,
        state_path=str(state_path),
        take_profit_cooldown_minutes=10,
        take_profit_cooldown_state_path=str(tp_path),
    )

    assert len(selected) == 1
    assert decisions == []
