from __future__ import annotations

import json

import pytest

from configs.schema import ExecutionConfig
from src.core.models import Order
from src.execution.account_store import AccountState, AccountStore
from src.execution.execution_engine import ExecutionEngine
from src.execution.position_store import PositionStore


class _TradeLogRecorder:
    def __init__(self):
        self.fills = []

    def append_fill(self, fill):
        self.fills.append(fill)


def _build_engine(tmp_path, *, cfg_kwargs=None, trade_log=None, run_id="test_run"):
    db_path = tmp_path / "positions.sqlite"
    cfg = ExecutionConfig(slippage_db_path=str(tmp_path / "slippage.sqlite"), **(cfg_kwargs or {}))
    pos_store = PositionStore(str(db_path))
    acc_store = AccountStore(str(db_path))
    acc_store.set(AccountState(cash_usdt=0.0, equity_peak_usdt=1_000.0, scale_basis_usdt=0.0))
    engine = ExecutionEngine(
        cfg,
        position_store=pos_store,
        account_store=acc_store,
        trade_log=trade_log,
        run_id=run_id,
    )
    return engine, pos_store, acc_store


def test_dry_run_rebalance_sell_keeps_remaining_position_with_zero_costs(tmp_path) -> None:
    trade_log = _TradeLogRecorder()
    engine, pos_store, acc_store = _build_engine(
        tmp_path,
        cfg_kwargs={"fee_bps": 0.0, "slippage_bps": 0.0},
        trade_log=trade_log,
        run_id="unit-test",
    )
    pos_store.upsert_buy("BTC/USDT", qty=1.0, px=100.0, now_ts="2026-03-20T00:00:00Z")

    engine.execute(
        [
            Order(
                symbol="BTC/USDT",
                side="sell",
                intent="REBALANCE",
                notional_usdt=40.0,
                signal_price=100.0,
                meta={},
            )
        ]
    )

    remaining = pos_store.get("BTC/USDT")
    account = acc_store.get()

    assert remaining is not None
    assert remaining.qty == pytest.approx(0.6)
    assert account.cash_usdt == pytest.approx(40.0)
    assert len(trade_log.fills) == 1
    assert trade_log.fills[0].qty == pytest.approx(0.4)
    assert trade_log.fills[0].notional_usdt == pytest.approx(40.0)


def test_rebalance_sell_reduces_position_instead_of_full_close(tmp_path) -> None:
    engine, pos_store, acc_store = _build_engine(tmp_path)
    pos_store.upsert_buy("BTC/USDT", qty=10.0, px=100.0, now_ts="2026-04-04T00:00:00Z")

    engine.execute(
        [
            Order(
                symbol="BTC/USDT",
                side="sell",
                intent="REBALANCE",
                notional_usdt=200.0,
                signal_price=100.0,
                meta={},
            )
        ]
    )

    remaining = pos_store.get("BTC/USDT")
    account = acc_store.get()

    assert remaining is not None
    assert remaining.qty == pytest.approx(8.0)
    assert account.cash_usdt == pytest.approx(199.78)


def test_rebalance_sell_caps_to_local_position_for_cash_and_fees(tmp_path) -> None:
    engine, pos_store, acc_store = _build_engine(tmp_path)
    pos_store.upsert_buy("BTC/USDT", qty=1.0, px=100.0, now_ts="2026-04-04T00:00:00Z")

    engine.execute(
        [
            Order(
                symbol="BTC/USDT",
                side="sell",
                intent="REBALANCE",
                notional_usdt=200.0,
                signal_price=100.0,
                meta={},
            )
        ]
    )

    account = acc_store.get()

    assert pos_store.get("BTC/USDT") is None
    assert account.cash_usdt == pytest.approx(99.89)


def test_execution_engine_writes_cost_events_to_runtime_dir(tmp_path) -> None:
    trade_log = _TradeLogRecorder()
    engine, _, _ = _build_engine(
        tmp_path,
        cfg_kwargs={
            "order_store_path": str(tmp_path / "shadow_orders.sqlite"),
            "fee_bps": 0.0,
            "slippage_bps": 0.0,
        },
        trade_log=trade_log,
        run_id="shadow-run",
    )

    engine.execute(
        [
            Order(
                symbol="BTC/USDT",
                side="buy",
                intent="OPEN_LONG",
                notional_usdt=50.0,
                signal_price=100.0,
                meta={"window_start_ts": 1700000000, "window_end_ts": 1700003600, "regime": "Trending"},
            )
        ]
    )

    runtime_dir = tmp_path / "shadow_cost_events"
    files = list(runtime_dir.glob("*.jsonl"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text(encoding="utf-8").splitlines()[0])
    assert payload["run_id"] == "shadow-run"
    assert payload["symbol"] == "BTC/USDT"
    assert not (tmp_path / "cost_events").exists()
