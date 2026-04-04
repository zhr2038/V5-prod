from __future__ import annotations

import pytest

from configs.schema import ExecutionConfig
from src.core.models import Order
from src.execution.account_store import AccountState, AccountStore
from src.execution.execution_engine import ExecutionEngine
from src.execution.position_store import PositionStore


def _build_engine(tmp_path):
    db_path = tmp_path / "positions.sqlite"
    cfg = ExecutionConfig(slippage_db_path=str(tmp_path / "slippage.sqlite"))
    pos_store = PositionStore(str(db_path))
    acc_store = AccountStore(str(db_path))
    acc_store.set(AccountState(cash_usdt=0.0, equity_peak_usdt=1_000.0, scale_basis_usdt=0.0))
    engine = ExecutionEngine(cfg, position_store=pos_store, account_store=acc_store, run_id="test_run")
    return engine, pos_store, acc_store


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
