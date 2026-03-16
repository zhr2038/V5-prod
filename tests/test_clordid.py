from __future__ import annotations

import re

from src.execution.clordid import make_cl_ord_id


def test_clordid_stable_same_intent() -> None:
    a = make_cl_ord_id(
        run_id="20260215_120000",
        inst_id="BTC-USDT",
        intent="REBALANCE",
        decision_hash="abc" * 10,
        side="buy",
        ord_type="market",
        td_mode="cash",
    )
    b = make_cl_ord_id(
        run_id="20260215_120000",
        inst_id="BTC-USDT",
        intent="REBALANCE",
        decision_hash="abc" * 10,
        side="buy",
        ord_type="market",
        td_mode="cash",
    )
    assert a == b


def test_clordid_changes_on_decision_hash() -> None:
    a = make_cl_ord_id(
        run_id="r",
        inst_id="ETH-USDT",
        intent="OPEN_LONG",
        decision_hash="1" * 64,
        side="buy",
        ord_type="market",
        td_mode="cash",
    )
    b = make_cl_ord_id(
        run_id="r",
        inst_id="ETH-USDT",
        intent="OPEN_LONG",
        decision_hash="2" * 64,
        side="buy",
        ord_type="market",
        td_mode="cash",
    )
    assert a != b


def test_clordid_len_and_charset() -> None:
    cid = make_cl_ord_id(
        run_id="2026-02-15T12:00:00Z",
        inst_id="SOL-USDT",
        intent="REBALANCE",
        decision_hash="deadbeef" * 8,
        side="sell",
        ord_type="limit",
        td_mode="cash",
    )
    assert len(cid) <= 32
    assert re.match(r"^[A-Za-z0-9]+$", cid)
