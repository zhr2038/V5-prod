from __future__ import annotations

from src.reporting.fill_trade_exporter import compute_slippage


def test_slippage_buy_positive_when_worse() -> None:
    bps, usdt, _, _ = compute_slippage(side="buy", fill_px=101.0, qty=2.0, bid=99.0, ask=101.0, mid=100.0)
    assert bps is not None and bps > 0
    assert usdt == 2.0


def test_slippage_sell_positive_when_worse() -> None:
    bps, usdt, _, _ = compute_slippage(side="sell", fill_px=99.0, qty=2.0, bid=99.0, ask=101.0, mid=100.0)
    assert bps is not None and bps > 0
    assert usdt == 2.0


def test_slippage_missing_mid_is_none() -> None:
    bps, usdt, _, _ = compute_slippage(side="buy", fill_px=101.0, qty=2.0, bid=None, ask=None, mid=None)
    assert bps is None
    assert usdt is None
