from decimal import Decimal

import pytest

from src.research.quote_portfolio import QuotePortfolio


def row(*, now=101, buy_fee="USDT", sell_fee="USDT", lot="0.001", minimum="0.001"):
    return {"quote": {"bid": 100, "ask": 100, "ts": now}, "instrument": {
        "lot_size": lot, "minimum_qty": minimum, "minimum_notional_usdt": 1,
        "buy_fee_currency": buy_fee, "sell_fee_currency": sell_fee,
        "observed_ts": 1, "source_hash": "a" * 64}}


def intent(*, side="buy", key="entry", decision=100, notional=50):
    return {"intent_id": key, "symbol": "BNB/USDT", "side": side, "decision_ts": decision, "notional_usdt": notional}


def test_shared_quote_accounting_charges_each_fee_once():
    book = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=0)
    entry = book.fill(intent(), row(), now=101)
    assert entry["price"] == 100 and entry["quantity"] == Decimal("0.5")
    assert book.cash == Decimal("49.9500")
    book.fill(intent(side="sell", key="exit", decision=102), row(now=103), now=103)
    mark = book.mark({}, now=103)
    assert mark["net_equity_increment_usdt"] == Decimal("-0.1000")
    assert mark["fee_usdt"] == Decimal("0.1000")
    assert sum(lot["net_pnl_usdt"] for lot in book.closed_lots) == Decimal("-0.1000")


def test_base_fee_and_quantity_steps_retain_residual_dust_and_cost_basis():
    book = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=0)
    entry = book.fill(intent(), row(buy_fee="BNB"), now=101)
    assert entry["fee_currency"] == "BNB" and entry["fee_amount"] == Decimal("0.0005")
    assert book.positions["BNB/USDT"]["qty"] == Decimal("0.4995")
    book.fill(intent(side="sell", key="exit", decision=102), row(now=103), now=103)
    mark = book.mark({"BNB/USDT": row(now=103)}, now=103)
    assert book.positions["BNB/USDT"]["qty"] == Decimal("0.0005")
    assert mark["dust_positions"]["BNB/USDT"] == Decimal("0.0005")
    assert mark["liquidation_value_usdt"] == 0
    assert mark["equity_usdt"] == Decimal("99.8501")
    assert mark["unrealized_pnl_usdt"] < 0


def test_independent_books_do_not_share_cash_positions_fills_or_peak():
    books = [QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=5) for _ in range(2)]
    books[0].fill(intent(), row(), now=101)
    assert books[1].cash == 100 and books[1].positions == {} and books[1].fills == []


@pytest.mark.parametrize("change", [{"lot_size": "0"}, {"minimum_qty": "2"}, {"buy_fee_currency": "OKB"}, {"observed_ts": 102}])
def test_bad_instrument_or_unexecutable_quantity_cannot_fill(change):
    book = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=0)
    market = row()
    market["instrument"].update(change)
    with pytest.raises(ValueError):
        book.fill(intent(), market, now=101)
    assert book.cash == 100 and not book.fills


def test_next_quote_duplicate_and_out_of_order_rules():
    book = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=0)
    with pytest.raises(ValueError, match="subsequent"):
        book.fill(intent(), row(now=100), now=100)
    book.fill(intent(), row(), now=101)
    assert book.fill(intent(), row(), now=101) is None
    with pytest.raises(ValueError, match="out_of_order"):
        book.fill(intent(key="old", decision=90), row(now=99), now=99)


def test_stale_quote_never_produces_fictitious_liquidation_value():
    book = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=0)
    book.fill(intent(), row(), now=101)
    with pytest.raises(ValueError, match="stale"):
        book.mark({"BNB/USDT": row()}, now=222)


def test_rounding_cannot_exceed_cash_or_intent_notional():
    book = QuotePortfolio(initial_cash=10, fee_bps=10, slippage_bps=0)
    fill = book.fill(intent(notional=100), row(lot="0.03"), now=101)
    assert fill["quantity"] == Decimal("0.09") and book.cash >= 0
