from src.execution.okx_router import OKXOrderRouter


def test_router_limit_when_spread_tight():
    r = OKXOrderRouter(max_spread_pct=0.03)
    d = r.decide(best_bid=99, best_ask=101)
    assert d.order_type == "limit"


def test_router_market_when_spread_wide():
    r = OKXOrderRouter(max_spread_pct=0.0001)
    d = r.decide(best_bid=99, best_ask=101)
    assert d.order_type == "market"
    assert d.params.get("tgtCcy") == "quote_ccy"
