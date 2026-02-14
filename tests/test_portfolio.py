from configs.schema import AlphaConfig, RiskConfig
from src.portfolio.portfolio_engine import PortfolioEngine
from src.core.models import MarketSeries


def test_portfolio_caps_single_weight():
    pe = PortfolioEngine(alpha_cfg=AlphaConfig(long_top_pct=0.5), risk_cfg=RiskConfig(max_single_weight=0.25))
    scores = {"A/USDT": 10.0, "B/USDT": 9.0, "C/USDT": 1.0, "D/USDT": 0.0}

    md = {}
    for s in scores.keys():
        md[s] = MarketSeries(symbol=s, timeframe="1h", ts=list(range(200)), open=[1.0]*200, high=[1.0]*200, low=[1.0]*200, close=[1.0 + i*0.0001 for i in range(200)], volume=[1000.0]*200)

    snap = pe.allocate(scores=scores, market_data=md, regime_mult=1.0)
    assert snap.target_weights
    assert all(w <= 0.25 + 1e-9 for w in snap.target_weights.values())
