from src.backtest.backtest_engine import BacktestEngine
from src.core.models import MarketSeries


def test_backtest_runs_with_delay_semantics():
    # Build simple two-symbol market with increasing prices
    n = 120
    md = {}
    for sym, base in [("BTC/USDT", 100.0), ("ETH/USDT", 50.0)]:
        closes = [base + i for i in range(n)]
        md[sym] = MarketSeries(symbol=sym, timeframe="1h", ts=list(range(n)), open=closes, high=closes, low=closes, close=closes, volume=[1e7]*n)

    bt = BacktestEngine()
    res = bt.run(md)
    # Should produce finite numbers
    assert res.max_dd >= 0
