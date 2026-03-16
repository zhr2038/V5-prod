from src.risk.atr_trailing import update_atr_trailing
from src.core.models import MarketSeries


def test_atr_trailing_updates_high():
    s = MarketSeries(
        symbol="X/USDT",
        timeframe="1h",
        ts=list(range(40)),
        open=[1.0]*40,
        high=[1.1]*40,
        low=[0.9]*40,
        close=[1.0 + 0.01*i for i in range(40)],
        volume=[1.0]*40,
    )
    st1 = update_atr_trailing(s, None)
    assert st1.highest_price == s.close[-1]
    s2 = s
    s2.close[-1] = s.close[-1] + 1.0
    st2 = update_atr_trailing(s2, st1)
    assert st2.highest_price == s2.close[-1]
