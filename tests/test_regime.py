from configs.schema import RegimeConfig, RegimeState
from src.regime.regime_engine import RegimeEngine
from src.core.models import MarketSeries


def _series(closes, highs=None, lows=None):
    highs = highs or closes
    lows = lows or closes
    n = len(closes)
    return MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=list(range(n)),
        open=closes,
        high=highs,
        low=lows,
        close=closes,
        volume=[1.0] * n,
    )


def test_sideways_low_atr():
    cfg = RegimeConfig(atr_threshold=0.02, atr_very_low=0.5)  # very high -> force sideways
    eng = RegimeEngine(cfg)
    s = _series([100.0 + 0.01 * i for i in range(80)])
    r = eng.detect(s)
    assert r.state == RegimeState.SIDEWAYS


def test_trending_when_ma_up_and_atr_high():
    cfg = RegimeConfig(atr_threshold=0.0001, atr_very_low=0.0000001)
    eng = RegimeEngine(cfg)
    closes = [100 + i for i in range(80)]
    highs = [c * 1.02 for c in closes]
    lows = [c * 0.98 for c in closes]
    r = eng.detect(_series(closes, highs=highs, lows=lows))
    assert r.state == RegimeState.TRENDING
