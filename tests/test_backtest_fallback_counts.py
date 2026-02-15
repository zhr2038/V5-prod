from __future__ import annotations

from src.backtest.backtest_engine import BacktestEngine
from src.core.models import MarketSeries, Order


class StubCostModel:
    def __init__(self):
        self.n = 0

    def resolve(self, symbol, regime, router_action, notional_usdt):
        self.n += 1
        if self.n <= 3:
            return 6.0, 10.0, {"fallback_level": "L0_exact"}
        return 6.0, 10.0, {"fallback_level": "L4_global"}


class StubPipeline:
    def run(self, market_data_1h, positions, cash_usdt, equity_peak_usdt):
        # always create one small buy so cost_model.resolve is exercised
        return type(
            "Out",
            (),
            {
                "orders": [
                    Order(
                        symbol="BTC/USDT",
                        side="buy",
                        intent="OPEN_LONG",
                        notional_usdt=10.0,
                        signal_price=float(market_data_1h["BTC/USDT"].close[-1]),
                        meta={},
                    )
                ],
                "regime": type("R", (), {"state": "Sideways"})(),
            },
        )()


def test_backtest_records_fallback_counts():
    n = 120
    closes = [100.0 + i for i in range(n)]
    md = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=list(range(n)),
            open=closes,
            high=closes,
            low=closes,
            close=closes,
            volume=[1e7] * n,
        )
    }

    bt = BacktestEngine(cost_model=StubCostModel(), cost_model_meta={"mode": "calibrated"})
    res = bt.run(md, pipeline=StubPipeline())
    fc = (res.cost_assumption or {}).get("fallback_level_counts") or {}
    assert fc.get("L0_exact", 0) > 0
    assert fc.get("L4_global", 0) > 0
