from types import SimpleNamespace

from configs.schema import AppConfig
from src.backtest.backtest_engine import BacktestEngine
from src.core.models import MarketSeries, Order


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


def test_backtest_uses_configured_initial_equity():
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

    class CapturePipeline:
        def __init__(self):
            self.cash_seen = []
            self.clock = None

        def run(self, market_data_1h, positions, cash_usdt, equity_peak_usdt):
            self.cash_seen.append(float(cash_usdt))
            return SimpleNamespace(regime=SimpleNamespace(state="Trending"), orders=[])

    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.backtest.initial_equity_usdt = 50.0
    pipeline = CapturePipeline()

    BacktestEngine().run(md, pipeline=pipeline, cfg=cfg)

    assert pipeline.cash_seen
    assert pipeline.cash_seen[0] == 50.0


def test_backtest_turnover_is_normalized_by_initial_equity():
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

    class OneShotBuyPipeline:
        def __init__(self):
            self.done = False
            self.clock = None

        def run(self, market_data_1h, positions, cash_usdt, equity_peak_usdt):
            if self.done:
                return SimpleNamespace(regime=SimpleNamespace(state="Trending"), orders=[])
            self.done = True
            price = float(next(iter(market_data_1h.values())).close[-1])
            return SimpleNamespace(
                regime=SimpleNamespace(state="Trending"),
                orders=[
                    Order(
                        symbol="BTC/USDT",
                        side="buy",
                        intent="OPEN_LONG",
                        notional_usdt=float(cash_usdt),
                        signal_price=price,
                        meta={},
                    )
                ],
            )

    cfg_small = AppConfig(symbols=["BTC/USDT"])
    cfg_small.backtest.initial_equity_usdt = 10.0
    cfg_large = AppConfig(symbols=["BTC/USDT"])
    cfg_large.backtest.initial_equity_usdt = 100.0

    res_small = BacktestEngine(fee_bps=0.0, slippage_bps=0.0).run(md, pipeline=OneShotBuyPipeline(), cfg=cfg_small)
    res_large = BacktestEngine(fee_bps=0.0, slippage_bps=0.0).run(md, pipeline=OneShotBuyPipeline(), cfg=cfg_large)

    assert res_small.turnover == res_large.turnover


def test_backtest_partial_sell_keeps_remaining_position_value():
    n = 120
    closes = [100.0] * n
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

    class BuyThenHalfSellPipeline:
        def __init__(self):
            self.step = 0
            self.clock = None

        def run(self, market_data_1h, positions, cash_usdt, equity_peak_usdt):
            self.step += 1
            price = float(next(iter(market_data_1h.values())).close[-1])
            if self.step == 1:
                return SimpleNamespace(
                    regime=SimpleNamespace(state="Trending"),
                    orders=[
                        Order(
                            symbol="BTC/USDT",
                            side="buy",
                            intent="OPEN_LONG",
                            notional_usdt=100.0,
                            signal_price=price,
                            meta={},
                        )
                    ],
                )
            if self.step == 2:
                return SimpleNamespace(
                    regime=SimpleNamespace(state="Trending"),
                    orders=[
                        Order(
                            symbol="BTC/USDT",
                            side="sell",
                            intent="REBALANCE",
                            notional_usdt=50.0,
                            signal_price=price,
                            meta={},
                        )
                    ],
                )
            return SimpleNamespace(regime=SimpleNamespace(state="Trending"), orders=[])

    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.backtest.initial_equity_usdt = 100.0

    res = BacktestEngine(fee_bps=0.0, slippage_bps=0.0).run(md, pipeline=BuyThenHalfSellPipeline(), cfg=cfg)

    assert abs(res.max_dd) < 1e-9
    assert abs(res.cagr) < 1e-6
