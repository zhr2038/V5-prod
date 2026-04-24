from __future__ import annotations

from types import SimpleNamespace

from src.backtest.backtest_engine import BacktestEngine, _BacktestClock
from src.core.models import MarketSeries


def test_backtest_engine_uses_latest_slice_timestamp_when_series_is_unsorted() -> None:
    captured: dict[str, int] = {}

    class FakePipeline:
        def __init__(self) -> None:
            self.clock = _BacktestClock()

        def run(self, market_data, **kwargs):
            if "clock_ts_ms" not in captured:
                captured["clock_ts_ms"] = int(self.clock.now().timestamp() * 1000)
            return SimpleNamespace(
                regime=SimpleNamespace(state="SIDEWAYS"),
                orders=[],
            )

    base_ts = 1_710_000_000_000
    unsorted_ts = [base_ts + i * 3_600_000 for i in range(80)]
    unsorted_ts[0], unsorted_ts[60] = unsorted_ts[60], unsorted_ts[0]
    closes = [100.0 + i for i in range(80)]
    market_data = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=unsorted_ts,
            open=closes,
            high=[value + 1.0 for value in closes],
            low=[value - 1.0 for value in closes],
            close=closes,
            volume=[1.0 for _ in closes],
        )
    }

    cfg = SimpleNamespace(backtest=SimpleNamespace(initial_equity_usdt=100.0))
    engine = BacktestEngine()

    result = engine.run(market_data, pipeline=FakePipeline(), cfg=cfg)

    assert result.turnover == 0.0
    assert captured["clock_ts_ms"] == base_ts + 60 * 3_600_000
