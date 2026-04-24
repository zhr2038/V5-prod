from __future__ import annotations

from datetime import datetime, timezone

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.position_store import Position


class _FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 4, 24, 0, 0, tzinfo=timezone.utc)


class _FakeStore:
    def __init__(self, positions):
        self._positions = positions
        self.calls = []

    def list(self):
        return list(self._positions)

    def mark_position(self, **kwargs):
        self.calls.append(kwargs)


def _pipe() -> V5Pipeline:
    pipe = V5Pipeline(AppConfig(symbols=["BTC/USDT"]))
    pipe.clock = _FixedClock()
    return pipe


def test_mark_to_market_uses_latest_bar_when_market_series_is_unsorted() -> None:
    pipe = _pipe()
    store = _FakeStore(
        [
            Position(
                symbol="BTC/USDT",
                qty=2.0,
                avg_px=100.0,
                entry_ts="2026-04-01T00:00:00Z",
                highest_px=120.0,
                last_update_ts="2026-04-24T00:00:00Z",
                last_mark_px=120.0,
                unrealized_pnl_pct=0.2,
            )
        ]
    )
    market_data = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[1_710_216_000_000, 1_710_212_400_000],
            open=[119.0, 80.0],
            high=[130.0, 81.0],
            low=[118.0, 79.0],
            close=[120.0, 80.0],
            volume=[1.0, 1.0],
        )
    }

    pipe.mark_to_market(store, market_data)

    assert len(store.calls) == 1
    assert store.calls[0]["mark_px"] == 120.0
    assert store.calls[0]["high_px"] == 130.0


def test_compute_equity_uses_latest_bar_when_market_series_is_unsorted() -> None:
    pipe = _pipe()
    positions = [
        Position(
            symbol="BTC/USDT",
            qty=2.0,
            avg_px=100.0,
            entry_ts="2026-04-01T00:00:00Z",
            highest_px=120.0,
            last_update_ts="2026-04-24T00:00:00Z",
            last_mark_px=120.0,
            unrealized_pnl_pct=0.2,
        )
    ]
    market_data = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[1_710_216_000_000, 1_710_212_400_000],
            open=[119.0, 80.0],
            high=[130.0, 81.0],
            low=[118.0, 79.0],
            close=[120.0, 80.0],
            volume=[1.0, 1.0],
        )
    }

    equity = pipe.compute_equity(50.0, positions, market_data)

    assert equity == 290.0
