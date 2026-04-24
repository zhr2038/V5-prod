from __future__ import annotations

from datetime import datetime, timezone

from src.core.clock import SystemClock
from src.core.models import MarketSeries
from src.execution.position_store import Position
from src.risk.exit_policy import ExitConfig, ExitPolicy


class _FixedClock(SystemClock):
    def __init__(self, now_dt: datetime):
        self._now = now_dt

    def now(self) -> datetime:
        return self._now


def test_exit_policy_regime_exit_uses_latest_bar_when_series_is_unsorted() -> None:
    policy = ExitPolicy(ExitConfig(), clock=_FixedClock(datetime(2026, 4, 24, tzinfo=timezone.utc)))
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
            high=[121.0, 82.0],
            low=[118.0, 79.0],
            close=[120.0, 80.0],
            volume=[1.0, 1.0],
        )
    }

    orders = policy.evaluate(positions, market_data, regime_state="Risk-Off")

    assert len(orders) == 1
    assert orders[0].signal_price == 120.0
    assert orders[0].notional_usdt == 240.0


def test_exit_policy_atr_trailing_uses_latest_bar_when_series_is_unsorted() -> None:
    policy = ExitPolicy(
        ExitConfig(enable_regime_exit=False, time_stop_days=20),
        clock=_FixedClock(datetime(2026, 4, 24, tzinfo=timezone.utc)),
    )
    positions = [
        Position(
            symbol="BTC/USDT",
            qty=1.0,
            avg_px=100.0,
            entry_ts="2026-03-01T00:00:00Z",
            highest_px=150.0,
            last_update_ts="2026-04-24T00:00:00Z",
            last_mark_px=120.0,
            unrealized_pnl_pct=0.0,
        )
    ]
    base_ts = 1_710_000_000_000
    sorted_ts = [base_ts + i * 3_600_000 for i in range(15)]
    sorted_close = [95.0 + i for i in range(15)]
    sorted_close[-1] = 110.0
    market_data = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[sorted_ts[-1], *sorted_ts[:-1]],
            open=[sorted_close[-1], *sorted_close[:-1]],
            high=[sorted_close[-1] + 1.0, *[value + 1.0 for value in sorted_close[:-1]]],
            low=[sorted_close[-1] - 1.0, *[value - 1.0 for value in sorted_close[:-1]]],
            close=[sorted_close[-1], *sorted_close[:-1]],
            volume=[1.0 for _ in sorted_close],
        )
    }

    orders = policy.evaluate(positions, market_data, regime_state="Sideways")

    assert len(orders) == 1
    assert orders[0].meta["reason"] == "atr_trailing"
    assert orders[0].signal_price == 110.0
