from __future__ import annotations

import pytest

from src.core.models import MarketSeries
from src.risk.atr_trailing import ATRTrailingState, atr, update_atr_trailing


def _series(*, unsorted: bool) -> MarketSeries:
    base_ts = 1_710_000_000_000
    ts = [base_ts + i * 3_600_000 for i in range(15)]
    open_px = [100.0 + i for i in range(15)]
    high_px = [101.0 + i for i in range(15)]
    low_px = [99.0 + i for i in range(15)]
    close_px = [100.5 + i for i in range(15)]
    volume = [1.0 for _ in range(15)]

    if unsorted:
        for values in (ts, open_px, high_px, low_px, close_px, volume):
            latest = values.pop()
            values.insert(0, latest)

    return MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=ts,
        open=open_px,
        high=high_px,
        low=low_px,
        close=close_px,
        volume=volume,
    )


def test_atr_matches_sorted_series_when_input_is_unsorted() -> None:
    sorted_series = _series(unsorted=False)
    unsorted_series = _series(unsorted=True)

    assert atr(unsorted_series, n=14) == pytest.approx(atr(sorted_series, n=14))


def test_update_atr_trailing_uses_latest_bar_when_input_is_unsorted() -> None:
    sorted_series = _series(unsorted=False)
    unsorted_series = _series(unsorted=True)
    state = ATRTrailingState(highest_price=105.0, stop_price=0.0)

    sorted_state = update_atr_trailing(sorted_series, state, atr_mult=2.2, n=14)
    unsorted_state = update_atr_trailing(unsorted_series, state, atr_mult=2.2, n=14)

    assert unsorted_state.highest_price == pytest.approx(sorted_state.highest_price)
    assert unsorted_state.stop_price == pytest.approx(sorted_state.stop_price)
