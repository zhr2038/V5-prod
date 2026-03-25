from __future__ import annotations

from main import _merge_managed_symbols, _validate_market_data_snapshot
from src.core.models import MarketSeries


def _series(symbol: str, count: int = 3) -> MarketSeries:
    vals = [float(i + 1) for i in range(count)]
    return MarketSeries(
        symbol=symbol,
        timeframe="1h",
        ts=[i * 1000 for i in range(count)],
        open=vals,
        high=vals,
        low=vals,
        close=vals,
        volume=vals,
    )


def test_market_data_validation_requires_btc_when_enabled():
    ok, reason, valid = _validate_market_data_snapshot(
        symbols=["BTC/USDT", "ETH/USDT"],
        market_data={"ETH/USDT": _series("ETH/USDT")},
        require_symbol="BTC/USDT",
        min_coverage_ratio=0.5,
    )

    assert not ok
    assert "BTC/USDT" in reason
    assert "ETH/USDT" in valid


def test_market_data_validation_enforces_coverage_ratio():
    ok, reason, _valid = _validate_market_data_snapshot(
        symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"],
        market_data={
            "BTC/USDT": _series("BTC/USDT"),
            "ETH/USDT": _series("ETH/USDT"),
        },
        require_symbol="BTC/USDT",
        min_coverage_ratio=0.75,
    )

    assert not ok
    assert "coverage too low" in reason


def test_market_data_validation_filters_empty_series():
    ok, reason, valid = _validate_market_data_snapshot(
        symbols=["BTC/USDT", "ETH/USDT"],
        market_data={
            "BTC/USDT": _series("BTC/USDT"),
            "ETH/USDT": MarketSeries(
                symbol="ETH/USDT",
                timeframe="1h",
                ts=[],
                open=[],
                high=[],
                low=[],
                close=[],
                volume=[],
            ),
        },
        require_symbol="BTC/USDT",
        min_coverage_ratio=0.5,
    )

    assert ok
    assert reason == ""
    assert list(valid.keys()) == ["BTC/USDT"]


def test_merge_managed_symbols_keeps_base_and_adds_held_without_duplicates():
    managed = _merge_managed_symbols(
        ["BTC/USDT", "ETH/USDT", "HYPE/USDT"],
        ["OKB/USDT", "ETH/USDT", "ADA/USDT"],
    )

    assert managed == ["BTC/USDT", "ETH/USDT", "HYPE/USDT", "OKB/USDT", "ADA/USDT"]
