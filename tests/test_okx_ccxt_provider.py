import pytest

from src.data import okx_ccxt_provider
from src.data.okx_ccxt_provider import OKXCCXTProvider


class _FakeOKX:
    def __init__(self, *_args, **_kwargs):
        self.market_by_symbol = {}
        self.tickers = {}
        self.single_tickers = {}
        self.fail_fetch_tickers = False
        self.fail_symbols = set()
        self.fetch_ticker_calls = []

    def load_markets(self):
        return None

    def market(self, symbol):
        if symbol in self.market_by_symbol:
            return self.market_by_symbol[symbol]
        raise KeyError(symbol)

    def fetch_tickers(self, symbols):
        if self.fail_fetch_tickers:
            raise RuntimeError("batch failed")
        return dict(self.tickers)

    def fetch_ticker(self, symbol):
        self.fetch_ticker_calls.append(symbol)
        if symbol in self.fail_symbols:
            raise RuntimeError(f"{symbol} unavailable")
        return dict(self.single_tickers.get(symbol) or {})


def _provider(monkeypatch: pytest.MonkeyPatch) -> OKXCCXTProvider:
    monkeypatch.setattr(okx_ccxt_provider.ccxt, "okx", _FakeOKX)
    return OKXCCXTProvider()


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        ("BTC/USDT", "BTC-USDT"),
        ("BTC-USDT", "BTC-USDT"),
        ("BTCUSDT", "BTC-USDT"),
        ("OKX:BTC-USDT", "BTC-USDT"),
        ("okx:btc_usdt", "BTC-USDT"),
        ("BNBUSDT", "BNB-USDT"),
        ("OKX:BNB-USDT", "BNB-USDT"),
    ],
)
def test_symbol_to_inst_id_normalizes_symbol_variants(
    monkeypatch: pytest.MonkeyPatch,
    symbol: str,
    expected: str,
) -> None:
    provider = _provider(monkeypatch)

    assert provider._symbol_to_inst_id(symbol) == expected


def test_symbol_to_inst_id_prefers_ccxt_market_id(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _provider(monkeypatch)
    provider.ex.market_by_symbol["BTC/USDT"] = {"id": "BTC-USDT"}

    assert provider._symbol_to_inst_id("BTC/USDT") == "BTC-USDT"


def test_fetch_top_of_book_keeps_valid_quotes_when_batch_or_symbol_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _provider(monkeypatch)
    provider.ex.fail_fetch_tickers = True
    provider.ex.fail_symbols.add("BAD/USDT")
    provider.ex.single_tickers = {
        "BTC/USDT": {"bid": 100.0, "ask": 100.2, "timestamp": 1_700_000_000_000},
        "SOL/USDT": {"bid": 50.0, "ask": 50.2, "timestamp": 1_700_000_000_500},
    }
    monkeypatch.setattr(okx_ccxt_provider.time, "time", lambda: 1_700_000_001.0)

    quotes = provider.fetch_top_of_book(["BTC/USDT", "BAD/USDT", "SOL/USDT"])

    assert set(quotes) == {"BTC/USDT", "SOL/USDT"}
    assert quotes["BTC/USDT"]["mid"] == pytest.approx(100.1)
    assert quotes["BTC/USDT"]["arrival_mid"] == pytest.approx(100.1)
    assert quotes["BTC/USDT"]["quote_age_ms"] == 1000
    assert quotes["SOL/USDT"]["quote_ts"] == "2023-11-14T22:13:20.500000Z"


def test_history_ohlcv_excludes_unconfirmed_and_not_yet_closed_candles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _provider(monkeypatch)
    start = 1_700_000_000_000
    hour = 3_600_000
    rows = [
        [start, 100.0, 101.0, 99.0, 100.5, 10.0, 1],
        [start + hour, 101.0, 102.0, 100.0, 101.5, 11.0, 0],
        [start + 2 * hour, 102.0, 103.0, 101.0, 102.5, 12.0, None],
        [start + 3 * hour, 103.0, 104.0, 102.0, 103.5, 13.0, None],
        [start + 4 * hour, 104.0, 105.0, 103.0, 104.5, 14.0, 1],
    ]
    monkeypatch.setattr(
        provider,
        "_fetch_history_candles",
        lambda *args, **kwargs: rows,
    )

    series = provider._fetch_symbol_ohlcv(
        "BTC/USDT",
        "1h",
        limit=10,
        end_ts_ms=start + 4 * hour,
    )

    assert series.ts == [start, start + 2 * hour, start + 3 * hour]
    assert series.close == [100.5, 102.5, 103.5]
