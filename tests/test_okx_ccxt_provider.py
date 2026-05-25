import pytest

from src.data import okx_ccxt_provider
from src.data.okx_ccxt_provider import OKXCCXTProvider


class _FakeOKX:
    def __init__(self, *_args, **_kwargs):
        self.market_by_symbol = {}

    def load_markets(self):
        return None

    def market(self, symbol):
        if symbol in self.market_by_symbol:
            return self.market_by_symbol[symbol]
        raise KeyError(symbol)


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
