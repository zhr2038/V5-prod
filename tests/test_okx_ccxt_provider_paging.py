from __future__ import annotations

from src.data.okx_ccxt_provider import OKXCCXTProvider


class _DummyExchange:
    def load_markets(self):
        return {}

    def market(self, symbol):
        return {"id": symbol.replace("/", "-")}

    def fetch_tickers(self, symbols):
        return {}

    def fetch_ticker(self, symbol):
        return {}


def test_fetch_ohlcv_paginates_and_respects_end_ts(monkeypatch):
    monkeypatch.setattr("src.data.okx_ccxt_provider.ccxt.okx", lambda *_args, **_kwargs: _DummyExchange())
    provider = OKXCCXTProvider(rate_limit=True)
    provider.max_ohlcv_batch = 3

    all_ts = [1_000, 2_000, 3_000, 4_000, 5_000, 6_000, 7_000, 8_000, 9_000]

    def fake_fetch(inst_id, timeframe, *, after_ms, limit):
        assert inst_id == "BTC-USDT"
        assert timeframe == "1h"
        selected = [ts for ts in reversed(all_ts) if ts < after_ms][:limit]
        return [[ts, 1.0, 2.0, 0.5, 1.5, 10.0] for ts in selected]

    monkeypatch.setattr(provider, "_fetch_history_candles", fake_fetch)
    out = provider.fetch_ohlcv(["BTC/USDT"], timeframe="1h", limit=5, end_ts_ms=7_600)

    assert out["BTC/USDT"].ts == [3_000, 4_000, 5_000, 6_000, 7_000]
