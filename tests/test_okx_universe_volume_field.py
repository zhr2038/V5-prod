from __future__ import annotations

from src.data.universe.okx_universe import OKXUniverseProvider


def test_universe_uses_volCcy24h_when_volCcyQuote_missing(monkeypatch):
    p = OKXUniverseProvider(min_24h_quote_volume_usdt=100.0)

    monkeypatch.setattr(
        p,
        "_fetch_instruments",
        lambda: [
            {"instId": "BTC-USDT", "baseCcy": "BTC", "quoteCcy": "USDT"},
        ],
    )
    monkeypatch.setattr(
        p,
        "_fetch_tickers",
        lambda: [
            {"instId": "BTC-USDT", "volCcy24h": "200.0", "vol24h": "1", "last": "100"},
        ],
    )
    monkeypatch.setattr(p, "_load_cache", lambda now_ts: None)
    monkeypatch.setattr(p, "_save_cache", lambda now_ts, symbols: None)

    out = p.get_universe(now_ts=1.0)
    assert out == ["BTC/USDT"]


def test_universe_fallback_vol24h_times_last(monkeypatch):
    p = OKXUniverseProvider(min_24h_quote_volume_usdt=100.0)

    monkeypatch.setattr(
        p,
        "_fetch_instruments",
        lambda: [
            {"instId": "BTC-USDT", "baseCcy": "BTC", "quoteCcy": "USDT"},
        ],
    )
    monkeypatch.setattr(
        p,
        "_fetch_tickers",
        lambda: [
            {"instId": "BTC-USDT", "vol24h": "2", "last": "60"},
        ],
    )
    monkeypatch.setattr(p, "_load_cache", lambda now_ts: None)
    monkeypatch.setattr(p, "_save_cache", lambda now_ts, symbols: None)

    out = p.get_universe(now_ts=1.0)
    assert out == ["BTC/USDT"]
