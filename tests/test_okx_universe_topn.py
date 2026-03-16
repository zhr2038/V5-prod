from __future__ import annotations

from src.data.universe.okx_universe import OKXUniverseProvider


def test_universe_respects_top_n(monkeypatch):
    p = OKXUniverseProvider(min_24h_quote_volume_usdt=0.0, top_n=2)

    monkeypatch.setattr(
        p,
        "_fetch_instruments",
        lambda: [
            {"instId": "A-USDT", "baseCcy": "A", "quoteCcy": "USDT"},
            {"instId": "B-USDT", "baseCcy": "B", "quoteCcy": "USDT"},
            {"instId": "C-USDT", "baseCcy": "C", "quoteCcy": "USDT"},
        ],
    )
    # Quote volume differs
    monkeypatch.setattr(
        p,
        "_fetch_tickers",
        lambda: [
            {"instId": "A-USDT", "volCcy24h": "300"},
            {"instId": "B-USDT", "volCcy24h": "200"},
            {"instId": "C-USDT", "volCcy24h": "100"},
        ],
    )
    monkeypatch.setattr(p, "_load_cache", lambda now_ts: None)
    monkeypatch.setattr(p, "_save_cache", lambda now_ts, symbols: None)

    out = p.get_universe(now_ts=1.0)
    assert out == ["A/USDT", "B/USDT"]
