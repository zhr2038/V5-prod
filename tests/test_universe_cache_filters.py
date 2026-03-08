from __future__ import annotations

from src.data.universe.okx_universe import OKXUniverseProvider


def test_cached_universe_reapplies_blacklist_and_exclude_symbols(tmp_path):
    cache_path = tmp_path / "universe_cache.json"
    blacklist_path = tmp_path / "blacklist.json"
    blacklist_path.write_text('{"symbols": []}', encoding="utf-8")

    provider = OKXUniverseProvider(
        cache_path=str(cache_path),
        cache_ttl_sec=3600,
        top_n=10,
        min_24h_quote_volume_usdt=0.0,
        blacklist_path=str(blacklist_path),
        exclude_symbols=["XAUT/USDT"],
    )
    provider._save_cache(100.0, ["BTC/USDT", "XAUT/USDT", "ETH/USDT"])

    blacklist_path.write_text('{"symbols": ["ETH/USDT"]}', encoding="utf-8")
    out = provider.get_universe(now_ts=101.0)

    assert out == ["BTC/USDT"]


def test_universe_cache_invalidates_when_filter_signature_changes(tmp_path, monkeypatch):
    cache_path = tmp_path / "universe_cache.json"
    blacklist_path = tmp_path / "blacklist.json"
    blacklist_path.write_text('{"symbols": []}', encoding="utf-8")

    p1 = OKXUniverseProvider(
        cache_path=str(cache_path),
        cache_ttl_sec=3600,
        top_n=10,
        min_24h_quote_volume_usdt=1.0,
        blacklist_path=str(blacklist_path),
    )
    p1._save_cache(100.0, ["BTC/USDT"])

    p2 = OKXUniverseProvider(
        cache_path=str(cache_path),
        cache_ttl_sec=3600,
        top_n=10,
        min_24h_quote_volume_usdt=2.0,
        blacklist_path=str(blacklist_path),
    )
    called = {"instruments": 0, "tickers": 0}

    def fake_instruments():
        called["instruments"] += 1
        return []

    def fake_tickers():
        called["tickers"] += 1
        return []

    monkeypatch.setattr(p2, "_fetch_instruments", fake_instruments)
    monkeypatch.setattr(p2, "_fetch_tickers", fake_tickers)

    out = p2.get_universe(now_ts=101.0)

    assert out == []
    assert called["instruments"] == 1
    assert called["tickers"] == 1
