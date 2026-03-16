from src.data.universe.okx_universe import OKXUniverseProvider


def test_universe_build_filters_basic():
    p = OKXUniverseProvider(min_24h_quote_volume_usdt=5.0, exclude_stablecoins=True)

    instruments = [
        {"instId": "AAA-USDT", "baseCcy": "AAA", "quoteCcy": "USDT"},
        {"instId": "USDC-USDT", "baseCcy": "USDC", "quoteCcy": "USDT"},
        {"instId": "BBB-BTC", "baseCcy": "BBB", "quoteCcy": "BTC"},
    ]
    tickers = [
        {"instId": "AAA-USDT", "volCcyQuote": "10"},
        {"instId": "USDC-USDT", "volCcyQuote": "999999"},
        {"instId": "BBB-BTC", "volCcyQuote": "999"},
    ]

    items = p._build(instruments, tickers)  # type: ignore
    syms = [x.symbol for x in items]
    assert "AAA/USDT" in syms
    assert "USDC/USDT" not in syms  # stablecoin excluded


def test_universe_cache_roundtrip(tmp_path):
    cache = tmp_path / "u.json"
    p = OKXUniverseProvider(cache_path=str(cache), cache_ttl_sec=3600)
    p._save_cache(100.0, ["AAA/USDT"])  # type: ignore
    got = p._load_cache(200.0)  # type: ignore
    assert got == ["AAA/USDT"]

    # expired
    p2 = OKXUniverseProvider(cache_path=str(cache), cache_ttl_sec=50)
    assert p2._load_cache(200.0) is None  # type: ignore
