from __future__ import annotations

from src.data.universe.okx_universe import OKXUniverseProvider, UniverseItem


def test_refine_single_ticker_reorders_by_single_ticker(monkeypatch):
    p = OKXUniverseProvider(min_24h_quote_volume_usdt=0.0, top_n=2, refine_with_single_ticker=True, refine_single_ticker_max_candidates=3, refine_single_ticker_sleep_sec=0.0)

    # Pretend batch-built items are sorted A>B>C
    items = [
        UniverseItem(symbol="A/USDT", inst_id="A-USDT", quote_volume_usdt_24h=300.0),
        UniverseItem(symbol="B/USDT", inst_id="B-USDT", quote_volume_usdt_24h=200.0),
        UniverseItem(symbol="C/USDT", inst_id="C-USDT", quote_volume_usdt_24h=100.0),
    ]

    def _fetch_ticker(inst_id: str):
        # Single-ticker says C is biggest
        if inst_id == "C-USDT":
            return {"instId": inst_id, "volCcy24h": "999"}
        if inst_id == "A-USDT":
            return {"instId": inst_id, "volCcy24h": "111"}
        if inst_id == "B-USDT":
            return {"instId": inst_id, "volCcy24h": "222"}
        return None

    monkeypatch.setattr(p, "_fetch_ticker", _fetch_ticker)

    out = p._refine_by_single_ticker(items)
    assert [x.symbol for x in out] == ["C/USDT", "B/USDT"]
