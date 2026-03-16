from __future__ import annotations

import json

import pytest

from src.data.okx_instruments import OKXSpotInstrumentsCache


def _write_cache(path, *, ts: float) -> None:
    payload = {
        "ts": ts,
        "data": [
            {
                "instId": "BTC-USDT",
                "baseCcy": "BTC",
                "quoteCcy": "USDT",
                "minSz": "0.00001",
                "lotSz": "0.00001",
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_get_spec_uses_fresh_cache_without_fetch(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "okx_spot_instruments.json"
    _write_cache(cache_path, ts=1_700_000_000.0)

    monkeypatch.setattr("src.data.okx_instruments.time.time", lambda: 1_700_000_100.0)

    def _unexpected_fetch(self):
        raise AssertionError("fetch should not be called when cache is fresh")

    monkeypatch.setattr(OKXSpotInstrumentsCache, "_fetch", _unexpected_fetch)

    cache = OKXSpotInstrumentsCache(cache_path=str(cache_path), ttl_sec=3600)
    spec = cache.get_spec("BTC-USDT")

    assert spec is not None
    assert spec.inst_id == "BTC-USDT"
    assert spec.min_sz == 0.00001


def test_get_spec_falls_back_to_stale_cache_when_fetch_fails(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "okx_spot_instruments.json"
    _write_cache(cache_path, ts=1_700_000_000.0)

    monkeypatch.setattr("src.data.okx_instruments.time.time", lambda: 1_700_100_000.0)

    def _fail_fetch(self):
        raise RuntimeError("network down")

    monkeypatch.setattr(OKXSpotInstrumentsCache, "_fetch", _fail_fetch)

    cache = OKXSpotInstrumentsCache(cache_path=str(cache_path), ttl_sec=3600)
    spec = cache.get_spec("BTC-USDT")

    assert spec is not None
    assert spec.inst_id == "BTC-USDT"
    assert spec.lot_sz == 0.00001


def test_get_spec_raises_when_fetch_fails_without_cache(tmp_path, monkeypatch) -> None:
    def _fail_fetch(self):
        raise RuntimeError("network down")

    monkeypatch.setattr(OKXSpotInstrumentsCache, "_fetch", _fail_fetch)

    cache = OKXSpotInstrumentsCache(cache_path=str(tmp_path / "missing.json"), ttl_sec=3600)
    with pytest.raises(RuntimeError, match="network down"):
        cache.get_spec("BTC-USDT")
