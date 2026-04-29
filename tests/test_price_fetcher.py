from __future__ import annotations

from src.execution.price_fetcher import PriceFetcher


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def test_fetch_all_prices_does_not_return_stale_cache_on_okx_error(monkeypatch) -> None:
    fetcher = PriceFetcher()
    fetcher._prices = {"BTC/USDT": 100.0}

    def fake_get(*args, **kwargs):
        return _FakeResponse({"code": "50011", "msg": "rate limit", "data": []})

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    assert fetcher.fetch_all_prices() == {}


def test_fetch_all_prices_does_not_return_stale_cache_on_exception(monkeypatch) -> None:
    fetcher = PriceFetcher()
    fetcher._prices = {"BTC/USDT": 100.0}

    def fake_get(*args, **kwargs):
        raise TimeoutError("network timeout")

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    assert fetcher.fetch_all_prices() == {}
