from __future__ import annotations

import scripts.collect_funding_sentiment as funding_mod


class _FakeResponse:
    def json(self):
        return {
            "code": "0",
            "data": [
                {
                    "fundingRate": "0.00012",
                    "nextFundingTime": "1770000000000",
                }
            ],
        }


def test_get_okx_funding_rate_uses_params(monkeypatch) -> None:
    captured = {}

    def fake_get(url, *, params=None, timeout=0, **kwargs):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(funding_mod.requests, "get", fake_get)

    result = funding_mod.get_okx_funding_rate("BTC-USDT-SWAP")

    assert result == {
        "funding_rate": 0.00012,
        "next_funding_time": "1770000000000",
        "method": "okx_api",
    }
    assert captured == {
        "url": "https://www.okx.com/api/v5/public/funding-rate",
        "params": {"instId": "BTC-USDT-SWAP"},
        "timeout": 10,
    }
