from __future__ import annotations

import json
from datetime import datetime, timezone

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


def test_collect_funding_sentiment_writes_utc_cache(monkeypatch, tmp_path) -> None:
    fixed_now = datetime(2026, 5, 25, 4, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(funding_mod, "_utc_now", lambda: fixed_now)
    monkeypatch.setattr(funding_mod, "get_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(
        funding_mod,
        "get_all_symbols",
        lambda: {
            "BTC-USDT": {
                "tier": "large",
                "tier_weight": 0.5,
                "weight_in_tier": 1.0,
                "total_weight": 1.0,
            }
        },
    )
    monkeypatch.setattr(
        funding_mod,
        "get_okx_funding_rate",
        lambda inst_id: {"funding_rate": 0.00012, "method": "okx_api"},
    )

    funding_mod.collect_funding_sentiment()

    symbol_file = tmp_path / "funding_BTC-USDT_20260525_04.json"
    composite_file = tmp_path / "funding_COMPOSITE_20260525_04.json"
    assert symbol_file.exists()
    assert composite_file.exists()
    assert json.loads(symbol_file.read_text(encoding="utf-8"))["collected_at"] == "2026-05-25T04:00:00Z"
    assert json.loads(composite_file.read_text(encoding="utf-8"))["collected_at"] == "2026-05-25T04:00:00Z"
