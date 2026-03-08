from __future__ import annotations

import httpx
import pytest

from configs.schema import ExchangeConfig
from src.execution.okx_private_client import OKXPrivateClient, OKXPrivateClientError
from src.utils.retry import RetryConfig


class FakeHTTPClient:
    def __init__(self, actions):
        self.actions = list(actions)
        self.calls = 0

    def request(self, *args, **kwargs):
        self.calls += 1
        action = self.actions.pop(0)
        if isinstance(action, Exception):
            raise action
        return action

    def close(self):
        return None


def _make_client() -> OKXPrivateClient:
    return OKXPrivateClient(
        ExchangeConfig(api_key="k", api_secret="s", passphrase="p"),
        retry_cfg=RetryConfig(max_attempts=3, base_delay_sec=0.0, max_delay_sec=0.0, jitter_frac=0.0),
    )


def _resp(payload: dict) -> httpx.Response:
    req = httpx.Request("POST", "https://www.okx.com/api/v5/trade/order")
    return httpx.Response(200, json=payload, request=req)


def test_place_order_transport_error_not_retried() -> None:
    client = _make_client()
    fake = FakeHTTPClient([httpx.ReadTimeout("slow upstream")])
    client._client = fake

    with pytest.raises(OKXPrivateClientError):
        client.place_order({"instId": "BTC-USDT", "tdMode": "cash", "side": "buy", "ordType": "market", "sz": "0.01"})

    assert fake.calls == 1


def test_place_order_retries_rate_limit_response() -> None:
    client = _make_client()
    fake = FakeHTTPClient(
        [
            _resp({"code": "50011", "msg": "rate limit", "data": []}),
            _resp({"code": "0", "msg": "", "data": [{"sCode": "0", "sMsg": "", "ordId": "123"}]}),
        ]
    )
    client._client = fake

    resp = client.place_order({"instId": "BTC-USDT", "tdMode": "cash", "side": "buy", "ordType": "market", "sz": "0.01"})

    assert fake.calls == 2
    assert resp.okx_code == "0"
    assert resp.data["data"][0]["ordId"] == "123"
