from __future__ import annotations

import base64
import hashlib
import hmac

from src.execution.okx_private_client import sign_okx


def _expected(secret: str, prehash: str) -> str:
    mac = hmac.new(secret.encode("utf-8"), prehash.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


def test_sign_okx_prehash_concat() -> None:
    secret = "testsecret"
    ts = "2020-12-08T09:08:57.715Z"
    method = "GET"
    path = "/api/v5/account/balance?ccy=USDT"
    body = ""
    got = sign_okx(api_secret=secret, timestamp=ts, method=method, request_path=path, body=body)
    assert got == _expected(secret, ts + method + path + body)


def test_sign_okx_body_empty_is_stable() -> None:
    secret = "s"
    ts = "2020-01-01T00:00:00.000Z"
    method = "POST"
    path = "/api/v5/trade/order"

    s1 = sign_okx(api_secret=secret, timestamp=ts, method=method, request_path=path, body="")
    s2 = sign_okx(api_secret=secret, timestamp=ts, method=method, request_path=path, body="")
    assert s1 == s2


def test_sign_okx_method_is_uppercased() -> None:
    secret = "abc"
    ts = "2020-01-01T00:00:00.000Z"
    method = "get"  # lower
    path = "/api/v5/account/balance"
    body = ""
    got = sign_okx(api_secret=secret, timestamp=ts, method=method, request_path=path, body=body)
    assert got == _expected(secret, ts + "GET" + path + body)
