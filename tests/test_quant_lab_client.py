from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.quant_lab_client.client import QuantLabClient
from src.quant_lab_client.exceptions import QuantLabValidationError


class _Response:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _HTTP:
    def __init__(self) -> None:
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"method": "GET", "url": url, "params": params, "headers": headers, "timeout": timeout})
        if url.endswith("/v1/health"):
            return _Response({"status": "ok", "service": "quant-lab", "mode": "read-only"})
        if url.endswith("/v1/risk/live-permission"):
            return _Response({"strategy": "v5", "version": "5.0.0", "permission": "SELL_ONLY", "reasons": ["required_alpha_gate_quarantine"]})
        if url.endswith("/v1/costs/estimate"):
            return _Response({"symbol": "BTC-USDT", "regime": "normal", "notional_usdt": 200, "quantile": "p75", "total_cost_bps": 1.2, "source": "public_spread_proxy"})
        return _Response({"alpha_id": "v5", "status": "QUARANTINE", "passed": False})


def test_quant_lab_client_uses_get_and_redacts_token(tmp_path: Path) -> None:
    http = _HTTP()
    log_path = tmp_path / "requests.jsonl"
    client = QuantLabClient(
        base_url="https://quant-lab.local",
        api_token="super-secret-token",
        http_client=http,
        request_log_path=log_path,
        run_id="run-1",
    )

    health = client.get_health()
    permission = client.get_live_permission(strategy="v5", version="5.0.0")
    cost = client.estimate_cost(symbol="BTC/USDT", regime="normal", notional_usdt=200, quantile="p75")

    assert health.mode == "read-only"
    assert permission.permission == "SELL_ONLY"
    assert cost.symbol == "BTC-USDT"
    assert {call["method"] for call in http.calls} == {"GET"}
    assert http.calls[0]["headers"]["Authorization"] == "Bearer super-secret-token"
    text = log_path.read_text(encoding="utf-8")
    assert "super-secret-token" not in text
    rows = [json.loads(line) for line in text.splitlines()]
    assert rows[0]["method"] == "GET"
    assert rows[0]["endpoint_path"] == "/v1/health"
    assert "Authorization" not in text
    assert rows[1]["query_keys"] == ["strategy", "version"]


def test_quant_lab_health_requires_read_only(tmp_path: Path) -> None:
    class BadHTTP(_HTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            return _Response({"status": "ok", "service": "quant-lab", "mode": "write-enabled"})

    client = QuantLabClient(base_url="http://quant-lab.local", http_client=BadHTTP(), request_log_path=tmp_path / "r.jsonl")

    with pytest.raises(QuantLabValidationError):
        client.get_health()


def test_https_with_token_sends_authorization(tmp_path: Path) -> None:
    http = _HTTP()
    client = QuantLabClient(
        base_url="https://qyun2.hrhome.top:8027",
        api_token="super-secret-token",
        http_client=http,
        request_log_path=tmp_path / "requests.jsonl",
    )

    client.get_health()

    assert http.calls[0]["headers"]["Authorization"] == "Bearer super-secret-token"


def test_localhost_http_with_token_sends_authorization(tmp_path: Path) -> None:
    http = _HTTP()
    client = QuantLabClient(
        base_url="http://127.0.0.1:8027",
        api_token="super-secret-token",
        http_client=http,
        request_log_path=tmp_path / "requests.jsonl",
    )

    client.get_health()

    assert http.calls[0]["headers"]["Authorization"] == "Bearer super-secret-token"


def test_public_http_token_enforce_fails_fast(tmp_path: Path) -> None:
    with pytest.raises(QuantLabValidationError, match="public HTTP"):
        QuantLabClient(
            base_url="http://qyun2.hrhome.top:8027",
            api_token="super-secret-token",
            mode="enforce",
            http_client=_HTTP(),
            request_log_path=tmp_path / "requests.jsonl",
        )


def test_public_http_token_shadow_strips_token_and_warns(tmp_path: Path) -> None:
    http = _HTTP()
    with pytest.warns(RuntimeWarning, match="public HTTP"):
        client = QuantLabClient(
            base_url="http://qyun2.hrhome.top:8027",
            api_token="super-secret-token",
            mode="shadow",
            http_client=http,
            request_log_path=tmp_path / "requests.jsonl",
        )

    client.get_health()
    assert client.api_token is None
    assert client.token_auth_disabled_reason == "public_http_token_stripped"
    assert "Authorization" not in http.calls[0]["headers"]
    assert "super-secret-token" not in (tmp_path / "requests.jsonl").read_text(encoding="utf-8")


def test_public_http_without_token_shadow_allowed(tmp_path: Path) -> None:
    http = _HTTP()
    client = QuantLabClient(
        base_url="http://qyun2.hrhome.top:8027",
        api_token=None,
        mode="shadow",
        http_client=http,
        request_log_path=tmp_path / "requests.jsonl",
    )

    client.get_health()

    assert "Authorization" not in http.calls[0]["headers"]


def test_from_config_public_http_shadow_strips_env_token(monkeypatch, tmp_path: Path) -> None:
    from configs.schema import QuantLabConfig

    http = _HTTP()
    monkeypatch.setenv("QUANT_LAB_API_TOKEN", "super-secret-token")
    cfg = QuantLabConfig(
        enabled=True,
        mode="shadow",
        base_url="http://qyun2.hrhome.top:8027",
        request_log_path=str(tmp_path / "requests.jsonl"),
    )

    with pytest.warns(RuntimeWarning, match="public HTTP"):
        client = QuantLabClient.from_config(cfg, http_client=http)

    client.get_health()
    assert client.api_token is None
    assert "Authorization" not in http.calls[0]["headers"]
