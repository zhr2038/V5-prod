from __future__ import annotations

import json
import os
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
            params = params or {}
            return _Response(
                {
                    "symbol": params.get("normalized_symbol") or params.get("symbol") or "BTC-USDT",
                    "regime": "normal",
                    "notional_usdt": 200,
                    "quantile": "p75",
                    "total_cost_bps": 1.2,
                    "total_cost_bps_p50": 1.0,
                    "total_cost_bps_p75": 1.2,
                    "total_cost_bps_p90": 2.0,
                    "required_edge_bps": 1.8,
                    "source": "public_spread_proxy",
                    "cost_model_version": "cost_bucket_daily:2026-05-11",
                }
            )
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
    assert rows[1]["event_type"] == "request"
    assert rows[1]["fallback_used"] is False
    assert {"strategy", "version", "request_id", "event_id", "ts_utc"} <= set(rows[1]["query_keys"])
    cost_params = http.calls[2]["params"]
    assert cost_params["symbol"] == "BTC/USDT"
    assert cost_params["request_symbol"] == "BTC/USDT"
    assert cost_params["normalized_symbol"] == "BTC-USDT"
    assert cost_params["venue"] == "OKX"
    assert cost_params["instrument_type"] == "spot"
    assert cost_params["strategy_id"] == "v5"
    assert "expected_edge_bps" in cost_params
    assert "request_id" in cost_params
    assert "event_id" in cost_params
    assert cost_params["run_id"] == "run-1"
    assert "ts_utc" in cost_params
    assert cost_params["requested_regime"] == "normal"
    assert cost_params["requested_quantile"] == "p75"
    assert cost_params["contract_version"] == "v5.quant_lab.telemetry.v2"


def test_cost_request_normalizes_concatenated_usdt_symbol(tmp_path: Path) -> None:
    http = _HTTP()
    client = QuantLabClient(
        base_url="https://quant-lab.local",
        http_client=http,
        request_log_path=tmp_path / "requests.jsonl",
        run_id="run-1",
    )

    cost = client.estimate_cost(
        symbol="BNBUSDT",
        regime="normal",
        notional_usdt=200,
        quantile="p75",
        side="buy",
        strategy_id="v5",
        expected_edge_bps=8.0,
        request_id="cost-1",
    )

    params = http.calls[0]["params"]
    assert params["symbol"] == "BNBUSDT"
    assert params["normalized_symbol"] == "BNB-USDT"
    assert params["side"] == "buy"
    assert params["expected_edge_bps"] == 8.0
    assert params["request_id"] == "cost-1"
    assert cost.symbol == "BNB-USDT"
    assert cost.total_cost_bps_p75 == 1.2
    assert cost.required_edge_bps == 1.8


@pytest.mark.parametrize("symbol", ["BNB/USDT", "BNB-USDT", "BNBUSDT"])
def test_cost_request_normalizes_bnb_symbol_variants(tmp_path: Path, symbol: str) -> None:
    http = _HTTP()
    client = QuantLabClient(
        base_url="https://quant-lab.local",
        http_client=http,
        request_log_path=tmp_path / f"{symbol.replace('/', '_').replace('-', '_')}.jsonl",
    )

    client.estimate_cost(symbol=symbol, regime="normal", notional_usdt=200, quantile="p75")

    params = http.calls[0]["params"]
    assert params["symbol"] == symbol
    assert params["normalized_symbol"] == "BNB-USDT"


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


def test_public_http_token_allowed_with_explicit_config(tmp_path: Path) -> None:
    http = _HTTP()
    client = QuantLabClient(
        base_url="http://qyun2.hrhome.top:8027",
        api_token="super-secret-token",
        mode="shadow",
        allow_insecure_http_with_token=True,
        http_client=http,
        request_log_path=tmp_path / "requests.jsonl",
    )

    client.get_health()

    assert http.calls[0]["headers"]["Authorization"] == "Bearer super-secret-token"
    assert "super-secret-token" not in (tmp_path / "requests.jsonl").read_text(encoding="utf-8")


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


def test_from_config_reads_token_from_api_env_path(monkeypatch, tmp_path: Path) -> None:
    from configs.schema import QuantLabConfig

    http = _HTTP()
    monkeypatch.delenv("QUANT_LAB_API_TOKEN", raising=False)
    env_path = tmp_path / "api.env"
    env_path.write_text('QUANT_LAB_API_TOKEN="super-secret-token"\n', encoding="utf-8")
    cfg = QuantLabConfig(
        enabled=True,
        mode="shadow",
        base_url="http://qyun2.hrhome.top:8027",
        api_env_path=str(env_path),
        api_env_require_secure_permissions=False,
        allow_insecure_http_with_token=True,
        request_log_path=str(tmp_path / "requests.jsonl"),
    )

    client = QuantLabClient.from_config(cfg, http_client=http)
    client.get_health()

    assert http.calls[0]["headers"]["Authorization"] == "Bearer super-secret-token"
    assert client.api_env_path_present is True
    assert client.api_env_secure_permissions is True
    assert client.api_env_token_loaded is True
    assert "super-secret-token" not in (tmp_path / "requests.jsonl").read_text(encoding="utf-8")


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode bits are not reliable on Windows")
def test_from_config_api_env_0600_reads_token(monkeypatch, tmp_path: Path) -> None:
    from configs.schema import QuantLabConfig

    http = _HTTP()
    monkeypatch.delenv("QUANT_LAB_API_TOKEN", raising=False)
    env_path = tmp_path / "api.env"
    env_path.write_text('QUANT_LAB_API_TOKEN="super-secret-token"\n', encoding="utf-8")
    env_path.chmod(0o600)
    cfg = QuantLabConfig(
        enabled=True,
        mode="shadow",
        base_url="http://qyun2.hrhome.top:8027",
        api_env_path=str(env_path),
        allow_insecure_http_with_token=True,
        request_log_path=str(tmp_path / "requests.jsonl"),
    )

    client = QuantLabClient.from_config(cfg, http_client=http)
    client.get_health()

    assert http.calls[0]["headers"]["Authorization"] == "Bearer super-secret-token"
    assert client.api_env_secure_permissions is True
    assert client.api_env_token_loaded is True
    assert client.api_env_warning is None


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode bits are not reliable on Windows")
def test_from_config_api_env_0644_shadow_does_not_read_token(monkeypatch, tmp_path: Path) -> None:
    from configs.schema import QuantLabConfig

    http = _HTTP()
    monkeypatch.delenv("QUANT_LAB_API_TOKEN", raising=False)
    env_path = tmp_path / "api.env"
    env_path.write_text('QUANT_LAB_API_TOKEN="super-secret-token"\n', encoding="utf-8")
    env_path.chmod(0o644)
    cfg = QuantLabConfig(
        enabled=True,
        mode="shadow",
        base_url="http://qyun2.hrhome.top:8027",
        api_env_path=str(env_path),
        allow_insecure_http_with_token=True,
        request_log_path=str(tmp_path / "requests.jsonl"),
    )

    with pytest.warns(RuntimeWarning, match="api_env_path skipped"):
        client = QuantLabClient.from_config(cfg, http_client=http)
    client.get_health()

    assert client.api_token is None
    assert client.api_env_path_present is True
    assert client.api_env_secure_permissions is False
    assert client.api_env_token_loaded is False
    assert client.api_env_warning
    assert "Authorization" not in http.calls[0]["headers"]
    assert "super-secret-token" not in (tmp_path / "requests.jsonl").read_text(encoding="utf-8")


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink tests require reliable symlink support")
def test_from_config_api_env_symlink_default_does_not_read_token(monkeypatch, tmp_path: Path) -> None:
    from configs.schema import QuantLabConfig

    http = _HTTP()
    monkeypatch.delenv("QUANT_LAB_API_TOKEN", raising=False)
    target = tmp_path / "api.env.real"
    target.write_text('QUANT_LAB_API_TOKEN="super-secret-token"\n', encoding="utf-8")
    target.chmod(0o600)
    link = tmp_path / "api.env"
    link.symlink_to(target)
    cfg = QuantLabConfig(
        enabled=True,
        mode="shadow",
        base_url="http://qyun2.hrhome.top:8027",
        api_env_path=str(link),
        allow_insecure_http_with_token=True,
        request_log_path=str(tmp_path / "requests.jsonl"),
    )

    with pytest.warns(RuntimeWarning, match="api_env_path skipped"):
        client = QuantLabClient.from_config(cfg, http_client=http)
    client.get_health()

    assert client.api_token is None
    assert client.api_env_path_present is True
    assert client.api_env_secure_permissions is False
    assert client.api_env_token_loaded is False
    assert client.api_env_warning == "api_env_symlink_disallowed"
    assert "Authorization" not in http.calls[0]["headers"]


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode bits are not reliable on Windows")
def test_from_config_api_env_insecure_enforce_fails_fast(monkeypatch, tmp_path: Path) -> None:
    from configs.schema import QuantLabConfig

    monkeypatch.delenv("QUANT_LAB_API_TOKEN", raising=False)
    env_path = tmp_path / "api.env"
    env_path.write_text('QUANT_LAB_API_TOKEN="super-secret-token"\n', encoding="utf-8")
    env_path.chmod(0o644)
    cfg = QuantLabConfig(
        enabled=True,
        mode="enforce",
        fail_policy="sell_only",
        base_url="http://qyun2.hrhome.top:8027",
        api_env_path=str(env_path),
        allow_insecure_http_with_token=True,
        request_log_path=str(tmp_path / "requests.jsonl"),
    )

    with pytest.raises(QuantLabValidationError, match="api_env_path is not secure"):
        QuantLabClient.from_config(cfg, http_client=_HTTP())


def test_from_config_missing_api_env_path_is_not_fatal_in_enforce(monkeypatch, tmp_path: Path) -> None:
    from configs.schema import QuantLabConfig

    monkeypatch.delenv("QUANT_LAB_API_TOKEN", raising=False)
    cfg = QuantLabConfig(
        enabled=True,
        mode="enforce",
        fail_policy="sell_only",
        base_url="http://qyun2.hrhome.top:8027",
        api_env_path=str(tmp_path / "missing.env"),
        request_log_path=str(tmp_path / "requests.jsonl"),
    )

    client = QuantLabClient.from_config(cfg, http_client=_HTTP())

    assert client.api_token is None
    assert client.api_env_path_present is False
    assert client.api_env_secure_permissions is None
    assert client.api_env_token_loaded is False
