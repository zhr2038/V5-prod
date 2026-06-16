from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import timedelta

import pytest

import src.quant_lab_client.client as client_module
from src.quant_lab_client.client import QuantLabClient
from src.quant_lab_client.exceptions import QuantLabValidationError
from src.quant_lab_client.models import GateDecision


class _Response:
    def __init__(
        self,
        payload: dict,
        status_code: int = 200,
        headers: dict | None = None,
        elapsed=None,
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)
        self.headers = headers or {}
        self.elapsed = elapsed

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


def test_cost_semantic_cache_ignores_trace_fields(tmp_path: Path) -> None:
    http = _HTTP()
    log_path = tmp_path / "requests.jsonl"
    client = QuantLabClient(
        base_url="https://quant-lab.local",
        http_client=http,
        request_log_path=log_path,
        run_id="run-1",
        cache_ttl_seconds=0,
        cost_cache_ttl_seconds=300,
    )

    first = client.estimate_cost(
        symbol="BNB/USDT",
        regime="normal",
        notional_usdt=200,
        quantile="p75",
        side="buy",
        strategy_id="v5",
        expected_edge_bps=8.0,
        request_id="cost-1",
        event_id="event-1",
        ts_utc="2026-05-31T10:00:00Z",
    )
    client.run_id = "run-2"
    second = client.estimate_cost(
        symbol="BNB-USDT",
        regime="normal",
        notional_usdt=200,
        quantile="p75",
        side="buy",
        strategy_id="v5",
        expected_edge_bps=12.0,
        request_id="cost-2",
        event_id="event-2",
        ts_utc="2026-05-31T10:01:00Z",
    )

    cost_calls = [call for call in http.calls if call["url"].endswith("/v1/costs/estimate")]
    rows = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]

    assert first.symbol == "BNB-USDT"
    assert second.symbol == "BNB-USDT"
    assert len(cost_calls) == 1
    assert rows[0]["cached"] is False
    assert rows[1]["cached"] is True
    assert rows[1]["request_id"] == "cost-2"
    assert rows[1]["event_id"] == "event-2"


def test_cost_semantic_cache_normalizes_symbol_field_variants() -> None:
    first = QuantLabClient._cost_semantic_cache_key(
        {
            "symbol": "BNB/USDT",
            "regime": "normal",
            "notional_usdt": "200.004",
            "quantile": "p75",
            "side": "BUY",
            "venue": "okx",
            "instrument_type": "SPOT",
            "strategy_id": "v5",
            "request_id": "cost-1",
        }
    )
    second = QuantLabClient._cost_semantic_cache_key(
        {
            "request_symbol": "BNB-USDT",
            "requested_regime": "normal",
            "notional_usdt": "200.004",
            "requested_quantile": "p75",
            "side": "buy",
            "venue": "OKX",
            "instrument_type": "spot",
            "strategy_id": "v5",
            "event_id": "event-2",
        }
    )
    third = QuantLabClient._cost_semantic_cache_key(
        {
            "normalized_symbol": "BNB-USDT",
            "regime": "normal",
            "notional_usdt": "200.004",
            "quantile": "p75",
            "side": "buy",
            "venue": "OKX",
            "instrument_type": "spot",
            "strategy_id": "v5",
            "ts_utc": "2026-05-31T10:00:00Z",
        }
    )

    assert first == second == third


def test_get_json_uses_etag_after_ttl_expiry(tmp_path: Path) -> None:
    class ETagHTTP(_HTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append({"method": "GET", "url": url, "params": params, "headers": headers, "timeout": timeout})
            if headers and headers.get("If-None-Match") == '"abc"':
                return _Response({}, status_code=304, headers={"ETag": '"abc"'})
            return _Response([{"strategy_candidate": "v5.f4_volume_expansion_entry"}], headers={"ETag": '"abc"'})

    http = ETagHTTP()
    client = QuantLabClient(
        base_url="https://quant-lab.local",
        http_client=http,
        cache_ttl_seconds=0,
        request_log_path=tmp_path / "requests.jsonl",
    )

    first = client.get_json("/v1/strategy-opportunity-advisory/v5-compact")
    second = client.get_json("/v1/strategy-opportunity-advisory/v5-compact")

    assert first.data == [{"strategy_candidate": "v5.f4_volume_expansion_entry"}]
    assert second.data == first.data
    assert second.cached is True
    assert second.status_code == 304
    assert http.calls[1]["headers"]["If-None-Match"] == '"abc"'


def test_get_json_persists_etag_cache_across_clients(tmp_path: Path) -> None:
    cache_path = tmp_path / "quant_lab_http_cache.json"

    class FirstHTTP(_HTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append({"method": "GET", "url": url, "params": params, "headers": headers, "timeout": timeout})
            return _Response(
                {"rows": [{"strategy_candidate": "v5.f4_volume_expansion_entry"}]},
                headers={"ETag": '"persisted"'},
            )

    first_http = FirstHTTP()
    first_client = QuantLabClient(
        base_url="https://quant-lab.local",
        http_client=first_http,
        cache_ttl_seconds=0,
        request_log_path=tmp_path / "first_requests.jsonl",
        http_cache_path=cache_path,
    )
    first = first_client.get_json("/v1/strategy-opportunity-advisory/v5-compact")
    assert first.ok is True
    assert cache_path.is_file()

    class SecondHTTP(_HTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append({"method": "GET", "url": url, "params": params, "headers": headers, "timeout": timeout})
            assert headers and headers.get("If-None-Match") == '"persisted"'
            return _Response({}, status_code=304, headers={"ETag": '"persisted"'})

    second_http = SecondHTTP()
    second_client = QuantLabClient(
        base_url="https://quant-lab.local",
        http_client=second_http,
        cache_ttl_seconds=0,
        request_log_path=tmp_path / "second_requests.jsonl",
        http_cache_path=cache_path,
    )
    second = second_client.get_json("/v1/strategy-opportunity-advisory/v5-compact")

    assert second.cached is True
    assert second.status_code == 304
    assert second.data == first.data
    assert second_http.calls[0]["headers"]["If-None-Match"] == '"persisted"'


@pytest.mark.parametrize(
    "endpoint",
    [
        "/v1/strategy-opportunity-advisory/v5-compact",
        "/v1/strategy_opportunity_advisory",
        "/v1/reports/strategy-opportunity-advisory",
    ],
)
def test_strategy_advisory_endpoint_variants_are_persistent_cacheable(endpoint: str) -> None:
    assert QuantLabClient._persistent_cache_enabled_for_endpoint(endpoint) is True


def test_get_json_logs_segmented_latency_and_server_headers(tmp_path: Path) -> None:
    class HeaderHTTP(_HTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append({"method": "GET", "url": url, "params": params, "headers": headers, "timeout": timeout})
            return _Response(
                {"rows": [{"strategy_candidate": "v5.f4_volume_expansion_entry"}]},
                headers={
                    "X-Quant-Lab-Api-Cache-Hit": "true",
                    "X-Advisory-Response-Cache-Hit": "true",
                    "X-Quant-Lab-Lake-Scan-Ms": "1.25",
                    "X-Quant-Lab-Serialize-Ms": "2.5",
                    "X-Quant-Lab-Source-Signature-Ms": "0.75",
                },
                elapsed=timedelta(milliseconds=15),
            )

    log_path = tmp_path / "requests.jsonl"
    client = QuantLabClient(
        base_url="https://quant-lab.local",
        http_client=HeaderHTTP(),
        request_log_path=log_path,
        http_cache_path=tmp_path / "cache.json",
    )

    client.get_json("/v1/strategy-opportunity-advisory/v5-compact")

    row = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])
    assert row["ttfb_ms"] == 15.0
    assert row["response_bytes"] > 0
    assert row["server_header_lake_scan_ms"] == 1.25
    assert row["server_header_serialize_ms"] == 2.5
    assert row["server_header_source_signature_ms"] == 0.75
    assert row["server_cache_hit"] is True
    assert row["response_cache_hit"] is True
    assert "resolved_host" in row


def test_live_permission_cache_is_capped_by_client_ttl(tmp_path: Path, monkeypatch) -> None:
    class PermissionHTTP(_HTTP):
        def __init__(self) -> None:
            super().__init__()
            self.permissions = ["ALLOW", "ABORT"]

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append({"method": "GET", "url": url, "params": params, "headers": headers, "timeout": timeout})
            permission = self.permissions[min(len(self.calls) - 1, len(self.permissions) - 1)]
            return _Response(
                {
                    "strategy": "v5",
                    "version": "5.0.0",
                    "permission": permission,
                    "reasons": [],
                    "expires_at": "2999-01-01T00:00:00Z",
                }
            )

    now = 1_000.0
    monkeypatch.setattr(client_module.time, "time", lambda: now)
    http = PermissionHTTP()
    client = QuantLabClient(
        base_url="https://quant-lab.local",
        http_client=http,
        request_log_path=tmp_path / "requests.jsonl",
        cache_ttl_seconds=30,
    )

    first = client.get_live_permission(strategy="v5", version="5.0.0", request_id="one")
    now = 1_029.0
    second = client.get_live_permission(strategy="v5", version="5.0.0", request_id="two")
    now = 1_031.0
    third = client.get_live_permission(strategy="v5", version="5.0.0", request_id="three")

    assert first.permission == "ALLOW"
    assert second.permission == "ALLOW"
    assert third.permission == "ABORT"
    assert len(http.calls) == 2
    rows = [
        json.loads(line)
        for line in (tmp_path / "requests.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert [row["request_id"] for row in rows] == ["one", "two", "three"]
    assert [row["endpoint_path"] for row in rows] == [
        "/v1/risk/live-permission",
        "/v1/risk/live-permission",
        "/v1/risk/live-permission",
    ]
    assert rows[0]["client_cache_hit"] is False
    assert rows[1]["client_cache_hit"] is True
    assert rows[2]["client_cache_hit"] is False
    assert rows[1]["cached"] is True
    assert rows[1]["response_summary"]["permission"] == "ALLOW"
    assert rows[2]["response_summary"]["permission"] == "ABORT"


@pytest.mark.parametrize("symbol", ["OKX:BNB-USDT", "okx:bnb-usdt", "BNB/USDT", "BNB-USDT", "BNBUSDT"])
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


def test_quant_lab_health_requires_ok_status(tmp_path: Path) -> None:
    class BadHTTP(_HTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            return _Response({"status": "critical", "service": "quant-lab", "mode": "read-only"})

    client = QuantLabClient(base_url="http://quant-lab.local", http_client=BadHTTP(), request_log_path=tmp_path / "r.jsonl")

    with pytest.raises(QuantLabValidationError, match="health status"):
        client.get_health()


def test_quant_lab_health_accepts_healthy_status(tmp_path: Path) -> None:
    class HealthyHTTP(_HTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            return _Response({"status": "healthy", "service": "quant-lab", "mode": "read-only"})

    client = QuantLabClient(base_url="http://quant-lab.local", http_client=HealthyHTTP(), request_log_path=tmp_path / "r.jsonl")

    assert client.get_health().status == "healthy"


def test_quant_lab_deep_health_warning_is_visible_not_fatal(tmp_path: Path) -> None:
    class WarningHTTP(_HTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            if url.endswith("/v1/health/deep"):
                return _Response(
                    {
                        "status": "warning",
                        "service": "quant-lab",
                        "mode": "read-only",
                        "warnings": ["cost_health_warning"],
                        "cost_health": {"status": "warning"},
                        "data_health": {"status": "ok"},
                        "risk_permission_dependency_meta": {"status": "ok"},
                    }
                )
            return super().get(url, params=params, headers=headers, timeout=timeout)

    client = QuantLabClient(
        base_url="http://quant-lab.local",
        http_client=WarningHTTP(),
        request_log_path=tmp_path / "r.jsonl",
    )

    health = client.get_deep_health()

    assert health.status == "warning"
    assert health.warnings == ["cost_health_warning"]
    assert health.cost_health == {"status": "warning"}


def test_quant_lab_deep_health_critical_fails_fast(tmp_path: Path) -> None:
    class CriticalHTTP(_HTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            if url.endswith("/v1/health/deep"):
                return _Response(
                    {
                        "status": "critical",
                        "service": "quant-lab",
                        "mode": "read-only",
                        "warnings": ["data_health_critical"],
                        "data_health": {"status": "critical"},
                    }
                )
            return super().get(url, params=params, headers=headers, timeout=timeout)

    client = QuantLabClient(
        base_url="http://quant-lab.local",
        http_client=CriticalHTTP(),
        request_log_path=tmp_path / "r.jsonl",
    )

    with pytest.raises(QuantLabValidationError, match="/v1/health/deep status"):
        client.get_deep_health()


def test_gate_decision_string_false_passed_stays_false() -> None:
    decision = GateDecision.from_payload(
        {
            "alpha_id": "v5.core",
            "status": "QUARANTINE",
            "passed": "false",
        }
    )

    assert decision.passed is False


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
