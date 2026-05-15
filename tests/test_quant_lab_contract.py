from __future__ import annotations

import json
import shutil
import tarfile
import csv
import io
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from configs.schema import AppConfig
from src.core.models import Order
from src.quant_lab_client.client import QuantLabClient
from src.quant_lab_client.guard import QuantLabGuard
from src.quant_lab_client.models import CostEstimate, RiskPermission, symbol_to_quant_lab_symbol
from src.reporting import metrics, summary_writer
from src.reporting.quant_lab_audit import (
    CONTRACT_VERSION,
    EVENT_ID_GENERATION_VERSION,
    EVENT_TYPES,
    SCHEMA_VERSION,
    append_quant_lab_request,
    append_quant_lab_usage,
)
from src.reporting.v5_bundle_exporter import _build_cost_rows, _build_fallback_rows, _window_summary, export_v5_bundle


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "quant_lab_contract"
REQUIRED_EVENT_FIELDS = {"schema_version", "contract_version", "event_id", "request_id", "run_id", "ts_utc"}


def _json_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_contract_fixtures_have_required_event_fields() -> None:
    for path in FIXTURE_DIR.glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert REQUIRED_EVENT_FIELDS <= set(payload), path.name
        assert payload["schema_version"] == SCHEMA_VERSION
        assert payload["contract_version"] == CONTRACT_VERSION
        if "event_type" in payload:
            assert payload["event_type"] in EVENT_TYPES, path.name


def test_symbol_normalization_contract_fixtures() -> None:
    assert symbol_to_quant_lab_symbol("OKX:BNB-USDT") == "BNB-USDT"
    assert symbol_to_quant_lab_symbol("okx:bnb-usdt") == "BNB-USDT"
    assert symbol_to_quant_lab_symbol("BNB/USDT") == "BNB-USDT"
    assert symbol_to_quant_lab_symbol("BNB-USDT") == "BNB-USDT"
    assert symbol_to_quant_lab_symbol("BNBUSDT") == "BNB-USDT"
    assert _json_fixture("cost_request_bnb_slash.json")["normalized_symbol"] == "BNB-USDT"
    assert _json_fixture("cost_request_bnb_dash.json")["normalized_symbol"] == "BNB-USDT"


class _FixtureClient:
    phase = "live"

    def __init__(self, *, permission_payload: dict | None = None, cost_payload: dict | None = None, fail_cost: bool = False) -> None:
        self.permission_payload = permission_payload or _json_fixture("risk_permission_active.json")
        self.cost_payload = cost_payload or _json_fixture("cost_response_public_proxy.json")
        self.fail_cost = fail_cost
        self.run_id = "fixture-run-001"

    def get_health(self):
        return SimpleNamespace(status="ok", mode="read-only")

    def get_live_permission(self, *, strategy: str, version: str):
        return RiskPermission.from_payload({**self.permission_payload, "strategy": strategy, "version": version})

    def estimate_cost(self, *, symbol: str, regime: str, notional_usdt: float, quantile: str, **kwargs):
        if self.fail_cost:
            raise TimeoutError("fixture timeout")
        return CostEstimate.from_payload(
            {
                **self.cost_payload,
                "symbol": symbol_to_quant_lab_symbol(symbol),
                "regime": regime,
                "notional_usdt": notional_usdt,
                "quantile": quantile,
            }
        )


def _guard(tmp_path: Path, cfg: AppConfig, client: _FixtureClient) -> QuantLabGuard:
    return QuantLabGuard(client=client, cfg=cfg.quant_lab, usage_log_path=tmp_path / "quant_lab_usage.jsonl", run_id="fixture-run-001")


def test_global_default_cost_response_is_degraded(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.runtime_override_path = str(tmp_path / "mode.json")
    guard = _guard(tmp_path, cfg, _FixtureClient(cost_payload=_json_fixture("cost_response_global_default.json")))
    guard.check_startup_permission(cfg, "fixture-run-001")

    kept, rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 200.0, 600.0, {"expected_edge_bps": 60.0})],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    assert rows[0]["request_symbol"] == "BNB/USDT"
    assert rows[0]["normalized_symbol"] == "BNB-USDT"
    assert rows[0]["cost_source"] == "global_default"
    assert rows[0]["fallback_level"] == "GLOBAL_DEFAULT"
    assert rows[0]["degraded_cost_model"] is True
    assert rows[0]["fallback_used_for_cost_model"] is True
    assert rows[0]["diagnosis"] == "global_default_cost"
    assert rows[0]["as_of_ts"] == "2026-05-14T00:01:00Z"


def test_public_proxy_cost_response_is_not_degraded(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.runtime_override_path = str(tmp_path / "mode.json")
    guard = _guard(tmp_path, cfg, _FixtureClient(cost_payload=_json_fixture("cost_response_public_proxy.json")))
    guard.check_startup_permission(cfg, "fixture-run-001")

    _kept, rows = guard.enrich_orders_with_cost(
        [Order("BNB-USDT", "buy", "OPEN_LONG", 200.0, 600.0, {"expected_edge_bps": 60.0})],
        "normal",
        cfg,
    )

    assert rows[0]["normalized_symbol"] == "BNB-USDT"
    assert rows[0]["cost_source"] == "public_spread_proxy"
    assert rows[0]["degraded_cost_model"] is False
    assert rows[0]["fallback_used"] is False


def test_http_200_success_request_does_not_create_fallback() -> None:
    row = _json_fixture("request_success_200.json")
    assert _build_fallback_rows([row]) == []


def test_timeout_fixture_creates_fallback() -> None:
    rows = _build_fallback_rows([_json_fixture("fallback_timeout.json")])
    assert len(rows) == 1
    assert rows[0]["reason"] == "quant_lab_unavailable_allow_local_fallback"


def test_timeout_request_and_fallback_keep_stable_ids_in_bundle(tmp_path: Path) -> None:
    root = tmp_path / "root"
    reports = root / "reports"
    reports.mkdir(parents=True)
    request_id = "fixture-run-001:permission:v5:5.0.0"
    request_event_id = "fixture-timeout-request-event"
    append_quant_lab_request(
        reports / "quant_lab_requests.jsonl",
        {
            "run_id": "fixture-run-001",
            "ts_utc": "2026-05-14T00:07:00Z",
            "event_type": "request",
            "event_id": request_event_id,
            "request_id": request_id,
            "endpoint_path": "/v1/risk/live-permission",
            "status_code": None,
            "success": False,
            "latency_ms": 2000.0,
            "error_type": "QuantLabTimeout",
            "error_message_short": "timeout",
        },
    )
    append_quant_lab_usage(
        reports / "quant_lab_usage.jsonl",
        {
            "run_id": "fixture-run-001",
            "ts_utc": "2026-05-14T00:07:01Z",
            "event_type": "fallback",
            "request_id": request_id,
            "original_request_id": request_id,
            "original_event_id": request_event_id,
            "endpoint_path": "/v1/risk/live-permission",
            "success": False,
            "fallback_used": True,
            "fallback_reason": "quant_lab_unavailable_sell_only",
            "error_type": "QuantLabTimeout",
            "error_message_short": "timeout",
        },
    )
    raw_fallback = json.loads((reports / "quant_lab_usage.jsonl").read_text(encoding="utf-8").splitlines()[0])

    bundle_a = export_v5_bundle(reports_dir=reports, out_dir=root / "bundles-a", window_hours=72)
    bundle_b = export_v5_bundle(reports_dir=reports, out_dir=root / "bundles-b", window_hours=72)

    def fallback_row(bundle: Path) -> dict:
        with tarfile.open(bundle, "r:gz") as tf:
            text = tf.extractfile("summaries/quant_lab_fallbacks.csv").read().decode("utf-8")  # type: ignore[union-attr]
        return next(csv.DictReader(io.StringIO(text)))

    row_a = fallback_row(bundle_a)
    row_b = fallback_row(bundle_b)
    assert row_a["event_id"] == raw_fallback["event_id"]
    assert row_a["event_id"] == row_b["event_id"]
    assert row_a["request_id"] == request_id
    assert row_a["original_request_id"] == request_id
    assert row_a["original_event_id"] == request_event_id
    assert row_a["error_type"] == "QuantLabTimeout"


def test_cost_usage_summary_counts_degraded_and_symbol_hits() -> None:
    public_row = {
        **_json_fixture("cost_response_public_proxy.json"),
        "event_type": "cost_estimate",
        "run_id": "fixture-run-001",
        "ts": "2026-05-14T00:00:01Z",
        "schema_version": SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "event_id_generation_version": EVENT_ID_GENERATION_VERSION,
        "request_symbol": "BNB/USDT",
        "normalized_symbol": "BNB-USDT",
        "response_symbol": "BNB-USDT",
        "cost_source": "public_spread_proxy",
        "cost_contract_version": CONTRACT_VERSION,
    }
    global_row = {
        **_json_fixture("cost_response_global_default.json"),
        "event_type": "cost_estimate",
        "run_id": "fixture-run-001",
        "ts": "2026-05-14T00:01:01Z",
        "schema_version": SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "event_id_generation_version": EVENT_ID_GENERATION_VERSION,
        "request_symbol": "BNB/USDT",
        "normalized_symbol": "BNB-USDT",
        "response_symbol": "BNB-USDT",
        "cost_source": "global_default",
        "cost_contract_version": CONTRACT_VERSION,
    }

    cost_rows = _build_cost_rows([public_row, global_row])
    summary = _window_summary([public_row, global_row], [], [])

    assert cost_rows[0]["as_of_ts"] == "2026-05-14T00:00:00Z"
    assert cost_rows[1]["degraded_cost_model"] is True
    assert summary["cost_degraded_count"] == 1
    assert summary["global_default_cost_count"] == 1
    assert summary["current_contract_global_default_cost_count"] == 1
    assert summary["legacy_global_default_cost_count"] == 0
    assert summary["post_deployment_global_default_cost_count"] == 1
    assert summary["symbol_cost_hit_count"] == 1
    assert summary["cost_contract_version"] == CONTRACT_VERSION


def test_cost_usage_summary_separates_legacy_global_default_from_current_contract() -> None:
    now = "2026-05-14T12:00:00Z"
    legacy_bnb = {
        "event_type": "cost_estimate",
        "run_id": "legacy-bnb",
        "ts": "2026-05-12T12:00:00Z",
        "symbol": "BNB/USDT",
        "cost_source": "global_default",
        "cost_model_version": "global_default_v0",
        "cost_contract_version": CONTRACT_VERSION,
    }
    current_btc = {
        "event_type": "cost_estimate",
        "run_id": "current-btc",
        "ts": now,
        "schema_version": SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "event_id_generation_version": EVENT_ID_GENERATION_VERSION,
        "symbol": "BTC/USDT",
        "normalized_symbol": "BTC-USDT",
        "response_symbol": "BTC-USDT",
        "cost_source": "mixed_actual_proxy",
        "cost_model_version": "mixed_actual_proxy_v1",
        "cost_contract_version": CONTRACT_VERSION,
        "sample_count": 8,
    }

    summary = _window_summary(
        [legacy_bnb, current_btc],
        [],
        [],
        now=datetime.fromisoformat(now.replace("Z", "+00:00")),
    )

    assert summary["global_default_cost_count"] == 1
    assert summary["legacy_global_default_cost_count"] == 1
    assert summary["current_contract_global_default_cost_count"] == 0
    assert summary["latest_24h_global_default_cost_count"] == 0
    assert summary["post_deployment_global_default_cost_count"] == 0
    assert summary["cost_usage_legacy_rows"] == 1
    assert summary["cost_usage_current_contract_rows"] == 1
    assert summary["cost_usage_latest_24h_rows"] == 1


def test_shadow_raw_abort_records_effective_allow_and_would_block(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.runtime_override_path = str(tmp_path / "mode.json")
    guard = _guard(
        tmp_path,
        cfg,
        _FixtureClient(permission_payload=_json_fixture("risk_permission_stale.json")),
    )

    result = guard.check_startup_permission(cfg, "fixture-run-001")
    guard.record_final_permission(local_preflight_permission="ALLOW", final_permission="ALLOW")

    assert result.permission == "ABORT"
    assert result.effective_permission_decision == "ALLOW"
    assert result.would_block_if_enforced is True
    rows = [json.loads(line) for line in (tmp_path / "quant_lab_usage.jsonl").read_text(encoding="utf-8").splitlines()]
    final = [row for row in rows if row["event_type"] == "permission_audit" and row.get("legacy_event_type") == "final_permission"][-1]
    assert final["raw_permission_decision"] == "ABORT"
    assert final["effective_permission_decision"] == "ALLOW"
    assert final["would_block_if_enforced"] is True
    assert final["permission_gate_enforced"] is False
    assert REQUIRED_EVENT_FIELDS <= set(final)


def test_append_usage_adds_stable_event_and_request_ids(tmp_path: Path) -> None:
    row = {
        "run_id": "fixture-run-001",
        "ts_utc": "2026-05-14T00:06:00Z",
        "event_type": "final_permission",
        "raw_permission_decision": "ABORT",
        "effective_permission_decision": "ALLOW",
    }
    path = tmp_path / "usage.jsonl"
    append_quant_lab_usage(path, row)
    append_quant_lab_usage(path, row)

    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert events[0]["event_id"] == events[1]["event_id"]
    assert events[0]["request_id"] == events[1]["request_id"]
    assert REQUIRED_EVENT_FIELDS <= set(events[0])


class _Response:
    status_code = 200

    def json(self):
        return _json_fixture("cost_response_public_proxy.json")


class _HttpClient:
    def __init__(self) -> None:
        self.params = None

    def get(self, url, *, params=None, headers=None, timeout=None):
        self.params = dict(params or {})
        return _Response()


def test_cost_client_payload_uses_normalized_symbol_and_request_id(tmp_path: Path) -> None:
    http_client = _HttpClient()
    client = QuantLabClient(
        base_url="https://quant-lab.example",
        http_client=http_client,
        request_log_path=tmp_path / "quant_lab_requests.jsonl",
        run_id="fixture-run-001",
    )

    client.estimate_cost(
        symbol="BNB/USDT",
        regime="normal",
        notional_usdt=200.0,
        quantile="p75",
        side="buy",
        strategy_id="v5",
        expected_edge_bps=60.0,
        request_id="fixture-run-001:cost:0:BNB-USDT",
    )

    assert http_client.params["symbol"] == "BNB/USDT"
    assert http_client.params["request_symbol"] == "BNB/USDT"
    assert http_client.params["normalized_symbol"] == "BNB-USDT"
    assert http_client.params["venue"] == "OKX"
    assert http_client.params["instrument_type"] == "spot"
    assert http_client.params["request_id"] == "fixture-run-001:cost:0:BNB-USDT"
    assert "event_id" in http_client.params
    assert http_client.params["run_id"] == "fixture-run-001"
    assert "ts_utc" in http_client.params
    assert http_client.params["requested_regime"] == "normal"
    assert http_client.params["requested_quantile"] == "p75"
    assert http_client.params["contract_version"] == CONTRACT_VERSION
    request_row = json.loads((tmp_path / "quant_lab_requests.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert request_row["request_id"] == "fixture-run-001:cost:0:BNB-USDT"
    assert REQUIRED_EVENT_FIELDS <= set(request_row)


def test_trades_fixture_summary_counts_non_empty_trades(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)
    run_dir = tmp_path / "reports" / "runs" / "fixture-run-001"
    run_dir.mkdir(parents=True)
    shutil.copy(FIXTURE_DIR / "trades.csv", run_dir / "trades.csv")
    (run_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": "2026-05-14T00:00:00Z", "equity": 1000.0}),
                json.dumps({"ts": "2026-05-14T01:00:00Z", "equity": 1000.0}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/fixture-run-001")

    assert summary["trades_file_rows"] == 2
    assert summary["trades_counted_rows"] == 2
    assert summary["num_trades"] == 2


def test_bundle_manifest_includes_contract_schema_config_and_strategy(tmp_path: Path) -> None:
    root = tmp_path
    reports = root / "reports"
    reports.mkdir()
    (root / "configs").mkdir()
    (root / "configs" / "live_prod.yaml").write_text(
        "quant_lab:\n  enabled: true\n  mode: shadow\n  strategy_version: '5.9.0'\n",
        encoding="utf-8",
    )

    bundle = export_v5_bundle(reports_dir=reports, out_dir=root / "bundles", window_hours=72)

    with tarfile.open(bundle, "r:gz") as tf:
        manifest_member = next(member for member in tf.getmembers() if member.name == "manifest.json")
        manifest = json.loads(tf.extractfile(manifest_member).read().decode("utf-8"))  # type: ignore[union-attr]

    assert manifest["schema_version"] == SCHEMA_VERSION
    assert manifest["contract_version"] == CONTRACT_VERSION
    assert manifest["quant_lab_contract_version"] == CONTRACT_VERSION
    assert manifest["telemetry_schema_version"] == SCHEMA_VERSION
    assert manifest["telemetry_contract_version"] == CONTRACT_VERSION
    assert manifest["event_id_generation_version"] == "quant_lab_event_id_v1"
    assert manifest["config_hash"] != "not_observable"
    assert manifest["strategy_version"] == "5.9.0"


def test_bnb_dash_cost_client_payload_keeps_dash_symbol(tmp_path: Path) -> None:
    http_client = _HttpClient()
    client = QuantLabClient(
        base_url="https://quant-lab.example",
        http_client=http_client,
        request_log_path=tmp_path / "quant_lab_requests.jsonl",
        run_id="fixture-run-001",
    )

    client.estimate_cost(
        symbol="BNB-USDT",
        regime="normal",
        notional_usdt=200.0,
        quantile="p75",
        side="buy",
        strategy_id="v5",
        expected_edge_bps=60.0,
        request_id="fixture-run-001:cost:1:BNB-USDT",
    )

    assert http_client.params["symbol"] == "BNB-USDT"
    assert http_client.params["normalized_symbol"] == "BNB-USDT"
