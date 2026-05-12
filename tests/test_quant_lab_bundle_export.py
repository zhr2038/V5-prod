from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path

from src.reporting.v5_bundle_exporter import export_v5_bundle


def test_bundle_export_contains_quant_lab_files_and_sha(tmp_path: Path) -> None:
    root = tmp_path / "root"
    reports = root / "reports"
    configs = root / "configs"
    state = root / "state"
    out = tmp_path / "bundles"
    reports.mkdir(parents=True)
    configs.mkdir(parents=True)
    state.mkdir(parents=True)
    (configs / "live_prod.yaml").write_text(
        "\n".join(
            [
                "quant_lab:",
                "  enabled: true",
                "  mode: shadow",
                "  base_url: http://qyun2.hrhome.top:8027",
                "  api_token_env: QUANT_LAB_API_TOKEN",
                "  api_env_path: /home/ubuntu/.quant-lab/api.env",
                "  fail_policy: allow_local_fallback",
                "  allow_local_fallback_in_enforce: false",
                "  allow_insecure_http_with_token: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (state / "quant_lab_mode.json").write_text(
        json.dumps({"mode": "shadow", "reason": "test", "updated_by": "test", "updated_at": "2026-05-11T13:00:00Z"}),
        encoding="utf-8",
    )
    (reports / "quant_lab_usage.jsonl").write_text(
        json.dumps(
            {
                "ts": "2026-05-11T13:00:00Z",
                "run_id": "r1",
                "event_type": "cost_estimate",
                "mode": "shadow",
                "mode_source": "runtime_override",
                "called_api": True,
                "permission_gate_enforced": False,
                "cost_gate_enforced": False,
                "symbol": "BTC/USDT",
                "regime": "normal",
                "notional_usdt": 200,
                "quantile": "p75",
                "total_cost_bps": 1.0,
                "effective_total_cost_bps": 5.0,
                "local_cost_bps": 30.0,
                "local_cost_source": "execution.cost_aware_roundtrip_cost_bps",
                "source": "public_spread_proxy",
                "passed": True,
                "filtered": False,
                "would_filter": False,
                "would_filter_by_cost": False,
                "actually_filtered": False,
                "fallback_used": False,
            }
        )
        + "\n"
        + json.dumps(
            {
                "ts": "2026-05-11T13:00:01Z",
                "run_id": "r1",
                "event_type": "filter_order",
                "mode": "shadow",
                "mode_source": "runtime_override",
                "called_api": True,
                "side": "buy",
                "intent": "OPEN_LONG",
                "permission": "SELL_ONLY",
                "final_permission": "ALLOW",
                "permission_gate_enforced": False,
                "cost_gate_enforced": False,
                "would_filter": True,
                "actually_filtered": False,
                "order_filtered": False,
                "filter_reason": "quant_lab_sell_only",
            }
        )
        + "\n"
        + json.dumps(
            {
                "ts": "2026-05-11T13:00:02Z",
                "run_id": "r1",
                "event_type": "request_not_ok",
                "mode": "shadow",
                "success": True,
                "status_code": 200,
                "fallback_used": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (reports / "quant_lab_requests.jsonl").write_text(
        json.dumps({"ts": "2026-05-11T13:00:00Z", "run_id": "r1", "method": "GET", "endpoint_path": "/v1/costs/estimate", "success": True})
        + "\n"
        + json.dumps({"ts": "2026-05-11T13:00:01Z", "run_id": "r1", "method": "GET", "endpoint_path": "/v1/health", "ok": True, "status_code": 200})
        + "\n",
        encoding="utf-8",
    )

    bundle = export_v5_bundle(reports_dir=reports, out_dir=out, window_hours=72)
    sha_path = Path(str(bundle) + ".sha256")

    assert bundle.exists()
    assert sha_path.exists()
    assert hashlib.sha256(bundle.read_bytes()).hexdigest() in sha_path.read_text(encoding="utf-8")
    with tarfile.open(bundle, "r:gz") as tf:
        names = tf.getnames()
        assert "raw/quant_lab/quant_lab_usage.jsonl" in names
        assert "raw/quant_lab/quant_lab_requests.jsonl" in names
        assert "summaries/quant_lab_compliance.csv" in names
        assert "summaries/quant_lab_cost_usage.csv" in names
        assert "summaries/quant_lab_fallbacks.csv" in names
        assert "summaries/quant_lab_config_audit.json" in names
        assert "summaries/window_summary.json" in names
        assert "raw/state/quant_lab_mode.json" in names
        assert not any(Path(name).name == ".env" for name in names)
        compliance = tf.extractfile("summaries/quant_lab_compliance.csv").read().decode("utf-8")
        cost_usage = tf.extractfile("summaries/quant_lab_cost_usage.csv").read().decode("utf-8")
        fallbacks = tf.extractfile("summaries/quant_lab_fallbacks.csv").read().decode("utf-8")
        config_text = tf.extractfile("raw/config/live_prod.yaml").read().decode("utf-8")
        config_audit = json.loads(tf.extractfile("summaries/quant_lab_config_audit.json").read().decode("utf-8"))
        window = json.loads(tf.extractfile("summaries/window_summary.json").read().decode("utf-8"))
        assert "mode" in compliance.splitlines()[0]
        assert "called_api" in compliance.splitlines()[0]
        assert "permission_gate_enforced" in compliance.splitlines()[0]
        assert "cost_gate_enforced" in compliance.splitlines()[0]
        assert "shadow" in compliance
        assert "hypothetical_violation" in compliance
        assert "actual_violation" in compliance
        assert "true,false,false" in compliance
        assert "mode" in cost_usage.splitlines()[0]
        assert "cost_gate_enforced" in cost_usage.splitlines()[0]
        assert "would_filter" in cost_usage.splitlines()[0]
        assert "actually_filtered" in cost_usage.splitlines()[0]
        assert "would_filter_by_cost" in cost_usage.splitlines()[0]
        assert "fallback_used" in cost_usage.splitlines()[0]
        assert "local_cost_bps" in cost_usage.splitlines()[0]
        assert "local_cost_source" in cost_usage.splitlines()[0]
        assert "request_not_ok" not in fallbacks
        assert "allow_insecure_http_with_token: true" in config_text
        assert "allow_local_fallback_in_enforce: false" in config_text
        assert "api_token_env: QUANT_LAB_API_TOKEN" in config_text
        assert config_audit["mode"] == "shadow"
        assert config_audit["mode_source"] == "runtime_override"
        assert config_audit["allow_insecure_http_with_token"] is True
        assert config_audit["base_url_scheme"] == "http"
        assert config_audit["base_url_host"] == "qyun2.hrhome.top"
        assert window["quant_lab_mode"] == "shadow"
        assert window["quant_lab_mode_source"] == "runtime_override"
        assert window["quant_lab_request_success_count"] == 2
        assert window["quant_lab_request_error_count"] == 0
        assert window["quant_lab_actual_filter_count"] == 0
        assert window["quant_lab_hypothetical_filter_count"] == 1
        assert window["quant_lab_fallback_count"] == 0
