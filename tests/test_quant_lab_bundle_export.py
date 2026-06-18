from __future__ import annotations

import hashlib
import csv
import json
import subprocess
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.reporting.v5_bundle_exporter import (
    GIT_COMMAND_TIMEOUT_SEC,
    _dedupe_fill_rows,
    _git_command,
    _issues,
    _normalized_symbol,
    export_v5_bundle,
)


def test_bundle_export_normalized_symbol_handles_runtime_variants() -> None:
    assert _normalized_symbol("BNB/USDT") == "BNB-USDT"
    assert _normalized_symbol("BNB-USDT") == "BNB-USDT"
    assert _normalized_symbol("BNBUSDT") == "BNB-USDT"
    assert _normalized_symbol("OKX:BNB-USDT") == "BNB-USDT"
    assert _normalized_symbol("okx:bnb_usdt") == "BNB-USDT"


def test_bundle_export_fill_dedupe_uses_normalized_symbol() -> None:
    rows = [
        {
            "run_id": "r1",
            "order_id": "ord-1",
            "trade_id": "trade-1",
            "ts_utc": "2026-05-25T00:00:01Z",
            "symbol": "BNB/USDT",
            "qty": "0.02",
        },
        {
            "run_id": "r1",
            "order_id": "ord-1",
            "trade_id": "trade-1",
            "ts_utc": "2026-05-25T00:00:01Z",
            "symbol": "OKX:BNB-USDT",
            "qty": "0.02",
        },
    ]

    deduped = _dedupe_fill_rows(rows)

    assert len(deduped) == 1
    assert deduped[0]["symbol"] == "BNB/USDT"


def test_bundle_issues_flag_permission_contract_and_enforceable_failures() -> None:
    issues = _issues(
        rows=[
            {
                "event_type": "live_permission",
                "permission_contract_violation": "true",
                "raw_permission_enforceable": "false",
            }
        ],
        request_rows=[],
        cost_rows=[{"event_type": "cost_estimate"}],
        compliance_rows=[],
    )

    by_code = {item["code"]: item for item in issues}
    assert by_code["quant_lab_permission_contract_violation"]["severity"] == "high"
    assert by_code["quant_lab_permission_not_enforceable"]["severity"] == "medium"


def test_bundle_export_windows_raw_quant_lab_jsonl(tmp_path: Path) -> None:
    root = tmp_path / "root"
    reports = root / "reports"
    out = tmp_path / "bundles"
    reports.mkdir(parents=True)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    recent = (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    old = (now - timedelta(days=10)).isoformat().replace("+00:00", "Z")
    (reports / "quant_lab_usage.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts_utc": old, "run_id": "old_usage", "event_type": "health_check"}),
                json.dumps({"ts_utc": recent, "run_id": "recent_usage", "event_type": "health_check"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (reports / "quant_lab_requests.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"timestamp": old, "run_id": "old_request", "endpoint_path": "/v1/health"}),
                json.dumps({"timestamp": recent, "run_id": "recent_request", "endpoint_path": "/v1/health"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    bundle = export_v5_bundle(reports_dir=reports, out_dir=out, window_hours=72)

    with tarfile.open(bundle, "r:gz") as tf:
        usage_rows = [
            json.loads(line)
            for line in tf.extractfile("raw/quant_lab/quant_lab_usage.jsonl").read().decode("utf-8").splitlines()
        ]
        request_rows = [
            json.loads(line)
            for line in tf.extractfile("raw/quant_lab/quant_lab_requests.jsonl").read().decode("utf-8").splitlines()
        ]
    assert [row["run_id"] for row in usage_rows] == ["recent_usage"]
    assert [row["run_id"] for row in request_rows] == ["recent_request"]


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
                "  api_env_require_secure_permissions: true",
                "  allow_api_env_symlink: false",
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
    effective_config = {
        "strategy_version": "test-v5",
        "quant_lab_contract_version": "v5.quant_lab.telemetry.v2",
        "execution": {"market_impulse_probe_enabled": False},
    }
    (reports / "effective_live_config.json").write_text(json.dumps(effective_config), encoding="utf-8")
    (reports / "auto_risk_eval.json").write_text(
        json.dumps({"current_level": "PROTECT", "status": "ok"}),
        encoding="utf-8",
    )
    (reports / "event_candidates.json").write_text(
        json.dumps({"regime": "TRENDING", "candidates": [{"symbol": "BTC/USDT"}, {"symbol": "BNB/USDT"}]}),
        encoding="utf-8",
    )
    (reports / "quant_lab_usage.jsonl").write_text(
        json.dumps(
            {
                "ts": "2026-05-11T12:59:58Z",
                "run_id": "r1",
                "event_type": "health_check",
                "mode": "shadow",
                "local_mode": "shadow",
                "endpoint_path": "/v1/health",
                "success": True,
                "deep_health_status": "warning",
                "deep_health_warnings": ["cost_health_warning"],
                "deep_cost_health_status": "warning",
                "deep_cost_fallback_ratio": 1.0,
                "deep_cost_hard_fallback_ratio": 0.0,
                "deep_cost_soft_fallback_ratio": 1.0,
                "deep_cost_actual_rows": 0,
                "deep_cost_mixed_rows": 0,
                "deep_cost_proxy_rows": 33,
                "deep_cost_global_default_rows": 0,
                "deep_cost_proxy_only_count": 33,
                "deep_cost_symbols_missing": ["ALLO-USDT", "BCH-USDT"],
                "deep_cost_warnings": ["soft_fallback_ratio_gt_0.5", "all_rows_public_spread_proxy"],
                "contract_version": "v5.quant_lab.telemetry.v2",
            }
        )
        + "\n"
        + json.dumps(
            {
                "ts": "2026-05-11T12:59:59Z",
                "run_id": "r1",
                "event_type": "live_permission",
                "mode": "shadow",
                "local_mode": "shadow",
                "mode_source": "runtime_override",
                "quant_lab_requested_mode": "enforce",
                "quant_lab_effective_mode": "shadow",
                "enforce_readiness_status": "BLOCKED",
                "enforce_blocked_reasons": ["global_default_cost_count_high"],
                "enforce_blocked_reason": "global_default_cost_count_high",
                "contract_version_match": True,
                "telemetry_schema_version_match": True,
                "called_api": True,
                "permission_gate_enforced": False,
                "cost_gate_enforced": False,
                "raw_permission_decision": "ABORT",
                "raw_permission_status": "ACTIVE_ABORT",
                "raw_permission_enforceable": True,
                "quant_lab_permission": "ABORT",
                "effective_permission_decision": "ALLOW",
                "final_permission": "ALLOW",
                "would_block_if_enforced": True,
                "shadow_override_reason": "quant_lab_shadow_mode",
                "fallback_used": False,
                "fallback_reason": None,
                "remote_permission_as_of_ts": "2026-05-11T12:59:58Z",
                "remote_permission_expires_at": "2026-05-11T13:09:58Z",
                "remote_permission_status": "ACTIVE_ABORT",
                "remote_permission_source_bundle_ts": "2026-05-11T12:58:00Z",
                "remote_permission_telemetry_latest_ts": "2026-05-11T12:57:00Z",
                "remote_permission_contract_version": "v5.quant_lab.telemetry.v2",
                "permission_contract_violation": False,
                "contract_version": "v5.quant_lab.telemetry.v2",
            }
        )
        + "\n"
        + json.dumps(
            {
                "ts": "2026-05-11T13:00:00Z",
                "run_id": "r1",
                "event_type": "cost_estimate",
                "schema_version": "1.0.0",
                "contract_version": "v5.quant_lab.telemetry.v2",
                "event_id_generation_version": "quant_lab_event_id_v1",
                "mode": "shadow",
                "mode_source": "runtime_override",
                "called_api": True,
                "api_env_path_present": True,
                "api_env_secure_permissions": True,
                "api_env_token_loaded": True,
                "api_env_warning": None,
                "permission_gate_enforced": False,
                "cost_gate_enforced": False,
                "symbol": "BTC/USDT",
                "normalized_symbol": "BTC-USDT",
                "venue": "OKX",
                "instrument_type": "spot",
                "side": "buy",
                "strategy_id": "v5",
                "request_id": "cost-1",
                "regime": "normal",
                "notional_usdt": 200,
                "quantile": "p75",
                "total_cost_bps": 1.0,
                "effective_total_cost_bps": 5.0,
                "total_cost_bps_p50": 0.8,
                "total_cost_bps_p75": 1.0,
                "total_cost_bps_p90": 2.0,
                "local_cost_bps": 30.0,
                "local_cost_source": "execution.cost_aware_roundtrip_cost_bps",
                "source": "public_spread_proxy",
                "cost_source": "public_spread_proxy",
                "expected_edge_bps": 60.0,
                "expected_edge_source": "final_score_proxy",
                "min_required_edge_bps": 45.0,
                "required_edge_bps": 45.0,
                "passed": True,
                "filtered": False,
                "would_filter": False,
                "would_filter_by_cost": False,
                "would_block_by_cost": False,
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
                "would_block_if_enforced": True,
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
        + "\n"
        + json.dumps({"ts": "2026-05-11T13:00:02Z", "run_id": "r1", "method": "POST", "endpoint_path": "/v1/risk/live-permission", "success": True, "status_code": 200})
        + "\n"
        + json.dumps({"ts": "2026-05-11T13:00:03Z", "run_id": "r1", "method": "GET", "endpoint_path": "/v1/costs/estimate", "success": False, "error_type": "QuantLabTimeout"})
        + "\n",
        encoding="utf-8",
    )
    run_dir = reports / "runs" / "r1"
    run_dir.mkdir(parents=True)
    (run_dir / "trades.csv").write_text(
        "\n".join(
            [
                "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt",
                "2026-05-11T13:00:04Z,r1,BNB/USDT,OPEN_LONG,buy,0.02,600,12,0.012,0.001",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "summary.json").write_text(
        json.dumps({"run_id": "r1", "num_trades": 0, "budget": {"fills_count_today": 0}}),
        encoding="utf-8",
    )
    (run_dir / "candidate_snapshot.csv").write_text(
        "\n".join(
            [
                "candidate_id,run_id,ts_utc,symbol,strategy_candidate,final_decision,expected_edge_bps,required_edge_bps,cost_bps,cost_source,cost_model_version,cost_gate_verified",
                "cand_r1_bnb,r1,2026-05-11T13:00:00Z,BNB/USDT,f4_volume_swing,OPEN_LONG,60,45,30,cost_not_available,v5_local_execution.cost_aware_roundtrip_cost_bps,false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "order_lifecycle.csv").write_text(
        "\n".join(
            [
                "schema_version,lifecycle_id,run_id,ts_utc,symbol,normalized_symbol,side,intent,order_state,decision_ts,signal_price,arrival_bid,arrival_ask,arrival_mid,spread_bps_at_decision,submit_ts,order_type,order_px,cl_ord_id,exchange_order_id,first_fill_ts,last_fill_ts,fill_px,avg_fill_px,filled_qty,fee,fee_ccy,fee_usdt,notional_usdt,requested_notional_usdt,trade_ids,fill_count",
                "v5.order_lifecycle.v1,olc_r1_bnb,r1,2026-05-11T13:00:04Z,BNB/USDT,BNB-USDT,buy,OPEN_LONG,FILLED,2026-05-11T13:00:00Z,600,599,601,600,33.3333333333,2026-05-11T13:00:01Z,market,null,clid-1,okx-1,2026-05-11T13:00:04Z,2026-05-11T13:00:04Z,602,602,0.02,-0.01204,USDT,0.01204,12.04,12,trade-1,1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    bundle = export_v5_bundle(reports_dir=reports, out_dir=out, window_hours=24 * 3650)
    sha_path = Path(str(bundle) + ".sha256")

    assert bundle.exists()
    assert sha_path.exists()
    assert hashlib.sha256(bundle.read_bytes()).hexdigest() in sha_path.read_text(encoding="utf-8")
    with tarfile.open(bundle, "r:gz") as tf:
        names = tf.getnames()
        assert "raw/quant_lab/quant_lab_usage.jsonl" in names
        assert "raw/quant_lab/quant_lab_requests.jsonl" in names
        assert "summaries/quant_lab_compliance.csv" in names
        assert "summaries/quant_lab_permission_audit.csv" in names
        assert "summaries/quant_lab_mode_audit.csv" in names
        assert "summaries/quant_lab_cost_usage.csv" in names
        assert "summaries/runtime_cost_guard.csv" in names
        assert "summaries/cost_disagreement.csv" in names
        assert "summaries/quant_lab_fallbacks.csv" in names
        assert "summaries/enforce_readiness_snapshot.json" in names
        assert "summaries/quant_lab_config_audit.json" in names
        assert "summaries/trade_metrics.csv" in names
        assert "summaries/fill_metrics.csv" in names
        assert "summaries/candidate_snapshot.csv" in names
        assert "raw/reports/candidate_snapshot.csv" in names
        assert "summaries/order_lifecycle.csv" in names
        assert "summaries/paper_strategy_runs.csv" in names
        assert "summaries/paper_strategy_daily.csv" in names
        assert "summaries/paper_slippage_coverage.csv" in names
        assert "summaries/risk_on_multi_buy_shadow.csv" in names
        assert "summaries/fast_microstructure_strategy_shadow.csv" in names
        assert "raw/recent_runs/r1/candidate_snapshot.csv" in names
        assert "raw/recent_runs/r1/order_lifecycle.csv" in names
        assert "raw/state/auto_risk_eval.json" in names
        assert "raw/reports/event_candidates.json" in names
        assert "raw/effective_live_config.json" in names
        assert "raw/reports/effective_live_config.json" in names
        assert "reports/summary_trade_count_mismatch.csv" in names
        assert "summaries/window_summary.json" in names
        assert "summaries/market_context.json" in names
        assert "reports/index.html" in names
        assert "reports/index.json" in names
        assert "raw/large/.noindex" in names
        assert "raw/state/quant_lab_mode.json" in names
        assert not any(Path(name).name == ".env" for name in names)
        compliance = tf.extractfile("summaries/quant_lab_compliance.csv").read().decode("utf-8")
        permission_audit = tf.extractfile("summaries/quant_lab_permission_audit.csv").read().decode("utf-8")
        mode_audit = tf.extractfile("summaries/quant_lab_mode_audit.csv").read().decode("utf-8")
        cost_usage = tf.extractfile("summaries/quant_lab_cost_usage.csv").read().decode("utf-8")
        runtime_cost_guard = tf.extractfile("summaries/runtime_cost_guard.csv").read().decode("utf-8")
        cost_disagreement = tf.extractfile("summaries/cost_disagreement.csv").read().decode("utf-8")
        fallbacks = tf.extractfile("summaries/quant_lab_fallbacks.csv").read().decode("utf-8")
        config_text = tf.extractfile("raw/config/live_prod.yaml").read().decode("utf-8")
        effective_config_alias = tf.extractfile("raw/effective_live_config.json").read().decode("utf-8")
        effective_config_reports = tf.extractfile("raw/reports/effective_live_config.json").read().decode("utf-8")
        config_audit = json.loads(tf.extractfile("summaries/quant_lab_config_audit.json").read().decode("utf-8"))
        readiness_snapshot = json.loads(tf.extractfile("summaries/enforce_readiness_snapshot.json").read().decode("utf-8"))
        trade_metrics = list(csv.DictReader(tf.extractfile("summaries/trade_metrics.csv").read().decode("utf-8").splitlines()))
        fill_metrics = list(csv.DictReader(tf.extractfile("summaries/fill_metrics.csv").read().decode("utf-8").splitlines()))
        permission_audit_rows = list(csv.DictReader(permission_audit.splitlines()))
        candidate_snapshot = list(csv.DictReader(tf.extractfile("summaries/candidate_snapshot.csv").read().decode("utf-8").splitlines()))
        raw_candidate_snapshot = list(csv.DictReader(tf.extractfile("raw/reports/candidate_snapshot.csv").read().decode("utf-8").splitlines()))
        order_lifecycle = list(csv.DictReader(tf.extractfile("summaries/order_lifecycle.csv").read().decode("utf-8").splitlines()))
        mismatch_rows = list(csv.DictReader(tf.extractfile("reports/summary_trade_count_mismatch.csv").read().decode("utf-8").splitlines()))
        manifest = json.loads(tf.extractfile("manifest.json").read().decode("utf-8"))
        window = json.loads(tf.extractfile("summaries/window_summary.json").read().decode("utf-8"))
        market_context = json.loads(tf.extractfile("summaries/market_context.json").read().decode("utf-8"))
        report_index = json.loads(tf.extractfile("reports/index.json").read().decode("utf-8"))
        report_index_html = tf.extractfile("reports/index.html").read().decode("utf-8")
        assert "mode" in compliance.splitlines()[0]
        assert json.loads(effective_config_alias) == effective_config
        assert effective_config_alias == effective_config_reports
        assert "called_api" in compliance.splitlines()[0]
        assert "permission_gate_enforced" in compliance.splitlines()[0]
        assert "cost_gate_enforced" in compliance.splitlines()[0]
        assert "selected_entry_gate_cost_bps" in runtime_cost_guard.splitlines()[0]
        assert "quant_lab_roundtrip_cost_bps" in runtime_cost_guard.splitlines()[0]
        assert "v5_runtime_roundtrip_cost_bps" in cost_disagreement.splitlines()[0]
        assert "raw_permission_decision" in compliance.splitlines()[0]
        assert "raw_permission_status" in compliance.splitlines()[0]
        assert "raw_permission_enforceable" in compliance.splitlines()[0]
        assert "effective_permission_decision" in compliance.splitlines()[0]
        assert "would_block_if_enforced" in compliance.splitlines()[0]
        assert "shadow_override_reason" in compliance.splitlines()[0]
        assert "remote_permission_status" in compliance.splitlines()[0]
        assert "remote_permission_source_bundle_ts" in compliance.splitlines()[0]
        assert "remote_permission_contract_version" in compliance.splitlines()[0]
        assert "permission_contract_violation" in compliance.splitlines()[0]
        assert "deep_cost_hard_fallback_ratio" in permission_audit.splitlines()[0]
        assert "deep_cost_soft_fallback_ratio" in permission_audit.splitlines()[0]
        assert "shadow" in compliance
        assert "ABORT" in compliance
        assert "ALLOW" in compliance
        assert "ACTIVE_ABORT" in permission_audit
        assert "quant_lab_shadow_mode" in permission_audit
        deep_health_row = next(row for row in permission_audit_rows if row["deep_cost_health_status"] == "warning")
        assert deep_health_row["deep_health_status"] == "warning"
        assert deep_health_row["deep_health_warnings"] == "cost_health_warning"
        assert deep_health_row["deep_cost_hard_fallback_ratio"] == "0.0"
        assert deep_health_row["deep_cost_soft_fallback_ratio"] == "1.0"
        assert deep_health_row["deep_cost_proxy_rows"] == "33"
        assert deep_health_row["deep_cost_proxy_only_count"] == "33"
        assert deep_health_row["deep_cost_symbols_missing"] == "ALLO-USDT;BCH-USDT"
        assert deep_health_row["deep_cost_warnings"] == "soft_fallback_ratio_gt_0.5;all_rows_public_spread_proxy"
        assert "quant_lab_requested_mode" in mode_audit.splitlines()[0]
        assert "enforce_readiness_status" in mode_audit.splitlines()[0]
        assert "enforce" in mode_audit
        assert "BLOCKED" in mode_audit
        assert "global_default_cost_count_high" in mode_audit
        assert report_index["schema_version"] == "v5.static_report_index.v1"
        assert report_index["latest_trade_count"] == len(trade_metrics)
        assert report_index["candidate_snapshot_rows"] == len(candidate_snapshot)
        assert report_index["links"]["market_context"] == "../summaries/market_context.json"
        assert "V5 Follow-up Report" in report_index_html
        assert "market_context" in report_index_html
        assert "raw_large_file_count" in report_index_html
        assert market_context["schema_version"] == "v5.market_context.v1"
        assert market_context["auto_risk"]["current_level"] == "PROTECT"
        assert market_context["event_candidates"]["regime"] == "TRENDING"
        assert market_context["event_candidates"]["candidate_count"] == 2
        assert market_context["quant_lab_coordination"]["requested_mode"] == "enforce"
        assert "hypothetical_violation" in compliance
        assert "actual_violation" in compliance
        assert "true,false,false" in compliance
        assert "mode" in cost_usage.splitlines()[0]
        assert "request_symbol" in cost_usage.splitlines()[0]
        assert "normalized_symbol" in cost_usage.splitlines()[0]
        assert "response_symbol" in cost_usage.splitlines()[0]
        assert "venue" in cost_usage.splitlines()[0]
        assert "instrument_type" in cost_usage.splitlines()[0]
        assert "strategy_id" in cost_usage.splitlines()[0]
        assert "request_id" in cost_usage.splitlines()[0]
        assert "requested_regime" in cost_usage.splitlines()[0]
        assert "matched_regime" in cost_usage.splitlines()[0]
        assert "cost_source" in cost_usage.splitlines()[0]
        assert "cost_model_version" in cost_usage.splitlines()[0]
        assert "selected_total_cost_bps" in cost_usage.splitlines()[0]
        assert "total_cost_bps_p50" in cost_usage.splitlines()[0]
        assert "total_cost_bps_p75" in cost_usage.splitlines()[0]
        assert "total_cost_bps_p90" in cost_usage.splitlines()[0]
        assert "required_edge_bps" in cost_usage.splitlines()[0]
        assert "would_block_by_cost" in cost_usage.splitlines()[0]
        assert "fallback_used_for_cost_model" in cost_usage.splitlines()[0]
        assert "degraded_cost_model" in cost_usage.splitlines()[0]
        assert "diagnosis" in cost_usage.splitlines()[0]
        assert "warning" in cost_usage.splitlines()[0]
        assert "cost_gate_verified" in cost_usage.splitlines()[0]
        assert "fallback_reason" in cost_usage.splitlines()[0]
        assert "cost_gate_enforced" in cost_usage.splitlines()[0]
        assert "would_filter" in cost_usage.splitlines()[0]
        assert "actually_filtered" in cost_usage.splitlines()[0]
        assert "would_filter_by_cost" in cost_usage.splitlines()[0]
        assert "fallback_used" in cost_usage.splitlines()[0]
        assert "local_cost_bps" in cost_usage.splitlines()[0]
        assert "local_cost_source" in cost_usage.splitlines()[0]
        assert "expected_edge_source" in cost_usage.splitlines()[0]
        assert "BTC-USDT" in cost_usage
        assert "public_spread_proxy" in cost_usage
        assert "final_score_proxy" in cost_usage
        assert "request_not_ok" not in fallbacks
        assert "QuantLabTimeout" in fallbacks
        assert "allow_insecure_http_with_token: true" in config_text
        assert "allow_local_fallback_in_enforce: false" in config_text
        assert "api_env_require_secure_permissions: true" in config_text
        assert "allow_api_env_symlink: false" in config_text
        assert "api_token_env: QUANT_LAB_API_TOKEN" in config_text
        assert config_audit["mode"] == "shadow"
        assert trade_metrics[0]["num_trades"] == "1"
        assert trade_metrics[0]["fills_count_today"] == "1"
        assert fill_metrics[0]["normalized_symbol"] == "BNB-USDT"
        assert candidate_snapshot[0]["candidate_id"] == "cand_r1_bnb"
        assert candidate_snapshot[0]["symbol"] == "BNB/USDT"
        assert candidate_snapshot[0]["strategy_candidate"] == "f4_volume_swing"
        assert candidate_snapshot[0]["cost_source"] == "cost_not_available"
        assert raw_candidate_snapshot == candidate_snapshot
        assert "no_signal_reason" in candidate_snapshot[0]
        assert order_lifecycle[0]["lifecycle_id"] == "olc_r1_bnb"
        assert order_lifecycle[0]["arrival_mid"] == "600"
        assert order_lifecycle[0]["avg_fill_px"] == "602"
        assert mismatch_rows[0]["high_issue"] == "true"
        assert manifest["run_summary_invalid"] is True
        assert manifest["candidate_snapshot_schema_version"] == "v5.candidate_snapshot.v2"
        assert manifest["candidate_snapshot_rows"] == 1
        assert manifest["candidate_cost_source_coverage"] == 1.0
        assert manifest["order_lifecycle_schema_version"] == "v5.order_lifecycle.v1"
        assert manifest["order_lifecycle_rows"] == 1
        assert manifest["summary_trade_count_mismatch_high_issue_count"] == 1
        assert manifest["trade_export_schema_version"] == "v5.trade_export.v1"
        assert manifest["summary_metrics_version"] == "v5.summary_metrics.v1"
        assert manifest["git_dirty"] is False
        assert manifest["dirty_worktree"] is False
        assert manifest["provenance_status"] == "git_clean"
        assert manifest["code_provenance"] == "ok"
        assert window["fill_metrics_rows"] == 1
        assert window["candidate_snapshot_rows"] == 1
        assert window["candidate_cost_source_coverage"] == 1.0
        assert window["order_lifecycle_rows"] == 1
        assert config_audit["mode_source"] == "runtime_override"
        assert "api_env_path_present" in config_audit
        assert "api_env_secure_permissions" in config_audit
        assert config_audit["api_env_token_loaded"] is True
        assert config_audit["allow_insecure_http_with_token"] is True
        assert config_audit["base_url_scheme"] == "http"
        assert config_audit["base_url_host"] == "qyun2.hrhome.top"
        assert window["quant_lab_mode"] == "shadow"
        assert window["quant_lab_mode_source"] == "runtime_override"
        assert window["quant_lab_requested_mode"] == "enforce"
        assert window["quant_lab_effective_mode"] == "shadow"
        assert window["enforce_readiness_status"] == "BLOCKED"
        assert window["contract_version_match"] is True
        assert window["quant_lab_request_success_count"] == 3
        assert window["quant_lab_request_error_count"] == 1
        assert window["quant_lab_actual_filter_count"] == 0
        assert window["quant_lab_hypothetical_filter_count"] >= 1
        assert window["would_block_if_enforced_count"] >= 1
        assert window["effective_block_count"] == 0
        assert window["permission_contract_violation_count"] == 0
        assert window["quant_lab_fallback_count"] == 1
        assert window["quant_lab_actual_fallback_count"] == 1
        assert readiness_snapshot["quant_lab_requested_mode"] == "enforce"
        assert readiness_snapshot["quant_lab_effective_mode"] == "shadow"
        assert readiness_snapshot["status"] == "BLOCKED"
        assert readiness_snapshot["contract_version_match"] is True
        assert readiness_snapshot["global_default_cost_count"] == 0
        assert readiness_snapshot["current_contract_global_default_cost_count"] == 0
        assert readiness_snapshot["legacy_global_default_cost_count"] == 0
        assert window["cost_usage_current_contract_rows"] == 1
        assert window["cost_usage_legacy_rows"] == 0
        assert window["post_deployment_cost_usage_rows"] == 1
        assert window["post_deployment_global_default_cost_count"] == 0


def test_bundle_export_backfills_order_lifecycle_fill_fields_from_trades(tmp_path: Path) -> None:
    root = tmp_path / "root"
    reports = root / "reports"
    out = tmp_path / "bundles"
    run_dir = reports / "runs" / "r_fill"
    run_dir.mkdir(parents=True)
    (reports / "quant_lab_usage.jsonl").write_text("", encoding="utf-8")
    (reports / "quant_lab_requests.jsonl").write_text("", encoding="utf-8")
    (run_dir / "trades.csv").write_text(
        "\n".join(
            [
                "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee,fee_ccy,fee_usdt,slippage_usdt,order_id,trade_id",
                "2026-05-20T07:00:32Z,r_fill,BTC/USDT,OPEN_LONG,buy,0.00013568,77383.7,10.5,-0.0105,USDT,0.0105,0.001,clid-btc,trade-btc-1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "summary.json").write_text(json.dumps({"run_id": "r_fill", "num_trades": 1}), encoding="utf-8")
    (run_dir / "order_lifecycle.csv").write_text(
        "\n".join(
            [
                "schema_version,lifecycle_id,run_id,ts_utc,symbol,normalized_symbol,side,intent,order_state,decision_ts,signal_price,arrival_bid,arrival_ask,arrival_mid,spread_bps_at_decision,submit_ts,order_type,order_px,cl_ord_id,exchange_order_id,first_fill_ts,last_fill_ts,fill_px,avg_fill_px,filled_qty,fee,fee_ccy,fee_usdt,notional_usdt,requested_notional_usdt,trade_ids,fill_count",
                "v5.order_lifecycle.v1,olc_btc,r_fill,2026-05-20T07:00:35Z,BTC/USDT,BTC-USDT,buy,OPEN_LONG,FILLED,2026-05-20T07:00:00Z,77383.7,77380,77390,77385,1.29,2026-05-20T07:00:31Z,market,null,clid-btc,okx-btc,,,,77383.7,0.00013568,,,0,10.5,10.5,,0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    bundle = export_v5_bundle(reports_dir=reports, out_dir=out, window_hours=24 * 3650)

    with tarfile.open(bundle, "r:gz") as tf:
        order_lifecycle = list(csv.DictReader(tf.extractfile("summaries/order_lifecycle.csv").read().decode("utf-8").splitlines()))
        fill_metrics = list(csv.DictReader(tf.extractfile("summaries/fill_metrics.csv").read().decode("utf-8").splitlines()))

    assert len(fill_metrics) == 1
    assert len(order_lifecycle) == 1
    row = order_lifecycle[0]
    assert row["first_fill_ts"] == "2026-05-20T07:00:32Z"
    assert row["last_fill_ts"] == "2026-05-20T07:00:32Z"
    assert row["fill_px"] == "77383.7"
    assert row["avg_fill_px"] == "77383.7"
    assert row["filled_qty"] == "0.00013568"
    assert row["fee"] == "-0.0105"
    assert row["fee_ccy"] == "USDT"
    assert row["fee_usdt"] == "0.0105"
    assert row["trade_ids"] == "trade-btc-1"
    assert row["fill_count"] == "1"


def test_bundle_export_flags_missing_order_lifecycle_when_trades_exist(tmp_path: Path) -> None:
    root = tmp_path / "root"
    reports = root / "reports"
    out = tmp_path / "bundles"
    run_dir = reports / "runs" / "r_trade"
    run_dir.mkdir(parents=True)
    (reports / "quant_lab_usage.jsonl").write_text("", encoding="utf-8")
    (reports / "quant_lab_requests.jsonl").write_text("", encoding="utf-8")
    (run_dir / "trades.csv").write_text(
        "\n".join(
            [
                "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt",
                "2026-05-15T02:00:01Z,r_trade,BTC/USDT,CLOSE_LONG,sell,0.0002,78000,15.6,0.0156,0.002",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "summary.json").write_text(json.dumps({"run_id": "r_trade", "num_trades": 1}), encoding="utf-8")

    bundle = export_v5_bundle(reports_dir=reports, out_dir=out, window_hours=24 * 3650)

    with tarfile.open(bundle, "r:gz") as tf:
        names = tf.getnames()
        assert "summaries/order_lifecycle.csv" in names
        order_lifecycle = list(csv.DictReader(tf.extractfile("summaries/order_lifecycle.csv").read().decode("utf-8").splitlines()))
        issues = json.loads(tf.extractfile("summaries/issues_to_fix.json").read().decode("utf-8"))
        manifest = json.loads(tf.extractfile("manifest.json").read().decode("utf-8"))
        window = json.loads(tf.extractfile("summaries/window_summary.json").read().decode("utf-8"))

    assert order_lifecycle == []
    issue = next(item for item in issues if item["code"] == "order_lifecycle_missing_for_trades")
    assert issue["severity"] == "high"
    assert issue["trade_metric_fill_count"] == 1
    assert manifest["order_lifecycle_rows"] == 0
    assert manifest["order_lifecycle_trade_metric_fill_count"] == 1
    assert manifest["order_lifecycle_missing_high_issue"] is True
    assert window["order_lifecycle_missing_high_issue"] is True


def test_bundle_git_command_uses_timeout(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr("src.reporting.v5_bundle_exporter.shutil.which", lambda name: "/usr/bin/git")

    def fake_run(cmd, **kwargs):
        calls.append({"cmd": cmd, **kwargs})
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="abc123\n", stderr="")

    monkeypatch.setattr("src.reporting.v5_bundle_exporter.subprocess.run", fake_run)

    assert _git_command(tmp_path, ["rev-parse", "--short", "HEAD"]) == "abc123"
    assert calls[0]["timeout"] == GIT_COMMAND_TIMEOUT_SEC


def test_bundle_git_command_timeout_returns_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("src.reporting.v5_bundle_exporter.shutil.which", lambda name: "/usr/bin/git")

    def fake_run(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=kwargs.get("timeout"))

    monkeypatch.setattr("src.reporting.v5_bundle_exporter.subprocess.run", fake_run)

    assert _git_command(tmp_path, ["status", "--porcelain"]) == ""
