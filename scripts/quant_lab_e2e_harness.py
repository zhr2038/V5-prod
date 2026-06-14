from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import shutil
import sys
import tarfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.schema import AppConfig  # noqa: E402
from src.core.models import Order  # noqa: E402
from src.quant_lab_client.client import QuantLabClient  # noqa: E402
from src.quant_lab_client.guard import QuantLabGuard  # noqa: E402
from src.quant_lab_client.models import symbol_to_quant_lab_symbol  # noqa: E402
from src.reporting.quant_lab_audit import (  # noqa: E402
    CONTRACT_VERSION,
    EVENT_TYPE_FALLBACK,
    EVENT_TYPE_REQUEST,
    SCHEMA_VERSION,
    append_quant_lab_usage,
    normalize_quant_lab_event,
)
from src.reporting.summary_writer import write_summary  # noqa: E402
from src.reporting.v5_bundle_exporter import export_v5_bundle  # noqa: E402


FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "quant_lab_contract"
DEFAULT_OUT_DIR = PROJECT_ROOT / "tests" / "fixtures" / "quant_lab_e2e"
RUN_ID = "e2e-quant-lab-shadow-001"


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class _MockResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = dict(payload)
        self.status_code = status_code

    def json(self) -> dict[str, Any]:
        return dict(self._payload)


class MockQuantLabHTTP:
    """Small requests-like mock server for the Quant Lab e2e contract harness."""

    def __init__(self) -> None:
        self.cost_payloads = [
            _load_fixture("cost_response_public_proxy.json"),
            _load_fixture("cost_response_global_default.json"),
        ]
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, *, params: dict[str, Any] | None = None, headers=None, timeout=None) -> _MockResponse:
        path = urlparse(url).path
        query = dict(params or {})
        self.calls.append({"method": "GET", "path": path, "params": query})
        if path == "/v1/health":
            raise requests.Timeout("fixture health timeout")
        if path == "/v1/risk/live-permission":
            payload = _load_fixture("risk_permission_active_abort.json")
            payload.update(
                {
                    "request_id": query.get("request_id") or payload.get("request_id"),
                    "event_id": query.get("event_id") or payload.get("event_id"),
                    "run_id": query.get("run_id") or payload.get("run_id"),
                    "ts_utc": query.get("ts_utc") or payload.get("ts_utc"),
                    "strategy": query.get("strategy") or payload.get("strategy"),
                    "version": query.get("version") or payload.get("version"),
                }
            )
            return _MockResponse(payload)
        if path == "/v1/costs/estimate":
            payload = dict(self.cost_payloads.pop(0) if self.cost_payloads else _load_fixture("cost_response_global_default.json"))
            normalized_symbol = symbol_to_quant_lab_symbol(query.get("symbol") or query.get("normalized_symbol"))
            payload.update(
                {
                    "request_id": query.get("request_id") or payload.get("request_id"),
                    "event_id": query.get("event_id") or payload.get("event_id"),
                    "run_id": query.get("run_id") or payload.get("run_id"),
                    "ts_utc": query.get("ts_utc") or payload.get("ts_utc"),
                    "symbol": normalized_symbol,
                    "notional_usdt": float(query.get("notional_usdt") or payload.get("notional_usdt") or 0.0),
                    "quantile": query.get("quantile") or payload.get("quantile"),
                }
            )
            return _MockResponse(payload)
        return _MockResponse({"error": "not found", "path": path}, status_code=404)


def _reset_output_dir(out_dir: Path) -> None:
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    for child_name in ("workspace", "bundles"):
        child = (out_dir / child_name).resolve()
        if child.is_dir() and out_dir in child.parents:
            shutil.rmtree(child)
    for file_name in (
        "e2e_quant_lab_bundle_fixture.tar.gz",
        "e2e_quant_lab_bundle_fixture.tar.gz.sha256",
        "e2e_quant_lab_test_report.json",
    ):
        child = (out_dir / file_name).resolve()
        if child.is_file() and out_dir in child.parents:
            child.unlink()


def _write_config(root: Path) -> None:
    config_dir = root / "configs"
    state_dir = root / "state"
    config_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "quant_lab:",
                "  enabled: true",
                "  mode: shadow",
                "  base_url: https://quant-lab.mock",
                "  api_token_env: QUANT_LAB_API_TOKEN",
                "  fail_policy: allow_local_fallback",
                "  allow_local_fallback_in_enforce: false",
                "  allow_insecure_http_with_token: false",
                "  strategy_name: v5",
                "  strategy_version: '5.0.0'",
                "  runtime_override_path: state/quant_lab_mode.json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (state_dir / "quant_lab_mode.json").write_text(
        json.dumps(
            {
                "mode": "shadow",
                "reason": "e2e_contract_fixture",
                "updated_by": "quant_lab_e2e_harness",
                "updated_at": "2026-05-14T00:00:00Z",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _make_cfg(root: Path) -> AppConfig:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.base_url = "https://quant-lab.mock"
    cfg.quant_lab.fail_policy = "allow_local_fallback"
    cfg.quant_lab.audit_path = str(root / "reports" / "quant_lab_usage.jsonl")
    cfg.quant_lab.request_log_path = str(root / "reports" / "quant_lab_requests.jsonl")
    cfg.quant_lab.runtime_override_path = str(root / "state" / "quant_lab_mode.json")
    cfg.quant_lab.allow_runtime_override = True
    cfg.quant_lab.enforce_readiness_enabled = False
    cfg.quant_lab.cost_min_edge_multiplier = 1.5
    cfg.quant_lab.min_cost_bps_floor = 5.0
    cfg.execution.cost_aware_roundtrip_cost_bps = 30.0
    cfg.execution.fee_bps = 10.0
    cfg.execution.slippage_bps = 5.0
    return cfg


def _record_health_timeout(client: QuantLabClient, reports: Path) -> None:
    request = normalize_quant_lab_event(
        {
            "schema_version": SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "run_id": RUN_ID,
            "event_type": EVENT_TYPE_REQUEST,
            "request_id": f"{RUN_ID}:health",
            "event_id": "e2e-health-timeout-request",
            "ts_utc": "2026-05-14T00:05:00Z",
            "endpoint_path": "/v1/health",
        },
        default_event_type=EVENT_TYPE_REQUEST,
    )
    try:
        client.get_json(
            "/v1/health",
            params={
                "schema_version": request["schema_version"],
                "contract_version": request["contract_version"],
                "event_id": request["event_id"],
                "request_id": request["request_id"],
                "run_id": RUN_ID,
                "ts_utc": request["ts_utc"],
            },
        )
    except Exception as exc:
        append_quant_lab_usage(
            reports / "quant_lab_usage.jsonl",
            {
                "schema_version": SCHEMA_VERSION,
                "contract_version": CONTRACT_VERSION,
                "run_id": RUN_ID,
                "event_type": EVENT_TYPE_FALLBACK,
                "request_id": request["request_id"],
                "original_request_id": request["request_id"],
                "original_event_id": request["event_id"],
                "ts_utc": "2026-05-14T00:05:01Z",
                "endpoint_path": "/v1/health",
                "success": False,
                "fallback_used": True,
                "fallback_reason": "health_timeout_shadow_observation",
                "fallback_policy": "allow_local_fallback",
                "fallback_scope": "health_check",
                "action_taken": "continue_shadow",
                "error_type": type(exc).__name__,
                "error_message_short": "timeout",
            },
        )


def _write_trade_artifacts(run_dir: Path) -> dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "trades.csv").write_text(
        "\n".join(
            [
                "run_id,ts_utc,symbol,normalized_symbol,side,action,qty,price,notional_usdt,fee,fee_ccy,fee_usdt,slippage_usdt,order_id,trade_id,strategy_id,position_id",
                f"{RUN_ID},2026-05-14T00:10:00Z,BNB/USDT,BNB-USDT,buy,OPEN_LONG,0.1,600,60,0.006,USDT,0.006,0.002,ord-e2e-1,trd-e2e-1,v5,pos-e2e-1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "equity.jsonl").write_text(
        json.dumps({"ts": "2026-05-14T00:00:00Z", "equity": 1000.0}) + "\n",
        encoding="utf-8",
    )
    return write_summary(run_dir)


def _copy_bundle(bundle: Path, out_dir: Path) -> Path:
    target = out_dir / "e2e_quant_lab_bundle_fixture.tar.gz"
    shutil.copy2(bundle, target)
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    (out_dir / "e2e_quant_lab_bundle_fixture.tar.gz.sha256").write_text(
        f"{digest}  {target.name}\n",
        encoding="utf-8",
    )
    return target


def _read_bundle_csv(bundle: Path, member: str) -> list[dict[str, str]]:
    with tarfile.open(bundle, "r:gz") as tf:
        text = tf.extractfile(member).read().decode("utf-8")  # type: ignore[union-attr]
    return list(csv.DictReader(io.StringIO(text)))


def _read_bundle_json(bundle: Path, member: str) -> dict[str, Any]:
    with tarfile.open(bundle, "r:gz") as tf:
        return json.loads(tf.extractfile(member).read().decode("utf-8"))  # type: ignore[union-attr]


def run_harness(out_dir: str | Path = DEFAULT_OUT_DIR) -> dict[str, Any]:
    out = Path(out_dir).resolve()
    _reset_output_dir(out)
    root = out / "workspace"
    reports = root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    _write_config(root)
    cfg = _make_cfg(root)
    mock_http = MockQuantLabHTTP()
    client = QuantLabClient(
        base_url=cfg.quant_lab.base_url,
        timeout_seconds=0.2,
        max_retries=0,
        cache_ttl_seconds=0,
        cost_cache_ttl_seconds=0,
        http_client=mock_http,
        request_log_path=cfg.quant_lab.request_log_path,
        run_id=RUN_ID,
        phase="e2e_harness",
    )
    guard = QuantLabGuard(
        client=client,
        cfg=cfg.quant_lab,
        usage_log_path=cfg.quant_lab.audit_path,
        run_id=RUN_ID,
        phase="e2e_harness",
    )

    _record_health_timeout(client, reports)
    permission = guard.check_startup_permission(cfg, RUN_ID)
    guard.record_final_permission(local_preflight_permission="ALLOW", final_permission="ALLOW")

    orders = [
        Order("BNB/USDT", "buy", "OPEN_LONG", 200.0, 600.0, {"expected_edge_bps": 60.0}),
        Order("BNB-USDT", "buy", "OPEN_LONG", 200.0, 600.0, {"expected_edge_bps": 10.0}),
    ]
    orders = guard.filter_orders_by_permission(orders, permission)
    kept_orders, cost_rows = guard.enrich_orders_with_cost(orders, "normal", cfg)

    run_dir = reports / "runs" / RUN_ID
    (run_dir).mkdir(parents=True, exist_ok=True)
    (run_dir / "decision_audit.json").write_text(
        json.dumps({"run_id": RUN_ID, "quant_lab": guard.audit_payload()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary = _write_trade_artifacts(run_dir)
    bundle = export_v5_bundle(reports_dir=reports, out_dir=out / "bundles", window_hours=100000)
    fixture_bundle = _copy_bundle(bundle, out)

    cost_usage = _read_bundle_csv(fixture_bundle, "summaries/quant_lab_cost_usage.csv")
    permission_audit = _read_bundle_csv(fixture_bundle, "summaries/quant_lab_permission_audit.csv")
    fallbacks = _read_bundle_csv(fixture_bundle, "summaries/quant_lab_fallbacks.csv")
    trade_metrics = _read_bundle_csv(fixture_bundle, "summaries/trade_metrics.csv")
    manifest = _read_bundle_json(fixture_bundle, "manifest.json")

    raw_usage_ids = {
        row.get("event_id")
        for row in (json.loads(line) for line in (reports / "quant_lab_usage.jsonl").read_text(encoding="utf-8").splitlines())
        if row.get("event_id")
    }
    global_default_rows = [row for row in cost_usage if row.get("cost_source") == "global_default"]
    permission_rows = [row for row in permission_audit if row.get("raw_permission_decision") == "ABORT"]
    health_fallback_rows = [row for row in fallbacks if row.get("endpoint_path") == "/v1/health"]

    def truthy(value: Any) -> bool:
        return str(value).strip().lower() in {"1", "true", "yes", "y"}

    checks = {
        "request_event_ids_stable": bool(global_default_rows and global_default_rows[0].get("event_id") in raw_usage_ids),
        "global_default_degraded": bool(global_default_rows and truthy(global_default_rows[0].get("degraded_cost_model"))),
        "shadow_abort_effective_allow": bool(
            permission_rows
            and any(
                row.get("effective_permission_decision") == "ALLOW"
                and truthy(row.get("would_block_if_enforced"))
                and not truthy(row.get("permission_gate_enforced"))
                for row in permission_rows
            )
        ),
        "timeout_fallback_used": bool(health_fallback_rows and truthy(health_fallback_rows[0].get("fallback_used"))),
        "trades_summary_nonzero": int(summary.get("num_trades") or 0) > 0
        and bool(trade_metrics and int(float(trade_metrics[0].get("num_trades") or 0)) > 0),
        "manifest_contract_schema": manifest.get("contract_version") == CONTRACT_VERSION
        and manifest.get("schema_version") == SCHEMA_VERSION,
    }
    report = {
        "run_id": RUN_ID,
        "bundle_path": str(fixture_bundle),
        "report_path": str(out / "e2e_quant_lab_test_report.json"),
        "workspace": str(root),
        "mock_calls": mock_http.calls,
        "kept_order_count": len(kept_orders),
        "cost_usage_count": len(cost_usage),
        "permission_audit_count": len(permission_audit),
        "fallback_count": len(fallbacks),
        "summary_num_trades": summary.get("num_trades"),
        "manifest_contract_version": manifest.get("contract_version"),
        "manifest_schema_version": manifest.get("schema_version"),
        "checks": checks,
        "passed": all(checks.values()),
        "cost_rows": cost_rows,
    }
    (out / "e2e_quant_lab_test_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a Quant Lab e2e ingest bundle fixture")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args(argv)
    report = run_harness(args.out_dir)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
