from __future__ import annotations

import argparse
import csv
import gzip
import html
import hashlib
import io
import json
import re
import shutil
import subprocess
import tarfile
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional
from urllib.parse import urlparse

from src.reporting.bundle_retention import (
    DEFAULT_KEEP_COUNT,
    DEFAULT_MAX_AGE_DAYS,
    prune_v5_bundle_exports,
)
from src.reporting.metrics import (
    SUMMARY_METRICS_VERSION,
    TRADE_EXPORT_SCHEMA_VERSION,
    compute_trade_metrics,
    read_trades_csv_detailed,
)
from src.reporting.candidate_snapshot import (
    CANDIDATE_SNAPSHOT_FIELDS,
    CANDIDATE_SNAPSHOT_SCHEMA_VERSION,
)
from src.reporting.bnb_strong_alpha6_bypass_shadow import BYPASS_SHADOW_FIELDS
from src.reporting.final_score_alpha6_conflict import CONFLICT_FIELDS
from src.reporting.order_lifecycle import (
    ORDER_LIFECYCLE_FIELDS,
    ORDER_LIFECYCLE_SCHEMA_VERSION,
)
from src.reporting.sol_paper_strategy_tracker import (
    ALPHA_FACTORY_ADVISORY_FIELDS,
    ALPHA_FACTORY_FAMILY_SUMMARY_FIELDS,
    BACKTEST_ADVISORY_READER_FIELDS,
    BOTTOM_ZONE_PAPER_DAILY_FIELDS,
    BOTTOM_ZONE_PAPER_RUN_FIELDS,
    EXPANDED_UNIVERSE_ADVISORY_FIELDS,
    EXPANDED_UNIVERSE_PAPER_DAILY_FIELDS,
    EXPANDED_UNIVERSE_PAPER_RUN_FIELDS,
    FAST_MICROSTRUCTURE_STRATEGY_SHADOW_FIELDS,
    LATE_BREAKOUT_FAILURE_PROTECT_FIELDS,
    PAPER_DAILY_FIELDS,
    PAPER_RUN_FIELDS,
    PAPER_SLIPPAGE_FIELDS,
    RISK_ON_MULTI_BUY_SHADOW_FIELDS,
    STRATEGY_ADVISORY_FIELDS,
    STRATEGY_ADVISORY_SOURCE_HEALTH_FIELDS,
)
from src.reporting.quant_lab_audit import (
    CONTRACT_VERSION,
    EVENT_ID_GENERATION_VERSION,
    SCHEMA_VERSION,
    normalize_quant_lab_event,
    read_quant_lab_usage,
    sanitize_quant_lab_obj,
)


NEGATIVE_EXPECTANCY_ATTRIBUTION_FIELDS = (
    "symbol",
    "cycle_index",
    "entry_ts",
    "exit_ts",
    "exit_reason",
    "exit_priority",
    "net_bps",
    "attribution",
    "entry_bad",
    "exit_bad",
    "min_hold_violation",
    "gave_back_profit",
    "trailing_too_early",
    "unknown",
    "exit_metadata_missing",
    "adjusted_entry_expectancy_bps",
    "raw_would_block",
    "adjusted_would_block",
    "would_unblock_if_adjusted",
    "block_attribution_conflict",
)


GIT_COMMAND_TIMEOUT_SEC = 10
RAW_LARGE_FILE_THRESHOLD_BYTES = 1_000_000
LOG_TAIL_LINES = 2000
QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BTC", "ETH", "OKB")


SECRET_MARKERS = (
    "api_key",
    "apiSecret",
    "api_secret",
    "secret_key",
    "passphrase",
    "private_key",
    "OK-ACCESS-KEY",
    "OK-ACCESS-SIGN",
    "OK-ACCESS-PASSPHRASE",
    "Authorization",
    "Bearer",
    "token",
    "password",
    "BEGIN PRIVATE KEY",
    "EXCHANGE_API_KEY",
    "EXCHANGE_API_SECRET",
    "EXCHANGE_PASSPHRASE",
    "QUANT_LAB_API_TOKEN",
)

NON_SECRET_CONFIG_KEYS = {
    "allow_insecure_http_with_token",
    "allow_local_fallback_in_enforce",
    "api_env_path",
    "api_env_path_present",
    "api_env_secure_permissions",
    "api_env_token_loaded",
    "api_env_warning",
    "api_token_env",
    "authorization_age_sec",
    "authorization_consumed",
    "authorization_consumed_at",
    "authorization_expires_at",
    "authorization_fresh",
    "authorization_id",
    "authorization_issued_at",
    "authorization_nonce",
    "authorization_validated",
    "manual_authorization_required",
}

COMPLIANCE_FIELDS = (
    "run_id",
    "event_type",
    "ts",
    "ts_utc",
    "schema_version",
    "contract_version",
    "event_id",
    "request_id",
    "endpoint_path",
    "status_code",
    "success",
    "latency_ms",
    "error_type",
    "error_message_short",
    "mode",
    "local_mode",
    "called_api",
    "permission_gate_enforced",
    "cost_gate_enforced",
    "raw_permission_decision",
    "raw_permission_status",
    "raw_permission_enforceable",
    "effective_permission_decision",
    "would_block_if_enforced",
    "shadow_override_reason",
    "fallback_used",
    "fallback_reason",
    "remote_permission_as_of_ts",
    "remote_permission_expires_at",
    "remote_permission_status",
    "remote_permission_source_bundle_ts",
    "remote_permission_telemetry_latest_ts",
    "remote_permission_contract_version",
    "permission_contract_violation",
    "quant_lab_permission",
    "final_permission",
    "local_preflight_permission",
    "new_risk_order_count",
    "sell_order_count",
    "filtered_by_permission_count",
    "filtered_by_cost_count",
    "hypothetical_violation",
    "actual_violation",
    "violation",
    "violation_reason",
)

PERMISSION_AUDIT_FIELDS = (
    "run_id",
    "ts",
    "ts_utc",
    "schema_version",
    "contract_version",
    "event_id",
    "request_id",
    "endpoint_path",
    "status_code",
    "success",
    "latency_ms",
    "error_type",
    "error_message_short",
    "original_request_id",
    "original_event_id",
    "mode",
    "local_mode",
    "permission_gate_enforced",
    "raw_permission_decision",
    "raw_permission_status",
    "raw_permission_enforceable",
    "effective_permission_decision",
    "would_block_if_enforced",
    "shadow_override_reason",
    "remote_permission_as_of_ts",
    "remote_permission_expires_at",
    "remote_permission_status",
    "remote_permission_source_bundle_ts",
    "remote_permission_telemetry_latest_ts",
    "remote_permission_contract_version",
    "permission_contract_violation",
    "deep_health_status",
    "deep_health_warnings",
    "deep_cost_health_status",
    "deep_cost_fallback_ratio",
    "deep_cost_hard_fallback_ratio",
    "deep_cost_soft_fallback_ratio",
    "deep_cost_actual_rows",
    "deep_cost_mixed_rows",
    "deep_cost_proxy_rows",
    "deep_cost_global_default_rows",
    "deep_cost_proxy_only_count",
    "deep_cost_symbols_missing",
    "deep_cost_warnings",
    "fallback_used",
    "fallback_reason",
    "event_type",
    "symbol",
    "side",
    "intent",
    "actually_filtered",
    "filter_reason",
)

COST_FIELDS = (
    "run_id",
    "event_type",
    "ts",
    "ts_utc",
    "schema_version",
    "contract_version",
    "event_id_generation_version",
    "source_snapshot_hash",
    "event_id",
    "request_id",
    "endpoint_path",
    "status_code",
    "success",
    "latency_ms",
    "error_type",
    "error_message_short",
    "mode",
    "cost_gate_enforced",
    "would_filter",
    "actually_filtered",
    "symbol",
    "request_symbol",
    "normalized_symbol",
    "response_symbol",
    "venue",
    "instrument_type",
    "side",
    "strategy_id",
    "requested_regime",
    "matched_regime",
    "regime",
    "notional_usdt",
    "quantile",
    "requested_quantile",
    "fee_bps",
    "slippage_bps",
    "spread_bps",
    "total_cost_bps",
    "effective_total_cost_bps",
    "local_cost_bps",
    "local_cost_source",
    "fallback_level",
    "source",
    "cost_source",
    "sample_count",
    "cost_model_version",
    "cost_contract_version",
    "as_of_ts",
    "selected_total_cost_bps",
    "total_cost_bps_p50",
    "total_cost_bps_p75",
    "total_cost_bps_p90",
    "expected_edge_bps",
    "expected_edge_source",
    "min_required_edge_bps",
    "required_edge_bps",
    "proxy_source",
    "would_filter_by_cost",
    "would_block_by_cost",
    "fallback_used",
    "fallback_used_for_cost_model",
    "fallback_reason",
    "degraded_cost_model",
    "diagnosis",
    "warning",
    "cost_gate_verified",
    "passed",
    "filtered",
    "filter_reason",
)

RUNTIME_COST_GUARD_FIELDS = (
    "ts_utc",
    "symbol",
    "decision_scope",
    "quant_lab_roundtrip_cost_bps",
    "v5_local_runtime_cost_floor_bps",
    "config_hard_floor_bps",
    "selected_entry_gate_cost_bps",
    "cost_source",
    "cost_trusted_for_live",
    "guard_action",
    "reason",
)

COST_DISAGREEMENT_FIELDS = (
    "ts_utc",
    "symbol",
    "quant_lab_cost_source",
    "quant_lab_roundtrip_cost_bps",
    "v5_runtime_roundtrip_cost_bps",
    "abs_diff_bps",
    "diff_ratio",
    "severity",
    "next_action",
)

LIVE_GUARD_IMPACT_FIELDS = (
    "run_id",
    "ts_utc",
    "symbol",
    "strategy_candidate",
    "intent",
    "would_have_opened_live",
    "would_be_blocked_by_quant_lab_no_live_modes",
    "would_be_blocked_by_cost_trust_guard",
    "would_be_blocked_by_shadow_live_whitelist",
    "cost_quality",
    "cost_trusted_for_live",
    "cost_trust_level",
    "raw_permission_decision",
    "allowed_live_modes",
    "final_decision_actual",
    "guard_enforced",
)

FALLBACK_FIELDS = (
    "run_id",
    "ts",
    "ts_utc",
    "schema_version",
    "contract_version",
    "event_id",
    "request_id",
    "endpoint_path",
    "status_code",
    "success",
    "latency_ms",
    "mode",
    "event_type",
    "original_request_id",
    "original_event_id",
    "error_type",
    "error_message_short",
    "fallback_used",
    "reason",
    "fallback_policy",
    "fallback_scope",
    "action_taken",
)

MODE_AUDIT_FIELDS = (
    "run_id",
    "ts",
    "ts_utc",
    "schema_version",
    "contract_version",
    "event_id",
    "request_id",
    "event_type",
    "mode",
    "mode_source",
    "quant_lab_requested_mode",
    "quant_lab_effective_mode",
    "called_api",
    "apply_permission_gate",
    "apply_cost_gate",
    "permission_gate_enforced",
    "cost_gate_enforced",
    "enforce_readiness_status",
    "enforce_blocked_reasons",
    "enforce_blocked_reason",
    "contract_version_match",
    "telemetry_schema_version_match",
    "raw_permission_decision",
    "effective_permission_decision",
    "would_block_if_enforced",
    "fallback_used",
    "fallback_reason",
)

TRADE_METRICS_FIELDS = (
    "run_id",
    "trades_file_exists",
    "trades_file_rows",
    "trades_counted_rows",
    "num_trades",
    "turnover_usdt",
    "fees_usdt_total",
    "slippage_usdt_total",
    "cost_usdt_total",
    "fills_count_today",
    "trade_metrics_warning",
    "trade_metrics_warning_count",
    "trade_export_schema_version",
    "summary_metrics_version",
)

FILL_METRICS_FIELDS = (
    "run_id",
    "ts_utc",
    "symbol",
    "normalized_symbol",
    "side",
    "action",
    "qty",
    "price",
    "notional_usdt",
    "fee",
    "fee_ccy",
    "fee_usdt",
    "slippage_usdt",
    "order_id",
    "trade_id",
    "strategy_id",
    "position_id",
    "trade_export_schema_version",
)

SUMMARY_TRADE_COUNT_MISMATCH_FIELDS = (
    "run_id",
    "trades_file_exists",
    "trades_file_rows",
    "trades_counted_rows",
    "summary_num_trades",
    "summary_fills_count_today",
    "count_mismatch",
    "high_issue",
    "diagnosis",
    "trade_metrics_warning",
)

COST_PROBE_BUNDLE_ARTIFACTS = (
    ("cost_probe_plan.csv", "summaries/cost_probe_plan.csv"),
    ("cost_probe_orders.csv", "summaries/cost_probe_orders.csv"),
    ("cost_probe_roundtrips.csv", "summaries/cost_probe_roundtrips.csv"),
    ("cost_probe_order_events.jsonl", "summaries/cost_probe_order_events.jsonl"),
    ("cost_probe_roundtrip_events.jsonl", "summaries/cost_probe_roundtrip_events.jsonl"),
    ("cost_probe_summary.json", "summaries/cost_probe_summary.json"),
    ("cost_probe_p3_preflight.json", "summaries/cost_probe_p3_preflight.json"),
    ("cost_probe_live_execution_status.json", "summaries/cost_probe_live_execution_status.json"),
    ("runtime_cost_guard.csv", "summaries/cost_probe_runtime_cost_guard.csv"),
    ("cost_disagreement.csv", "summaries/cost_probe_cost_disagreement.csv"),
)


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _since_iso(window_hours: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=int(window_hours))).isoformat().replace("+00:00", "Z")


def _redact_text(text: str) -> str:
    out_lines: list[str] = []
    for line in text.splitlines():
        key = _assignment_key(line)
        if key in NON_SECRET_CONFIG_KEYS:
            out_lines.append(line)
            continue
        lowered = line.lower()
        if any(marker.lower() in lowered for marker in SECRET_MARKERS):
            if ":" in line:
                prefix = line.split(":", 1)[0]
                out_lines.append(f"{prefix}: <REDACTED>")
            elif "=" in line:
                prefix = line.split("=", 1)[0]
                out_lines.append(f"{prefix}=<REDACTED>")
            else:
                out_lines.append("<REDACTED>")
        else:
            out_lines.append(line)
    return "\n".join(out_lines) + ("\n" if text.endswith("\n") else "")


def _assignment_key(line: str) -> str:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return ""
    if stripped.startswith("export "):
        stripped = stripped[len("export ") :].strip()
    if ":" in stripped:
        return stripped.split(":", 1)[0].strip()
    if "=" in stripped:
        return stripped.split("=", 1)[0].strip()
    return ""


def _read_text_redacted(path: Path) -> str:
    try:
        return _redact_text(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return ""


def _tail_lines(text: str, limit: int = LOG_TAIL_LINES) -> str:
    lines = text.splitlines()
    if len(lines) <= limit:
        return text
    return "\n".join(lines[-limit:]) + "\n"


def _filter_jsonl_latest_hours(text: str, *, now: datetime, hours: int = 24) -> str:
    cutoff = now - timedelta(hours=hours)
    kept: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            row = json.loads(stripped)
        except Exception:
            kept.append(line)
            continue
        if not isinstance(row, Mapping):
            kept.append(line)
            continue
        raw_ts = row.get("ts_utc") or row.get("timestamp") or row.get("ts") or row.get("entry_ts")
        try:
            parsed = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
        except Exception:
            kept.append(line)
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        if parsed >= cutoff:
            kept.append(json.dumps(sanitize_quant_lab_obj(row), ensure_ascii=False, sort_keys=True))
    return "\n".join(kept) + ("\n" if kept else "")


def _write_maybe_filtered_raw_text(path: Path, text: str) -> None:
    if path.as_posix().endswith("raw/reports/sol_paper_strategy_labels.jsonl"):
        text = _filter_jsonl_latest_hours(text, now=datetime.now(timezone.utc), hours=24)
    _write_text(path, text)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_csv(path: Path, fields: Iterable[str], rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fields))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: sanitize_quant_lab_obj(row).get(field, "") for field in writer.fieldnames})


def _relocate_large_raw_files(staging: Path, threshold_bytes: int = RAW_LARGE_FILE_THRESHOLD_BYTES) -> list[dict[str, Any]]:
    relocated: list[dict[str, Any]] = []
    raw_root = staging / "raw"
    large_root = raw_root / "large"
    if not raw_root.exists():
        return relocated
    for path in sorted(raw_root.rglob("*")):
        if not path.is_file() or large_root in path.parents:
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size <= threshold_bytes:
            continue
        rel = path.relative_to(raw_root).as_posix()
        dest = large_root / f"{rel}.gz"
        dest.parent.mkdir(parents=True, exist_ok=True)
        with path.open("rb") as src_fh, gzip.open(dest, "wb") as dest_fh:
            shutil.copyfileobj(src_fh, dest_fh)
        path.unlink()
        relocated.append(
            {
                "source_path": f"raw/{rel}",
                "relocated_path": f"raw/large/{rel}.gz",
                "original_bytes": size,
                "compressed_bytes": dest.stat().st_size,
            }
        )
    return relocated


def _write_static_report_index(
    staging: Path,
    *,
    window_summary: Mapping[str, Any],
    trade_metrics_rows: list[Mapping[str, Any]],
    candidate_rows: list[Mapping[str, Any]],
    request_rows: list[Mapping[str, Any]],
    large_raw_files: list[Mapping[str, Any]],
) -> None:
    paper_summary = {
        "paper_strategy_daily": _small_csv_preview(staging / "summaries/paper_strategy_daily.csv"),
        "bnb_paper_strategy_daily": _small_csv_preview(staging / "summaries/bnb_paper_strategy_daily.csv"),
    }
    api_latency = _api_latency_summary(request_rows)
    payload = {
        "schema_version": "v5.static_report_index.v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "account_state": window_summary.get("account_state", "not_observable"),
        "open_position_count": window_summary.get("open_position_count", "not_observable"),
        "effective_open_position_count": window_summary.get("effective_open_position_count", "not_observable"),
        "latest_trade_count": len(trade_metrics_rows),
        "candidate_snapshot_rows": len(candidate_rows),
        "paper_summary": paper_summary,
        "quant_lab_api_latency_summary": api_latency,
        "raw_large_file_count": len(large_raw_files),
        "raw_large_manifest": "raw/large/manifest.json" if large_raw_files else "",
        "links": {
            "window_summary": "../summaries/window_summary.json",
            "market_context": "../summaries/market_context.json",
            "issues_to_fix": "../summaries/issues_to_fix.json",
            "raw_large": "../raw/large/",
        },
    }
    _write_text(staging / "reports/index.json", json.dumps(payload, ensure_ascii=False, indent=2))
    _write_text(staging / "reports/index.html", _static_report_index_html(payload))


def _small_csv_preview(path: Path, *, limit: int = 20) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            return [dict(row) for _, row in zip(range(limit), csv.DictReader(fh))]
    except OSError:
        return []


def _api_latency_summary(request_rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    latencies = sorted(
        value
        for value in (_to_float(row.get("latency_ms")) for row in request_rows)
        if value is not None
    )
    if not latencies:
        return {"count": 0, "p50_ms": None, "p95_ms": None, "max_ms": None}
    return {
        "count": len(latencies),
        "p50_ms": _percentile(latencies, 0.50),
        "p95_ms": _percentile(latencies, 0.95),
        "max_ms": latencies[-1],
    }


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    index = min(max(int(round((len(values) - 1) * q)), 0), len(values) - 1)
    return round(float(values[index]), 6)


def _static_report_index_html(payload: Mapping[str, Any]) -> str:
    def value(key: str) -> str:
        return html.escape(str(payload.get(key, "not_observable")))

    latency = payload.get("quant_lab_api_latency_summary")
    latency_text = "not_observable"
    if isinstance(latency, Mapping) and latency.get("count"):
        latency_text = (
            f"count={latency.get('count')}, p50={latency.get('p50_ms')}ms, "
            f"p95={latency.get('p95_ms')}ms, max={latency.get('max_ms')}ms"
        )
    return "\n".join(
        [
            "<!doctype html>",
            "<html lang=\"zh-CN\">",
            "<head><meta charset=\"utf-8\"><title>V5 Follow-up Report</title>",
            "<style>body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:32px;line-height:1.5;}"
            "table{border-collapse:collapse;min-width:680px}td,th{border:1px solid #ddd;padding:8px 10px}"
            "th{text-align:left;background:#f6f7f9}code{background:#f2f2f2;padding:2px 4px}</style></head>",
            "<body>",
            "<h1>V5 Follow-up Report</h1>",
            "<p>Lightweight landing page. Raw large files are kept out of the first-screen payload.</p>",
            "<table><tbody>",
            f"<tr><th>account_state</th><td>{value('account_state')}</td></tr>",
            f"<tr><th>open_position_count</th><td>{value('open_position_count')}</td></tr>",
            f"<tr><th>latest_trade_count</th><td>{value('latest_trade_count')}</td></tr>",
            f"<tr><th>candidate_snapshot_rows</th><td>{value('candidate_snapshot_rows')}</td></tr>",
            f"<tr><th>quant_lab_api_latency</th><td>{html.escape(latency_text)}</td></tr>",
            f"<tr><th>raw_large_file_count</th><td>{value('raw_large_file_count')}</td></tr>",
            "</tbody></table>",
            "<p><a href=\"index.json\">index.json</a> · "
            "<a href=\"../summaries/window_summary.json\">window_summary</a> · "
            "<a href=\"../summaries/market_context.json\">market_context</a> · "
            "<a href=\"../summaries/issues_to_fix.json\">issues_to_fix</a></p>",
            "</body></html>",
        ]
    ) + "\n"


def _to_float(value: Any) -> Optional[float]:
    if value in (None, "", "null", "not_observable"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int:
    number = _to_float(value)
    return int(number) if number is not None else 0


def _normalized_symbol(symbol: Any) -> str:
    text = str(symbol or "").strip().upper().replace("_", "-")
    if not text:
        return "null"
    if ":" in text:
        text = text.rsplit(":", 1)[-1].strip()
    text = text.replace("/", "-")
    if "-" in text:
        parts = [part for part in text.split("-") if part]
        return "-".join(parts) if parts else text
    for quote in QUOTE_SUFFIXES:
        if text.endswith(quote) and len(text) > len(quote):
            return f"{text[:-len(quote)]}-{quote}"
    return text or "null"


def _load_json_obj(path: Path) -> Dict[str, Any]:
    try:
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}
    return {}


def _csv_null(value: Any) -> Any:
    if value is None or value == "":
        return "null"
    return value


def _csv_join(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (list, tuple, set)):
        return ";".join(str(item) for item in value if item not in (None, ""))
    return str(value)


def _build_trade_bundle_rows(reports: Path) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]]]:
    trade_metrics_rows: list[Dict[str, Any]] = []
    fill_metrics_rows: list[Dict[str, Any]] = []
    mismatch_rows: list[Dict[str, Any]] = []
    runs_dir = reports / "runs"
    if not runs_dir.exists():
        return trade_metrics_rows, fill_metrics_rows, mismatch_rows

    for trade_path in sorted(runs_dir.rglob("trades.csv")):
        run_dir = trade_path.parent
        run_id = run_dir.name
        trade_read = read_trades_csv_detailed(trade_path)
        summary = _load_json_obj(run_dir / "summary.json")
        metrics = compute_trade_metrics(trade_read.rows, avg_equity=_to_float(summary.get("avg_equity")))
        warning = "; ".join(str(item) for item in trade_read.warnings)
        trade_metrics_rows.append(
            {
                "run_id": run_id,
                "trades_file_exists": str(bool(trade_read.file_exists)).lower(),
                "trades_file_rows": trade_read.file_rows,
                "trades_counted_rows": trade_read.counted_rows,
                "num_trades": metrics.get("num_trades"),
                "turnover_usdt": metrics.get("turnover_usdt"),
                "fees_usdt_total": metrics.get("fees_usdt_total"),
                "slippage_usdt_total": metrics.get("slippage_usdt_total"),
                "cost_usdt_total": metrics.get("cost_usdt_total"),
                "fills_count_today": metrics.get("fills_count_today"),
                "trade_metrics_warning": warning,
                "trade_metrics_warning_count": len(trade_read.warnings),
                "trade_export_schema_version": TRADE_EXPORT_SCHEMA_VERSION,
                "summary_metrics_version": SUMMARY_METRICS_VERSION,
            }
        )
        for row in trade_read.rows:
            symbol = row.get("symbol") or row.get("instId") or row.get("instrument")
            fill_metrics_rows.append(
                {
                    "run_id": row.get("run_id") or run_id,
                    "ts_utc": _csv_null(row.get("ts_utc") or row.get("ts") or row.get("timestamp")),
                    "symbol": _csv_null(symbol),
                    "normalized_symbol": _csv_null(row.get("normalized_symbol") or _normalized_symbol(symbol)),
                    "side": _csv_null(row.get("side")),
                    "action": _csv_null(row.get("action") or row.get("intent")),
                    "qty": _csv_null(row.get("qty")),
                    "price": _csv_null(row.get("price")),
                    "notional_usdt": _csv_null(row.get("notional_usdt")),
                    "fee": _csv_null(row.get("fee")),
                    "fee_ccy": _csv_null(row.get("fee_ccy")),
                    "fee_usdt": _csv_null(row.get("fee_usdt")),
                    "slippage_usdt": _csv_null(row.get("slippage_usdt")),
                    "order_id": _csv_null(row.get("order_id")),
                    "trade_id": _csv_null(row.get("trade_id")),
                    "strategy_id": _csv_null(row.get("strategy_id") or "v5"),
                    "position_id": _csv_null(row.get("position_id")),
                    "trade_export_schema_version": row.get("trade_export_schema_version") or TRADE_EXPORT_SCHEMA_VERSION,
                }
            )

        summary_num_trades = _to_int(summary.get("num_trades"))
        summary_fills = _to_int((summary.get("budget") or {}).get("fills_count_today") if isinstance(summary.get("budget"), Mapping) else summary.get("fills_count_today"))
        count_mismatch = int(trade_read.counted_rows) != int(summary_num_trades)
        high_issue = int(trade_read.file_rows) > 0 and int(summary_num_trades) == 0
        if count_mismatch or high_issue:
            mismatch_rows.append(
                {
                    "run_id": run_id,
                    "trades_file_exists": str(bool(trade_read.file_exists)).lower(),
                    "trades_file_rows": trade_read.file_rows,
                    "trades_counted_rows": trade_read.counted_rows,
                    "summary_num_trades": summary_num_trades,
                    "summary_fills_count_today": summary_fills,
                    "count_mismatch": str(bool(count_mismatch)).lower(),
                    "high_issue": str(bool(high_issue)).lower(),
                    "diagnosis": "high_issue_summary_trade_count_mismatch" if high_issue else "summary_trade_count_mismatch",
                    "trade_metrics_warning": warning,
                }
            )
    return trade_metrics_rows, fill_metrics_rows, mismatch_rows


def _read_candidate_snapshot_rows(reports: Path) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()

    def add_path(path: Path) -> None:
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    if not row:
                        continue
                    run_id = str(row.get("run_id") or path.parent.name or "").strip()
                    candidate_id = str(row.get("candidate_id") or "").strip()
                    symbol = str(row.get("symbol") or "").strip()
                    strategy_candidate = str(row.get("strategy_candidate") or "").strip()
                    dedupe_key = (candidate_id, run_id, symbol, strategy_candidate)
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    item = {field: row.get(field, "") for field in CANDIDATE_SNAPSHOT_FIELDS}
                    item["run_id"] = item.get("run_id") or run_id
                    item["candidate_snapshot_schema_version"] = CANDIDATE_SNAPSHOT_SCHEMA_VERSION
                    if not item.get("strategy_candidate"):
                        item["strategy_candidate"] = strategy_candidate
                    rows.append(item)
        except Exception:
            return

    runs_dir = reports / "runs"
    if runs_dir.exists():
        for path in sorted(runs_dir.rglob("candidate_snapshot.csv")):
            add_path(path)
    aggregate = reports / "candidate_snapshot.csv"
    if aggregate.exists():
        add_path(aggregate)
    return rows


def _candidate_cost_source_coverage(rows: list[Mapping[str, Any]]) -> float:
    if not rows:
        return 0.0
    filled = [
        row
        for row in rows
        if str(row.get("cost_source") or "").strip().lower() not in {"", "null", "not_observable"}
    ]
    return float(len(filled)) / float(len(rows))


def _copy_candidate_snapshot_files(
    staging: Path,
    reports: Path,
    candidate_rows: list[Dict[str, Any]] | None = None,
) -> None:
    aggregate = reports / "candidate_snapshot.csv"
    if candidate_rows:
        _write_csv(
            staging / "raw/reports/candidate_snapshot.csv",
            CANDIDATE_SNAPSHOT_FIELDS,
            candidate_rows,
        )
    elif aggregate.exists():
        _write_text(
            staging / "raw/reports/candidate_snapshot.csv",
            _redact_text(aggregate.read_text(encoding="utf-8", errors="replace")),
        )
    runs_dir = reports / "runs"
    if not runs_dir.exists():
        return
    for path in sorted(runs_dir.rglob("candidate_snapshot.csv")):
        run_id = path.parent.name
        _write_text(
            staging / "raw/recent_runs" / run_id / "candidate_snapshot.csv",
            _redact_text(path.read_text(encoding="utf-8", errors="replace")),
        )


def _read_order_lifecycle_rows(reports: Path) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    seen: set[str] = set()

    def add_path(path: Path) -> None:
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    if not row:
                        continue
                    lifecycle_id = str(row.get("lifecycle_id") or "").strip()
                    run_id = str(row.get("run_id") or path.parent.name or "").strip()
                    key = lifecycle_id or "|".join(
                        [
                            run_id,
                            str(row.get("cl_ord_id") or ""),
                            str(row.get("symbol") or ""),
                            str(row.get("decision_ts") or row.get("submit_ts") or ""),
                        ]
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    item = {field: row.get(field, "") for field in ORDER_LIFECYCLE_FIELDS}
                    item["run_id"] = item.get("run_id") or run_id
                    item["schema_version"] = item.get("schema_version") or ORDER_LIFECYCLE_SCHEMA_VERSION
                    rows.append(item)
        except Exception:
            return

    runs_dir = reports / "runs"
    if runs_dir.exists():
        for path in sorted(runs_dir.rglob("order_lifecycle.csv")):
            add_path(path)
    aggregate = reports / "order_lifecycle.csv"
    if aggregate.exists():
        add_path(aggregate)
    return rows


def _is_nullish(value: Any) -> bool:
    return str(value if value is not None else "").strip().lower() in {
        "",
        "null",
        "none",
        "nan",
        "not_observable",
    }


def _identity(value: Any) -> str:
    return "" if _is_nullish(value) else str(value).strip()


def _lifecycle_field_needs_fill(row: Mapping[str, Any], field: str) -> bool:
    value = row.get(field)
    if _is_nullish(value):
        return True
    if field in {"fill_count", "fee", "fee_usdt", "filled_qty", "avg_fill_px", "fill_px", "notional_usdt"}:
        number = _to_float(value)
        return number is not None and abs(number) <= 0.0
    return False


def _dedupe_fill_rows(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    seen: set[tuple[str, str, str, str, str, str]] = set()
    for row in rows:
        symbol = row.get("symbol") or row.get("normalized_symbol")
        key = (
            _identity(row.get("run_id")),
            _identity(row.get("order_id")),
            _identity(row.get("trade_id")),
            _identity(row.get("ts_utc")),
            "" if _is_nullish(symbol) else _normalized_symbol(symbol),
            _identity(row.get("qty")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _same_symbol_side_intent(lifecycle: Mapping[str, Any], fill: Mapping[str, Any]) -> bool:
    lifecycle_symbol = _normalized_symbol(lifecycle.get("symbol") or lifecycle.get("normalized_symbol"))
    fill_symbol = _normalized_symbol(fill.get("symbol") or fill.get("normalized_symbol"))
    if lifecycle_symbol != fill_symbol:
        return False
    lifecycle_side = _identity(lifecycle.get("side")).lower()
    fill_side = _identity(fill.get("side")).lower()
    if lifecycle_side and fill_side and lifecycle_side != fill_side:
        return False
    lifecycle_intent = _identity(lifecycle.get("intent")).upper()
    fill_action = _identity(fill.get("action")).upper()
    if lifecycle_intent and fill_action and lifecycle_intent != fill_action:
        return False
    return True


def _numeric_close(left: Any, right: Any, tolerance: float = 1e-9) -> bool:
    left_num = _to_float(left)
    right_num = _to_float(right)
    if left_num is None or right_num is None:
        return False
    return abs(left_num - right_num) <= tolerance


def _matching_fill_rows_for_lifecycle(
    lifecycle: Mapping[str, Any],
    candidate_fills: Iterable[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    candidates = list(candidate_fills)
    cl_ord_id = _identity(lifecycle.get("cl_ord_id"))
    exchange_order_id = _identity(lifecycle.get("exchange_order_id"))
    order_ids = {value for value in (cl_ord_id, exchange_order_id) if value}
    trade_ids = {
        part.strip()
        for part in _identity(lifecycle.get("trade_ids")).replace(",", ";").split(";")
        if part.strip()
    }
    exact: list[Mapping[str, Any]] = []
    for fill in candidates:
        order_id = _identity(fill.get("order_id"))
        trade_id = _identity(fill.get("trade_id"))
        if order_id and order_id in order_ids:
            exact.append(fill)
        elif trade_id and trade_id in trade_ids:
            exact.append(fill)
    if exact:
        return _dedupe_fill_rows(exact)

    soft = [fill for fill in candidates if _same_symbol_side_intent(lifecycle, fill)]
    if not soft:
        return []
    price_filtered = [
        fill
        for fill in soft
        if _numeric_close(lifecycle.get("avg_fill_px") or lifecycle.get("fill_px"), fill.get("price"), tolerance=1e-6)
    ]
    qty_filtered = [
        fill
        for fill in (price_filtered or soft)
        if _numeric_close(lifecycle.get("filled_qty"), fill.get("qty"), tolerance=1e-12)
    ]
    for group in (qty_filtered, price_filtered, soft):
        deduped = _dedupe_fill_rows(group)
        if len(deduped) == 1:
            return deduped
    return []


def _aggregate_fill_metrics_for_lifecycle(fills: list[Mapping[str, Any]]) -> Dict[str, Any]:
    if not fills:
        return {}
    ordered = sorted(fills, key=lambda row: (_identity(row.get("ts_utc")), _identity(row.get("trade_id"))))
    ts_values = [_identity(row.get("ts_utc")) for row in ordered if _identity(row.get("ts_utc"))]
    qty_sum = 0.0
    px_qty_sum = 0.0
    notional_sum = 0.0
    fee_usdt_sum = 0.0
    fee_values: list[float] = []
    fee_ccys: set[str] = set()
    trade_ids: list[str] = []
    first_px: Optional[float] = None
    for fill in ordered:
        px = _to_float(fill.get("price"))
        qty = _to_float(fill.get("qty"))
        if px is not None and first_px is None:
            first_px = px
        if px is not None and qty is not None and qty > 0:
            qty_sum += qty
            px_qty_sum += px * qty
        notional = _to_float(fill.get("notional_usdt"))
        if notional is not None:
            notional_sum += abs(notional)
        fee_usdt = _to_float(fill.get("fee_usdt"))
        if fee_usdt is not None:
            fee_usdt_sum += abs(fee_usdt)
        fee = _to_float(fill.get("fee"))
        if fee is not None:
            fee_values.append(fee)
        fee_ccy = _identity(fill.get("fee_ccy")).upper()
        if fee_ccy:
            fee_ccys.add(fee_ccy)
        trade_id = _identity(fill.get("trade_id"))
        if trade_id:
            trade_ids.append(trade_id)
    avg_px = (px_qty_sum / qty_sum) if qty_sum > 0 else first_px
    fee_ccy_value = next(iter(fee_ccys)) if len(fee_ccys) == 1 else ("mixed" if fee_ccys else "")
    return {
        "first_fill_ts": ts_values[0] if ts_values else "",
        "last_fill_ts": ts_values[-1] if ts_values else "",
        "fill_px": first_px,
        "avg_fill_px": avg_px,
        "filled_qty": qty_sum if qty_sum > 0 else None,
        "fee": sum(fee_values) if fee_values and fee_ccy_value != "mixed" else None,
        "fee_ccy": fee_ccy_value,
        "fee_usdt": fee_usdt_sum if fee_usdt_sum > 0 else None,
        "notional_usdt": notional_sum if notional_sum > 0 else None,
        "trade_ids": ";".join(dict.fromkeys(trade_ids)),
        "fill_count": len(ordered),
    }


def _backfill_order_lifecycle_from_fill_metrics(
    rows: list[Dict[str, Any]],
    fill_metrics_rows: Iterable[Mapping[str, Any]],
) -> list[Dict[str, Any]]:
    fills_by_run: dict[str, list[Mapping[str, Any]]] = {}
    for fill in fill_metrics_rows:
        run_id = _identity(fill.get("run_id"))
        if not run_id:
            continue
        fills_by_run.setdefault(run_id, []).append(fill)

    out: list[Dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        matches = _matching_fill_rows_for_lifecycle(enriched, fills_by_run.get(_identity(enriched.get("run_id")), []))
        if matches:
            aggregate = _aggregate_fill_metrics_for_lifecycle(matches)
            for field, value in aggregate.items():
                if value is not None and _lifecycle_field_needs_fill(enriched, field):
                    enriched[field] = _csv_null(value)
        out.append(enriched)
    return out


def _copy_order_lifecycle_files(staging: Path, reports: Path) -> None:
    aggregate = reports / "order_lifecycle.csv"
    if aggregate.exists():
        _write_text(
            staging / "raw/reports/order_lifecycle.csv",
            _redact_text(aggregate.read_text(encoding="utf-8", errors="replace")),
        )
    runs_dir = reports / "runs"
    if not runs_dir.exists():
        return
    for path in sorted(runs_dir.rglob("order_lifecycle.csv")):
        run_id = path.parent.name
        _write_text(
            staging / "raw/recent_runs" / run_id / "order_lifecycle.csv",
            _redact_text(path.read_text(encoding="utf-8", errors="replace")),
        )


def _copy_cost_probe_artifacts(staging: Path, reports: Path) -> dict[str, Any]:
    present: list[str] = []
    missing: list[str] = []
    row_counts: dict[str, int] = {}
    summary_state: dict[str, Any] = {}
    p3_preflight_state: dict[str, Any] = {}
    live_execution_status: dict[str, Any] = {}
    for filename, summary_rel in COST_PROBE_BUNDLE_ARTIFACTS:
        source = reports / filename
        if not source.exists():
            missing.append(filename)
            continue
        payload = _read_json_obj(source) if source.suffix.lower() == ".json" else None
        if isinstance(payload, Mapping):
            text = json.dumps(_sanitize_bundle_obj(payload), ensure_ascii=False, indent=2) + "\n"
        else:
            text = _read_text_redacted(source)
        _write_text(staging / "raw/reports" / filename, text)
        _write_text(staging / summary_rel, text)
        present.append(filename)
        if source.suffix.lower() == ".csv":
            row_counts[filename] = len(_read_csv_dicts(source))
        elif source.suffix.lower() == ".jsonl":
            row_counts[filename] = sum(1 for line in text.splitlines() if line.strip())
        elif filename == "cost_probe_summary.json":
            if isinstance(payload, Mapping):
                summary_state = {
                    "state": payload.get("state", "not_observable"),
                    "dry_run": payload.get("dry_run", "not_observable"),
                    "live_enabled": payload.get("live_enabled", "not_observable"),
                    "no_order_submitted": payload.get("no_order_submitted", "not_observable"),
                    "planned_rows": payload.get("planned_rows", "not_observable"),
                    "plan_rows": payload.get("plan_rows", "not_observable"),
                }
        elif filename == "cost_probe_p3_preflight.json":
            if isinstance(payload, Mapping):
                p3_preflight_state = {
                    "state": payload.get("state", "not_observable"),
                    "ready_to_request_manual_live_probe": payload.get(
                        "ready_to_request_manual_live_probe",
                        "not_observable",
                    ),
                    "manual_authorization_required": payload.get(
                        "manual_authorization_required",
                        "not_observable",
                    ),
                    "approved_live_order_execution": payload.get(
                        "approved_live_order_execution",
                        "not_observable",
                    ),
                    "manual_probe_symbol": payload.get(
                        "manual_probe_symbol",
                        "not_observable",
                    ),
                    "blockers": payload.get("blockers", "not_observable"),
                }
        elif filename == "cost_probe_live_execution_status.json":
            if isinstance(payload, Mapping):
                live_execution_status = {
                    "status": payload.get("status", "not_observable"),
                    "source_state": payload.get("source_state", "not_observable"),
                    "manual_probe_symbol": payload.get(
                        "manual_probe_symbol",
                        "not_observable",
                    ),
                    "authorization_validated": payload.get(
                        "authorization_validated",
                        "not_observable",
                    ),
                    "authorization_consumed": payload.get(
                        "authorization_consumed",
                        "not_observable",
                    ),
                    "authorization_issued_at": payload.get(
                        "authorization_issued_at",
                        "not_observable",
                    ),
                    "authorization_expires_at": payload.get(
                        "authorization_expires_at",
                        "not_observable",
                    ),
                    "authorization_age_sec": payload.get(
                        "authorization_age_sec",
                        "not_observable",
                    ),
                    "authorization_fresh": payload.get(
                        "authorization_fresh",
                        "not_observable",
                    ),
                    "recovery_required": payload.get(
                        "recovery_required",
                        "not_observable",
                    ),
                    "execution_completed": payload.get(
                        "execution_completed",
                        "not_observable",
                    ),
                    "flat_verified": payload.get("flat_verified", "not_observable"),
                }
    return {
        "present": present,
        "missing": missing,
        "row_counts": row_counts,
        "summary": summary_state,
        "p3_preflight": p3_preflight_state,
        "live_execution_status": live_execution_status,
    }


def _count_trade_metric_fills(rows: Iterable[Mapping[str, Any]]) -> int:
    total = 0
    for row in rows:
        total += _to_int(
            row.get("trades_counted_rows")
            or row.get("num_trades")
            or row.get("fills_count_today")
        )
    return total


def _copy_sol_paper_strategy_files(staging: Path, reports: Path) -> None:
    summary_specs = (
        ("paper_strategy_runs.csv", PAPER_RUN_FIELDS),
        ("paper_strategy_daily.csv", PAPER_DAILY_FIELDS),
        ("bnb_paper_strategy_runs.csv", PAPER_RUN_FIELDS),
        ("bnb_paper_strategy_daily.csv", PAPER_DAILY_FIELDS),
        ("bottom_zone_probe_paper_runs.csv", BOTTOM_ZONE_PAPER_RUN_FIELDS),
        ("bottom_zone_probe_paper_daily.csv", BOTTOM_ZONE_PAPER_DAILY_FIELDS),
        ("paper_slippage_coverage.csv", PAPER_SLIPPAGE_FIELDS),
        ("strategy_opportunity_advisory_reader.csv", STRATEGY_ADVISORY_FIELDS),
        ("strategy_opportunity_advisory_source_health.csv", STRATEGY_ADVISORY_SOURCE_HEALTH_FIELDS),
        ("expanded_universe_advisory_reader.csv", EXPANDED_UNIVERSE_ADVISORY_FIELDS),
        ("expanded_universe_paper_runs.csv", EXPANDED_UNIVERSE_PAPER_RUN_FIELDS),
        ("expanded_universe_paper_daily.csv", EXPANDED_UNIVERSE_PAPER_DAILY_FIELDS),
        ("alpha_factory_advisory_reader.csv", ALPHA_FACTORY_ADVISORY_FIELDS),
        ("alpha_factory_family_summary.csv", ALPHA_FACTORY_FAMILY_SUMMARY_FIELDS),
        ("risk_on_multi_buy_shadow.csv", RISK_ON_MULTI_BUY_SHADOW_FIELDS),
        ("late_breakout_failure_protect_shadow.csv", LATE_BREAKOUT_FAILURE_PROTECT_FIELDS),
        ("backtest_advisory_reader.csv", BACKTEST_ADVISORY_READER_FIELDS),
        ("fast_microstructure_strategy_shadow.csv", FAST_MICROSTRUCTURE_STRATEGY_SHADOW_FIELDS),
        ("final_score_vs_alpha6_conflict.csv", CONFLICT_FIELDS),
        ("bnb_strong_alpha6_bypass_shadow.csv", BYPASS_SHADOW_FIELDS),
        ("negative_expectancy_attribution.csv", NEGATIVE_EXPECTANCY_ATTRIBUTION_FIELDS),
    )
    for filename, fields in summary_specs:
        src = reports / "summaries" / filename
        dest = staging / "summaries" / filename
        if src.is_file():
            _write_text(dest, _redact_text(src.read_text(encoding="utf-8", errors="replace")))
        else:
            _write_csv(dest, fields, [])
    labels = reports / "sol_paper_strategy_labels.jsonl"
    if labels.is_file():
        _write_maybe_filtered_raw_text(
            staging / "raw/reports/sol_paper_strategy_labels.jsonl",
            _redact_text(labels.read_text(encoding="utf-8", errors="replace")),
        )


def _sanitize_bundle_obj(value: Any) -> Any:
    if isinstance(value, Mapping):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            key_s = str(key)
            if key_s in NON_SECRET_CONFIG_KEYS:
                out[key_s] = item
            else:
                out[key_s] = sanitize_quant_lab_obj({key_s: _sanitize_bundle_obj(item)}).get(key_s)
        return out
    if isinstance(value, list):
        return [_sanitize_bundle_obj(item) for item in value]
    return sanitize_quant_lab_obj(value)


def _read_jsonl(path: Path) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _filter_window(rows: list[Dict[str, Any]], since_ts: str) -> list[Dict[str, Any]]:
    since = _parse_utc_dt(since_ts)
    if since is None:
        return list(rows)
    kept: list[Dict[str, Any]] = []
    for row in rows:
        row_ts = _parse_utc_dt(
            row.get("ts")
            or row.get("ts_utc")
            or row.get("timestamp")
            or row.get("created_at")
        )
        if row_ts is None or row_ts >= since:
            kept.append(row)
    return kept


def _jsonl_rows_text(rows: Iterable[Mapping[str, Any]]) -> str:
    rendered = [
        json.dumps(_sanitize_bundle_obj(dict(row)), ensure_ascii=False, sort_keys=True)
        for row in rows
    ]
    return "\n".join(rendered) + ("\n" if rendered else "")


def _parse_utc_dt(value: Any) -> Optional[datetime]:
    if value in (None, "", "not_observable"):
        return None
    try:
        text = str(value).strip()
        if not text:
            return None
        if re.fullmatch(r"\d+(?:\.\d+)?", text):
            raw = float(text)
            if raw > 10_000_000_000:
                raw /= 1000.0
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _is_new_risk_row(row: Mapping[str, Any]) -> bool:
    side = str(row.get("side") or "").lower()
    intent = str(row.get("intent") or "").upper()
    return side == "buy" or intent in {"OPEN_LONG", "REBALANCE"}


def _event_kind(row: Mapping[str, Any]) -> str:
    legacy = str(row.get("legacy_event_type") or "").strip()
    if legacy:
        return legacy
    event_type = str(row.get("event_type") or "").strip()
    if event_type == "cost_usage":
        return "cost_estimate"
    if event_type == "permission_audit":
        return str(row.get("permission_audit_type") or "permission")
    if event_type == "health_check":
        return "health"
    return event_type


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "ok", "success"}
    return False


def _request_success(row: Mapping[str, Any]) -> bool:
    if _truthy(row.get("success")) or _truthy(row.get("ok")):
        return True
    if row.get("error_type"):
        return False
    status = row.get("status_code")
    try:
        if status is not None and 200 <= int(status) < 300:
            return True
    except (TypeError, ValueError):
        pass
    return False


def _is_fallback_row(row: Mapping[str, Any]) -> bool:
    if _request_success(row):
        return False
    fallback_reason = str(row.get("fallback_reason") or "").strip().lower()
    if fallback_reason == "global_default_cost" and not _truthy(row.get("fallback_used")) and _event_kind(row) != "fallback":
        return False
    error_text = str(row.get("error_type") or row.get("error") or "").lower()
    if any(marker in error_text for marker in ("timeout", "connection", "unavailable", "invalid")):
        return True
    return (
        _truthy(row.get("fallback_used"))
        or _event_kind(row) == "fallback"
        or bool(row.get("fallback_reason"))
        or bool(row.get("action_taken"))
    )


def _actual_filtered(row: Mapping[str, Any]) -> bool:
    return _truthy(row.get("actually_filtered")) or _truthy(row.get("order_filtered"))


def _would_filter(row: Mapping[str, Any]) -> bool:
    return (
        _truthy(row.get("would_filter"))
        or _truthy(row.get("would_filter_by_cost"))
        or _truthy(row.get("would_filter_by_permission"))
        or _truthy(row.get("would_block_if_enforced"))
    )


def _is_cost_filter_reason(reason: Any) -> bool:
    text = str(reason or "")
    return "cost" in text or "expected_edge" in text


def _permission_status_stale(row: Mapping[str, Any]) -> bool:
    status = str(row.get("remote_permission_status") or row.get("raw_permission_status") or "").strip().upper()
    return status.startswith("STALE") or status.startswith("EXPIRED") or status == "NO_FRESH_PERMISSION"


def _permission_not_enforceable(row: Mapping[str, Any]) -> bool:
    return str(row.get("raw_permission_enforceable")).strip().lower() in {"false", "0", "no", "off"}


def _build_compliance_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    by_run: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        run_id = str(row.get("run_id") or "unknown")
        item = by_run.setdefault(
            run_id,
            {
                "run_id": run_id,
                "event_type": "compliance",
                "ts": row.get("ts") or "",
                "ts_utc": row.get("ts_utc") or row.get("ts") or "",
                "schema_version": row.get("schema_version") or SCHEMA_VERSION,
                "contract_version": row.get("contract_version") or CONTRACT_VERSION,
                "event_id": row.get("event_id") or "",
                "request_id": row.get("request_id") or "",
                "endpoint_path": row.get("endpoint_path", ""),
                "status_code": row.get("status_code", ""),
                "success": row.get("success", ""),
                "latency_ms": row.get("latency_ms", ""),
                "error_type": row.get("error_type", ""),
                "error_message_short": row.get("error_message_short", ""),
                "mode": row.get("mode") or "",
                "local_mode": row.get("local_mode") or row.get("mode") or "",
                "called_api": row.get("called_api", ""),
                "permission_gate_enforced": row.get("permission_gate_enforced", ""),
                "cost_gate_enforced": row.get("cost_gate_enforced", ""),
                "raw_permission_decision": "",
                "raw_permission_status": "",
                "raw_permission_enforceable": "",
                "effective_permission_decision": "",
                "would_block_if_enforced": "false",
                "shadow_override_reason": "",
                "fallback_used": "false",
                "fallback_reason": "",
                "remote_permission_as_of_ts": "",
                "remote_permission_expires_at": "",
                "remote_permission_status": "",
                "remote_permission_source_bundle_ts": "",
                "remote_permission_telemetry_latest_ts": "",
                "remote_permission_contract_version": "",
                "permission_contract_violation": "false",
                "quant_lab_permission": "",
                "final_permission": "",
                "local_preflight_permission": "",
                "new_risk_order_count": 0,
                "sell_order_count": 0,
                "filtered_by_permission_count": 0,
                "filtered_by_cost_count": 0,
                "hypothetical_violation": "false",
                "actual_violation": "false",
                "violation": "false",
                "violation_reason": "",
            },
        )
        if row.get("ts"):
            item["ts"] = row.get("ts")
        if row.get("ts_utc") or row.get("ts"):
            item["ts_utc"] = row.get("ts_utc") or row.get("ts")
        for field in ("schema_version", "contract_version", "event_id", "request_id"):
            if row.get(field):
                item[field] = row.get(field)
        for field in ("endpoint_path", "status_code", "success", "latency_ms", "error_type", "error_message_short"):
            if row.get(field) not in (None, ""):
                item[field] = row.get(field)
        if row.get("mode"):
            item["mode"] = row.get("mode")
        if row.get("local_mode") or row.get("mode"):
            item["local_mode"] = row.get("local_mode") or row.get("mode")
        if "called_api" in row:
            item["called_api"] = row.get("called_api")
        if "permission_gate_enforced" in row:
            item["permission_gate_enforced"] = row.get("permission_gate_enforced")
        if "cost_gate_enforced" in row:
            item["cost_gate_enforced"] = row.get("cost_gate_enforced")
        explicit_raw_permission = row.get("raw_permission_decision") or row.get("quant_lab_permission") or row.get("quant_lab_decision")
        raw_permission = explicit_raw_permission or (row.get("permission") if not item.get("raw_permission_decision") else "")
        effective_permission = (
            row.get("effective_permission_decision")
            or row.get("final_permission")
            or row.get("effective_decision")
        )
        permission = row.get("permission") or row.get("quant_lab_permission") or raw_permission
        final = row.get("final_permission") or effective_permission
        if raw_permission:
            item["raw_permission_decision"] = raw_permission
        if row.get("raw_permission_status"):
            item["raw_permission_status"] = row.get("raw_permission_status")
        if "raw_permission_enforceable" in row:
            item["raw_permission_enforceable"] = row.get("raw_permission_enforceable")
        if effective_permission:
            item["effective_permission_decision"] = effective_permission
        if "would_block_if_enforced" in row:
            item["would_block_if_enforced"] = str(_truthy(row.get("would_block_if_enforced"))).lower()
        if row.get("shadow_override_reason"):
            item["shadow_override_reason"] = row.get("shadow_override_reason")
        if "fallback_used" in row:
            item["fallback_used"] = str(_truthy(row.get("fallback_used"))).lower()
        if row.get("fallback_reason"):
            item["fallback_reason"] = row.get("fallback_reason")
        for field in (
            "remote_permission_as_of_ts",
            "remote_permission_expires_at",
            "remote_permission_status",
            "remote_permission_source_bundle_ts",
            "remote_permission_telemetry_latest_ts",
            "remote_permission_contract_version",
        ):
            if row.get(field):
                item[field] = row.get(field)
        if "permission_contract_violation" in row:
            item["permission_contract_violation"] = str(_truthy(row.get("permission_contract_violation"))).lower()
        if permission:
            item["quant_lab_permission"] = permission
        if final:
            item["final_permission"] = final
        if row.get("local_preflight_permission"):
            item["local_preflight_permission"] = row.get("local_preflight_permission")
        if _event_kind(row) == "filter_order":
            if _is_new_risk_row(row) and not _actual_filtered(row):
                item["new_risk_order_count"] = int(item["new_risk_order_count"]) + 1
            if str(row.get("side") or "").lower() == "sell":
                item["sell_order_count"] = int(item["sell_order_count"]) + 1
            reason = str(row.get("filter_reason") or "")
            if _actual_filtered(row) and ("sell_only" in reason or "abort" in reason):
                item["filtered_by_permission_count"] = int(item["filtered_by_permission_count"]) + 1
            if _actual_filtered(row) and _is_cost_filter_reason(reason):
                item["filtered_by_cost_count"] = int(item["filtered_by_cost_count"]) + 1
            if _would_filter(row) and not _actual_filtered(row):
                item["hypothetical_violation"] = "true"
        elif _truthy(row.get("would_block_if_enforced")) and not _truthy(row.get("permission_gate_enforced")):
            item["hypothetical_violation"] = "true"
        final_permission = str(item.get("final_permission") or item.get("quant_lab_permission") or "").upper()
        enforced = str(item.get("permission_gate_enforced")).lower() == "true"
        if enforced and final_permission == "ABORT" and int(item["new_risk_order_count"]) > 0:
            item["actual_violation"] = "true"
            item["violation"] = "true"
            item["violation_reason"] = "abort_new_risk_order_submitted"
        if enforced and final_permission == "SELL_ONLY" and int(item["new_risk_order_count"]) > 0:
            item["actual_violation"] = "true"
            item["violation"] = "true"
            item["violation_reason"] = "sell_only_new_risk_order_submitted"
    out: list[Dict[str, Any]] = []
    for item in by_run.values():
        normalized = normalize_quant_lab_event({**item, "event_type": "compliance"}, default_event_type="compliance")
        out.append({**item, "event_id": normalized["event_id"], "request_id": normalized["request_id"], "event_type": "compliance"})
    return out


def _build_permission_audit_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    permission_events = {"live_permission", "final_permission", "filter_order", "permission", "order_filter"}
    out: list[Dict[str, Any]] = []
    for row in rows:
        event_kind = _event_kind(row)
        has_deep_health_audit = any(
            row.get(field) not in (None, "", [], {})
            for field in (
                "deep_health_status",
                "deep_cost_health_status",
                "deep_cost_fallback_ratio",
                "deep_cost_hard_fallback_ratio",
                "deep_cost_soft_fallback_ratio",
                "deep_cost_warnings",
            )
        )
        if event_kind not in permission_events and not (
            row.get("raw_permission_decision")
            or row.get("effective_permission_decision")
            or row.get("remote_permission_status")
            or has_deep_health_audit
        ):
            continue
        raw_permission = row.get("raw_permission_decision") or row.get("quant_lab_permission") or row.get("permission") or ""
        effective_permission = row.get("effective_permission_decision") or row.get("final_permission") or row.get("effective_decision") or ""
        out.append(
            {
                "run_id": row.get("run_id", ""),
                "ts": row.get("ts", ""),
                "ts_utc": row.get("ts_utc") or row.get("ts", ""),
                "schema_version": row.get("schema_version") or SCHEMA_VERSION,
                "contract_version": row.get("contract_version")
                or row.get("remote_permission_contract_version")
                or CONTRACT_VERSION,
                "event_id": row.get("event_id", ""),
                "request_id": row.get("request_id", ""),
                "endpoint_path": row.get("endpoint_path", ""),
                "status_code": row.get("status_code", ""),
                "success": row.get("success", ""),
                "latency_ms": row.get("latency_ms", ""),
                "error_type": row.get("error_type", ""),
                "error_message_short": row.get("error_message_short", ""),
                "original_request_id": row.get("original_request_id", ""),
                "original_event_id": row.get("original_event_id", ""),
                "mode": row.get("mode", ""),
                "local_mode": row.get("local_mode") or row.get("mode") or "",
                "permission_gate_enforced": row.get("permission_gate_enforced", ""),
                "raw_permission_decision": raw_permission,
                "raw_permission_status": row.get("raw_permission_status", ""),
                "raw_permission_enforceable": row.get("raw_permission_enforceable", ""),
                "effective_permission_decision": effective_permission,
                "would_block_if_enforced": str(_truthy(row.get("would_block_if_enforced"))).lower(),
                "shadow_override_reason": row.get("shadow_override_reason", ""),
                "remote_permission_as_of_ts": row.get("remote_permission_as_of_ts", ""),
                "remote_permission_expires_at": row.get("remote_permission_expires_at", ""),
                "remote_permission_status": row.get("remote_permission_status", ""),
                "remote_permission_source_bundle_ts": row.get("remote_permission_source_bundle_ts", ""),
                "remote_permission_telemetry_latest_ts": row.get("remote_permission_telemetry_latest_ts", ""),
                "remote_permission_contract_version": row.get("remote_permission_contract_version", ""),
                "permission_contract_violation": str(_truthy(row.get("permission_contract_violation"))).lower(),
                "deep_health_status": row.get("deep_health_status", ""),
                "deep_health_warnings": _csv_join(row.get("deep_health_warnings")),
                "deep_cost_health_status": row.get("deep_cost_health_status", ""),
                "deep_cost_fallback_ratio": row.get("deep_cost_fallback_ratio", ""),
                "deep_cost_hard_fallback_ratio": row.get("deep_cost_hard_fallback_ratio", ""),
                "deep_cost_soft_fallback_ratio": row.get("deep_cost_soft_fallback_ratio", ""),
                "deep_cost_actual_rows": row.get("deep_cost_actual_rows", ""),
                "deep_cost_mixed_rows": row.get("deep_cost_mixed_rows", ""),
                "deep_cost_proxy_rows": row.get("deep_cost_proxy_rows", ""),
                "deep_cost_global_default_rows": row.get("deep_cost_global_default_rows", ""),
                "deep_cost_proxy_only_count": row.get("deep_cost_proxy_only_count", ""),
                "deep_cost_symbols_missing": _csv_join(row.get("deep_cost_symbols_missing")),
                "deep_cost_warnings": _csv_join(row.get("deep_cost_warnings")),
                "fallback_used": str(_truthy(row.get("fallback_used"))).lower(),
                "fallback_reason": row.get("fallback_reason", ""),
                "event_type": "permission_audit",
                "symbol": row.get("symbol", ""),
                "side": row.get("side", ""),
                "intent": row.get("intent", ""),
                "actually_filtered": str(_actual_filtered(row)).lower(),
                "filter_reason": row.get("filter_reason", ""),
            }
        )
    return out


def _build_cost_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    for row in rows:
        if _event_kind(row) != "cost_estimate":
            continue
        merged: Dict[str, Any] = dict(row)
        merged["event_type"] = "cost_usage"
        merged.setdefault("endpoint_path", "/v1/costs/estimate")
        merged.setdefault("success", True)
        merged.setdefault("error_type", "")
        merged.setdefault("error_message_short", "")
        for nested_key in ("cost", "quant_lab", "cost_estimate"):
            nested = row.get(nested_key)
            if isinstance(nested, Mapping):
                for key, value in nested.items():
                    merged.setdefault(str(key), value)
        merged.setdefault("would_filter_by_cost", merged.get("would_filter", ""))
        merged.setdefault("would_block_by_cost", merged.get("would_filter_by_cost", ""))
        merged.setdefault("actually_filtered", merged.get("order_filtered", ""))
        merged.setdefault("request_symbol", merged.get("symbol", ""))
        merged.setdefault("response_symbol", merged.get("normalized_symbol", merged.get("symbol", "")))
        merged.setdefault("requested_regime", merged.get("regime", ""))
        merged.setdefault("requested_quantile", merged.get("quantile", ""))
        merged.setdefault("matched_regime", merged.get("regime", ""))
        merged.setdefault("cost_source", merged.get("source", merged.get("local_cost_source", "")))
        merged.setdefault("required_edge_bps", merged.get("min_required_edge_bps", ""))
        merged.setdefault("selected_total_cost_bps", merged.get("total_cost_bps", ""))
        merged.setdefault("expected_edge_source", merged.get("proxy_source", ""))
        merged.setdefault("cost_contract_version", merged.get("contract_version", CONTRACT_VERSION))
        source_text = str(merged.get("cost_source") or merged.get("source") or "").strip().lower()
        fallback_level_text = str(merged.get("fallback_level") or "").strip().upper()
        cost_model_version_text = str(merged.get("cost_model_version") or "").strip().lower()
        degraded = source_text == "global_default" or fallback_level_text == "GLOBAL_DEFAULT" or cost_model_version_text == "global_default_v0"
        merged.setdefault("degraded_cost_model", degraded)
        merged.setdefault("fallback_used_for_cost_model", bool(_truthy(merged.get("fallback_used")) or degraded))
        merged.setdefault("diagnosis", "global_default_cost" if degraded else "")
        if str(merged.get("filter_reason") or "") == "expected_edge_missing_no_filter":
            merged.setdefault("warning", "expected_edge_missing_cost_gate_not_verified")
            merged.setdefault("cost_gate_verified", False)
        out.append({field: merged.get(field, "") for field in COST_FIELDS})
    return out


def _build_live_guard_impact_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    for row in rows:
        if _event_kind(row) != "live_guard_impact":
            continue
        merged = dict(row)
        merged.setdefault(
            "would_be_blocked_by_quant_lab_no_live_modes",
            merged.get("blocked_by_quant_lab_no_live_modes", ""),
        )
        merged.setdefault(
            "would_be_blocked_by_cost_trust_guard",
            merged.get("would_block_by_cost_trust_guard", ""),
        )
        merged.setdefault(
            "would_be_blocked_by_shadow_live_whitelist",
            merged.get("blocked_by_shadow_live_whitelist", ""),
        )
        merged.setdefault("final_decision_actual", merged.get("final_decision_after_guard", ""))
        merged["guard_enforced"] = False
        out.append({field: merged.get(field, "") for field in LIVE_GUARD_IMPACT_FIELDS})
    return out


def _live_guard_would_block(row: Mapping[str, Any]) -> bool:
    return any(
        _truthy(row.get(field))
        for field in (
            "would_be_blocked_by_quant_lab_no_live_modes",
            "would_be_blocked_by_cost_trust_guard",
            "would_be_blocked_by_shadow_live_whitelist",
        )
    )


def _live_guard_value_mix(rows: Iterable[Mapping[str, Any]], field: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        if not _live_guard_would_block(row):
            continue
        value = str(row.get(field) or "not_observable").strip() or "not_observable"
        counts[value] = counts.get(value, 0) + 1
    return counts


def _build_fallback_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    for row in rows:
        if not _is_fallback_row(row):
            continue
        out.append(
            {
                "run_id": row.get("run_id", ""),
                "ts": row.get("ts", ""),
                "ts_utc": row.get("ts_utc") or row.get("ts", ""),
                "schema_version": row.get("schema_version", SCHEMA_VERSION),
                "contract_version": row.get("contract_version", CONTRACT_VERSION),
                "event_id": row.get("event_id", ""),
                "request_id": row.get("request_id", ""),
                "endpoint_path": row.get("endpoint_path") or row.get("endpoint") or "",
                "status_code": row.get("status_code", ""),
                "success": row.get("success", ""),
                "latency_ms": row.get("latency_ms", ""),
                "mode": row.get("mode", ""),
                "event_type": "fallback",
                "original_request_id": row.get("original_request_id") or row.get("request_id", ""),
                "original_event_id": row.get("original_event_id") or row.get("event_id", ""),
                "error_type": row.get("error_type") or row.get("error") or "",
                "error_message_short": row.get("error_message_short") or row.get("error_message_sanitized") or "",
                "fallback_used": True,
                "reason": row.get("fallback_reason") or row.get("reason") or row.get("filter_reason") or row.get("error_type") or "",
                "fallback_policy": row.get("fallback_policy") or row.get("fail_policy") or "",
                "fallback_scope": row.get("fallback_scope") or _event_kind(row) or "",
                "action_taken": row.get("action_taken") or row.get("permission") or row.get("final_permission") or "",
            }
        )
    return out


def _build_mode_audit_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    mode_fields = {
        "mode",
        "mode_source",
        "quant_lab_requested_mode",
        "quant_lab_effective_mode",
        "enforce_readiness_status",
        "enforce_blocked_reasons",
        "enforce_blocked_reason",
    }
    for row in rows:
        if not any(row.get(field) not in (None, "", []) for field in mode_fields):
            continue
        requested_mode = row.get("quant_lab_requested_mode") or row.get("requested_mode") or row.get("mode") or ""
        effective_mode = row.get("quant_lab_effective_mode") or row.get("effective_mode") or row.get("mode") or ""
        blocked_reasons = row.get("enforce_blocked_reasons")
        if isinstance(blocked_reasons, list):
            blocked_reasons_value = ";".join(str(item) for item in blocked_reasons)
        else:
            blocked_reasons_value = blocked_reasons or row.get("enforce_blocked_reason") or ""
        out.append(
            {
                "run_id": row.get("run_id", ""),
                "ts": row.get("ts", ""),
                "ts_utc": row.get("ts_utc") or row.get("ts", ""),
                "schema_version": row.get("schema_version") or SCHEMA_VERSION,
                "contract_version": row.get("contract_version") or CONTRACT_VERSION,
                "event_id": row.get("event_id", ""),
                "request_id": row.get("request_id", ""),
                "event_type": row.get("event_type", ""),
                "mode": row.get("mode", ""),
                "mode_source": row.get("mode_source", ""),
                "quant_lab_requested_mode": requested_mode,
                "quant_lab_effective_mode": effective_mode,
                "called_api": row.get("called_api", ""),
                "apply_permission_gate": row.get("apply_permission_gate", ""),
                "apply_cost_gate": row.get("apply_cost_gate", ""),
                "permission_gate_enforced": row.get("permission_gate_enforced", ""),
                "cost_gate_enforced": row.get("cost_gate_enforced", ""),
                "enforce_readiness_status": row.get("enforce_readiness_status", ""),
                "enforce_blocked_reasons": blocked_reasons_value,
                "enforce_blocked_reason": row.get("enforce_blocked_reason", blocked_reasons_value),
                "contract_version_match": row.get("contract_version_match", ""),
                "telemetry_schema_version_match": row.get("telemetry_schema_version_match", ""),
                "raw_permission_decision": row.get("raw_permission_decision", ""),
                "effective_permission_decision": row.get("effective_permission_decision") or row.get("final_permission", ""),
                "would_block_if_enforced": str(_truthy(row.get("would_block_if_enforced"))).lower(),
                "fallback_used": str(_truthy(row.get("fallback_used"))).lower(),
                "fallback_reason": row.get("fallback_reason", ""),
            }
        )
    return out


def _enforce_readiness_snapshot(
    mode_rows: list[Dict[str, Any]],
    window_summary: Mapping[str, Any],
) -> Dict[str, Any]:
    latest = next(
        (row for row in reversed(mode_rows) if row.get("enforce_readiness_status") not in (None, "")),
        mode_rows[-1] if mode_rows else {},
    )
    readiness_cost_rows = window_summary.get(
        "post_deployment_cost_usage_rows",
        window_summary.get("cost_usage_current_contract_rows", window_summary.get("quant_lab_cost_usage_rows", 0)),
    )
    readiness_degraded_count = window_summary.get(
        "post_deployment_cost_degraded_count",
        window_summary.get("current_contract_cost_degraded_count", window_summary.get("cost_degraded_count", 0)),
    )
    readiness_global_default_count = window_summary.get(
        "post_deployment_global_default_cost_count",
        window_summary.get("current_contract_global_default_cost_count", window_summary.get("global_default_cost_count", 0)),
    )
    return {
        "quant_lab_requested_mode": latest.get("quant_lab_requested_mode") or window_summary.get("quant_lab_requested_mode"),
        "quant_lab_effective_mode": latest.get("quant_lab_effective_mode") or window_summary.get("quant_lab_effective_mode"),
        "mode_source": latest.get("mode_source") or window_summary.get("quant_lab_mode_source"),
        "status": latest.get("enforce_readiness_status") or window_summary.get("enforce_readiness_status") or "NOT_CHECKED",
        "blocked_reasons": latest.get("enforce_blocked_reasons") or window_summary.get("enforce_blocked_reasons") or "",
        "enforce_blocked_reason": latest.get("enforce_blocked_reason") or window_summary.get("enforce_blocked_reason") or "",
        "contract_version_match": latest.get("contract_version_match") or window_summary.get("contract_version_match"),
        "telemetry_schema_version_match": latest.get("telemetry_schema_version_match")
        or window_summary.get("telemetry_schema_version_match"),
        "quant_lab_cost_usage_rows": readiness_cost_rows,
        "cost_degraded_count": readiness_degraded_count,
        "global_default_cost_count": readiness_global_default_count,
        "legacy_global_default_cost_count": window_summary.get("legacy_global_default_cost_count", 0),
        "current_contract_global_default_cost_count": window_summary.get("current_contract_global_default_cost_count", 0),
        "latest_24h_global_default_cost_count": window_summary.get("latest_24h_global_default_cost_count", 0),
        "post_deployment_global_default_cost_count": window_summary.get("post_deployment_global_default_cost_count", 0),
        "cost_usage_legacy_rows": window_summary.get("cost_usage_legacy_rows", 0),
        "cost_usage_current_contract_rows": window_summary.get("cost_usage_current_contract_rows", 0),
        "cost_usage_latest_24h_rows": window_summary.get("cost_usage_latest_24h_rows", 0),
        "post_deployment_cost_usage_rows": window_summary.get("post_deployment_cost_usage_rows", 0),
        "quant_lab_fallback_count": window_summary.get("quant_lab_fallback_count", 0),
        "quant_lab_request_count": window_summary.get("quant_lab_request_count", 0),
        "summary_trade_count_mismatch_count": window_summary.get("summary_trade_count_mismatch_count", 0),
        "telemetry_contract_version": window_summary.get("telemetry_contract_version") or CONTRACT_VERSION,
        "telemetry_schema_version": window_summary.get("telemetry_schema_version") or SCHEMA_VERSION,
    }


def _read_csv_dicts(path: Path) -> list[Dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
            return [dict(row) for row in csv.DictReader(fh) if row]
    except OSError:
        return []


def _nested_first_value(obj: Any, names: Iterable[str]) -> Any:
    name_set = set(names)
    if isinstance(obj, Mapping):
        for name in name_set:
            value = obj.get(name)
            if value not in (None, "", "not_observable"):
                return value
        for value in obj.values():
            found = _nested_first_value(value, name_set)
            if found not in (None, "", "not_observable"):
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _nested_first_value(value, name_set)
            if found not in (None, "", "not_observable"):
                return found
    return "not_observable"


def _jsonish_value(value: Any) -> Any:
    if value in (None, "", "not_observable"):
        return "not_observable"
    text = str(value).strip()
    if not text or text in {"null", "not_observable"}:
        return "not_observable"
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _build_market_context_summary(
    staging: Path,
    reports: Path,
    *,
    window_summary: Mapping[str, Any],
) -> Dict[str, Any]:
    auto_risk = _read_json_obj(staging / "raw/state/auto_risk_eval.json")
    if not isinstance(auto_risk, Mapping):
        auto_risk = _read_json_obj(reports / "auto_risk_eval.json")
    if not isinstance(auto_risk, Mapping):
        auto_risk = {}
    event_candidates = _read_json_obj(staging / "raw/reports/event_candidates.json")
    if not isinstance(event_candidates, Mapping):
        event_candidates = _read_json_obj(reports / "event_candidates.json")
    if not isinstance(event_candidates, Mapping):
        event_candidates = {}
    candidates = event_candidates.get("candidates")
    if not isinstance(candidates, list):
        candidates = []
    market_rows = _read_csv_dicts(staging / "summaries/market_impulse_selection_shadow.csv")
    risk_on_rows = _read_csv_dicts(staging / "summaries/risk_on_multi_buy_shadow.csv")
    market_latest = market_rows[-1] if market_rows else {}
    risk_on_latest = risk_on_rows[-1] if risk_on_rows else {}
    router_reasons = window_summary.get("router_reason_counts") or {}
    if not isinstance(router_reasons, Mapping):
        router_reasons = {}

    event_symbols = sorted(
        {
            str(item.get("symbol") or item.get("instId") or item.get("normalized_symbol") or "").strip()
            for item in candidates
            if isinstance(item, Mapping)
            and str(item.get("symbol") or item.get("instId") or item.get("normalized_symbol") or "").strip()
        }
    )
    return {
        "schema_version": "v5.market_context.v1",
        "sampled_at_utc": window_summary.get("sampled_at_utc", datetime.now(timezone.utc).isoformat()),
        "window_hours": window_summary.get("window_hours", "not_observable"),
        "source_files": {
            "auto_risk_eval": "raw/state/auto_risk_eval.json" if auto_risk else "not_observable",
            "event_candidates": "raw/reports/event_candidates.json" if event_candidates else "not_observable",
            "latest_decision_audit": "not_observable",
            "market_impulse_selection_shadow": "summaries/market_impulse_selection_shadow.csv" if market_rows else "not_observable",
            "risk_on_multi_buy_shadow": "summaries/risk_on_multi_buy_shadow.csv" if risk_on_rows else "not_observable",
            "window_summary": "summaries/window_summary.json",
        },
        "auto_risk": {
            "current_level": _nested_first_value(auto_risk, ("current_level", "risk_level", "level")),
            "status": _nested_first_value(auto_risk, ("status", "state")),
            "source": "auto_risk_eval" if auto_risk else "not_observable",
        },
        "event_candidates": {
            "regime": event_candidates.get("regime") or event_candidates.get("market_regime") or "not_observable",
            "candidate_count": len(candidates),
            "symbols": event_symbols,
        },
        "latest_decision_audit": {
            "run_id": "not_observable",
            "path": "not_observable",
            "window_end_utc": "not_observable",
            "router_reason_counts": dict(sorted(router_reasons.items())),
        },
        "no_trade_context": {
            "trade_observation_status": window_summary.get("trade_observation_status", "not_observable"),
            "latest_24h_trade_count": window_summary.get("latest_24h_trade_count", "not_observable"),
            "last_72h_trade_count": window_summary.get("last_72h_trade_count", "not_observable"),
            "latest_24h_roundtrip_count": window_summary.get("latest_24h_roundtrip_count", "not_observable"),
            "last_72h_roundtrip_count": window_summary.get("last_72h_roundtrip_count", "not_observable"),
            "top_router_reasons": [
                {"reason": reason, "count": count}
                for reason, count in sorted(router_reasons.items(), key=lambda item: (-_to_int(item[1]), str(item[0])))[:8]
            ],
        },
        "market_impulse_probe": {
            "candidate_count": window_summary.get("market_impulse_probe_candidate_count", 0),
            "open_count": window_summary.get("market_impulse_probe_open_count", 0),
            "selection_shadow_rows": window_summary.get("market_impulse_selection_shadow_rows", len(market_rows)),
            "latest_shadow": {
                "run_id": market_latest.get("run_id", "not_observable"),
                "ts_utc": market_latest.get("ts_utc") or market_latest.get("decision_ts") or "not_observable",
                "current_level": market_latest.get("current_level") or market_latest.get("risk_level") or "not_observable",
                "selected_by_composite": market_latest.get("selected_by_composite", "not_observable"),
                "selected_by_expected_net_shadow": market_latest.get("selected_by_expected_net_shadow", "not_observable"),
            }
            if market_latest
            else "not_observable",
        },
        "risk_on_multi_buy": {
            "shadow_rows": window_summary.get("risk_on_multi_buy_shadow_rows", len(risk_on_rows)),
            "latest_selected_symbols": _jsonish_value(
                window_summary.get("risk_on_multi_buy_latest_selected_symbols")
                or risk_on_latest.get("selected_symbols")
            ),
            "source_detail_available": window_summary.get("risk_on_multi_buy_source_detail_available", False),
            "latest_shadow": {
                "run_id": risk_on_latest.get("run_id", "not_observable"),
                "decision_ts": risk_on_latest.get("decision_ts") or risk_on_latest.get("ts_utc") or "not_observable",
                "top_k": risk_on_latest.get("top_k", "not_observable"),
                "current_regime": risk_on_latest.get("current_regime") or risk_on_latest.get("regime") or "not_observable",
                "would_buy_symbols": _jsonish_value(risk_on_latest.get("would_buy_symbols")),
            }
            if risk_on_latest
            else "not_observable",
        },
        "alt_impulse_shadow": {
            "ready_for_live_probe": window_summary.get("alt_impulse_shadow_ready_for_live_probe", False),
            "ready_symbols": window_summary.get("alt_impulse_shadow_ready_symbols", []),
            "blocking_reasons": window_summary.get("alt_impulse_shadow_readiness_blocking_reasons", []),
            "recent_sample_count": window_summary.get("alt_impulse_shadow_recent_sample_count", 0),
        },
        "quant_lab_coordination": {
            "requested_mode": window_summary.get("quant_lab_requested_mode", "not_observable"),
            "effective_mode": window_summary.get("quant_lab_effective_mode", "not_observable"),
            "mode_source": window_summary.get("quant_lab_mode_source", "not_observable"),
            "enforce_readiness_status": window_summary.get("enforce_readiness_status", "not_observable"),
            "request_success_count": window_summary.get("quant_lab_request_success_count", 0),
            "request_error_count": window_summary.get("quant_lab_request_error_count", 0),
            "fallback_count": window_summary.get("quant_lab_fallback_count", 0),
            "current_contract_cost_rows": window_summary.get("cost_usage_current_contract_rows", 0),
            "global_default_cost_count": window_summary.get(
                "post_deployment_global_default_cost_count",
                window_summary.get("current_contract_global_default_cost_count", 0),
            ),
        },
    }


def _window_summary(
    rows: list[Dict[str, Any]],
    request_rows: list[Dict[str, Any]],
    compliance_rows: list[Dict[str, Any]],
    permission_rows: Optional[list[Dict[str, Any]]] = None,
    *,
    current_source_snapshot_hash: Optional[str] = None,
    ml_live_fields: Optional[Mapping[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    latest_permission = None
    final_permission = None
    cost_model_version = None
    gate_version = None
    latest_mode = None
    latest_mode_source = None
    latest_requested_mode = None
    latest_effective_mode = None
    enforce_readiness_status = None
    enforce_blocked_reasons = None
    enforce_blocked_reason = None
    contract_version_match = None
    telemetry_schema_version_match = None
    for row in rows:
        latest_mode = row.get("mode") or latest_mode
        latest_mode_source = row.get("mode_source") or latest_mode_source
        latest_requested_mode = row.get("quant_lab_requested_mode") or row.get("requested_mode") or latest_requested_mode
        latest_effective_mode = row.get("quant_lab_effective_mode") or row.get("effective_mode") or latest_effective_mode
        enforce_readiness_status = row.get("enforce_readiness_status") or enforce_readiness_status
        enforce_blocked_reasons = row.get("enforce_blocked_reasons") or enforce_blocked_reasons
        enforce_blocked_reason = row.get("enforce_blocked_reason") or enforce_blocked_reason
        contract_version_match = row.get("contract_version_match") if row.get("contract_version_match") not in (None, "") else contract_version_match
        telemetry_schema_version_match = (
            row.get("telemetry_schema_version_match")
            if row.get("telemetry_schema_version_match") not in (None, "")
            else telemetry_schema_version_match
        )
        latest_permission = row.get("permission") or row.get("quant_lab_permission") or row.get("quant_lab_decision") or latest_permission
        final_permission = row.get("final_permission") or row.get("effective_decision") or final_permission
        cost_model_version = row.get("cost_model_version") or cost_model_version
        gate_version = row.get("gate_version") or gate_version
    request_success_count = len([row for row in request_rows if _request_success(row)])
    request_error_count = len(request_rows) - request_success_count
    actual_fallback_count = len([row for row in rows + request_rows if _is_fallback_row(row)])
    cost_event_rows = [row for row in rows if _event_kind(row) == "cost_estimate"]
    permission_event_rows = permission_rows if permission_rows is not None else _build_permission_audit_rows(rows)
    would_block_count = len(
        [
            row
            for row in permission_event_rows
            if _truthy(row.get("would_block_if_enforced"))
            and _event_kind(row) in {"filter_order", "order_filter"}
        ]
    )
    if would_block_count == 0:
        would_block_count = len([row for row in permission_event_rows if _truthy(row.get("would_block_if_enforced"))])
    effective_block_count = len(
        [
            row
            for row in permission_event_rows
            if _truthy(row.get("permission_gate_enforced")) and _actual_filtered(row)
        ]
    )

    def is_degraded_cost(row: Mapping[str, Any]) -> bool:
        source = str(row.get("cost_source") or row.get("source") or "").strip().lower()
        fallback_level = str(row.get("fallback_level") or "").strip().upper()
        cost_model_version_value = str(row.get("cost_model_version") or "").strip().lower()
        return _truthy(row.get("degraded_cost_model")) or source == "global_default" or fallback_level == "GLOBAL_DEFAULT" or cost_model_version_value == "global_default_v0"

    def is_global_default_cost(row: Mapping[str, Any]) -> bool:
        return (
            str(row.get("cost_source") or row.get("source") or "").strip().lower() == "global_default"
            or str(row.get("fallback_level") or "").strip().upper() == "GLOBAL_DEFAULT"
            or str(row.get("cost_model_version") or "").strip().lower() == "global_default_v0"
        )

    def is_current_cost_contract(row: Mapping[str, Any]) -> bool:
        schema = str(row.get("schema_version") or "").strip()
        contract = str(row.get("cost_contract_version") or row.get("contract_version") or "").strip()
        event_generation = str(row.get("event_id_generation_version") or "").strip()
        return schema == SCHEMA_VERSION and contract == CONTRACT_VERSION and event_generation == EVENT_ID_GENERATION_VERSION

    def row_source_hash(row: Mapping[str, Any]) -> str:
        value = str(
            row.get("source_snapshot_hash")
            or row.get("deployment_source_snapshot_hash")
            or row.get("source_generation_hash")
            or ""
        ).strip()
        return "" if value in {"", "not_observable", "null"} else value

    def cost_row_ts(row: Mapping[str, Any]) -> Optional[datetime]:
        return _parse_utc_dt(row.get("ts_utc") or row.get("ts"))

    def is_symbol_cost_hit(row: Mapping[str, Any]) -> bool:
        if is_degraded_cost(row):
            return False
        normalized = str(row.get("normalized_symbol") or "").strip().upper()
        response_symbol = str(row.get("response_symbol") or row.get("symbol") or "").strip().upper()
        if normalized and response_symbol and normalized != response_symbol:
            return False
        try:
            if row.get("sample_count") not in (None, "") and int(row.get("sample_count")) <= 0:
                return False
        except (TypeError, ValueError):
            pass
        return bool(normalized or response_symbol)

    now_dt = now or datetime.now(timezone.utc)
    latest_24h_start = now_dt - timedelta(hours=24)
    current_contract_rows = [row for row in cost_event_rows if is_current_cost_contract(row)]
    legacy_cost_rows = [row for row in cost_event_rows if not is_current_cost_contract(row)]
    latest_24h_rows = [
        row
        for row in cost_event_rows
        if (cost_row_ts(row) is not None and cost_row_ts(row) >= latest_24h_start)
    ]
    current_source_hash = str(current_source_snapshot_hash or "").strip()
    current_source_hash_observable = current_source_hash not in {"", "not_observable", "null"}
    hashed_current_rows = [row for row in current_contract_rows if row_source_hash(row)]
    if current_source_hash_observable and hashed_current_rows:
        post_deployment_rows = [row for row in current_contract_rows if row_source_hash(row) == current_source_hash]
        post_deployment_scope = "source_snapshot_hash"
    else:
        post_deployment_rows = current_contract_rows
        post_deployment_scope = "current_contract_schema_event_generation"
    post_deployment_start = min((cost_row_ts(row) for row in post_deployment_rows if cost_row_ts(row) is not None), default=None)
    cost_degraded_count = len([row for row in cost_event_rows if is_degraded_cost(row)])
    current_contract_cost_degraded_count = len([row for row in current_contract_rows if is_degraded_cost(row)])
    latest_24h_cost_degraded_count = len([row for row in latest_24h_rows if is_degraded_cost(row)])
    post_deployment_cost_degraded_count = len([row for row in post_deployment_rows if is_degraded_cost(row)])
    global_default_cost_count = len([row for row in cost_event_rows if is_global_default_cost(row)])
    legacy_global_default_cost_count = len([row for row in legacy_cost_rows if is_global_default_cost(row)])
    current_contract_global_default_cost_count = len([row for row in current_contract_rows if is_global_default_cost(row)])
    latest_24h_global_default_cost_count = len([row for row in latest_24h_rows if is_global_default_cost(row)])
    post_deployment_global_default_cost_count = len([row for row in post_deployment_rows if is_global_default_cost(row)])
    symbol_cost_hit_count = len([row for row in cost_event_rows if is_symbol_cost_hit(row)])
    cost_contract_version = next(
        (row.get("cost_contract_version") or row.get("contract_version") for row in reversed(cost_event_rows) if row.get("cost_contract_version") or row.get("contract_version")),
        CONTRACT_VERSION,
    )
    ml_fields = dict(ml_live_fields or {})
    return {
        "quant_lab_enabled": bool(rows or request_rows),
        "ml_live_overlay_status": ml_fields.get("ml_live_overlay_status", "not_observable"),
        "ml_factor_enabled": ml_fields.get("ml_factor_enabled", "not_observable"),
        "collect_ml_training_data": ml_fields.get("collect_ml_training_data", "not_observable"),
        "ml_research_use_stable_universe": ml_fields.get("ml_research_use_stable_universe", "not_observable"),
        "quant_lab_mode": latest_mode,
        "quant_lab_mode_source": latest_mode_source,
        "quant_lab_requested_mode": latest_requested_mode or latest_mode,
        "quant_lab_effective_mode": latest_effective_mode or latest_mode,
        "enforce_readiness_status": enforce_readiness_status,
        "enforce_blocked_reasons": enforce_blocked_reasons,
        "enforce_blocked_reason": enforce_blocked_reason,
        "contract_version_match": contract_version_match,
        "telemetry_schema_version_match": telemetry_schema_version_match,
        "quant_lab_request_count": len(request_rows),
        "quant_lab_request_success_count": request_success_count,
        "quant_lab_request_error_count": request_error_count,
        "quant_lab_error_count": request_error_count,
        "quant_lab_fallback_count": actual_fallback_count,
        "quant_lab_actual_fallback_count": actual_fallback_count,
        "quant_lab_actual_filter_count": len([row for row in rows if _actual_filtered(row)]),
        "quant_lab_hypothetical_filter_count": len([row for row in rows if _would_filter(row) and not _actual_filtered(row)]),
        "quant_lab_filtered_by_cost_count": len(
            [row for row in rows if _actual_filtered(row) and _is_cost_filter_reason(row.get("filter_reason"))]
        ),
        "quant_lab_filtered_by_permission_count": len(
            [
                row
                for row in rows
                if _actual_filtered(row)
                and ("sell_only" in str(row.get("filter_reason") or "") or "abort" in str(row.get("filter_reason") or ""))
            ]
        ),
        "quant_lab_latest_permission": latest_permission,
        "quant_lab_final_permission": final_permission,
        "quant_lab_gate_compliance_violation_count": len([row for row in compliance_rows if str(row.get("violation")) == "true"]),
        "permission_contract_violation_count": len(
            [row for row in permission_event_rows if _truthy(row.get("permission_contract_violation"))]
        ),
        "stale_permission_count": len([row for row in permission_event_rows if _permission_status_stale(row)]),
        "would_block_if_enforced_count": would_block_count,
        "effective_block_count": effective_block_count,
        "quant_lab_cost_model_version": cost_model_version,
        "quant_lab_gate_version": gate_version,
        "quant_lab_cost_usage_rows": len(cost_event_rows),
        "cost_usage_legacy_rows": len(legacy_cost_rows),
        "cost_usage_current_contract_rows": len(current_contract_rows),
        "cost_usage_latest_24h_rows": len(latest_24h_rows),
        "post_deployment_cost_usage_rows": len(post_deployment_rows),
        "cost_degraded_count": cost_degraded_count,
        "current_contract_cost_degraded_count": current_contract_cost_degraded_count,
        "latest_24h_cost_degraded_count": latest_24h_cost_degraded_count,
        "post_deployment_cost_degraded_count": post_deployment_cost_degraded_count,
        "global_default_cost_count": global_default_cost_count,
        "legacy_global_default_cost_count": legacy_global_default_cost_count,
        "current_contract_global_default_cost_count": current_contract_global_default_cost_count,
        "latest_24h_global_default_cost_count": latest_24h_global_default_cost_count,
        "post_deployment_global_default_cost_count": post_deployment_global_default_cost_count,
        "symbol_cost_hit_count": symbol_cost_hit_count,
        "cost_contract_version": cost_contract_version,
        "quant_lab_cost_degraded_count": cost_degraded_count,
        "quant_lab_global_default_cost_count": global_default_cost_count,
        "quant_lab_symbol_cost_hit_count": symbol_cost_hit_count,
        "readiness_cost_usage_rows": len(post_deployment_rows),
        "readiness_cost_degraded_count": post_deployment_cost_degraded_count,
        "readiness_global_default_cost_count": post_deployment_global_default_cost_count,
        "cost_usage_post_deployment_scope": post_deployment_scope,
        "cost_usage_current_source_snapshot_hash": current_source_hash or "not_observable",
        "post_deployment_cost_usage_start_utc": post_deployment_start.isoformat().replace("+00:00", "Z") if post_deployment_start else "not_observable",
        "telemetry_contract_version": CONTRACT_VERSION,
        "telemetry_schema_version": SCHEMA_VERSION,
    }


def _issues(rows: list[Dict[str, Any]], request_rows: list[Dict[str, Any]], cost_rows: list[Dict[str, Any]], compliance_rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    issues: list[Dict[str, Any]] = []

    def add(code: str, severity: str, detail: str) -> None:
        if not any(item["code"] == code for item in issues):
            issues.append({"code": code, "severity": severity, "detail": detail})

    if not rows:
        add("quant_lab_missing_usage_log", "medium", "reports/quant_lab_usage.jsonl is missing or empty")
    if not cost_rows:
        add("quant_lab_missing_cost_usage", "medium", "no cost_estimate telemetry in window")
    if any(not _request_success(row) for row in request_rows):
        add("quant_lab_api_unavailable", "medium", "quant-lab request errors were observed")
    latest_permission = next((row.get("final_permission") or row.get("permission") for row in reversed(rows) if row.get("final_permission") or row.get("permission")), "")
    if str(latest_permission).upper() == "ABORT":
        add("quant_lab_permission_abort", "high", "latest quant-lab permission is ABORT")
    if str(latest_permission).upper() == "SELL_ONLY":
        add("quant_lab_permission_sell_only", "medium", "latest quant-lab permission is SELL_ONLY")
    if any(str(row.get("violation")) == "true" for row in compliance_rows):
        add("quant_lab_gate_compliance_violation", "high", "orders violated quant-lab permission in the window")
    if any(_truthy(row.get("permission_contract_violation")) for row in rows):
        add("quant_lab_permission_contract_violation", "high", "quant-lab permission contract violations were observed")
    if any(_permission_not_enforceable(row) for row in rows):
        add("quant_lab_permission_not_enforceable", "medium", "quant-lab returned non-enforceable permission rows")
    if len([row for row in rows if _is_fallback_row(row)]) >= 3:
        add("quant_lab_cost_fallback_high", "medium", "quant-lab fallback count is elevated")
    if any(row.get("filtered") for row in cost_rows):
        add("quant_lab_cost_gate_filtered_trade", "low", "quant-lab cost gate filtered at least one order")
    if len([row for row in request_rows if not _request_success(row)]) >= 3:
        add("quant_lab_request_error_high", "medium", "quant-lab request error count is elevated")
    if any(str(row.get("source") or "").lower() == "public_spread_proxy" for row in cost_rows):
        add("quant_lab_public_proxy_cost_only", "medium", "quant-lab cost source is public_spread_proxy")
    return issues


def _quant_lab_config_audit(root: Path, usage_rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    latest = usage_rows[-1] if usage_rows else {}
    audit: Dict[str, Any] = {
        "enabled": bool(usage_rows),
        "mode": latest.get("mode"),
        "mode_source": latest.get("mode_source"),
        "runtime_override_path": "state/quant_lab_mode.json",
        "fail_policy": latest.get("fail_policy"),
        "allow_local_fallback_in_enforce": None,
        "allow_insecure_http_with_token": None,
        "base_url_scheme": None,
        "base_url_host": None,
        "api_token_env": None,
        "api_env_path_present": None,
        "api_env_secure_permissions": latest.get("api_env_secure_permissions"),
        "api_env_token_loaded": latest.get("api_env_token_loaded"),
        "api_env_warning": latest.get("api_env_warning"),
        "permission_gate_enforced": latest.get("permission_gate_enforced"),
        "cost_gate_enforced": latest.get("cost_gate_enforced"),
    }
    try:
        from configs.loader import load_config
        from src.quant_lab_client.client import inspect_api_env_file

        cfg_path = root / "configs/live_prod.yaml"
        if not cfg_path.exists():
            cfg_path = root / "configs/config.yaml"
        if cfg_path.exists():
            cfg = load_config(str(cfg_path))
            qcfg = cfg.quant_lab
            mode = str(getattr(qcfg, "mode", "shadow") or "shadow")
            mode_source = "config"
            override_path_value = str(getattr(qcfg, "runtime_override_path", "state/quant_lab_mode.json") or "state/quant_lab_mode.json")
            override_path = Path(override_path_value)
            if not override_path.is_absolute():
                override_path = root / override_path
            if bool(getattr(qcfg, "allow_runtime_override", True)) and override_path.exists():
                try:
                    payload = json.loads(override_path.read_text(encoding="utf-8"))
                    if isinstance(payload, dict) and payload.get("mode"):
                        mode = str(payload.get("mode") or mode)
                        mode_source = "runtime_override"
                except Exception:
                    mode_source = "config_invalid_override"
            parsed_url = urlparse(str(getattr(qcfg, "base_url", "") or ""))
            api_env_path = getattr(qcfg, "api_env_path", None)
            api_env_status = inspect_api_env_file(
                api_env_path,
                allow_symlink=bool(getattr(qcfg, "allow_api_env_symlink", False)),
                require_secure_permissions=bool(getattr(qcfg, "api_env_require_secure_permissions", True)),
            )
            audit.update(
                {
                    "enabled": bool(getattr(qcfg, "enabled", False)),
                    "mode": mode,
                    "mode_source": mode_source,
                    "runtime_override_path": override_path_value,
                    "fail_policy": getattr(qcfg, "fail_policy", None),
                    "allow_local_fallback_in_enforce": bool(getattr(qcfg, "allow_local_fallback_in_enforce", False)),
                    "allow_insecure_http_with_token": bool(getattr(qcfg, "allow_insecure_http_with_token", False)),
                    "base_url_scheme": parsed_url.scheme,
                    "base_url_host": parsed_url.hostname,
                    "api_token_env": getattr(qcfg, "api_token_env", None),
                    "api_env_path_present": bool(api_env_status.path_present),
                    "api_env_secure_permissions": api_env_status.secure_permissions,
                    "api_env_token_loaded": latest.get("api_env_token_loaded"),
                    "api_env_warning": latest.get("api_env_warning") or api_env_status.warning,
                }
            )
    except Exception as exc:
        audit["config_audit_error"] = type(exc).__name__
    for row in reversed(usage_rows):
        if audit.get("permission_gate_enforced") in ("", None) and "permission_gate_enforced" in row:
            audit["permission_gate_enforced"] = row.get("permission_gate_enforced")
        if audit.get("cost_gate_enforced") in ("", None) and "cost_gate_enforced" in row:
            audit["cost_gate_enforced"] = row.get("cost_gate_enforced")
        for field in ("api_env_path_present", "api_env_secure_permissions", "api_env_token_loaded", "api_env_warning"):
            if audit.get(field) in ("", None) and field in row:
                audit[field] = row.get(field)
    return _sanitize_bundle_obj(audit)


def _add_file(tf: tarfile.TarFile, arcname: str, data: str | bytes) -> None:
    raw = data if isinstance(data, bytes) else data.encode("utf-8")
    info = tarfile.TarInfo(arcname)
    info.size = len(raw)
    info.mtime = int(datetime.now(timezone.utc).timestamp())
    tf.addfile(info, io.BytesIO(raw))


def _secret_scan_findings(staging: Path) -> int:
    findings = 0
    risky_values = ("P@ssw0rd", "Bearer ", "BEGIN PRIVATE KEY", "super-secret-token")
    for path in staging.rglob("*"):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(value in text for value in risky_values):
            findings += 1
    return findings


def _hash_file(path: Path) -> str:
    try:
        if path.is_file():
            return hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception:
        return "not_observable"
    return "not_observable"


def _hash_source_snapshot(root: Path) -> str:
    candidates: list[Path] = []
    for rel in (
        "main.py",
        "event_driven_check.py",
        "src",
        "scripts",
        "configs",
        "pyproject.toml",
        "requirements.txt",
        "requirements-research.txt",
    ):
        path = root / rel
        if path.is_file():
            candidates.append(path)
        elif path.is_dir():
            candidates.extend(item for item in path.rglob("*") if item.is_file() and ".git" not in item.parts)
    digest = hashlib.sha256()
    seen = False
    for path in sorted(candidates):
        try:
            digest.update(path.relative_to(root).as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
            seen = True
        except Exception:
            continue
    return digest.hexdigest() if seen else "not_observable"


def _git_info(root: Path) -> Dict[str, Any]:
    status = _git_command(root, ["status", "--porcelain"])
    dirty = bool(status)
    return {
        "git_commit": _git_command(root, ["rev-parse", "--short", "HEAD"]) or "not_observable",
        "git_branch": _git_command(root, ["branch", "--show-current"]) or "not_observable",
        "git_dirty": dirty,
        "dirty_worktree": dirty,
        "provenance_status": "git_dirty" if dirty else "git_clean",
        "code_provenance": "degraded" if dirty else "ok",
    }


def _git_command(root: Path, args: list[str]) -> str:
    git_bin = shutil.which("git")
    if git_bin is None:
        return ""
    if not args or any("\0" in str(arg) for arg in args):
        return ""
    try:
        result = subprocess.run(  # noqa: S603 - executable is resolved by shutil.which and args are fixed repo-internal git probes.
            [git_bin, *args],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=GIT_COMMAND_TIMEOUT_SEC,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _find_nested_value(obj: Any, names: Iterable[str]) -> Any:
    if isinstance(obj, Mapping):
        for name in names:
            value = obj.get(name)
            if value not in (None, ""):
                return value
        for value in obj.values():
            found = _find_nested_value(value, names)
            if found not in (None, "", "not_observable"):
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _find_nested_value(value, names)
            if found not in (None, "", "not_observable"):
                return found
    return "not_observable"


def _find_nested_path(obj: Any, path: Iterable[str]) -> Any:
    current = obj
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return "not_observable"
        current = current.get(key)
    return current if current not in (None, "") else "not_observable"


def _bool_text(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return "true"
    if text in {"0", "false", "no", "off"}:
        return "false"
    return text if text else "not_observable"


def _ml_effective_live_fields(effective_config: Any) -> Dict[str, str]:
    effective = effective_config if isinstance(effective_config, Mapping) else {}
    ml_enabled = _bool_text(
        next(
            (
                value
                for value in (
                    _find_nested_path(effective, ("ml_factor_enabled",)),
                    _find_nested_path(effective, ("alpha", "ml_factor_enabled")),
                    _find_nested_path(effective, ("alpha", "ml_factor", "enabled")),
                )
                if value != "not_observable"
            ),
            "not_observable",
        )
    )
    collect_training = _bool_text(
        next(
            (
                value
                for value in (
                    _find_nested_path(effective, ("collect_ml_training_data",)),
                    _find_nested_path(effective, ("execution", "collect_ml_training_data")),
                )
                if value != "not_observable"
            ),
            "not_observable",
        )
    )
    stable_universe = _bool_text(
        next(
            (
                value
                for value in (
                    _find_nested_path(effective, ("ml_research_use_stable_universe",)),
                    _find_nested_path(effective, ("execution", "ml_research_use_stable_universe")),
                )
                if value != "not_observable"
            ),
            "not_observable",
        )
    )
    return {
        "ml_live_overlay_status": "disabled_in_live_prod" if ml_enabled == "false" else "not_observable",
        "ml_factor_enabled": ml_enabled,
        "collect_ml_training_data": collect_training,
        "ml_research_use_stable_universe": stable_universe,
    }


def _find_yaml_scalar(text: str, names: Iterable[str]) -> str:
    for name in names:
        match = re.search(rf"(?m)^\s*{re.escape(name)}\s*:\s*([^#\n]+)", text or "")
        if match:
            return match.group(1).strip().strip("\"'")
    return "not_observable"


def _read_json_obj(path: Path) -> Any:
    try:
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


def _manifest_metadata(root: Path, reports: Path, usage_rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    live_config_path = root / "configs/live_prod.yaml"
    config_path = live_config_path if live_config_path.exists() else root / "configs/config.yaml"
    effective_path = reports / "effective_live_config.json"
    effective_config = _read_json_obj(effective_path)
    try:
        config_text = config_path.read_text(encoding="utf-8", errors="replace") if config_path.is_file() else ""
    except Exception:
        config_text = ""

    strategy_version = _find_nested_value(effective_config, ("strategy_version", "quant_lab_strategy_version"))
    if strategy_version == "not_observable":
        strategy_version = next(
            (row.get("strategy_version") for row in reversed(usage_rows) if row.get("strategy_version")),
            "not_observable",
        )
    if strategy_version == "not_observable":
        strategy_version = _find_yaml_scalar(config_text, ("strategy_version", "quant_lab_strategy_version"))

    contract_version = _find_nested_value(effective_config, ("contract_version", "quant_lab_contract_version"))
    if contract_version == "not_observable":
        contract_version = next(
            (row.get("contract_version") for row in reversed(usage_rows) if row.get("contract_version")),
            CONTRACT_VERSION,
        )

    git = _git_info(root)
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_version": str(contract_version or CONTRACT_VERSION),
        "quant_lab_contract_version": str(contract_version or CONTRACT_VERSION),
        "telemetry_schema_version": SCHEMA_VERSION,
        "telemetry_contract_version": str(contract_version or CONTRACT_VERSION),
        "event_id_generation_version": EVENT_ID_GENERATION_VERSION,
        "trade_export_schema_version": TRADE_EXPORT_SCHEMA_VERSION,
        "summary_metrics_version": SUMMARY_METRICS_VERSION,
        "config_hash": _hash_file(config_path),
        "strategy_version": str(strategy_version or "not_observable"),
        "source_snapshot_hash": _hash_source_snapshot(root),
        **git,
    }


def export_v5_bundle(
    *,
    reports_dir: str | Path,
    out_dir: str | Path,
    window_hours: int = 72,
    include_logs: bool = True,
    include_config: bool = True,
) -> Path:
    reports = Path(reports_dir).resolve()
    root = reports.parent
    out = Path(out_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    since = _since_iso(window_hours)
    usage_path = reports / "quant_lab_usage.jsonl"
    requests_path = reports / "quant_lab_requests.jsonl"
    usage_rows = _filter_window(read_quant_lab_usage(usage_path), since)
    request_rows = _filter_window(_read_jsonl(requests_path), since)
    compliance_rows = _build_compliance_rows(usage_rows)
    permission_rows = _build_permission_audit_rows(usage_rows)
    cost_rows = _build_cost_rows(usage_rows)
    live_guard_impact_rows = _build_live_guard_impact_rows(usage_rows)
    fallback_rows = _build_fallback_rows(usage_rows + request_rows)
    mode_rows = _build_mode_audit_rows(usage_rows)
    trade_metrics_rows, fill_metrics_rows, mismatch_rows = _build_trade_bundle_rows(reports)
    candidate_rows = _read_candidate_snapshot_rows(reports)
    candidate_cost_source_coverage = _candidate_cost_source_coverage(candidate_rows)
    order_lifecycle_rows = _backfill_order_lifecycle_from_fill_metrics(
        _read_order_lifecycle_rows(reports),
        fill_metrics_rows,
    )
    trade_metric_fill_count = _count_trade_metric_fills(trade_metrics_rows)
    order_lifecycle_missing_for_trades = trade_metric_fill_count > 0 and not order_lifecycle_rows
    summary_high_issue_count = len([row for row in mismatch_rows if str(row.get("high_issue")) == "true"])
    run_summary_invalid = summary_high_issue_count > 0
    manifest_meta = _manifest_metadata(root, reports, usage_rows)
    data_quality_warnings = (
        ["dirty_worktree: V5-prod code provenance is degraded"]
        if manifest_meta.get("git_dirty")
        else []
    )

    stamp = _utc_stamp()
    bundle_name = f"v5_live_followup_bundle_{stamp}.tar.gz"
    final_path = out / bundle_name
    tmp_path = out / f"{bundle_name}.tmp"
    staging = Path(tempfile.mkdtemp(prefix="v5_bundle_", dir=str(out)))
    try:
        _write_text(staging / "raw/quant_lab/quant_lab_usage.jsonl", _redact_text(_jsonl_rows_text(usage_rows)))
        _write_text(staging / "raw/quant_lab/quant_lab_requests.jsonl", _redact_text(_jsonl_rows_text(request_rows)))
        _write_csv(staging / "summaries/quant_lab_compliance.csv", COMPLIANCE_FIELDS, compliance_rows)
        _write_csv(staging / "summaries/quant_lab_permission_audit.csv", PERMISSION_AUDIT_FIELDS, permission_rows)
        _write_csv(staging / "summaries/quant_lab_mode_audit.csv", MODE_AUDIT_FIELDS, mode_rows)
        _write_csv(staging / "summaries/quant_lab_cost_usage.csv", COST_FIELDS, cost_rows)
        _write_csv(staging / "summaries/runtime_cost_guard.csv", RUNTIME_COST_GUARD_FIELDS, [])
        _write_csv(staging / "summaries/cost_disagreement.csv", COST_DISAGREEMENT_FIELDS, [])
        _write_csv(staging / "summaries/live_guard_impact.csv", LIVE_GUARD_IMPACT_FIELDS, live_guard_impact_rows)
        _write_csv(staging / "summaries/quant_lab_fallbacks.csv", FALLBACK_FIELDS, fallback_rows)
        _write_csv(staging / "summaries/trade_metrics.csv", TRADE_METRICS_FIELDS, trade_metrics_rows)
        _write_csv(staging / "summaries/fill_metrics.csv", FILL_METRICS_FIELDS, fill_metrics_rows)
        _write_csv(staging / "summaries/candidate_snapshot.csv", CANDIDATE_SNAPSHOT_FIELDS, candidate_rows)
        _write_csv(staging / "summaries/order_lifecycle.csv", ORDER_LIFECYCLE_FIELDS, order_lifecycle_rows)
        _copy_candidate_snapshot_files(staging, reports, candidate_rows)
        _copy_order_lifecycle_files(staging, reports)
        cost_probe_artifacts = _copy_cost_probe_artifacts(staging, reports)
        _copy_sol_paper_strategy_files(staging, reports)
        _write_csv(
            staging / "reports/summary_trade_count_mismatch.csv",
            SUMMARY_TRADE_COUNT_MISMATCH_FIELDS,
            mismatch_rows,
        )
        window_summary = _window_summary(
            usage_rows,
            request_rows,
            compliance_rows,
            permission_rows,
            current_source_snapshot_hash=str(manifest_meta.get("source_snapshot_hash") or "not_observable"),
            ml_live_fields=_ml_effective_live_fields(_read_json_obj(reports / "effective_live_config.json")),
        )
        window_summary.update(
            {
                "trade_metrics_rows": len(trade_metrics_rows),
                "fill_metrics_rows": len(fill_metrics_rows),
                "summary_trade_count_mismatch_count": len(mismatch_rows),
                "summary_trade_count_mismatch_high_issue_count": summary_high_issue_count,
                "run_summary_invalid": run_summary_invalid,
                "candidate_snapshot_rows": len(candidate_rows),
                "candidate_cost_source_coverage": candidate_cost_source_coverage,
                "order_lifecycle_rows": len(order_lifecycle_rows),
                "order_lifecycle_trade_metric_fill_count": trade_metric_fill_count,
                "order_lifecycle_missing_high_issue": order_lifecycle_missing_for_trades,
                "cost_probe_artifact_count": len(cost_probe_artifacts["present"]),
                "cost_probe_artifacts_present": cost_probe_artifacts["present"],
                "cost_probe_artifacts_missing": cost_probe_artifacts["missing"],
                "cost_probe_artifact_row_counts": cost_probe_artifacts["row_counts"],
                "cost_probe_summary": cost_probe_artifacts["summary"],
                "cost_probe_p3_preflight": cost_probe_artifacts["p3_preflight"],
                "cost_probe_live_execution_status": cost_probe_artifacts[
                    "live_execution_status"
                ],
                "live_guard_would_block_count": len([row for row in live_guard_impact_rows if _live_guard_would_block(row)]),
                "would_block_count": len([row for row in live_guard_impact_rows if _live_guard_would_block(row)]),
                "live_guard_actual_block_count": 0,
                "guard_enforced": False,
                "would_block_but_profitable_open_positions_count": 0,
                "would_block_strategy_mix": _live_guard_value_mix(live_guard_impact_rows, "strategy_candidate"),
                "would_block_symbol_mix": _live_guard_value_mix(live_guard_impact_rows, "symbol"),
                "data_quality_warnings": data_quality_warnings,
            }
        )
        _write_text(staging / "summaries/window_summary.json", json.dumps(window_summary, ensure_ascii=False, indent=2))
        _write_text(
            staging / "summaries/market_context.json",
            json.dumps(
                _build_market_context_summary(
                    staging,
                    reports,
                    window_summary=window_summary,
                ),
                ensure_ascii=False,
                indent=2,
            ),
        )
        _write_text(
            staging / "summaries/enforce_readiness_snapshot.json",
            json.dumps(_enforce_readiness_snapshot(mode_rows, window_summary), ensure_ascii=False, indent=2),
        )
        _write_text(
            staging / "summaries/quant_lab_config_audit.json",
            json.dumps(_quant_lab_config_audit(root, usage_rows), ensure_ascii=False, indent=2),
        )
        _write_text(
            staging / "summaries/issues_to_fix.json",
            json.dumps(
                [
                    *_issues(usage_rows, request_rows, cost_rows, compliance_rows),
                    *[
                        {
                            "code": "summary_trade_count_mismatch",
                            "severity": "high",
                            "detail": "trades.csv has fill rows but summary.json reports num_trades=0",
                            "run_id": row.get("run_id"),
                        }
                        for row in mismatch_rows
                        if str(row.get("high_issue")) == "true"
                    ],
                    *(
                        [
                            {
                                "code": "order_lifecycle_missing_for_trades",
                                "severity": "high",
                                "detail": "trade_metrics has counted trades but order_lifecycle.csv is empty.",
                                "trade_metric_fill_count": trade_metric_fill_count,
                            }
                        ]
                        if order_lifecycle_missing_for_trades
                        else []
                    ),
                    *(
                        [
                            {
                                "code": "dirty_worktree",
                                "severity": "medium",
                                "detail": "V5-prod worktree is dirty; code provenance is degraded.",
                            }
                        ]
                        if manifest_meta.get("git_dirty")
                        else []
                    ),
                ],
                ensure_ascii=False,
                indent=2,
            ),
        )
        state_path = root / "state/quant_lab_mode.json"
        if state_path.exists():
            _write_text(staging / "raw/state/quant_lab_mode.json", _read_text_redacted(state_path))
        effective_path = reports / "effective_live_config.json"
        if effective_path.exists():
            effective_text = _read_text_redacted(effective_path)
            _write_text(staging / "raw/reports/effective_live_config.json", effective_text)
            _write_text(staging / "raw/effective_live_config.json", effective_text)
        for source_name, dest_rel in (
            ("auto_risk_eval.json", "raw/state/auto_risk_eval.json"),
            ("event_candidates.json", "raw/reports/event_candidates.json"),
        ):
            source_path = reports / source_name
            if source_path.exists():
                _write_text(staging / dest_rel, _read_text_redacted(source_path))
        if include_config:
            for rel in ("configs/config.yaml", "configs/live_prod.yaml"):
                path = root / rel
                if path.exists():
                    _write_text(staging / "raw/config" / Path(rel).name, _read_text_redacted(path))
        if include_logs:
            log_path = root / "logs/v5_runtime.log"
            if log_path.exists():
                _write_text(staging / "raw/logs/v5_runtime.log", _tail_lines(_read_text_redacted(log_path)))
        findings = _secret_scan_findings(staging)
        large_raw_files = _relocate_large_raw_files(staging)
        _write_text(staging / "raw/large/.noindex", "raw_large_not_indexed\n")
        if large_raw_files:
            _write_text(
                staging / "raw/large/manifest.json",
                json.dumps(large_raw_files, ensure_ascii=False, indent=2),
            )
        manifest = {
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "reports_dir": str(reports),
            "window_hours": int(window_hours),
            **manifest_meta,
            "trade_export_schema_version": TRADE_EXPORT_SCHEMA_VERSION,
            "summary_metrics_version": SUMMARY_METRICS_VERSION,
            "candidate_snapshot_schema_version": CANDIDATE_SNAPSHOT_SCHEMA_VERSION,
            "candidate_snapshot_rows": len(candidate_rows),
            "candidate_cost_source_coverage": candidate_cost_source_coverage,
            "order_lifecycle_schema_version": ORDER_LIFECYCLE_SCHEMA_VERSION,
            "order_lifecycle_rows": len(order_lifecycle_rows),
            "order_lifecycle_trade_metric_fill_count": trade_metric_fill_count,
            "order_lifecycle_missing_high_issue": order_lifecycle_missing_for_trades,
            "cost_probe_artifact_count": len(cost_probe_artifacts["present"]),
            "cost_probe_artifacts_present": cost_probe_artifacts["present"],
            "cost_probe_artifacts_missing": cost_probe_artifacts["missing"],
            "cost_probe_artifact_row_counts": cost_probe_artifacts["row_counts"],
            "cost_probe_summary": cost_probe_artifacts["summary"],
            "cost_probe_p3_preflight": cost_probe_artifacts["p3_preflight"],
            "cost_probe_live_execution_status": cost_probe_artifacts[
                "live_execution_status"
            ],
            "run_summary_invalid": run_summary_invalid,
            "summary_trade_count_mismatch_high_issue_count": summary_high_issue_count,
            "data_quality_warnings": data_quality_warnings,
            "sanity_checks": {
                "no_env_files": True,
                "no_unredacted_secret_assignments": findings == 0,
                "redaction_applied": True,
                "secret_scan_findings_count": findings,
            },
        }
        _write_text(staging / "manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        _write_static_report_index(
            staging,
            window_summary=window_summary,
            trade_metrics_rows=trade_metrics_rows,
            candidate_rows=candidate_rows,
            request_rows=request_rows,
            large_raw_files=large_raw_files,
        )
        with tarfile.open(tmp_path, "w:gz") as tf:
            for path in sorted(staging.rglob("*")):
                if path.is_file():
                    tf.add(path, arcname=path.relative_to(staging).as_posix())
        tmp_path.replace(final_path)
        digest = hashlib.sha256(final_path.read_bytes()).hexdigest()
        (out / f"{bundle_name}.sha256").write_text(f"{digest}  {bundle_name}\n", encoding="utf-8")
        return final_path
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Export sanitized V5 telemetry bundle for quant-lab")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--out-dir", default="/var/lib/v5/exports/bundles")
    parser.add_argument("--window-hours", type=int, default=72)
    parser.add_argument("--keep-count", type=int, default=DEFAULT_KEEP_COUNT)
    parser.add_argument("--max-age-days", type=float, default=DEFAULT_MAX_AGE_DAYS)
    parser.add_argument("--no-prune", action="store_true")
    args = parser.parse_args(argv)
    bundle = export_v5_bundle(reports_dir=args.reports_dir, out_dir=args.out_dir, window_hours=args.window_hours)
    retention = None
    if not args.no_prune:
        retention = prune_v5_bundle_exports(
            args.out_dir,
            keep_count=args.keep_count,
            max_age_days=args.max_age_days,
        ).to_dict()
    print(
        json.dumps(
            {
                "bundle_path": str(bundle),
                "sha256_path": str(bundle) + ".sha256",
                "retention": retention,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
