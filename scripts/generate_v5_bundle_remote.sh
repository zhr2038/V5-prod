#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-${V5_REMOTE_ROOT:-/home/ubuntu/clawd/v5-prod}}"

python3 - "$ROOT" <<'PY'
import csv
import datetime as dt
import fnmatch
import hashlib
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import urllib.parse
import urllib.request
from collections import Counter, defaultdict, deque
from pathlib import Path

ROOT = Path(sys.argv[1]).resolve()
NOW = dt.datetime.now(dt.timezone.utc)
STAMP = NOW.strftime("%Y%m%dT%H%M%SZ")
BUNDLE_STEM = f"v5_live_followup_bundle_{STAMP}"
OUT = Path("/tmp") / BUNDLE_STEM
TAR = Path(f"{OUT}.tar.gz")
SHA_PATH = Path(f"{TAR}.sha256")
PAYLOAD_DIRS = [
    "raw",
    "raw/state",
    "raw/recent_runs",
    "raw/logs",
    "raw/reports",
    "summaries",
]
RUN_FILES = ("decision_audit.json", "trades.csv", "equity.jsonl", "summary.json", "candidate_snapshot.csv", "order_lifecycle.csv")
STATE_FILES = [
    ("reports/kill_switch.json", "raw/state/kill_switch.json", True),
    ("reports/reconcile_status.json", "raw/state/reconcile_status.json", True),
    ("reports/ledger_status.json", "raw/state/ledger_status.json", True),
    ("reports/ledger_state.json", "raw/state/ledger_state.json", True),
    ("reports/auto_risk_eval.json", "raw/state/auto_risk_eval.json", True),
    ("reports/negative_expectancy_cooldown.json", "raw/state/negative_expectancy_cooldown.json", True),
    ("reports/profit_taking_state.json", "raw/state/profit_taking_state.json", False),
    ("reports/highest_px_state.json", "raw/state/highest_px_state.json", False),
    ("reports/stop_loss_state.json", "raw/state/stop_loss_state.json", False),
    ("reports/fixed_stop_loss_state.json", "raw/state/fixed_stop_loss_state.json", False),
    ("reports/market_impulse_probe_state.json", "raw/state/market_impulse_probe_state.json", False),
    ("reports/positions.json", "raw/state/positions.json", False),
]
CURRENT_REPORT_FILES = [
    ("reports/effective_live_config.json", "raw/reports/effective_live_config.json", False),
    ("reports/event_candidates.json", "raw/reports/event_candidates.json", False),
    ("reports/skipped_candidate_labels.jsonl", "raw/reports/skipped_candidate_labels.jsonl", False),
    ("reports/alt_impulse_shadow_labels.jsonl", "raw/reports/alt_impulse_shadow_labels.jsonl", False),
    ("reports/multi_position_swing_shadow_labels.jsonl", "raw/reports/multi_position_swing_shadow_labels.jsonl", False),
    ("reports/protect_sol_exception_shadow_labels.jsonl", "raw/reports/protect_sol_exception_shadow_labels.jsonl", False),
    ("reports/sol_paper_strategy_labels.jsonl", "raw/reports/sol_paper_strategy_labels.jsonl", False),
    ("reports/candidate_snapshot.csv", "raw/reports/candidate_snapshot.csv", False),
    ("reports/order_lifecycle.csv", "raw/reports/order_lifecycle.csv", False),
    ("reports/summaries/paper_strategy_runs.csv", "summaries/paper_strategy_runs.csv", False),
    ("reports/summaries/paper_strategy_daily.csv", "summaries/paper_strategy_daily.csv", False),
    ("reports/summaries/paper_slippage_coverage.csv", "summaries/paper_slippage_coverage.csv", False),
    ("reports/quant_lab_usage.jsonl", "raw/reports/quant_lab_usage.jsonl", False),
    ("reports/quant_lab_requests.jsonl", "raw/reports/quant_lab_requests.jsonl", False),
]
EXPANDED_UNIVERSE_ADVISORY_FIELDS = (
    "run_id",
    "ts_utc",
    "source_path",
    "advisory_source",
    "advisory_fresh",
    "advisory_age_sec",
    "advisory_contract_match",
    "stale_advisory_used",
    "api_fallback_attempted",
    "api_fallback_success",
    "as_of_ts",
    "generated_at",
    "expires_at",
    "contract_version",
    "quant_lab_git_commit",
    "source_version",
    "universe_type",
    "symbol",
    "symbol_in_live_universe",
    "live_symbols_unchanged",
    "strategy_id",
    "strategy_candidate",
    "experiment_name",
    "decision",
    "recommended_mode",
    "horizon_hours",
    "sample_count",
    "complete_sample_count",
    "response_action",
    "negative_advisory",
    "paper_tracking_allowed",
    "shadow_tracking_allowed",
    "max_paper_notional_usdt",
    "max_live_notional_usdt",
    "max_live_notional_usdt_ignored",
    "live_block_reasons",
    "would_block_if_enabled",
    "would_enter",
    "no_sample_reason",
    "advisory_reason",
    "live_order_effect",
)
EXPANDED_UNIVERSE_PAPER_RUN_FIELDS = (
    "run_id",
    "ts_utc",
    "paper_date",
    "universe_type",
    "symbol",
    "symbol_in_live_universe",
    "live_symbols_unchanged",
    "strategy_id",
    "strategy_candidate",
    "experiment_name",
    "tracking_mode",
    "decision",
    "recommended_mode",
    "response_action",
    "negative_advisory",
    "would_enter",
    "would_size_usdt",
    "max_paper_notional_usdt",
    "max_live_notional_usdt_ignored",
    "no_sample_reason",
    "advisory_source",
    "advisory_source_path",
    "advisory_fresh",
    "advisory_contract_match",
    "live_block_reasons",
    "live_order_effect",
)
ALPHA_FACTORY_ADVISORY_FIELDS = (
    "run_id",
    "ts_utc",
    "strategy_candidate",
    "symbol",
    "decision",
    "recommended_mode",
    "promotion_state",
    "alpha_factory_score",
    "advisory_source",
    "advisory_fresh",
    "advisory_age_sec",
    "response_action",
    "max_live_notional_usdt_ignored",
    "live_order_effect",
)
ALPHA_FACTORY_FAMILY_SUMMARY_FIELDS = (
    "run_id",
    "ts_utc",
    "family",
    "row_count",
    "display_only_count",
    "shadow_tracking_count",
    "paper_tracking_count",
    "negative_advisory_count",
    "max_live_notional_usdt_ignored",
    "live_order_effect",
    "strategy_candidates",
)
RISK_ON_MULTI_BUY_SHADOW_FIELDS = (
    "run_id",
    "ts_utc",
    "current_regime",
    "top_k",
    "selected_symbols",
    "would_buy_symbols",
    "actual_bought_symbols",
    "missed_symbols",
    "source_detail_available",
    "response_action",
    "live_order_effect",
)
STRATEGY_ADVISORY_SOURCE_HEALTH_FIELDS = (
    "run_id",
    "ts_utc",
    "local_row_count",
    "api_row_count",
    "selected_row_count",
    "latest_local_generated_at",
    "latest_api_generated_at",
    "selected_latest_generated_at",
    "advisory_source_lag_sec",
    "selected_source",
    "api_fallback_attempted",
    "api_fallback_success",
    "stale_reason",
    "warning",
    "freshness_inconsistency_warning",
)
ENTRY_QUALITY_REPORT_FILES = [
    ("missed_low_audit.csv", "raw/reports/entry_quality/missed_low_audit.csv", "csv"),
    ("missed_low_by_symbol.csv", "raw/reports/entry_quality/missed_low_by_symbol.csv", "csv"),
    ("late_entry_chase_shadow.csv", "raw/reports/entry_quality/late_entry_chase_shadow.csv", "csv"),
    ("late_entry_chase_threshold_advisory.json", "raw/reports/entry_quality/late_entry_chase_threshold_advisory.json", "json"),
    ("late_entry_chase_threshold_sensitivity.csv", "raw/reports/entry_quality/late_entry_chase_threshold_sensitivity.csv", "csv"),
    ("pullback_reversal_shadow_outcomes.csv", "raw/reports/entry_quality/pullback_reversal_shadow_outcomes.csv", "csv"),
    ("pullback_reversal_readiness.json", "raw/reports/entry_quality/pullback_reversal_readiness.json", "json"),
    ("entry_quality_summary.md", "raw/reports/entry_quality/entry_quality_summary.md", "text"),
]
ENTRY_QUALITY_SOURCE_DIRS = [
    "reports",
    "reports/entry_quality",
    "reports/quant_lab",
    "reports/quant_lab_latest",
    "reports/quant_lab/latest/reports",
    "/var/lib/v5-prod",
    "/var/lib/v5-prod/entry_quality",
    "/var/lib/v5-prod/quant_lab",
    "/var/lib/v5-prod/quant_lab/latest/reports",
]
ENTRY_QUALITY_SOURCE_ARCHIVES = [
    "/var/lib/v5-prod/quant_lab_latest_bundle.zip",
    "/var/lib/v5-prod/quant_lab_latest_bundle.tar.gz",
    "/var/lib/v5-prod/quant_lab_latest_bundle*.zip",
    "/var/lib/v5-prod/quant_lab_latest_bundle*.tar.gz",
    "/var/lib/v5-prod/quant_lab_expert_pack*.zip",
    "/var/lib/v5-prod/quant_lab_expert_pack*.tar.gz",
    "reports/quant_lab_latest_bundle.zip",
    "reports/quant_lab_latest_bundle.tar.gz",
    "reports/quant_lab/latest_bundle.zip",
    "reports/quant_lab/latest_bundle.tar.gz",
    "reports/quant_lab_expert_pack*.zip",
    "reports/quant_lab_expert_pack*.tar.gz",
]
ENTRY_QUALITY_REPORT_API_ENDPOINT_TEMPLATES = [
    "/v1/reports/entry-quality/{filename}",
    "/v1/reports/entry_quality/{filename}",
    "/v1/entry-quality/{filename}",
    "/v1/entry_quality/{filename}",
    "/v1/reports/{filename}",
    "/v1/reports/download?path=reports/{filename}",
    "/v1/reports/download?path=entry_quality/{filename}",
    "/v1/report?path=reports/{filename}",
    "/v1/report?path=entry_quality/{filename}",
]
ENTRY_QUALITY_STRATEGY_CANDIDATES = {
    "v5.entry_quality_missed_low_audit",
    "v5.late_entry_chase_guard_shadow",
    "v5.pullback_reversal_shadow_btc",
    "v5.pullback_reversal_shadow_sol",
    "v5.pullback_reversal_shadow_eth",
    "v5.pullback_reversal_shadow_bnb",
}
RISK_ON_MULTI_BUY_SHADOW_CANDIDATES = {
    "v5.risk_on_multi_buy_top1_shadow",
    "v5.risk_on_multi_buy_top2_shadow",
    "v5.risk_on_multi_buy_top3_shadow",
}
SECRET_KEY_RE = re.compile(
    r"(?i)(authorization|api[_-]?(?:key|secret)|secret|token|cookie|pass(?:word|phrase)|private[_-]?key|ok-access-(?:key|sign|passphrase))"
)
ASSIGNMENT_RE = re.compile(
    r"(?i)(authorization|api[_-]?(?:key|secret)|secret|token|cookie|pass(?:word|phrase)|private[_-]?key|ok-access-(?:key|sign|passphrase))"
    r"([\"']?\s*[:=]\s*[\"']?)([^\"'\s,;#}\]]+)"
)
AUTH_RE = re.compile(r"(?i)(authorization\s*[:=]\s*)(bearer|basic)\s+[^,\s;#}]+")
UNREDACTED_SECRET_RE = re.compile(
    r"(?i)(api[_-]?secret|passphrase|password|authorization|cookie|token)([\"']?\s*[:=]\s*[\"']?)"
    r"(?!<REDACTED>|REDACTED|null|none|false|true|0\b)[^\"'\s,;#}\]]+"
)
DUST_TERMS = ("dust", "anti_chase", "anti-chase", "anti chase")
PROBE_TERMS = ("probe", "candidate", "event_candidate")
PROBE_TYPES = ("market_impulse_probe", "btc_leadership_probe")
PROBE_EXIT_REASONS = {
    "probe_take_profit",
    "probe_stop_loss",
    "probe_trailing_stop",
    "probe_time_stop",
    "market_impulse_probe_time_stop",
}
CANDIDATE_SNAPSHOT_FIELDS = (
    "candidate_id",
    "run_id",
    "ts_utc",
    "symbol",
    "regime_state",
    "risk_level",
    "current_position",
    "current_weight",
    "target_weight_raw",
    "target_weight_after_risk",
    "final_score",
    "rank",
    "f1_mom_5d",
    "f2_mom_20d",
    "f3_vol_adj_ret",
    "f4_volume_expansion",
    "f5_rsi_trend_confirm",
    "alpha6_score",
    "alpha6_side",
    "ml_score",
    "mean_reversion_score",
    "expected_edge_bps",
    "expected_edge_source",
    "required_edge_bps",
    "cost_bps",
    "selected_total_cost_bps",
    "cost_source",
    "cost_source_quality",
    "degraded_cost_model",
    "candidate_cost_trusted",
    "cost_resolution_reason",
    "cost_model_version",
    "cost_gate_verified",
    "would_block_by_cost",
    "cost_reason",
    "eligible_before_filters",
    "final_decision",
    "block_reason",
    "no_signal_reason",
    "strategy_candidate",
)
ORDER_LIFECYCLE_FIELDS = (
    "schema_version",
    "lifecycle_id",
    "run_id",
    "ts_utc",
    "symbol",
    "normalized_symbol",
    "side",
    "intent",
    "order_state",
    "decision_ts",
    "signal_price",
    "arrival_bid",
    "arrival_ask",
    "arrival_mid",
    "spread_bps_at_decision",
    "submit_ts",
    "order_type",
    "order_px",
    "cl_ord_id",
    "exchange_order_id",
    "first_fill_ts",
    "last_fill_ts",
    "fill_px",
    "avg_fill_px",
    "filled_qty",
    "fee",
    "fee_ccy",
    "fee_usdt",
    "notional_usdt",
    "requested_notional_usdt",
    "trade_ids",
    "fill_count",
)
SOURCE_SNAPSHOT_PATHS = (
    "main.py",
    "event_driven_check.py",
    "src",
    "scripts",
    "configs",
    "pyproject.toml",
    "requirements.txt",
    "requirements-research.txt",
    "requirements-lock.txt",
    "requirements.lock",
    "poetry.lock",
    "uv.lock",
)
STRATEGY_SNAPSHOT_PATHS = (
    "main.py",
    "src/core/pipeline.py",
    "src/strategy",
    "src/alpha",
    "src/factors",
    "src/risk",
)
DEPENDENCY_LOCK_PATHS = (
    "requirements.txt",
    "requirements-research.txt",
    "requirements-lock.txt",
    "requirements.lock",
    "pyproject.toml",
    "poetry.lock",
    "uv.lock",
    "Pipfile.lock",
    "conda-lock.yml",
    "environment.yml",
)
DEPLOYMENT_VERSION_PATHS = (
    "deployment_version",
    "deployment_version.txt",
    "DEPLOYMENT_VERSION",
    "VERSION",
    ".deployment_version",
)
QUANT_LAB_SCHEMA_VERSION = "1.0.0"
QUANT_LAB_CONTRACT_VERSION = "v5.quant_lab.telemetry.v2"
QUANT_LAB_EVENT_ID_GENERATION_VERSION = "quant_lab_event_id_v1"
TRADE_EXPORT_SCHEMA_VERSION = "v5.trade_export.v1"
SUMMARY_METRICS_VERSION = "v5.summary_metrics.v1"
FLAT_EXIT_SIGNAL_REASONS = PROBE_EXIT_REASONS | {"stop_loss", "atr_trailing"}
BTC_LEADERSHIP_LABELABLE_REASONS = {
    "btc_leadership_probe_alpha6_score_too_low",
    "btc_leadership_probe_no_alpha6_buy",
    "btc_leadership_probe_f5_rsi_too_low",
    "btc_leadership_probe_risk_off",
}
BTC_LEADERSHIP_NOT_OBSERVABLE_REASONS = {
    "btc_leadership_probe_not_flat",
    "btc_leadership_probe_cooldown",
}
PROBE_COUNT_FIELDS = [
    "market_impulse_probe_candidate_count",
    "market_impulse_probe_open_count",
    "market_impulse_probe_blocked_count",
    "market_impulse_probe_quality_filter_block_count",
    "market_impulse_probe_unexecutable_notional_count",
    "btc_leadership_probe_candidate_count",
    "btc_leadership_probe_open_count",
    "btc_leadership_probe_blocked_count",
    "btc_leadership_probe_alpha6_score_too_low_count",
    "btc_leadership_probe_no_alpha6_buy_count",
    "btc_leadership_probe_cooldown_count",
    "btc_leadership_probe_not_flat_count",
    "probe_take_profit_count",
    "probe_stop_loss_count",
    "probe_trailing_stop_count",
    "probe_time_stop_count",
]
BTC_LEADERSHIP_CONFIG_KEYS = [
    "btc_leadership_probe_enabled",
    "btc_leadership_probe_only_in_protect",
    "btc_leadership_probe_target_w",
    "btc_leadership_probe_dynamic_sizing_enabled",
    "btc_leadership_probe_max_target_w",
    "btc_leadership_probe_cooldown_hours",
    "btc_leadership_probe_lookback_hours",
    "btc_leadership_probe_breakout_buffer_bps",
    "btc_leadership_probe_min_alpha6_score",
    "btc_leadership_probe_min_f5_rsi",
    "btc_leadership_probe_min_f4_volume",
    "btc_leadership_probe_require_regime_not_risk_off",
    "btc_leadership_probe_allow_single_negative_cycle_bypass",
    "btc_leadership_probe_max_negative_cycles_to_bypass",
    "btc_leadership_probe_min_net_expectancy_bps_to_bypass",
    "btc_leadership_probe_time_stop_hours",
]
PROBE_EXIT_CONFIG_KEYS = [
    "probe_take_profit_net_bps",
    "probe_stop_loss_net_bps",
    "probe_time_stop_hours",
    "probe_time_stop_min_net_bps",
]
LOG_EXTS = (".log", ".out", ".err")
MAX_COPY_BYTES = 20 * 1024 * 1024
MAX_LOG_BYTES = 5 * 1024 * 1024
RECENT_72H = NOW.timestamp() - 72 * 3600
RECENT_24H = NOW.timestamp() - 24 * 3600
WINDOW_72H_START = NOW - dt.timedelta(hours=72)
WINDOW_72H_END = NOW

missing_paths = []
collection_errors = []
notes = []
commands = []
copied_sources = {}
issues = []


def fail(message, code=2):
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(code)


def record_missing(path):
    if path not in missing_paths:
        missing_paths.append(path)


def add_issue(severity, code, message, evidence=None):
    issue = {
        "severity": severity,
        "code": code,
        "message": message,
        "evidence": evidence or {},
    }
    issues.append(issue)
    return issue


def _command_text(cmd):
    if isinstance(cmd, (list, tuple)):
        return " ".join(shlex.quote(str(part)) for part in cmd)
    return str(cmd)


def run_readonly(cmd):
    commands.append(_command_text(cmd))
    try:
        proc = subprocess.run(
            [str(part) for part in cmd],
            shell=False,
            cwd=str(ROOT) if ROOT.exists() else None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
        )
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except Exception as exc:
        collection_errors.append({"command": cmd, "error": repr(exc)})
        return 999, "", repr(exc)


def sanitize_text(text):
    def repl(match):
        return f"{match.group(1)}{match.group(2)}<REDACTED>"

    redacted = AUTH_RE.sub(lambda m: f"{m.group(1)}{m.group(2)} <REDACTED>", text)
    redacted = ASSIGNMENT_RE.sub(repl, redacted)
    return redacted


def sanitize_obj(obj):
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if SECRET_KEY_RE.search(str(key)):
                result[key] = "<REDACTED>"
            else:
                result[key] = sanitize_obj(value)
        return result
    if isinstance(obj, list):
        return [sanitize_obj(item) for item in obj]
    return obj


def read_text_limited(path, limit):
    size = path.stat().st_size
    with path.open("rb") as fh:
        if size > limit:
            fh.seek(max(0, size - limit))
            data = fh.read()
            prefix = f"[TRUNCATED: last {len(data)} of {size} bytes]\n"
        else:
            data = fh.read()
            prefix = ""
    return prefix + data.decode("utf-8", "replace")


def write_text(dest_rel, text):
    dest = OUT / dest_rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text, encoding="utf-8")
    return dest


def ensure_csv_header(dest_rel, fields):
    dest = OUT / dest_rel
    if dest.is_file():
        return False
    write_text(dest_rel, ",".join(fields) + "\n")
    copied_sources[dest_rel] = "generated empty header"
    notes.append(f"generated empty summary header: {dest_rel}")
    return True


def copy_sanitized(src_rel, dest_rel, required=False, limit=MAX_COPY_BYTES):
    src = ROOT / src_rel
    if not src.is_file():
        record_missing(src_rel)
        if required:
            notes.append(f"required source missing: {src_rel}")
        return False
    try:
        text = read_text_limited(src, limit)
        if src.suffix.lower() == ".json":
            try:
                parsed = json.loads(text)
                text = json.dumps(sanitize_obj(parsed), ensure_ascii=False, indent=2) + "\n"
            except Exception:
                text = sanitize_text(text)
        else:
            text = sanitize_text(text)
        write_text(dest_rel, text)
        copied_sources[dest_rel] = str(src)
        return True
    except Exception as exc:
        collection_errors.append({"source": str(src), "dest": dest_rel, "error": repr(exc)})
        return False


def yaml_section_text_from_raw(section_name):
    path = OUT / "raw" / "config_live_prod.yaml"
    if not path.is_file():
        path = ROOT / "configs" / "live_prod.yaml"
    try:
        text = path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""
    except Exception as exc:
        collection_errors.append({"source": str(path), "error": f"entry_quality_config_read: {exc!r}"})
        return ""
    match = re.search(
        rf"(?ms)^{re.escape(section_name)}:\s*\n(.*?)(?=^[A-Za-z_][A-Za-z0-9_]*:\s*|\Z)",
        text or "",
    )
    return match.group(1) if match else ""


def parse_yaml_scalar(section_text, key, default=""):
    match = re.search(rf"(?m)^\s*{re.escape(key)}\s*:\s*([^#\n]+)", section_text or "")
    if not match:
        return default
    return match.group(1).strip().strip("\"'")


def parse_bool_text(value, default=False):
    text = str(value or "").strip().lower()
    if text in {"true", "yes", "1", "on"}:
        return True
    if text in {"false", "no", "0", "off"}:
        return False
    return default


def quant_lab_token_from_env_file(path_text, token_env):
    if not path_text:
        return ""
    path = Path(str(path_text)).expanduser()
    if not path.is_file():
        return ""
    try:
        for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == token_env:
                return value.strip().strip("\"'")
    except Exception as exc:
        collection_errors.append({"source": str(path), "error": f"entry_quality_api_env_read: {exc!r}"})
    return ""


def normalize_entry_quality_report_text(text, file_kind):
    if file_kind == "json":
        try:
            parsed = json.loads(text)
            return json.dumps(sanitize_obj(parsed), ensure_ascii=False, indent=2) + "\n"
        except Exception:
            return sanitize_text(text)
    return sanitize_text(text)


def csv_text_from_dict_rows(rows):
    dict_rows = [row for row in rows if isinstance(row, dict)]
    if not dict_rows:
        return ""
    fields = []
    seen = set()
    for row in dict_rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    from io import StringIO
    sio = StringIO()
    writer = csv.DictWriter(sio, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(dict_rows)
    return sio.getvalue()


def entry_quality_payload_to_text(payload, filename, file_kind):
    if isinstance(payload, str):
        return payload
    if file_kind == "json":
        if isinstance(payload, dict):
            for key in ("content", "text", "raw", filename):
                value = payload.get(key)
                if isinstance(value, str):
                    return value
            for key in ("data", "payload", "report"):
                value = payload.get(key)
                if isinstance(value, dict):
                    return json.dumps(value, ensure_ascii=False, indent=2) + "\n"
        return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if isinstance(payload, dict):
        for key in ("content", "text", "raw", filename):
            value = payload.get(key)
            if isinstance(value, str):
                return value
        for key in ("rows", "items", "data", "report"):
            value = payload.get(key)
            if isinstance(value, list):
                text = csv_text_from_dict_rows(value)
                if text:
                    return text
    if isinstance(payload, list):
        text = csv_text_from_dict_rows(payload)
        if text:
            return text
    return ""


def entry_quality_archive_paths():
    paths = []
    seen = set()
    for raw in ENTRY_QUALITY_SOURCE_ARCHIVES:
        base = Path(raw)
        if not base.is_absolute():
            base = ROOT / base
        matches = sorted(base.parent.glob(base.name)) if any(ch in base.name for ch in "*?[") else [base]
        for path in matches:
            if not path.is_file():
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            paths.append(path)
    paths.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    return paths


def read_entry_quality_from_archive(archive_path, filename):
    candidates = []
    try:
        if archive_path.suffix.lower() == ".zip":
            import zipfile
            with zipfile.ZipFile(archive_path) as zf:
                for name in zf.namelist():
                    normalized = name.replace("\\", "/")
                    if normalized.endswith(f"/{filename}") or normalized == filename:
                        score = 0
                        if normalized.endswith(f"reports/{filename}"):
                            score = 3
                        elif "/reports/" in normalized:
                            score = 2
                        elif "/entry_quality/" in normalized:
                            score = 1
                        candidates.append((score, normalized))
                if not candidates:
                    return ""
                _, member = sorted(candidates, reverse=True)[0]
                data = zf.read(member)[:MAX_COPY_BYTES]
                return data.decode("utf-8", "replace")
        with tarfile.open(archive_path) as tf:
            for member in tf.getmembers():
                normalized = member.name.replace("\\", "/")
                if not member.isfile():
                    continue
                if normalized.endswith(f"/{filename}") or normalized == filename:
                    score = 0
                    if normalized.endswith(f"reports/{filename}"):
                        score = 3
                    elif "/reports/" in normalized:
                        score = 2
                    elif "/entry_quality/" in normalized:
                        score = 1
                    candidates.append((score, normalized, member))
            if not candidates:
                return ""
            _, _, member = sorted(candidates, key=lambda item: (item[0], item[1]), reverse=True)[0]
            fh = tf.extractfile(member)
            if fh is None:
                return ""
            data = fh.read(MAX_COPY_BYTES)
            return data.decode("utf-8", "replace")
    except Exception as exc:
        collection_errors.append({"source": str(archive_path), "filename": filename, "error": f"entry_quality_archive_read: {exc!r}"})
    return ""


def fetch_entry_quality_report_from_api(filename, file_kind):
    quant_lab_section = yaml_section_text_from_raw("quant_lab")
    if not parse_bool_text(parse_yaml_scalar(quant_lab_section, "enabled", "false"), False):
        return "", ""
    base_url = parse_yaml_scalar(quant_lab_section, "base_url", "http://qyun2.hrhome.top:8027").rstrip("/")
    token_env = parse_yaml_scalar(quant_lab_section, "api_token_env", "QUANT_LAB_API_TOKEN") or "QUANT_LAB_API_TOKEN"
    token = os.environ.get(token_env, "").strip()
    if not token:
        token = quant_lab_token_from_env_file(parse_yaml_scalar(quant_lab_section, "api_env_path", ""), token_env)
    if not token:
        collection_errors.append({"source": f"entry_quality_report_api:{filename}", "error": "token_missing"})
        return "", ""
    quoted = urllib.parse.quote(filename)
    errors = []
    for template in ENTRY_QUALITY_REPORT_API_ENDPOINT_TEMPLATES:
        endpoint = template.format(filename=quoted)
        url = f"{base_url}{endpoint}"
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{urllib.parse.urlencode({'format': 'raw'})}"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "text/csv,application/json,text/markdown,text/plain,*/*",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                raw = response.read(MAX_COPY_BYTES)
                content_type = str(response.headers.get("content-type") or "").lower()
            text = raw.decode("utf-8", "replace")
            if "json" in content_type:
                try:
                    converted = entry_quality_payload_to_text(json.loads(text), filename, file_kind)
                    if converted:
                        return converted, f"api:{endpoint}"
                except Exception:
                    pass
            if text.strip():
                return text, f"api:{endpoint}"
        except Exception as exc:
            errors.append(f"{endpoint}: {type(exc).__name__}")
    if errors:
        collection_errors.append({"source": f"entry_quality_report_api:{filename}", "error": "; ".join(errors[:5])})
    return "", ""


def copy_entry_quality_report(filename, dest_rel, file_kind):
    for source_dir in ENTRY_QUALITY_SOURCE_DIRS:
        base = Path(source_dir)
        if not base.is_absolute():
            base = ROOT / base
        src = base / filename
        if not src.is_file():
            continue
        try:
            text = normalize_entry_quality_report_text(read_text_limited(src, MAX_COPY_BYTES), file_kind)
            write_text(dest_rel, text)
            copied_sources[dest_rel] = str(src)
            return True
        except Exception as exc:
            collection_errors.append({"source": str(src), "dest": dest_rel, "error": repr(exc)})
    for archive_path in entry_quality_archive_paths():
        text = read_entry_quality_from_archive(archive_path, filename)
        if not text:
            continue
        text = normalize_entry_quality_report_text(text, file_kind)
        write_text(dest_rel, text)
        copied_sources[dest_rel] = f"{archive_path}:{filename}"
        return True
    text, source = fetch_entry_quality_report_from_api(filename, file_kind)
    if text:
        text = normalize_entry_quality_report_text(text, file_kind)
        write_text(dest_rel, text)
        copied_sources[dest_rel] = source
        return True
    return False


def parse_run_time(run_name):
    for fmt in ("%Y%m%d_%H", "%Y%m%dT%H%M%SZ", "%Y%m%d_%H%M%S"):
        try:
            return dt.datetime.strptime(run_name, fmt).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            pass
    return None


def is_recent_run_dir(path, cutoff_ts):
    parsed = parse_run_time(path.name)
    if parsed is not None:
        return parsed.timestamp() >= cutoff_ts
    for fname in RUN_FILES:
        f = path / fname
        if f.is_file() and f.stat().st_mtime >= cutoff_ts:
            return True
    return False


def find_run_root():
    candidates = [
        ROOT / "reports" / "runs" / "prod",
        ROOT / "reports" / "runs",
        ROOT / "runs" / "prod",
        ROOT / "runs",
        ROOT / "reports" / "prod",
    ]
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return None


def copy_recent_runs():
    run_root = find_run_root()
    copied = []
    recent_24_decisions = []
    if run_root is None:
        record_missing("reports/runs/prod or reports/runs")
        return copied, recent_24_decisions

    notes.append(f"run_root={run_root}")
    run_dirs = sorted([p for p in run_root.iterdir() if p.is_dir()], key=lambda p: p.name)
    selected = [p for p in run_dirs if is_recent_run_dir(p, RECENT_72H)]
    if not selected and run_dirs:
        selected = run_dirs[-96:]
        notes.append("no run dirs matched last72h; fallback to latest 96 run directories by name")

    for run_dir in selected:
        run_dest = f"raw/recent_runs/{run_dir.name}"
        copied_any = False
        for fname in RUN_FILES:
            src = run_dir / fname
            if not src.is_file():
                continue
            dest_rel = f"{run_dest}/{fname}"
            try:
                text = read_text_limited(src, MAX_COPY_BYTES)
                if fname.endswith(".json"):
                    try:
                        parsed = json.loads(text)
                        text = json.dumps(sanitize_obj(parsed), ensure_ascii=False, indent=2) + "\n"
                    except Exception:
                        text = sanitize_text(text)
                else:
                    text = sanitize_text(text)
                write_text(dest_rel, text)
                copied_sources[dest_rel] = str(src)
                copied_any = True
                if fname == "decision_audit.json" and is_recent_run_dir(run_dir, RECENT_24H):
                    recent_24_decisions.append(dest_rel)
            except Exception as exc:
                collection_errors.append({"source": str(src), "dest": dest_rel, "error": repr(exc)})
        if copied_any:
            copied.append(run_dir.name)
    if not copied:
        record_missing("reports/runs/prod/* last72h lightweight files")
    return copied, recent_24_decisions


def copy_current_reports():
    for src_rel, dest_rel, required in CURRENT_REPORT_FILES:
        copy_sanitized(src_rel, dest_rel, required=required)
    for src_rel, dest_rel in (
        ("reports/strategy_opportunity_advisory.csv", "raw/reports/strategy_opportunity_advisory.csv"),
        ("reports/quant_lab/strategy_opportunity_advisory.csv", "raw/reports/quant_lab/strategy_opportunity_advisory.csv"),
        ("reports/quant_lab_latest/strategy_opportunity_advisory.csv", "raw/reports/quant_lab_latest/strategy_opportunity_advisory.csv"),
        ("reports/quant_lab/latest/reports/strategy_opportunity_advisory.csv", "raw/reports/quant_lab/latest/reports/strategy_opportunity_advisory.csv"),
        ("reports/risk_on_multi_buy_shadow.csv", "raw/reports/risk_on_multi_buy_shadow.csv"),
        ("reports/summaries/strategy_opportunity_advisory_reader.csv", "summaries/strategy_opportunity_advisory_reader.csv"),
    ):
        if (ROOT / src_rel).is_file():
            copy_sanitized(src_rel, dest_rel)
    for filename, dest_rel, file_kind in ENTRY_QUALITY_REPORT_FILES:
        if not copy_entry_quality_report(filename, dest_rel, file_kind):
            collection_errors.append({
                "source": f"entry_quality_report:{filename}",
                "dest": dest_rel,
                "error": "missing_from_local_archive_and_api",
            })
    for dest_rel in ("raw/reports/quant_lab_usage.jsonl", "raw/reports/quant_lab_requests.jsonl"):
        dest = OUT / dest_rel
        if not dest.is_file():
            write_text(dest_rel, "")
    for src_rel, dest_rel, fields in (
        (
            "reports/summaries/expanded_universe_advisory_reader.csv",
            "summaries/expanded_universe_advisory_reader.csv",
            EXPANDED_UNIVERSE_ADVISORY_FIELDS,
        ),
        (
            "reports/summaries/strategy_opportunity_advisory_source_health.csv",
            "summaries/strategy_opportunity_advisory_source_health.csv",
            STRATEGY_ADVISORY_SOURCE_HEALTH_FIELDS,
        ),
        (
            "reports/summaries/expanded_universe_paper_runs.csv",
            "summaries/expanded_universe_paper_runs.csv",
            EXPANDED_UNIVERSE_PAPER_RUN_FIELDS,
        ),
        (
            "reports/summaries/alpha_factory_advisory_reader.csv",
            "summaries/alpha_factory_advisory_reader.csv",
            ALPHA_FACTORY_ADVISORY_FIELDS,
        ),
        (
            "reports/summaries/alpha_factory_family_summary.csv",
            "summaries/alpha_factory_family_summary.csv",
            ALPHA_FACTORY_FAMILY_SUMMARY_FIELDS,
        ),
        (
            "reports/summaries/risk_on_multi_buy_shadow.csv",
            "summaries/risk_on_multi_buy_shadow.csv",
            RISK_ON_MULTI_BUY_SHADOW_FIELDS,
        ),
    ):
        if (ROOT / src_rel).is_file():
            copy_sanitized(src_rel, dest_rel)
        else:
            ensure_csv_header(dest_rel, fields)

    matched = False
    for base in (ROOT / "reports", ROOT / "reports" / "summaries"):
        if not base.is_dir():
            continue
        for src in sorted(base.glob("skipped_candidate_outcomes*.csv")):
            rel = src.relative_to(ROOT).as_posix()
            dest = f"raw/reports/{src.relative_to(ROOT / 'reports').as_posix()}"
            copy_sanitized(rel, dest)
            matched = True
    if not matched:
        record_missing("reports/skipped_candidate_outcomes*.csv")


def merge_candidate_snapshot_reports():
    paths = []
    aggregate = OUT / "raw" / "reports" / "candidate_snapshot.csv"
    if aggregate.is_file():
        paths.append(aggregate)
    paths.extend(sorted((OUT / "raw" / "recent_runs").glob("*/candidate_snapshot.csv")))
    rows = []
    fields = list(CANDIDATE_SNAPSHOT_FIELDS)
    seen = set()
    for path in paths:
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for field in reader.fieldnames or []:
                    if field not in fields:
                        fields.append(field)
                for row in reader:
                    if not row:
                        continue
                    run_id = flatten_value(row.get("run_id") or path.parent.name)
                    row["run_id"] = run_id
                    candidate_id = flatten_value(row.get("candidate_id"))
                    symbol = flatten_value(row.get("symbol"))
                    strategy = flatten_value(row.get("strategy_candidate"))
                    key = (candidate_id, run_id, symbol, strategy)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(row)
        except Exception as exc:
            collection_errors.append({"source": str(path), "dest": "raw/reports/candidate_snapshot.csv", "error": repr(exc)})
    for dest_rel in ("raw/reports/candidate_snapshot.csv", "summaries/candidate_snapshot.csv"):
        dest = OUT / dest_rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in fields})
    copied_sources["raw/reports/candidate_snapshot.csv"] = "merged reports/candidate_snapshot.csv + raw/recent_runs/*/candidate_snapshot.csv"
    copied_sources["summaries/candidate_snapshot.csv"] = copied_sources["raw/reports/candidate_snapshot.csv"]
    notes.append(f"merged candidate_snapshot rows={len(rows)} from sources={len(paths)}")
    return len(rows)


def read_candidate_snapshot_summary_rows():
    path = OUT / "summaries" / "candidate_snapshot.csv"
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            return [dict(row) for row in csv.DictReader(fh) if row]
    except Exception as exc:
        collection_errors.append({"source": str(path), "error": f"candidate_snapshot_summary_load: {exc!r}"})
        return []


def candidate_cost_source_coverage(rows):
    if not rows:
        return 0.0
    filled = [
        row for row in rows
        if str(row.get("cost_source") or "").strip().lower() not in {"", "null", "not_observable"}
    ]
    return len(filled) / len(rows)


def copy_logs():
    log_roots = [ROOT / "logs", ROOT / "reports", ROOT / "runtime", ROOT]
    seen = set()
    copied = []
    for base in log_roots:
        if not base.is_dir():
            continue
        for current, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d not in {".git", ".venv", "node_modules", "__pycache__"}]
            depth = len(Path(current).relative_to(base).parts)
            if depth > 2:
                dirs[:] = []
            for name in files:
                lower = name.lower()
                if not lower.endswith(LOG_EXTS):
                    continue
                src = Path(current) / name
                key = str(src.resolve())
                if key in seen:
                    continue
                seen.add(key)
                try:
                    if src.stat().st_mtime < RECENT_72H and name != "v5_runtime.log":
                        continue
                    rel = src.relative_to(ROOT).as_posix()
                    safe_rel = rel.replace("/", "__")
                    text = sanitize_text(read_text_limited(src, MAX_LOG_BYTES))
                    write_text(f"raw/logs/{safe_rel}", text)
                    copied_sources[f"raw/logs/{safe_rel}"] = str(src)
                    copied.append(rel)
                except Exception as exc:
                    collection_errors.append({"source": str(src), "dest": "raw/logs", "error": repr(exc)})
    if not copied:
        record_missing("logs/*.log last72h")
    return copied


def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception as exc:
        collection_errors.append({"source": str(path), "error": f"json_load: {exc!r}"})
        return None


def load_jsonl(path):
    rows = []
    if not path.is_file():
        return rows
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for line_no, line in enumerate(fh, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    item = json.loads(text)
                    if isinstance(item, dict):
                        rows.append(item)
                except Exception as exc:
                    collection_errors.append({"source": str(path), "line": line_no, "error": f"jsonl_load: {exc!r}"})
    except Exception as exc:
        collection_errors.append({"source": str(path), "error": f"jsonl_open: {exc!r}"})
    return rows


def load_csv_dicts(path):
    rows = []
    if not path.is_file():
        return rows
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if row:
                    rows.append(dict(row))
    except Exception as exc:
        collection_errors.append({"source": str(path), "error": f"csv_load: {exc!r}"})
    return rows


def truthy_text(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def flatten_value(value):
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def iter_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from iter_dicts(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_dicts(item)


def contains_term(obj, terms):
    text = json.dumps(obj, ensure_ascii=False, sort_keys=True).lower()
    return any(term in text for term in terms)


def write_csv(path, rows, fieldnames):
    dest = OUT / path
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_summaries(copied_runs, copied_logs, recent_24_decisions, provenance_meta):
    not_obs = "not_observable"
    candidate_snapshot_rows = read_candidate_snapshot_summary_rows()
    candidate_cost_source_coverage_value = candidate_cost_source_coverage(candidate_snapshot_rows)

    def as_float(value):
        if value in (None, "", not_obs):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def as_int(value):
        if value in (None, "", not_obs):
            return 0
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0

    def first_value(obj, names, default=not_obs):
        if not isinstance(obj, dict):
            return default
        for name in names:
            if name in obj and obj[name] not in (None, ""):
                return obj[name]
        return default

    def safe_json(obj):
        return json.dumps(sanitize_obj(obj), ensure_ascii=False, sort_keys=True)

    def run_ts(run_id, audit=None):
        if isinstance(audit, dict):
            ts = first_value(audit, ("timestamp", "ts", "generated_at", "as_of"), "")
            if ts:
                return flatten_value(ts)
            for key in ("now_ts", "window_end_ts"):
                if audit.get(key):
                    try:
                        return dt.datetime.fromtimestamp(float(audit[key]), tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        pass
        parsed = parse_run_time(run_id)
        return parsed.strftime("%Y-%m-%dT%H:%M:%SZ") if parsed else not_obs

    def probe_type_of(obj):
        if not isinstance(obj, dict):
            return not_obs
        for key in ("probe_type", "entry_reason"):
            val = obj.get(key)
            if val in PROBE_TYPES:
                return val
        if obj.get("btc_leadership_probe") is True:
            return "btc_leadership_probe"
        if obj.get("market_impulse_probe") is True:
            return "market_impulse_probe"
        text = safe_json(obj)
        if "btc_leadership_probe" in text:
            return "btc_leadership_probe"
        if "market_impulse_probe" in text:
            return "market_impulse_probe"
        return not_obs

    def state_map(name):
        path = OUT / "raw" / "state" / name
        if not path.is_file():
            return {}
        data = load_json(path)
        return data if isinstance(data, dict) else {}

    def state_present(state, symbol):
        if not symbol or symbol == not_obs or not isinstance(state, dict):
            return False
        if symbol in state:
            return True
        return any(isinstance(v, dict) and v.get("symbol") == symbol for v in state.values())

    def state_entry(state, symbol):
        if not symbol or symbol == not_obs or not isinstance(state, dict):
            return None
        if isinstance(state.get(symbol), dict):
            return state.get(symbol)
        for value in state.values():
            if isinstance(value, dict) and value.get("symbol") == symbol:
                return value
        return None

    def all_dicts(obj):
        if isinstance(obj, dict):
            yield obj
            for value in obj.values():
                yield from all_dicts(value)
        elif isinstance(obj, list):
            for item in obj:
                yield from all_dicts(item)

    def collect_config_keys_from_json(obj, prefix=""):
        keys = set()
        if isinstance(obj, dict):
            for key, value in obj.items():
                keys.add(str(key))
                keys |= collect_config_keys_from_json(value, f"{prefix}.{key}" if prefix else str(key))
        elif isinstance(obj, list):
            for item in obj:
                keys |= collect_config_keys_from_json(item, prefix)
        return keys

    live_config_text = (OUT / "raw" / "config_live_prod.yaml").read_text(encoding="utf-8", errors="replace") if (OUT / "raw" / "config_live_prod.yaml").is_file() else ""
    effective_config_path = OUT / "raw" / "reports" / "effective_live_config.json"
    effective_data = load_json(effective_config_path) if effective_config_path.is_file() else {}
    effective_keys = collect_config_keys_from_json(effective_data)

    def find_numeric_config_value(obj, key):
        if isinstance(obj, dict):
            if key in obj:
                number = as_float(obj.get(key))
                if number is not None:
                    return number
            for value in obj.values():
                number = find_numeric_config_value(value, key)
                if number is not None:
                    return number
        elif isinstance(obj, list):
            for value in obj:
                number = find_numeric_config_value(value, key)
                if number is not None:
                    return number
        return None

    def parse_live_config_number(text, key):
        match = re.search(rf"(?m)^\s*{re.escape(key)}\s*:\s*([-+]?\d+(?:\.\d+)?)\s*(?:#.*)?$", text)
        return as_float(match.group(1)) if match else None

    def config_number(key):
        value = find_numeric_config_value(effective_data, key)
        if value is not None:
            return value
        return parse_live_config_number(live_config_text, key)

    def find_config_value(obj, key):
        if isinstance(obj, dict):
            if key in obj:
                return obj.get(key)
            for value in obj.values():
                found = find_config_value(value, key)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for value in obj:
                found = find_config_value(value, key)
                if found is not None:
                    return found
        return None

    def parse_live_config_bool(text, key):
        match = re.search(rf"(?m)^\s*{re.escape(key)}\s*:\s*(true|false|yes|no|1|0)\s*(?:#.*)?$", text or "", re.IGNORECASE)
        if not match:
            return None
        return match.group(1).strip().lower() in {"true", "yes", "1"}

    def config_bool(key, default=False):
        value = find_config_value(effective_data, key)
        if isinstance(value, bool):
            return value
        if value is not None:
            text = str(value).strip().lower()
            if text in {"true", "yes", "1"}:
                return True
            if text in {"false", "no", "0"}:
                return False
        parsed = parse_live_config_bool(live_config_text, key)
        return default if parsed is None else parsed

    def collect_config_keys_from_yaml_text(text):
        if not text:
            return set()
        return set(re.findall(r"(?m)^\s*(?!#)([A-Za-z_][A-Za-z0-9_]*)\s*:", text))

    live_config_keys = collect_config_keys_from_yaml_text(live_config_text)

    def live_section_text(section_name):
        match = re.search(
            rf"(?ms)^{re.escape(section_name)}:\s*\n(.*?)(?=^[A-Za-z_][A-Za-z0-9_]*:\s*|\Z)",
            live_config_text or "",
        )
        return match.group(1) if match else ""

    top_level_quant_lab_authoritative = bool(
        re.search(r"(?m)^\s{2}enabled\s*:\s*true\s*(?:#.*)?$", live_section_text("quant_lab"), re.IGNORECASE)
    )

    DEFAULT_LABEL_HORIZONS = [4, 8, 12, 24, 48, 72, 120]
    LEGACY_LABEL_HORIZONS = [4, 8, 12, 24]

    def normalize_horizon_list(raw, fallback):
        out = []
        seen = set()
        for item in raw or []:
            value = as_int(item)
            if value <= 0 or value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out or list(fallback)

    def find_list_config_value(obj, key):
        if isinstance(obj, dict):
            if key in obj and isinstance(obj.get(key), list):
                return obj.get(key)
            for value in obj.values():
                found = find_list_config_value(value, key)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for value in obj:
                found = find_list_config_value(value, key)
                if found is not None:
                    return found
        return None

    def parse_live_config_int_list(text, key):
        match = re.search(rf"(?m)^\s*{re.escape(key)}\s*:\s*\[([^\]]*)\]\s*(?:#.*)?$", text or "")
        if not match:
            return None
        values = []
        for part in match.group(1).split(","):
            value = as_int(part.strip())
            if value > 0:
                values.append(value)
        return values or None

    def config_int_list(key):
        return find_list_config_value(effective_data, key) or parse_live_config_int_list(live_config_text, key)

    def parse_live_config_string_list(text, key):
        match = re.search(rf"(?m)^\s*{re.escape(key)}\s*:\s*\[([^\]]*)\]\s*(?:#.*)?$", text or "")
        if not match:
            return None
        values = []
        for part in match.group(1).split(","):
            value = part.strip().strip("\"'")
            if value:
                values.append(value)
        return values or None

    def config_string_list(key, fallback):
        value = find_list_config_value(effective_data, key)
        if isinstance(value, list):
            return value
        return parse_live_config_string_list(live_config_text, key) or list(fallback)

    legacy_label_horizons = normalize_horizon_list(config_int_list("skipped_candidate_horizons_hours"), LEGACY_LABEL_HORIZONS)
    label_horizons = (
        legacy_label_horizons
        if legacy_label_horizons != LEGACY_LABEL_HORIZONS
        else normalize_horizon_list(config_int_list("extended_label_horizons_hours"), DEFAULT_LABEL_HORIZONS)
    )
    protect_sol_exception_horizons = normalize_horizon_list(
        config_int_list("protect_sol_exception_horizons_hours"),
        [4, 8, 12, 24, 48, 72],
    )

    CONFIG_CONSUMPTION_FIXED_KEYS = {
        "split_orders",
        "split_interval_sec",
        "market_impulse_probe_time_stop_hours",
        "probe_exit_enabled",
    }
    INTENTIONALLY_INACTIVE_CONFIG_KEYS = {
        "split_orders",
        "split_interval_sec",
    }
    LEGACY_EXECUTION_QUANT_LAB_KEYS = {
        "quant_lab_enabled",
        "quant_lab_base_url",
        "quant_lab_timeout_sec",
        "quant_lab_fail_policy",
        "quant_lab_token_env",
        "quant_lab_default_alpha_id",
        "quant_lab_strategy",
        "quant_lab_strategy_version",
        "quant_lab_cost_regime_default",
        "quant_lab_cost_quantile",
        "quant_lab_gate_check_enabled",
        "quant_lab_health_check_enabled",
        "quant_lab_usage_path",
        "quant_lab_requests_path",
    }
    CONFIG_CONSUMPTION_FIXED_KEYS |= LEGACY_EXECUTION_QUANT_LAB_KEYS
    CONFIG_CONSUMPTION_PREFIXES = (
        "btc_leadership_probe_",
        "market_impulse_probe_",
        "swing_",
        "protect_recovery_",
        "protect_negative_expectancy_short_cycle_",
        "protect_alt_short_cycle_",
        "open_long_entry_guard_fail_open_",
        "multi_position_swing_shadow_",
        "alt_impulse_shadow_",
        "paper_strategy_",
        "protect_profit_lock_",
        "same_symbol_reentry_",
        "swing_min_hold_",
    )
    CONFIG_CONSUMPTION_DIAGNOSTICS_PREFIXES = (
        "multi_position_swing_shadow_",
        "alt_impulse_shadow_",
        "protect_sol_exception_",
        "paper_strategy_",
        "swing_atr_soft_exit_shadow_",
    )

    def key_pattern(key):
        return re.compile(rf"(?<![A-Za-z0-9_]){re.escape(key)}(?![A-Za-z0-9_])")

    def discover_audited_config_keys(text):
        found = {key for key in CONFIG_CONSUMPTION_FIXED_KEYS if key_pattern(key).search(text or "")}
        for prefix in CONFIG_CONSUMPTION_PREFIXES:
            found.update(re.findall(rf"(?<![A-Za-z0-9_]){re.escape(prefix)}[A-Za-z0-9_]+", text or ""))
        return found

    def read_source_file(path):
        try:
            if path.stat().st_size > 2 * 1024 * 1024:
                return ""
            return path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            collection_errors.append({"source": str(path), "error": f"config_consumption_scan: {exc!r}"})
            return ""

    def schema_source_texts():
        candidates = []
        configs_dir = ROOT / "configs"
        if configs_dir.is_dir():
            candidates.extend(sorted(configs_dir.glob("**/*schema*.py")))
            schema_py = configs_dir / "schema.py"
            if schema_py.is_file():
                candidates.append(schema_py)
        seen = set()
        texts = {}
        for path in candidates:
            try:
                resolved = path.resolve()
            except Exception:
                resolved = path
            if resolved in seen or not path.is_file():
                continue
            seen.add(resolved)
            texts[path.relative_to(ROOT).as_posix()] = read_source_file(path)
        return texts

    def runtime_source_texts():
        texts = {}
        candidates = []
        main_py = ROOT / "main.py"
        if main_py.is_file():
            candidates.append(main_py)
        scan_dirs = [
            ROOT / "src" / "core",
            ROOT / "src" / "reporting",
            ROOT / "src" / "execution",
            ROOT / "src" / "risk",
            ROOT / "src" / "backtest",
        ]
        for scan_dir in scan_dirs:
            if scan_dir.is_dir():
                candidates.extend(sorted(scan_dir.rglob("*.py")))
        bundle_script = ROOT / "scripts" / "generate_v5_bundle_remote.sh"
        if bundle_script.is_file():
            candidates.append(bundle_script)
        seen = set()
        excluded_parts = {"__pycache__", "research"}
        for path in candidates:
            try:
                rel = path.relative_to(ROOT)
            except ValueError:
                continue
            if any(part in excluded_parts for part in rel.parts):
                continue
            try:
                resolved = path.resolve()
            except Exception:
                resolved = path
            if resolved in seen or not path.is_file():
                continue
            seen.add(resolved)
            texts[rel.as_posix()] = read_source_file(path)
        return texts

    def config_consumer_file_category(rel):
        rel = str(rel)
        if rel == "main.py" or rel.startswith("src/core/") or rel.startswith("src/execution/") or rel.startswith("src/risk/"):
            return "live_runtime"
        if rel.startswith("src/reporting/") or rel.startswith("src/backtest/") or rel == "scripts/generate_v5_bundle_remote.sh":
            return "diagnostics"
        return "diagnostics"

    def config_key_is_diagnostics(key):
        return any(str(key).startswith(prefix) for prefix in CONFIG_CONSUMPTION_DIAGNOSTICS_PREFIXES)

    def build_config_runtime_consumption_audit():
        schema_texts = schema_source_texts()
        runtime_texts = runtime_source_texts()
        schema_keys = set()
        for text in schema_texts.values():
            schema_keys |= discover_audited_config_keys(text)

        # Broad prefixes such as swing_ also match runtime counters and local variables.
        # Keep the audit focused on config-shaped keys declared in live/effective/schema.
        prefix_keys = {
            key for key in (live_config_keys | effective_keys | schema_keys)
            if any(str(key).startswith(prefix) for prefix in CONFIG_CONSUMPTION_PREFIXES)
        }
        candidate_keys = sorted(CONFIG_CONSUMPTION_FIXED_KEYS | prefix_keys)
        rows = []
        for key in candidate_keys:
            pattern = key_pattern(key)
            defined = key in schema_keys or any(pattern.search(text) for text in schema_texts.values())
            present_live = key in live_config_keys
            present_effective = key in effective_keys
            matched_consumer_files = [
                rel for rel, text in runtime_texts.items()
                if pattern.search(text)
            ]
            live_consumer_files = [
                rel for rel in matched_consumer_files
                if config_consumer_file_category(rel) == "live_runtime"
            ]
            diagnostics_consumer_files = [
                rel for rel in matched_consumer_files
                if config_consumer_file_category(rel) == "diagnostics"
            ]
            diagnostics_key = config_key_is_diagnostics(key)
            intentionally_inactive = key in INTENTIONALLY_INACTIVE_CONFIG_KEYS
            if intentionally_inactive:
                consumer_category = "intentionally_inactive"
                consumer_files = []
            elif live_consumer_files:
                consumer_category = "live_runtime"
                consumer_files = live_consumer_files
            elif diagnostics_key and diagnostics_consumer_files:
                consumer_category = "diagnostics"
                consumer_files = diagnostics_consumer_files
            elif defined and not (present_live or present_effective):
                consumer_category = "schema_only"
                consumer_files = []
            else:
                consumer_category = "not_consumed"
                consumer_files = []
            consumed = bool(consumer_files)
            legacy_quant_lab_inactive = bool(key in LEGACY_EXECUTION_QUANT_LAB_KEYS and top_level_quant_lab_authoritative)
            if intentionally_inactive:
                consumer_category = "intentionally_inactive"
                consumer_files = []
                consumed = False
                diagnosis = "intentionally_inactive"
            elif legacy_quant_lab_inactive:
                consumer_category = "legacy_inactive"
                consumer_files = []
                consumed = False
                diagnosis = "legacy_execution_quant_lab_inactive_top_level_authoritative"
            elif (present_live or present_effective) and consumed and consumer_category == "live_runtime":
                diagnosis = "live_runtime_consumed"
            elif (present_live or present_effective) and consumed and consumer_category == "diagnostics":
                diagnosis = "diagnostics_consumed"
            elif present_live and not consumed:
                diagnosis = "configured_not_consumed"
                if not diagnostics_key:
                    add_issue(
                        "low",
                        "config_key_not_consumed",
                        "Config key is present in live_prod.yaml but was not observed in live runtime source consumption paths.",
                        {
                            "config_key": key,
                            "present_in_effective_config": present_effective,
                            "defined_in_schema": bool(defined),
                            "consumer_category": consumer_category,
                        },
                    )
            elif present_effective and not consumed:
                diagnosis = "effective_config_not_consumed"
            elif defined:
                diagnosis = "defined_not_configured"
            elif consumed:
                diagnosis = f"{consumer_category}_consumed_not_configured"
            else:
                diagnosis = "not_observable"
            rows.append({
                "config_key": key,
                "defined_in_schema": str(bool(defined)).lower(),
                "present_in_live_prod": str(bool(present_live)).lower(),
                "present_in_effective_config": str(bool(present_effective)).lower(),
                "consumed_in_runtime_code": str(bool(consumed)).lower(),
                "consumer_category": consumer_category,
                "consumer_files": ";".join(consumer_files) if consumer_files else not_obs,
                "diagnosis": diagnosis,
            })
        return rows

    config_dust_usdt_ignore = config_number("dust_usdt_ignore") or 0.0
    config_min_trade_value_usdt = config_number("min_trade_value_usdt") or 0.0
    global_dust_threshold_usdt = max(config_dust_usdt_ignore, 1.0, 0.1 * config_min_trade_value_usdt)
    FACTOR_KEYS = [
        "f1_mom_5d",
        "f2_mom_20d",
        "f3_vol_adj_ret",
        "f4_volume_expansion",
        "f5_rsi_trend_confirm",
    ]
    FACTOR_ALIASES = {
        "f1_mom_5d": ("f1_mom_5d",),
        "f2_mom_20d": ("f2_mom_20d",),
        "f3_vol_adj_ret": ("f3_vol_adj_ret", "f3_vol_adj_ret_20d"),
        "f4_volume_expansion": ("f4_volume_expansion",),
        "f5_rsi_trend_confirm": ("f5_rsi_trend_confirm",),
    }

    def factor_bucket_value(bucket, factor):
        if not isinstance(bucket, dict):
            return None
        for key in FACTOR_ALIASES.get(factor, (factor,)):
            if key not in bucket:
                continue
            value = as_float(bucket.get(key))
            if value is not None:
                return value
        return None

    def normalize_factor_weights(weights):
        out = {}
        if not isinstance(weights, dict):
            return out
        for factor in FACTOR_KEYS:
            value = factor_bucket_value(weights, factor)
            if value is not None:
                out[factor] = value
        return out

    CONFIG_FACTOR_WEIGHTS = {}
    for factor in FACTOR_KEYS:
        for alias in FACTOR_ALIASES.get(factor, (factor,)):
            value = config_number(alias)
            if value is not None:
                CONFIG_FACTOR_WEIGHTS[factor] = value
                break

    def parse_time_to_hours_ago(value):
        if value in (None, "", not_obs):
            return None
        try:
            if isinstance(value, (int, float)) or str(value).isdigit():
                raw = float(value)
                if raw > 10_000_000_000:
                    raw /= 1000.0
                return max(0.0, (NOW.timestamp() - raw) / 3600.0)
            text = str(value).replace("Z", "+00:00")
            parsed = dt.datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return max(0.0, (NOW - parsed.astimezone(dt.timezone.utc)).total_seconds() / 3600.0)
        except Exception:
            return None

    def parse_dt_utc(value):
        if value in (None, "", not_obs):
            return None
        try:
            text = str(value).strip()
            if not text:
                return None
            if re.fullmatch(r"\d+(?:\.\d+)?", text):
                raw = float(text)
                if raw > 10_000_000_000:
                    raw /= 1000.0
                return dt.datetime.fromtimestamp(raw, tz=dt.timezone.utc)
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = dt.datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc)
        except Exception:
            return None

    def canonical_ts_utc(value):
        parsed = parse_dt_utc(value)
        if parsed:
            return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
        value = flatten_value(value)
        return value if value else not_obs

    def fmt_num(value, digits=10):
        if value is None:
            return not_obs
        try:
            number = float(value)
        except (TypeError, ValueError):
            return not_obs
        text = f"{number:.{digits}f}".rstrip("0").rstrip(".")
        if text in ("", "-0"):
            return "0"
        return text

    def iso_or_not_obs(value):
        parsed = parse_dt_utc(value)
        return parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ").replace(".000000Z", "Z") if parsed else not_obs

    def probe_type_from_reason(reason):
        reason = flatten_value(reason)
        if reason == "market_impulse_probe" or reason.startswith("market_impulse_probe_"):
            return "market_impulse_probe"
        if reason == "btc_leadership_probe" or reason.startswith("btc_leadership_probe_"):
            return "btc_leadership_probe"
        if reason in PROBE_EXIT_REASONS:
            return "probe"
        return not_obs

    def exit_priority_for_reason(reason):
        reason = flatten_value(reason)
        if not reason or reason == not_obs:
            return "unknown"
        hard_exact = {
            "hard_stop_loss",
            "stop_loss",
            "fixed_stop_loss",
            "profit_taking_stop_loss_hit",
            "regime_exit",
            "risk_off",
            "risk_off_forced_close",
            "kill_switch",
            "manual_kill",
            "manual_kill_switch",
            "reconcile_fail",
            "reconcile_failure",
            "exchange_account_anomaly",
            "account_anomaly",
            "exchange_anomaly",
        }
        if reason in hard_exact or reason.startswith(("dynamic_stop_", "hard_stop_", "risk_off_", "reconcile_", "kill_switch_", "exchange_", "account_", "profit_taking_stop_loss_hit")):
            return "hard"
        soft_exact = {"atr_trailing", "profit_take", "take_profit", "soft_stop", "weak_signal_exit", "protect_profit_lock_trailing", "time_stop", "zero_target_close", "normal_zero_target_close", "target_rebalance_sell", "force_close_unscored", "target_churn"}
        if reason in soft_exact or reason.startswith(("profit_taking_", "profit_partial_", "rank_exit_", "weak_signal_", "soft_stop_", "zero_target", "normal_zero_target", "replacement_target")):
            return "soft"
        return "unknown"

    def bool_text(value):
        if isinstance(value, bool):
            return str(value).lower()
        text = flatten_value(value).strip().lower()
        if text in {"true", "1", "yes"}:
            return "true"
        if text in {"false", "0", "no"}:
            return "false"
        return text if text else not_obs

    def observe_symbol_price(symbol, ts_value, px_value, source):
        symbol = flatten_value(symbol)
        px = as_float(px_value)
        ts_dt = parse_dt_utc(ts_value)
        if symbol == not_obs or px is None or ts_dt is None:
            return
        price_observations_by_symbol[symbol].append(
            {
                "ts_dt": ts_dt,
                "ts_utc": ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "price": px,
                "source": source,
            }
        )

    def estimate_held_24h_outcome(symbol, entry_dt, entry_px, actual_gross_bps, actual_net_bps):
        entry_px_f = as_float(entry_px)
        if entry_dt is None or entry_px_f is None or entry_px_f <= 0:
            return ("not_observable_no_entry_time_or_price", None, None, None)
        target_dt = entry_dt + dt.timedelta(hours=24)
        observations = sorted(
            price_observations_by_symbol.get(symbol, []),
            key=lambda item: item["ts_dt"],
        )
        for obs in observations:
            if obs["ts_dt"] < target_dt:
                continue
            lag_hours = (obs["ts_dt"] - target_dt).total_seconds() / 3600.0
            if lag_hours > 2.0:
                break
            gross_bps_24h = (float(obs["price"]) - entry_px_f) / entry_px_f * 10000.0
            cost_bps = 0.0
            actual_gross_f = as_float(actual_gross_bps)
            actual_net_f = as_float(actual_net_bps)
            if actual_gross_f is not None and actual_net_f is not None:
                cost_bps = max(0.0, actual_gross_f - actual_net_f)
            net_bps_24h = gross_bps_24h - cost_bps
            return ("observed_from_recent_run_price", net_bps_24h, gross_bps_24h, obs)
        return ("not_observable_no_24h_price", None, None, None)

    def first_observed(*values):
        for value in values:
            if value not in (None, "", not_obs):
                return flatten_value(value)
        return not_obs

    def bool_observed(value):
        if value in (None, "", not_obs):
            return not_obs
        if isinstance(value, bool):
            return str(value).lower()
        text = flatten_value(value).strip().lower()
        if text in {"1", "true", "yes", "y"}:
            return "true"
        if text in {"0", "false", "no", "n"}:
            return "false"
        return str(bool(value)).lower()

    def truthy_observed(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = flatten_value(value).strip().lower()
        return text in {"1", "true", "yes", "y", "ok", "success"}

    def quant_lab_event_kind(row):
        if not isinstance(row, dict):
            return ""
        legacy = flatten_value(row.get("legacy_event_type") or "").strip()
        if legacy:
            return legacy
        event_type = flatten_value(row.get("event_type") or "").strip()
        if event_type == "cost_usage":
            return "cost_estimate"
        if event_type == "permission_audit":
            return flatten_value(row.get("permission_audit_type") or "permission")
        if event_type == "health_check":
            return "health"
        return event_type

    def quant_lab_request_success(row):
        if not isinstance(row, dict):
            return False
        if truthy_observed(row.get("success")) or truthy_observed(row.get("ok")):
            return True
        if row.get("error_type") not in (None, "", not_obs):
            return False
        status = row.get("status_code")
        try:
            return status is not None and 200 <= int(status) < 300
        except (TypeError, ValueError):
            return False

    def quant_lab_is_fallback(row):
        if not isinstance(row, dict):
            return False
        if quant_lab_request_success(row):
            return False
        fallback_reason = flatten_value(row.get("fallback_reason")).strip().lower()
        if fallback_reason == "global_default_cost" and not truthy_observed(row.get("fallback_used")) and quant_lab_event_kind(row) != "fallback":
            return False
        error_text = flatten_value(first_observed(row.get("error_type"), row.get("error"), "")).lower()
        if any(marker in error_text for marker in ("timeout", "connection", "unavailable", "invalid")):
            return True
        return (
            truthy_observed(row.get("fallback_used"))
            or quant_lab_event_kind(row) == "fallback"
            or row.get("fallback_reason") not in (None, "", not_obs)
            or row.get("action_taken") not in (None, "", not_obs)
        )

    def btc_label_key(run_id, ts_utc, symbol, skip_reason):
        return (
            flatten_value(run_id) or not_obs,
            canonical_ts_utc(ts_utc),
            flatten_value(symbol) or not_obs,
            flatten_value(skip_reason) or not_obs,
        )

    def btc_label_key_text(key):
        return "|".join(key)

    def btc_label_row_key(row, fallback=None):
        fallback = fallback or {}
        return btc_label_key(
            first_observed(first_value(row, ("run_id",), not_obs), fallback.get("run_id")),
            first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs), fallback.get("ts_utc")),
            first_observed(first_value(row, ("symbol", "instId"), not_obs), fallback.get("symbol")),
            first_observed(first_value(row, ("skip_reason", "reason", "blocked_reason"), not_obs), fallback.get("skip_reason")),
        )

    def btc_decision_ts_utc(item, audit, audit_ts):
        explicit = first_observed(
            first_value(item, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs),
            first_value(item, ("entry_ts_ms",), not_obs),
        )
        if explicit != not_obs:
            return canonical_ts_utc(explicit)
        for key in ("candidate_ts", "bar_ts", "signal_ts", "window_start_ts"):
            if audit.get(key):
                return canonical_ts_utc(audit.get(key))
        if audit.get("window_end_ts"):
            try:
                raw = float(audit["window_end_ts"])
                if raw > 10_000_000_000:
                    raw /= 1000.0
                return dt.datetime.fromtimestamp(raw - 3600.0, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                pass
        return canonical_ts_utc(audit_ts)

    def status_rank(row):
        status = flatten_value(first_value(row, ("label_status", "label_24h_status"), ""))
        rank = {"complete": 4, "pending": 3, "not_observable": 2}.get(status, 1)
        has_entry = as_float(first_value(row, ("entry_px", "price", "px"), not_obs)) is not None
        return (rank, 1 if has_entry else 0)

    def dedupe_rows_by_key(rows, key_func):
        deduped = {}
        passthrough = []
        duplicate_count = 0
        for row in rows:
            key = key_func(row)
            if not key or any(part == not_obs for part in key):
                passthrough.append(row)
                continue
            existing = deduped.get(key)
            if existing is None:
                deduped[key] = row
                continue
            duplicate_count += 1
            if status_rank(row) > status_rank(existing):
                deduped[key] = row
        return list(deduped.values()) + passthrough, duplicate_count

    def normalize_trade_intent(obj):
        intent = flatten_value(first_value(obj, ("intent", "tradeSide", "business"), "")).upper()
        side = flatten_value(first_value(obj, ("side",), "")).lower()
        if intent in {"OPEN_LONG", "CLOSE_LONG"}:
            return intent
        if side == "buy":
            return "OPEN_LONG"
        if side == "sell":
            return "CLOSE_LONG"
        return intent or not_obs

    def normalize_trade_symbol_for_contract(value):
        text = flatten_value(value).strip().upper()
        if not text or text == not_obs:
            return "null"
        if "/" in text:
            return text.replace("/", "-")
        if "-" in text:
            return text
        if text.endswith("USDT") and len(text) > 4:
            return f"{text[:-4]}-USDT"
        return text

    def csv_null(value):
        if value in (None, "", not_obs):
            return "null"
        return value

    def router_trade_reason(item, intent):
        reason = flatten_value(item.get("reason"))
        source_reason = flatten_value(item.get("source_reason"))
        if intent == "CLOSE_LONG" and source_reason:
            return source_reason
        return reason or source_reason or not_obs

    def router_trade_probe_type(item, reason):
        return first_observed(probe_type_of(item), probe_type_from_reason(reason), probe_type_from_reason(item.get("source_reason")))

    def strategy_signal_lookup_from_audit(audit):
        lookup = defaultdict(dict)
        strategies = audit.get("strategy_signals") if isinstance(audit, dict) else []
        if not isinstance(strategies, list):
            return lookup
        for strategy in strategies:
            if not isinstance(strategy, dict):
                continue
            name = flatten_value(first_value(strategy, ("strategy", "name"), ""))
            if not name:
                continue
            signals = strategy.get("signals") if isinstance(strategy.get("signals"), list) else []
            for signal in signals:
                if not isinstance(signal, dict):
                    continue
                symbol = flatten_value(first_value(signal, ("symbol", "instId"), ""))
                if symbol:
                    lookup[name][symbol] = signal
        return lookup

    def signal_factor_buckets(signal):
        if not isinstance(signal, dict):
            return {}, {}
        metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        raw = metadata.get("raw_factors") if isinstance(metadata.get("raw_factors"), dict) else {}
        z = metadata.get("z_factors") if isinstance(metadata.get("z_factors"), dict) else {}
        return raw, z

    def audit_factor_bucket(audit, bucket_name, symbol):
        containers = [
            audit.get(bucket_name),
            audit.get(f"alpha_{bucket_name}"),
            (audit.get("alpha_snapshot") or {}).get(bucket_name) if isinstance(audit.get("alpha_snapshot"), dict) else None,
            (audit.get("factor_snapshot") or {}).get(bucket_name) if isinstance(audit.get("factor_snapshot"), dict) else None,
        ]
        for container in containers:
            if isinstance(container, dict) and isinstance(container.get(symbol), dict):
                return container.get(symbol)
        return {}

    def top_score_value(row):
        if not isinstance(row, dict):
            return None
        return as_float(first_value(row, ("final_score", "score", "display_score"), not_obs))

    def router_decision_by_symbol(audit):
        out = {}
        decisions = audit.get("router_decisions") if isinstance(audit, dict) else []
        if not isinstance(decisions, list):
            return out
        for item in decisions:
            if not isinstance(item, dict):
                continue
            symbol = flatten_value(first_value(item, ("symbol", "instId"), ""))
            if symbol and symbol not in out:
                out[symbol] = item
        return out

    def target_explain_by_symbol(audit):
        out = {}
        rows = audit.get("target_execution_explain") if isinstance(audit, dict) else []
        if not isinstance(rows, list):
            return out
        for item in rows:
            if not isinstance(item, dict):
                continue
            symbol = flatten_value(first_value(item, ("symbol",), ""))
            if symbol and symbol not in out:
                out[symbol] = item
        return out

    def factor_contribution_base_rows(audit, run_id, audit_ts, audit_regime, audit_level):
        top_scores = audit.get("top_scores") if isinstance(audit.get("top_scores"), list) else []
        targets = audit.get("targets_post_risk") if isinstance(audit.get("targets_post_risk"), dict) else {}
        if not top_scores and not targets:
            return []
        top_map = {}
        ordered_symbols = []
        for idx, row in enumerate(top_scores, start=1):
            if not isinstance(row, dict):
                continue
            symbol = flatten_value(first_value(row, ("symbol", "instId"), ""))
            if not symbol:
                continue
            enriched = dict(row)
            enriched.setdefault("rank", idx)
            top_map[symbol] = enriched
            if symbol not in ordered_symbols:
                ordered_symbols.append(symbol)
        for symbol in targets.keys():
            symbol = flatten_value(symbol)
            if symbol and symbol not in ordered_symbols:
                ordered_symbols.append(symbol)

        signal_lookup = strategy_signal_lookup_from_audit(audit)
        alpha6_lookup = signal_lookup.get("Alpha6Factor") or {}
        explain_map = target_explain_by_symbol(audit)
        router_map = router_decision_by_symbol(audit)
        weights = normalize_factor_weights(audit.get("effective_alpha6_weights")) or dict(CONFIG_FACTOR_WEIGHTS)
        rows = []
        for symbol in ordered_symbols:
            top_row = top_map.get(symbol, {})
            explain = explain_map.get(symbol, {})
            router = router_map.get(symbol, {})
            signal = alpha6_lookup.get(symbol)
            raw_from_signal, z_from_signal = signal_factor_buckets(signal)
            raw_factors = (
                top_row.get("raw_factors") if isinstance(top_row.get("raw_factors"), dict) else {}
            ) or raw_from_signal or audit_factor_bucket(audit, "raw_factors", symbol)
            z_factors = (
                top_row.get("z_factors") if isinstance(top_row.get("z_factors"), dict) else {}
            ) or z_from_signal or audit_factor_bucket(audit, "z_factors", symbol)
            alpha6_score = as_float(first_value(signal or {}, ("score", "raw_score"), not_obs))
            if alpha6_score is None:
                alpha6_score = as_float(first_value(explain, ("alpha6_score",), not_obs))
            contributions = {}
            for factor in FACTOR_KEYS:
                z_value = factor_bucket_value(z_factors, factor)
                weight = weights.get(factor)
                contributions[factor] = (z_value * weight) if z_value is not None and weight is not None else None
            positive = [(factor, value) for factor, value in contributions.items() if value is not None and value > 0]
            numeric = [(factor, value) for factor, value in contributions.items() if value is not None]
            if positive:
                dominant_factor, dominant_value = max(positive, key=lambda item: (item[1], item[0]))
                denominator = sum(value for _, value in positive)
            elif numeric:
                dominant_factor, dominant_value = max(numeric, key=lambda item: (abs(item[1]), item[0]))
                denominator = sum(abs(value) for _, value in numeric)
            else:
                dominant_factor, dominant_value, denominator = not_obs, None, None
            dominant_pct = (abs(dominant_value) / denominator * 100.0) if dominant_value is not None and denominator else None
            router_action = flatten_value(first_value(router, ("action",), first_value(explain, ("router_action",), not_obs)))
            router_reason = flatten_value(first_value(router, ("reason", "source_reason"), first_value(explain, ("router_reason", "blocked_reason"), not_obs)))
            rows.append({
                "ts_utc": audit_ts,
                "run_id": run_id,
                "symbol": symbol,
                "final_score": fmt_num(top_score_value(top_row), 8),
                "alpha6_score": fmt_num(alpha6_score, 8),
                "raw_factors": safe_json(raw_factors) if raw_factors else not_obs,
                "z_factors": safe_json(z_factors) if z_factors else not_obs,
                "effective_factor_weights": safe_json(weights) if weights else not_obs,
                "contribution_f1_mom_5d": fmt_num(contributions["f1_mom_5d"], 8),
                "contribution_f2_mom_20d": fmt_num(contributions["f2_mom_20d"], 8),
                "contribution_f3_vol_adj_ret": fmt_num(contributions["f3_vol_adj_ret"], 8),
                "contribution_f4_volume_expansion": fmt_num(contributions["f4_volume_expansion"], 8),
                "contribution_f5_rsi_trend_confirm": fmt_num(contributions["f5_rsi_trend_confirm"], 8),
                "dominant_factor": dominant_factor,
                "dominant_factor_contribution_pct": fmt_num(dominant_pct, 6),
                "router_action": router_action or not_obs,
                "router_reason": router_reason or not_obs,
                "forward_4h_net_bps": not_obs,
                "forward_8h_net_bps": not_obs,
                "forward_12h_net_bps": not_obs,
                "forward_24h_net_bps": not_obs,
            })
        return rows

    audit_paths = sorted((OUT / "raw" / "recent_runs").glob("*/decision_audit.json"))
    trade_paths = sorted((OUT / "raw" / "recent_runs").glob("*/trades.csv"))
    router_rows = []
    probe_rows = []
    dust_rows = []
    trade_rows = []
    raw_trade_events = []
    lifecycle_rows = []
    btc_blocked_rows = []
    maturity_rows = []
    open_position_rows = []
    open_probe_watch_rows = []
    dust_residual_roundtrip_rows = []
    early_exit_rows = []
    high_score_blocked_rows = []
    market_impulse_selection_shadow_rows = []
    factor_contribution_rows = []
    f3_dominant_swing_guard_cases = []
    f3_dominant_swing_guard_outcomes = []
    quant_lab_compliance_rows = []
    quant_lab_permission_audit_rows = []
    quant_lab_mode_audit_rows = []
    quant_lab_cost_usage_rows = []
    live_guard_impact_rows = []
    quant_lab_fallback_rows = []
    quant_lab_shadow_outcome_rows = []
    quant_lab_shadow_outcomes_by_permission = []
    quant_lab_request_success_count = 0
    quant_lab_request_error_count = 0
    trade_file_stats_by_run = {}
    trade_metrics_rows = []
    fill_metrics_rows = []
    order_lifecycle_rows = []
    summary_trade_count_mismatch_rows = []
    audit_high_score_but_not_executed_count = 0
    dust_residual_position_keys = set()
    dust_residual_row_keys = set()
    reason_counts = Counter()
    probe_counts = Counter({field: 0 for field in PROBE_COUNT_FIELDS})
    latest_dust_by_symbol = {}
    exit_signal_by_symbol = defaultdict(list)
    router_trade_decisions = defaultdict(deque)
    btc_skip_decisions_by_key = {}
    btc_skip_decision_duplicates_removed = 0
    btc_seen_in_decision_audit = False
    market_probe_seen = False
    probe_trade_rows = []
    covered_trade_event_ids = set()
    trade_read_errors = 0
    latest_symbol_context = {}
    event_candidate_price_by_symbol = {}
    price_observations_by_symbol = defaultdict(list)
    entry_context_by_run_symbol = {}
    audit_by_run = {}

    profit_state = state_map("profit_taking_state.json")
    highest_state = state_map("highest_px_state.json")
    stop_state = state_map("stop_loss_state.json")
    fixed_stop_state = state_map("fixed_stop_loss_state.json")
    negative_expectancy_state = state_map("negative_expectancy_cooldown.json")
    ledger_state = state_map("ledger_state.json")
    positions_state = state_map("positions.json")
    state_maps = {
        "profit_taking_state_present": profit_state,
        "highest_px_state_present": highest_state,
        "stop_loss_state_present": stop_state,
        "fixed_stop_loss_state_present": fixed_stop_state,
    }

    for audit_path in audit_paths:
        run_id = audit_path.parent.name
        audit = load_json(audit_path)
        if not isinstance(audit, dict):
            continue
        audit_by_run[run_id] = audit
        audit_text = safe_json(audit)
        if "btc_leadership_probe" in audit_text:
            btc_seen_in_decision_audit = True
        if "market_impulse_probe" in audit_text:
            market_probe_seen = True
        audit_ts = run_ts(run_id, audit)
        audit_regime = flatten_value(first_value(audit, ("regime", "market_regime"), not_obs))
        audit_level = flatten_value(first_value(audit, ("current_level", "risk_level"), not_obs))
        quant_lab = audit.get("quant_lab") if isinstance(audit.get("quant_lab"), dict) else {}
        if quant_lab:
            permission = quant_lab.get("permission") if isinstance(quant_lab.get("permission"), dict) else quant_lab
            filtered_orders = quant_lab.get("filtered_orders") if isinstance(quant_lab.get("filtered_orders"), list) else []
            cost_estimates = quant_lab.get("cost_estimates") if isinstance(quant_lab.get("cost_estimates"), list) else []
            filtered_count = sum(1 for row in filtered_orders if isinstance(row, dict) and row.get("filtered"))
            buy_filtered_count = sum(
                1
                for row in filtered_orders
                if isinstance(row, dict) and row.get("filtered") and str(row.get("side", "")).lower() == "buy"
            )
            quant_lab_compliance_rows.append({
                "source": f"decision_audit:{audit_path.relative_to(OUT).as_posix()}",
                "run_id": run_id,
                "ts_utc": audit_ts,
                "event_type": "audit_summary",
                "mode": flatten_value(first_observed(quant_lab.get("mode"), permission.get("mode"), not_obs)),
                "local_mode": flatten_value(first_observed(quant_lab.get("local_mode"), quant_lab.get("mode"), permission.get("mode"), not_obs)),
                "mode_source": flatten_value(first_observed(quant_lab.get("mode_source"), permission.get("mode_source"), not_obs)),
                "quant_lab_requested_mode": flatten_value(first_observed(quant_lab.get("quant_lab_requested_mode"), quant_lab.get("requested_mode"), permission.get("quant_lab_requested_mode"), not_obs)),
                "quant_lab_effective_mode": flatten_value(first_observed(quant_lab.get("quant_lab_effective_mode"), quant_lab.get("mode"), permission.get("quant_lab_effective_mode"), not_obs)),
                "called_api": bool_observed(first_observed(quant_lab.get("called_api"), permission.get("called_api"))),
                "apply_permission_gate": bool_observed(first_observed(quant_lab.get("apply_permission_gate"), permission.get("apply_permission_gate"))),
                "apply_cost_gate": bool_observed(first_observed(quant_lab.get("apply_cost_gate"), permission.get("apply_cost_gate"))),
                "permission_gate_enforced": bool_observed(first_observed(quant_lab.get("permission_gate_enforced"), permission.get("permission_gate_enforced"))),
                "cost_gate_enforced": bool_observed(first_observed(quant_lab.get("cost_gate_enforced"), permission.get("cost_gate_enforced"))),
                "enforce_readiness_status": flatten_value(first_observed(quant_lab.get("enforce_readiness_status"), permission.get("enforce_readiness_status"), not_obs)),
                "enforce_blocked_reasons": flatten_value(first_observed(quant_lab.get("enforce_blocked_reasons"), quant_lab.get("enforce_blocked_reason"), permission.get("enforce_blocked_reasons"), not_obs)),
                "enforce_blocked_reason": flatten_value(first_observed(quant_lab.get("enforce_blocked_reason"), permission.get("enforce_blocked_reason"), not_obs)),
                "contract_version_match": bool_observed(first_observed(quant_lab.get("contract_version_match"), permission.get("contract_version_match"))),
                "telemetry_schema_version_match": bool_observed(first_observed(quant_lab.get("telemetry_schema_version_match"), permission.get("telemetry_schema_version_match"))),
                "raw_permission_decision": flatten_value(first_observed(quant_lab.get("raw_permission_decision"), quant_lab.get("quant_lab_permission"), permission.get("decision"), quant_lab.get("permission"), not_obs)),
                "raw_permission_status": flatten_value(first_observed(quant_lab.get("raw_permission_status"), permission.get("raw_permission_status"), not_obs)),
                "raw_permission_enforceable": bool_observed(first_observed(quant_lab.get("raw_permission_enforceable"), permission.get("raw_permission_enforceable"))),
                "effective_permission_decision": flatten_value(first_observed(quant_lab.get("effective_permission_decision"), quant_lab.get("final_permission"), permission.get("effective_decision"), not_obs)),
                "would_block_if_enforced": bool_observed(quant_lab.get("would_block_if_enforced")),
                "shadow_override_reason": flatten_value(first_observed(quant_lab.get("shadow_override_reason"), permission.get("shadow_override_reason"), not_obs)),
                "fallback_reason": flatten_value(first_observed(quant_lab.get("fallback_reason"), permission.get("fallback_reason"), not_obs)),
                "remote_permission_as_of_ts": flatten_value(first_observed(quant_lab.get("remote_permission_as_of_ts"), quant_lab.get("last_response_ts"), not_obs)),
                "remote_permission_expires_at": flatten_value(quant_lab.get("remote_permission_expires_at") or not_obs),
                "remote_permission_status": flatten_value(quant_lab.get("remote_permission_status") or not_obs),
                "remote_permission_source_bundle_ts": flatten_value(first_observed(quant_lab.get("remote_permission_source_bundle_ts"), permission.get("remote_permission_source_bundle_ts"), not_obs)),
                "remote_permission_telemetry_latest_ts": flatten_value(first_observed(quant_lab.get("remote_permission_telemetry_latest_ts"), permission.get("remote_permission_telemetry_latest_ts"), not_obs)),
                "remote_permission_contract_version": flatten_value(first_observed(quant_lab.get("remote_permission_contract_version"), permission.get("remote_permission_contract_version"), quant_lab.get("contract_version"), not_obs)),
                "permission_contract_violation": bool_observed(first_observed(quant_lab.get("permission_contract_violation"), permission.get("permission_contract_violation"))),
                "contract_version": flatten_value(quant_lab.get("contract_version") or permission.get("contract_version") or not_obs),
                "permission_decision": flatten_value(first_observed(permission.get("decision"), quant_lab.get("raw_permission_decision"), quant_lab.get("quant_lab_permission"), quant_lab.get("permission"), not_obs)),
                "effective_decision": flatten_value(first_observed(permission.get("effective_decision"), quant_lab.get("effective_permission_decision"), quant_lab.get("final_permission"), not_obs)),
                "order_decision": not_obs,
                "fail_policy": flatten_value(permission.get("fail_policy") or not_obs),
                "fallback_used": str(bool(permission.get("fallback_used"))).lower(),
                "symbol": not_obs,
                "side": not_obs,
                "intent": not_obs,
                "orders_before": not_obs,
                "orders_after": not_obs,
                "orders_filtered": filtered_count,
                "buy_orders_filtered": buy_filtered_count,
                "filtered": str(bool(filtered_count)).lower(),
                "filter_reason": not_obs,
                "diagnosis": "ok" if not permission.get("fallback_used") else "fallback_policy_applied",
                "raw_json": safe_json(quant_lab),
            })
            for row in filtered_orders:
                if not isinstance(row, dict):
                    continue
                quant_lab_compliance_rows.append({
                    "source": f"decision_audit:{audit_path.relative_to(OUT).as_posix()}",
                    "run_id": run_id,
                    "ts_utc": audit_ts,
                    "event_type": "order_filter",
                    "mode": flatten_value(first_observed(row.get("mode"), quant_lab.get("mode"), not_obs)),
                    "local_mode": flatten_value(first_observed(row.get("local_mode"), row.get("mode"), quant_lab.get("mode"), not_obs)),
                    "mode_source": flatten_value(first_observed(row.get("mode_source"), quant_lab.get("mode_source"), not_obs)),
                    "quant_lab_requested_mode": flatten_value(first_observed(row.get("quant_lab_requested_mode"), quant_lab.get("quant_lab_requested_mode"), quant_lab.get("requested_mode"), not_obs)),
                    "quant_lab_effective_mode": flatten_value(first_observed(row.get("quant_lab_effective_mode"), quant_lab.get("quant_lab_effective_mode"), row.get("mode"), quant_lab.get("mode"), not_obs)),
                    "called_api": bool_observed(first_observed(row.get("called_api"), quant_lab.get("called_api"))),
                    "apply_permission_gate": bool_observed(first_observed(row.get("apply_permission_gate"), quant_lab.get("apply_permission_gate"))),
                    "apply_cost_gate": bool_observed(first_observed(row.get("apply_cost_gate"), quant_lab.get("apply_cost_gate"))),
                    "permission_gate_enforced": bool_observed(first_observed(row.get("permission_gate_enforced"), quant_lab.get("permission_gate_enforced"))),
                    "cost_gate_enforced": bool_observed(first_observed(row.get("cost_gate_enforced"), quant_lab.get("cost_gate_enforced"))),
                    "enforce_readiness_status": flatten_value(first_observed(row.get("enforce_readiness_status"), quant_lab.get("enforce_readiness_status"), not_obs)),
                    "enforce_blocked_reasons": flatten_value(first_observed(row.get("enforce_blocked_reasons"), row.get("enforce_blocked_reason"), quant_lab.get("enforce_blocked_reasons"), not_obs)),
                    "enforce_blocked_reason": flatten_value(first_observed(row.get("enforce_blocked_reason"), quant_lab.get("enforce_blocked_reason"), not_obs)),
                    "contract_version_match": bool_observed(first_observed(row.get("contract_version_match"), quant_lab.get("contract_version_match"))),
                    "telemetry_schema_version_match": bool_observed(first_observed(row.get("telemetry_schema_version_match"), quant_lab.get("telemetry_schema_version_match"))),
                    "raw_permission_decision": flatten_value(first_observed(row.get("raw_permission_decision"), row.get("quant_lab_permission"), row.get("permission_decision"), permission.get("decision"), quant_lab.get("permission"), not_obs)),
                    "raw_permission_status": flatten_value(first_observed(row.get("raw_permission_status"), quant_lab.get("raw_permission_status"), permission.get("raw_permission_status"), not_obs)),
                    "raw_permission_enforceable": bool_observed(first_observed(row.get("raw_permission_enforceable"), quant_lab.get("raw_permission_enforceable"), permission.get("raw_permission_enforceable"))),
                    "effective_permission_decision": flatten_value(first_observed(row.get("effective_permission_decision"), row.get("final_permission"), permission.get("effective_decision"), quant_lab.get("final_permission"), not_obs)),
                    "would_block_if_enforced": bool_observed(row.get("would_block_if_enforced")),
                    "shadow_override_reason": flatten_value(first_observed(row.get("shadow_override_reason"), quant_lab.get("shadow_override_reason"), permission.get("shadow_override_reason"), not_obs)),
                    "fallback_reason": flatten_value(first_observed(row.get("fallback_reason"), permission.get("fallback_reason"), not_obs)),
                    "remote_permission_as_of_ts": flatten_value(first_observed(row.get("remote_permission_as_of_ts"), quant_lab.get("remote_permission_as_of_ts"), quant_lab.get("last_response_ts"), not_obs)),
                    "remote_permission_expires_at": flatten_value(first_observed(row.get("remote_permission_expires_at"), quant_lab.get("remote_permission_expires_at"), not_obs)),
                    "remote_permission_status": flatten_value(first_observed(row.get("remote_permission_status"), quant_lab.get("remote_permission_status"), not_obs)),
                    "remote_permission_source_bundle_ts": flatten_value(first_observed(row.get("remote_permission_source_bundle_ts"), quant_lab.get("remote_permission_source_bundle_ts"), permission.get("remote_permission_source_bundle_ts"), not_obs)),
                    "remote_permission_telemetry_latest_ts": flatten_value(first_observed(row.get("remote_permission_telemetry_latest_ts"), quant_lab.get("remote_permission_telemetry_latest_ts"), permission.get("remote_permission_telemetry_latest_ts"), not_obs)),
                    "remote_permission_contract_version": flatten_value(first_observed(row.get("remote_permission_contract_version"), quant_lab.get("remote_permission_contract_version"), permission.get("remote_permission_contract_version"), quant_lab.get("contract_version"), not_obs)),
                    "permission_contract_violation": bool_observed(first_observed(row.get("permission_contract_violation"), quant_lab.get("permission_contract_violation"), permission.get("permission_contract_violation"))),
                    "contract_version": flatten_value(first_observed(row.get("contract_version"), quant_lab.get("contract_version"), permission.get("contract_version"), not_obs)),
                    "permission_decision": flatten_value(first_observed(row.get("permission_decision"), row.get("raw_permission_decision"), row.get("quant_lab_permission"), permission.get("decision"), quant_lab.get("permission"), not_obs)),
                    "effective_decision": flatten_value(first_observed(row.get("effective_decision"), row.get("effective_permission_decision"), row.get("final_permission"), permission.get("effective_decision"), quant_lab.get("final_permission"), not_obs)),
                    "order_decision": flatten_value(row.get("order_decision") or not_obs),
                    "fail_policy": flatten_value(permission.get("fail_policy") or not_obs),
                    "fallback_used": str(bool(row.get("fallback_used"))).lower(),
                    "symbol": flatten_value(row.get("symbol") or not_obs),
                    "side": flatten_value(row.get("side") or not_obs),
                    "intent": flatten_value(row.get("intent") or not_obs),
                    "orders_before": not_obs,
                    "orders_after": not_obs,
                    "orders_filtered": not_obs,
                    "buy_orders_filtered": not_obs,
                    "filtered": str(bool(row.get("filtered"))).lower(),
                    "filter_reason": flatten_value(row.get("filter_reason") or not_obs),
                    "diagnosis": "filtered" if row.get("filtered") else "passed",
                    "raw_json": safe_json(row),
                })
                if quant_lab_is_fallback(row):
                    quant_lab_fallback_rows.append({
                        "source": f"decision_audit:{audit_path.relative_to(OUT).as_posix()}",
                        "run_id": run_id,
                        "ts_utc": audit_ts,
                        "event_type": "order_filter",
                        "endpoint": not_obs,
                        "symbol": flatten_value(row.get("symbol") or not_obs),
                        "side": flatten_value(row.get("side") or not_obs),
                        "intent": flatten_value(row.get("intent") or not_obs),
                        "fail_policy": flatten_value(permission.get("fail_policy") or not_obs),
                        "effective_decision": flatten_value(row.get("effective_decision") or permission.get("effective_decision") or not_obs),
                        "fallback_used": str(quant_lab_is_fallback(row)).lower(),
                        "error": not_obs,
                        "diagnosis": flatten_value(row.get("filter_reason") or "fallback_policy_applied"),
                        "raw_json": safe_json(row),
                    })
            for row in cost_estimates:
                if not isinstance(row, dict):
                    continue
                required_edge = first_observed(row.get("required_edge_bps"), row.get("min_required_edge_bps"), not_obs)
                cost_source = first_observed(row.get("cost_source"), row.get("source"), row.get("local_cost_source"), not_obs)
                fallback_level = first_observed(row.get("fallback_level"), not_obs)
                cost_model_version_value = flatten_value(row.get("cost_model_version") or not_obs).strip().lower()
                degraded_cost = (
                    flatten_value(cost_source).strip().lower() == "global_default"
                    or flatten_value(fallback_level).strip().upper() == "GLOBAL_DEFAULT"
                    or cost_model_version_value == "global_default_v0"
                )
                cost_diagnosis = "global_default_cost" if degraded_cost else flatten_value(row.get("diagnosis") or ("fallback_cost" if row.get("fallback_used") else "ok"))
                quant_lab_cost_usage_rows.append({
                    "source": f"decision_audit:{audit_path.relative_to(OUT).as_posix()}",
                    "run_id": run_id,
                    "ts_utc": audit_ts,
                    "schema_version": flatten_value(first_observed(row.get("schema_version"), quant_lab.get("schema_version"), not_obs)),
                    "contract_version": flatten_value(first_observed(row.get("contract_version"), row.get("cost_contract_version"), quant_lab.get("contract_version"), not_obs)),
                    "event_id_generation_version": flatten_value(first_observed(row.get("event_id_generation_version"), quant_lab.get("event_id_generation_version"), not_obs)),
                    "source_snapshot_hash": flatten_value(first_observed(row.get("source_snapshot_hash"), quant_lab.get("source_snapshot_hash"), not_obs)),
                    "mode": flatten_value(first_observed(row.get("mode"), quant_lab.get("mode"), not_obs)),
                    "symbol": flatten_value(row.get("symbol") or not_obs),
                    "request_symbol": flatten_value(first_observed(row.get("request_symbol"), row.get("symbol"), not_obs)),
                    "normalized_symbol": flatten_value(row.get("normalized_symbol") or not_obs),
                    "response_symbol": flatten_value(first_observed(row.get("response_symbol"), row.get("normalized_symbol"), row.get("symbol"), not_obs)),
                    "venue": flatten_value(row.get("venue") or not_obs),
                    "instrument_type": flatten_value(row.get("instrument_type") or not_obs),
                    "side": flatten_value(row.get("side") or not_obs),
                    "intent": flatten_value(row.get("intent") or not_obs),
                    "notional_usdt": flatten_value(row.get("notional_usdt") if row.get("notional_usdt") is not None else not_obs),
                    "quantile": flatten_value(row.get("quantile") or not_obs),
                    "requested_quantile": flatten_value(first_observed(row.get("requested_quantile"), row.get("quantile"), not_obs)),
                    "strategy_id": flatten_value(first_observed(row.get("strategy_id"), row.get("alpha_id"), not_obs)),
                    "request_id": flatten_value(row.get("request_id") or not_obs),
                    "requested_regime": flatten_value(first_observed(row.get("requested_regime"), row.get("regime"), not_obs)),
                    "matched_regime": flatten_value(first_observed(row.get("matched_regime"), row.get("regime"), not_obs)),
                    "alpha_id": flatten_value(row.get("alpha_id") or not_obs),
                    "cost_bps": flatten_value(first_observed(row.get("cost_bps"), row.get("total_cost_bps"), row.get("effective_total_cost_bps"))),
                    "cost_usdt": flatten_value(row.get("cost_usdt") if row.get("cost_usdt") is not None else not_obs),
                    "cost_source": flatten_value(cost_source),
                    "cost_model_version": flatten_value(row.get("cost_model_version") or not_obs),
                    "cost_contract_version": flatten_value(first_observed(row.get("cost_contract_version"), row.get("contract_version"), QUANT_LAB_CONTRACT_VERSION)),
                    "as_of_ts": flatten_value(first_observed(row.get("as_of_ts"), row.get("response_ts"), not_obs)),
                    "fallback_level": flatten_value(fallback_level),
                    "sample_count": flatten_value(row.get("sample_count") if row.get("sample_count") is not None else not_obs),
                    "total_cost_bps": flatten_value(row.get("total_cost_bps") if row.get("total_cost_bps") is not None else not_obs),
                    "effective_total_cost_bps": flatten_value(row.get("effective_total_cost_bps") if row.get("effective_total_cost_bps") is not None else not_obs),
                    "selected_total_cost_bps": flatten_value(row.get("selected_total_cost_bps") if row.get("selected_total_cost_bps") is not None else first_observed(row.get("total_cost_bps"), not_obs)),
                    "total_cost_bps_p50": flatten_value(row.get("total_cost_bps_p50") if row.get("total_cost_bps_p50") is not None else not_obs),
                    "total_cost_bps_p75": flatten_value(row.get("total_cost_bps_p75") if row.get("total_cost_bps_p75") is not None else not_obs),
                    "total_cost_bps_p90": flatten_value(row.get("total_cost_bps_p90") if row.get("total_cost_bps_p90") is not None else not_obs),
                    "required_edge_bps": flatten_value(required_edge),
                    "expected_edge_bps": flatten_value(row.get("expected_edge_bps") if row.get("expected_edge_bps") is not None else not_obs),
                    "expected_edge_source": flatten_value(first_observed(row.get("expected_edge_source"), row.get("proxy_source"))),
                    "min_required_edge_bps": flatten_value(row.get("min_required_edge_bps") if row.get("min_required_edge_bps") is not None else not_obs),
                    "would_filter_by_cost": bool_observed(first_observed(row.get("would_filter_by_cost"), row.get("would_filter"))),
                    "would_block_by_cost": bool_observed(first_observed(row.get("would_block_by_cost"), row.get("would_filter_by_cost"), row.get("would_filter"))),
                    "actually_filtered": bool_observed(first_observed(row.get("actually_filtered"), row.get("order_filtered"))),
                    "cost_gate_enforced": bool_observed(first_observed(row.get("cost_gate_enforced"), quant_lab.get("cost_gate_enforced"))),
                    "quant_lab_decision": flatten_value(row.get("quant_lab_decision") or not_obs),
                    "fallback_used": str(bool(row.get("fallback_used"))).lower(),
                    "fallback_used_for_cost_model": str(bool(row.get("fallback_used") or degraded_cost)).lower(),
                    "fallback_reason": flatten_value(row.get("fallback_reason") or not_obs),
                    "degraded_cost_model": str(bool(degraded_cost)).lower(),
                    "filtered": str(bool(row.get("filtered"))).lower() if "filtered" in row else not_obs,
                    "filter_reason": flatten_value(row.get("filter_reason") or not_obs),
                    "warning": flatten_value(row.get("warning") or not_obs),
                    "cost_gate_verified": bool_observed(row.get("cost_gate_verified")),
                    "diagnosis": cost_diagnosis,
                    "raw_json": safe_json(row),
                })
                if quant_lab_is_fallback(row):
                    quant_lab_fallback_rows.append({
                        "source": f"decision_audit:{audit_path.relative_to(OUT).as_posix()}",
                        "run_id": run_id,
                        "ts_utc": audit_ts,
                        "event_type": "cost_estimate",
                        "endpoint": "/v1/costs/estimate",
                        "symbol": flatten_value(row.get("symbol") or not_obs),
                        "side": flatten_value(row.get("side") or not_obs),
                        "intent": flatten_value(row.get("intent") or not_obs),
                        "fail_policy": flatten_value(permission.get("fail_policy") or not_obs),
                        "effective_decision": flatten_value(permission.get("effective_decision") or not_obs),
                        "fallback_used": str(quant_lab_is_fallback(row)).lower(),
                        "error": flatten_value(row.get("error") or not_obs),
                        "diagnosis": flatten_value(row.get("fallback_reason") or row.get("filter_reason") or "cost_estimate_fallback"),
                        "raw_json": safe_json(row),
                    })
        counts = audit.get("counts") if isinstance(audit.get("counts"), dict) else {}
        for field in PROBE_COUNT_FIELDS:
            probe_counts[field] += as_int(counts.get(field))
        signal_lookup = strategy_signal_lookup_from_audit(audit)
        alpha6_lookup = signal_lookup.get("Alpha6Factor") or {}
        trend_lookup = signal_lookup.get("TrendFollowing") or {}
        factor_contribution_rows.extend(
            factor_contribution_base_rows(
                audit,
                run_id,
                audit_ts,
                audit_regime,
                audit_level,
            )
        )
        market_shadow = audit.get("market_impulse_shadow_selection")
        if isinstance(market_shadow, dict):
            market_impulse_selection_shadow_rows.append({
                "ts_utc": audit_ts,
                "run_id": run_id,
                "active": str(bool(market_shadow.get("active"))).lower(),
                "trend_buy_count": first_observed(market_shadow.get("trend_buy_count")),
                "btc_trend_score": first_observed(market_shadow.get("btc_trend_score")),
                "selected_live": first_observed(market_shadow.get("selected_live")),
                "selected_by_priority": first_observed(market_shadow.get("selected_by_priority")),
                "selected_by_trend_score": first_observed(market_shadow.get("selected_by_trend_score")),
                "selected_by_alpha6_confirmed": first_observed(market_shadow.get("selected_by_alpha6_confirmed")),
                "selected_by_expected_net_shadow": first_observed(market_shadow.get("selected_by_expected_net_shadow")),
                "candidates_json": safe_json(market_shadow.get("candidates") or []),
            })
        for item in audit.get("target_execution_explain") or []:
            if not isinstance(item, dict):
                continue
            symbol = flatten_value(item.get("symbol")) or not_obs
            if symbol != not_obs:
                alpha6_signal = alpha6_lookup.get(symbol, {})
                trend_signal = trend_lookup.get(symbol, {})
                raw_factors, _ = signal_factor_buckets(alpha6_signal)
                entry_context_by_run_symbol[(run_id, symbol)] = {
                    "ts_utc": audit_ts,
                    "current_level": first_observed(first_value(item, ("current_level", "risk_level"), not_obs), audit_level),
                    "regime": first_observed(first_value(item, ("regime", "market_regime"), not_obs), audit_regime),
                    "alpha6_score": first_observed(
                        item.get("alpha6_score"),
                        first_value(alpha6_signal, ("alpha6_score", "score", "final_score"), not_obs),
                    ),
                    "f4_volume_expansion": first_observed(
                        item.get("f4_volume_expansion"),
                        first_value(raw_factors, ("f4_volume_expansion", "f4"), not_obs),
                        first_value(alpha6_signal, ("f4_volume_expansion",), not_obs),
                    ),
                    "f5_rsi_trend_confirm": first_observed(
                        item.get("f5_rsi_trend_confirm"),
                        first_value(raw_factors, ("f5_rsi_trend_confirm", "f5"), not_obs),
                        first_value(alpha6_signal, ("f5_rsi_trend_confirm",), not_obs),
                    ),
                    "trend_score": first_observed(
                        item.get("trend_score"),
                        first_value(trend_signal, ("trend_score", "score", "final_score"), not_obs),
                    ),
                }
            high_score_blocked = str(item.get("high_score_but_not_executed", "")).strip().lower() == "true"
            if item.get("high_score_but_not_executed") is True:
                high_score_blocked = True
            if high_score_blocked:
                audit_high_score_but_not_executed_count += 1
            router_action = flatten_value(item.get("router_action")).lower()
            if not high_score_blocked or router_action != "skip":
                continue
            router_reason = first_observed(first_value(item, ("router_reason", "blocked_reason"), not_obs))
            matching_router_decision = {}
            for router_item in audit.get("router_decisions") or []:
                if not isinstance(router_item, dict):
                    continue
                if flatten_value(router_item.get("action")).lower() != "skip":
                    continue
                if flatten_value(router_item.get("symbol")) != symbol:
                    continue
                if flatten_value(router_item.get("reason")) != router_reason:
                    continue
                matching_router_decision = router_item
                break
            high_score_blocked_rows.append({
                "ts_utc": audit_ts,
                "run_id": run_id,
                "symbol": symbol,
                "final_score": first_observed(item.get("final_score")),
                "selected_rank": first_observed(item.get("selected_rank")),
                "target_w": first_observed(item.get("target_w")),
                "router_action": first_observed(item.get("router_action")),
                "router_reason": router_reason,
                "high_score_block_category": first_observed(item.get("high_score_block_category")),
                "trend_score": first_observed(item.get("trend_score")),
                "trend_side": first_observed(item.get("trend_side")),
                "alpha6_score": first_observed(item.get("alpha6_score")),
                "alpha6_side": first_observed(item.get("alpha6_side")),
                "f4_volume_expansion": first_observed(item.get("f4_volume_expansion")),
                "f5_rsi_trend_confirm": first_observed(item.get("f5_rsi_trend_confirm")),
                "current_level": first_observed(first_value(item, ("current_level",), audit_level)),
                "regime": first_observed(first_value(item, ("regime",), audit_regime)),
                "entry_px": first_observed(
                    first_value(item, ("entry_px", "latest_px", "current_px", "price", "px"), not_obs),
                    first_value(matching_router_decision, ("entry_px", "latest_px", "current_px", "price", "px"), not_obs),
                ),
                "last_exit_reason": first_observed(
                    first_value(matching_router_decision, ("last_exit_reason",), not_obs),
                    first_value(item, ("last_exit_reason",), not_obs),
                ),
                "last_exit_px": first_observed(
                    first_value(matching_router_decision, ("last_exit_px",), not_obs),
                    first_value(item, ("last_exit_px",), not_obs),
                ),
                "highest_px_before_exit": first_observed(
                    first_value(matching_router_decision, ("highest_px_before_exit",), not_obs),
                    first_value(item, ("highest_px_before_exit",), not_obs),
                ),
                "elapsed_hours": first_observed(
                    first_value(matching_router_decision, ("elapsed_hours",), not_obs),
                    first_value(item, ("elapsed_hours",), not_obs),
                ),
                "required_cooldown_hours": first_observed(
                    first_value(matching_router_decision, ("required_cooldown_hours",), not_obs),
                    first_value(item, ("required_cooldown_hours",), not_obs),
                ),
                "breakout_exception_met": first_observed(
                    first_value(matching_router_decision, ("breakout_exception_met",), not_obs),
                    first_value(item, ("breakout_exception_met",), not_obs),
                ),
            })
        for idx, item in enumerate(audit.get("router_decisions") or []):
            if not isinstance(item, dict):
                item = {"value": item}
            reason = flatten_value(item.get("reason"))
            source_reason = flatten_value(item.get("source_reason"))
            action = flatten_value(item.get("action"))
            symbol = flatten_value(item.get("symbol")) or not_obs
            probe_type = probe_type_of(item)
            if symbol != not_obs:
                context = latest_symbol_context.setdefault(symbol, {})
                context["ts_utc"] = audit_ts
                if audit_regime != not_obs:
                    context["regime"] = audit_regime
                if audit_level != not_obs:
                    context["current_level"] = audit_level
                px_value = first_value(item, ("latest_px", "last_px", "current_px", "price", "px"), not_obs)
                if as_float(px_value) is not None:
                    context["current_px"] = flatten_value(px_value)
                    observe_symbol_price(symbol, audit_ts, px_value, "router_decision")
            reason_counts[reason] += 1
            if reason == "btc_leadership_probe_alpha6_score_too_low":
                probe_counts["btc_leadership_probe_alpha6_score_too_low_count"] += 1
            if reason == "btc_leadership_probe_no_alpha6_buy":
                probe_counts["btc_leadership_probe_no_alpha6_buy_count"] += 1
            if reason == "btc_leadership_probe_cooldown":
                probe_counts["btc_leadership_probe_cooldown_count"] += 1
            if reason == "btc_leadership_probe_not_flat":
                probe_counts["btc_leadership_probe_not_flat_count"] += 1

            raw_json = safe_json(item)
            row = {
                "run_id": run_id,
                "audit_timestamp": audit_ts,
                "index": idx,
                "symbol": symbol,
                "action": action,
                "reason": reason,
                "source_reason": source_reason,
                "stage": flatten_value(item.get("stage")),
                "side": flatten_value(item.get("side")),
                "drift": flatten_value(item.get("drift")),
                "deadband": flatten_value(item.get("deadband")),
                "hold_hours": flatten_value(item.get("hold_hours", not_obs)),
                "min_hold_hours": flatten_value(item.get("min_hold_hours", not_obs)),
                "exit_allowed_before_min_hold": bool_text(item.get("exit_allowed_before_min_hold", not_obs)),
                "exit_blocked_by_min_hold": bool_text(item.get("exit_blocked_by_min_hold", not_obs)),
                "exit_priority": flatten_value(item.get("exit_priority", not_obs)),
                "min_hold_block_reason": flatten_value(item.get("min_hold_block_reason", not_obs)),
                "early_exit_opportunity_cost_bps": flatten_value(item.get("early_exit_opportunity_cost_bps", not_obs)),
                "raw_json": raw_json,
            }
            router_rows.append(row)

            router_intent = normalize_trade_intent(item)
            entry_reason_value = first_observed(
                item.get("entry_reason"),
                item.get("entry_type"),
                item.get("source_reason"),
                not_obs,
            )
            dominant_factor_value = flatten_value(first_observed(item.get("dominant_factor"), not_obs))
            swing_f3_blocked_value = bool_observed(item.get("swing_f3_dominant_blocked"))
            if (
                action == "create"
                and router_intent == "OPEN_LONG"
                and entry_reason_value == "normal_entry"
                and (dominant_factor_value == "f3_vol_adj_ret" or swing_f3_blocked_value == "true")
            ):
                f3_dominant_swing_guard_cases.append({
                    "ts_utc": audit_ts,
                    "run_id": run_id,
                    "symbol": symbol,
                    "action": action,
                    "side": flatten_value(item.get("side")),
                    "intent": router_intent,
                    "reason": reason,
                    "router_reason": reason,
                    "entry_reason": entry_reason_value,
                    "dominant_factor": dominant_factor_value,
                    "dominant_factor_contribution_pct": flatten_value(first_observed(
                        item.get("dominant_factor_contribution_pct"),
                        item.get("contribution_pct"),
                        not_obs,
                    )),
                    "swing_f3_dominant_blocked": swing_f3_blocked_value,
                    "swing_hold_position": bool_observed(item.get("swing_hold_position")),
                    "f4_volume_expansion": flatten_value(first_observed(
                        item.get("f4_volume_expansion"),
                        item.get("f4"),
                        not_obs,
                    )),
                    "f5_rsi_trend_confirm": flatten_value(first_observed(
                        item.get("f5_rsi_trend_confirm"),
                        item.get("f5"),
                        not_obs,
                    )),
                    "swing_hold_block_reason": flatten_value(first_observed(
                        item.get("swing_hold_block_reason"),
                        item.get("swing_audit_reason"),
                        not_obs,
                    )),
                    "factor_contribution_source": flatten_value(first_observed(
                        item.get("factor_contribution_source"),
                        not_obs,
                    )),
                })
            if action == "create" and router_intent in {"OPEN_LONG", "CLOSE_LONG"}:
                trade_reason = router_trade_reason(item, router_intent)
                router_trade_decisions[(run_id, symbol, router_intent)].append({
                    "run_id": run_id,
                    "ts_utc": audit_ts,
                    "symbol": symbol,
                    "intent": router_intent,
                    "reason": trade_reason,
                    "raw_reason": reason,
                    "source_reason": source_reason,
                    "probe_type": router_trade_probe_type(item, trade_reason),
                    "swing_hold_position": item.get("swing_hold_position", not_obs),
                    "swing_min_hold_hours": first_value(item, ("swing_min_hold_hours", "required_hold_hours"), not_obs),
                    "raw_json": raw_json,
                })
                if router_intent == "OPEN_LONG" and symbol != not_obs:
                    context = entry_context_by_run_symbol.setdefault((run_id, symbol), {})
                    context.setdefault("ts_utc", audit_ts)
                    if audit_level != not_obs:
                        context.setdefault("current_level", audit_level)
                    if audit_regime != not_obs:
                        context.setdefault("regime", audit_regime)
                    for source_key, dest_key in (
                        ("alpha6_score", "alpha6_score"),
                        ("f4_volume_expansion", "f4_volume_expansion"),
                        ("f5_rsi_trend_confirm", "f5_rsi_trend_confirm"),
                        ("trend_score", "trend_score"),
                    ):
                        observed = first_observed(item.get(source_key))
                        if observed != not_obs:
                            context.setdefault(dest_key, observed)

            if reason.startswith("btc_leadership_probe_") and action == "skip":
                decision_ts_utc = btc_decision_ts_utc(item, audit, audit_ts)
                decision_key = btc_label_key(run_id, decision_ts_utc, symbol, reason)
                decision = {
                    "run_id": run_id,
                    "ts_utc": decision_ts_utc,
                    "index": idx,
                    "item": item,
                    "unique_key": decision_key,
                }
                if decision_key in btc_skip_decisions_by_key:
                    btc_skip_decision_duplicates_removed += 1
                else:
                    btc_skip_decisions_by_key[decision_key] = decision

            if contains_term(item, PROBE_TERMS) or reason in PROBE_EXIT_REASONS:
                event_type = "router_decision"
                if action == "skip" or "blocked" in reason or reason.endswith("_too_low") or reason.endswith("_not_flat"):
                    event_type = "blocked"
                elif reason in PROBE_EXIT_REASONS:
                    event_type = "exit_signal"
                elif action in {"buy", "open"} or reason in PROBE_TYPES:
                    event_type = "open"
                probe_rows.append({
                    "source": str(audit_path.relative_to(OUT)),
                    "run_id": run_id,
                    "ts_utc": audit_ts,
                    "symbol": symbol,
                    "probe_type": probe_type,
                    "event_type": event_type,
                    "action": action,
                    "reason": reason,
                    "status": "observed",
                    "alpha6_score": flatten_value(item.get("alpha6_score")),
                    "f4_volume_expansion": flatten_value(item.get("f4_volume_expansion")),
                    "f5_rsi_trend_confirm": flatten_value(item.get("f5_rsi_trend_confirm")),
                    "rolling_high": flatten_value(item.get("rolling_high")),
                    "breakout_met": flatten_value(item.get("breakout_met", not_obs)),
                    "net_expectancy_bps": flatten_value(item.get("net_expectancy_bps")),
                    "raw_json": raw_json,
                })

            if contains_term(item, DUST_TERMS) or reason in {"anti_chase_add_size", "dust_residual_no_close_order"}:
                raw_held = first_value(item, ("raw_held_value_usdt", "held_value_usdt"))
                effective_held = first_value(item, ("effective_held_value_usdt", "held_value_usdt"))
                dust_threshold = first_value(item, ("dust_threshold_usdt",), 1.0)
                raw_f = as_float(raw_held)
                eff_f = as_float(effective_held)
                dust_f = as_float(dust_threshold)
                anti_chase = reason == "anti_chase_add_size"
                bug = bool(raw_f is not None and dust_f is not None and raw_f < dust_f and (anti_chase or (eff_f is not None and eff_f != 0.0)))
                diagnosis = "dust_residual_no_close_order_correctly_suppressed" if reason == "dust_residual_no_close_order" and not bug else ("high_issue" if bug else not_obs)
                if bug:
                    add_issue(
                        "high",
                        "dust_held_value_triggers_anti_chase_add_size",
                        "Dust-sized held value triggered anti-chase/add-size or did not zero effective held value.",
                        {"run_id": run_id, "symbol": symbol, "reason": reason, "raw_held_value_usdt": raw_held, "effective_held_value_usdt": effective_held, "dust_threshold_usdt": dust_threshold},
                    )
                latest_dust_by_symbol[symbol] = {
                    "raw_held_value_usdt": raw_held,
                    "effective_held_value_usdt": effective_held,
                    "dust_threshold_usdt": dust_threshold,
                    "reason": reason,
                }
                dust_rows.append({
                    "source": str(audit_path.relative_to(OUT)),
                    "run_id": run_id,
                    "ts_utc": audit_ts,
                    "symbol": symbol,
                    "raw_held_value_usdt": flatten_value(raw_held),
                    "effective_held_value_usdt": flatten_value(effective_held),
                    "dust_threshold_usdt": flatten_value(dust_threshold),
                    "reason": reason,
                    "anti_chase_triggered": str(anti_chase).lower(),
                    "dust_position_ignored_for_add_size": flatten_value(item.get("dust_position_ignored_for_add_size", reason == "dust_residual_no_close_order")),
                    "bug_suspected": str(bug).lower(),
                    "diagnosis": diagnosis,
                    "raw_json": raw_json,
                })

            actual_exit_reason = reason if reason in FLAT_EXIT_SIGNAL_REASONS else ""
            if actual_exit_reason:
                exit_signal_by_symbol[symbol].append({"run_id": run_id, "ts_utc": audit_ts, "reason": actual_exit_reason, "source_reason": source_reason})
                lifecycle_rows.append({
                    "ts_utc": audit_ts,
                    "run_id": run_id,
                    "symbol": symbol,
                    "probe_type": probe_type,
                    "entry_ts": not_obs,
                    "entry_px": not_obs,
                    "exit_ts": audit_ts,
                    "exit_px": flatten_value(first_value(item, ("exit_px", "px", "price"))),
                    "exit_reason": actual_exit_reason,
                    "gross_bps": flatten_value(item.get("gross_bps", not_obs)),
                    "net_bps": flatten_value(item.get("net_bps", not_obs)),
                    "remaining_value_usdt": flatten_value(first_value(item, ("remaining_value_usdt", "held_value_usdt", "raw_held_value_usdt"))),
                    "dust_threshold_usdt": flatten_value(first_value(item, ("dust_threshold_usdt",), not_obs)),
                    "state_still_present_after_close": not_obs,
                    "profit_taking_state_present": str(state_present(profit_state, symbol)).lower(),
                    "highest_px_state_present": str(state_present(highest_state, symbol)).lower(),
                    "stop_loss_state_present": str(state_present(stop_state, symbol)).lower(),
                    "fixed_stop_loss_state_present": str(state_present(fixed_stop_state, symbol)).lower(),
                    "repeated_exit_signal_after_flat": "false",
                    "diagnosis": "probe_exit_signal_observed" if actual_exit_reason in PROBE_EXIT_REASONS else "exit_signal_observed",
                })

        for item in iter_dicts(audit):
            if contains_term(item, PROBE_TERMS):
                probe_rows.append({
                    "source": str(audit_path.relative_to(OUT)),
                    "run_id": run_id,
                    "ts_utc": audit_ts,
                    "symbol": flatten_value(first_value(item, ("symbol",), not_obs)),
                    "probe_type": probe_type_of(item),
                    "event_type": "audit_node",
                    "action": flatten_value(item.get("action", not_obs)) if isinstance(item, dict) else not_obs,
                    "reason": flatten_value(item.get("reason", not_obs)) if isinstance(item, dict) else not_obs,
                    "status": "observed",
                    "alpha6_score": flatten_value(item.get("alpha6_score", not_obs)) if isinstance(item, dict) else not_obs,
                    "f4_volume_expansion": flatten_value(item.get("f4_volume_expansion", not_obs)) if isinstance(item, dict) else not_obs,
                    "f5_rsi_trend_confirm": flatten_value(item.get("f5_rsi_trend_confirm", not_obs)) if isinstance(item, dict) else not_obs,
                    "rolling_high": flatten_value(item.get("rolling_high", not_obs)) if isinstance(item, dict) else not_obs,
                    "breakout_met": flatten_value(item.get("breakout_met", not_obs)) if isinstance(item, dict) else not_obs,
                    "net_expectancy_bps": flatten_value(item.get("net_expectancy_bps", not_obs)) if isinstance(item, dict) else not_obs,
                    "raw_json": safe_json(item),
                })

    event_candidates = OUT / "raw" / "reports" / "event_candidates.json"
    if event_candidates.is_file():
        data = load_json(event_candidates)
        event_regime = flatten_value(first_value(data, ("regime", "market_regime"), not_obs)) if isinstance(data, dict) else not_obs
        event_level = flatten_value(first_value(data, ("current_level", "risk_level"), not_obs)) if isinstance(data, dict) else not_obs
        for item in iter_dicts(data):
            if isinstance(item, dict):
                symbol = flatten_value(first_value(item, ("symbol",), not_obs))
                if symbol != not_obs:
                    context = latest_symbol_context.setdefault(symbol, {})
                    px_value = first_value(item, ("current_px", "latest_px", "last_px", "price", "px"), not_obs)
                    if as_float(px_value) is not None:
                        context["current_px"] = flatten_value(px_value)
                        event_candidate_price_by_symbol[symbol] = {
                            "current_px": flatten_value(px_value),
                            "ts_utc": not_obs,
                        }
                        event_candidate_price_by_symbol[symbol.replace("-", "/").upper()] = {
                            "current_px": flatten_value(px_value),
                            "ts_utc": not_obs,
                        }
                    if event_regime != not_obs:
                        context["regime"] = event_regime
                    if event_level != not_obs:
                        context["current_level"] = event_level
            if contains_term(item, PROBE_TERMS):
                probe_rows.append({
                    "source": str(event_candidates.relative_to(OUT)),
                    "run_id": not_obs,
                    "ts_utc": not_obs,
                    "symbol": flatten_value(first_value(item, ("symbol",), not_obs)),
                    "probe_type": probe_type_of(item),
                    "event_type": "event_candidate",
                    "action": flatten_value(item.get("action", not_obs)) if isinstance(item, dict) else not_obs,
                    "reason": flatten_value(item.get("reason", not_obs)) if isinstance(item, dict) else not_obs,
                    "status": "observed",
                    "alpha6_score": flatten_value(item.get("alpha6_score", not_obs)) if isinstance(item, dict) else not_obs,
                    "f4_volume_expansion": flatten_value(item.get("f4_volume_expansion", not_obs)) if isinstance(item, dict) else not_obs,
                    "f5_rsi_trend_confirm": flatten_value(item.get("f5_rsi_trend_confirm", not_obs)) if isinstance(item, dict) else not_obs,
                    "rolling_high": flatten_value(item.get("rolling_high", not_obs)) if isinstance(item, dict) else not_obs,
                    "breakout_met": flatten_value(item.get("breakout_met", not_obs)) if isinstance(item, dict) else not_obs,
                    "net_expectancy_bps": flatten_value(item.get("net_expectancy_bps", not_obs)) if isinstance(item, dict) else not_obs,
                    "raw_json": safe_json(item),
                })

    def take_router_trade_decision(run_id, symbol, intent):
        decisions = router_trade_decisions.get((run_id, symbol, intent))
        if decisions:
            return decisions.popleft()
        return {}

    def build_trade_event(trade_path, run_id, idx, item):
        timestamp = flatten_value(first_value(item, ("timestamp", "ts", "time"), not_obs))
        symbol = flatten_value(first_value(item, ("symbol", "instId"), not_obs))
        intent = normalize_trade_intent(item)
        router_info = take_router_trade_decision(run_id, symbol, intent)
        entry_reason = flatten_value(first_value(item, ("entry_reason", "open_reason"), not_obs))
        exit_reason = flatten_value(first_value(item, ("exit_reason", "close_reason"), not_obs))
        if intent == "OPEN_LONG":
            entry_reason = first_observed(entry_reason, router_info.get("reason"), first_value(item, ("reason",), not_obs))
        elif intent == "CLOSE_LONG":
            exit_reason = first_observed(exit_reason, router_info.get("reason"), first_value(item, ("reason",), not_obs))
        else:
            entry_reason = first_observed(entry_reason, first_value(item, ("reason",), not_obs))
        probe_type = first_observed(
            probe_type_of(item),
            router_info.get("probe_type"),
            probe_type_from_reason(entry_reason),
            probe_type_from_reason(exit_reason),
        )
        qty = as_float(first_value(item, ("qty", "amount", "sz"), not_obs))
        price = as_float(first_value(item, ("price", "px"), not_obs))
        notional = as_float(first_value(item, ("notional_usdt", "notional", "cost"), not_obs))
        if notional is None and qty is not None and price is not None:
            notional = qty * price
        fee = as_float(first_value(item, ("fee_usdt", "fee", "commission_usdt", "commission"), not_obs))
        source_file = str(trade_path.relative_to(OUT))
        observe_symbol_price(symbol, timestamp, price, f"trade:{source_file}")
        event = {
            "event_id": f"{source_file}:{idx + 1}",
            "run_id": run_id,
            "source_file": source_file,
            "row_number": idx + 1,
            "timestamp": timestamp,
            "ts_dt": parse_dt_utc(timestamp) or parse_run_time(run_id),
            "symbol": symbol,
            "intent": intent,
            "side": flatten_value(first_value(item, ("side",), not_obs)),
            "qty": qty,
            "price": price,
            "notional_usdt": notional,
            "fee_usdt": fee,
            "entry_reason": entry_reason,
            "exit_reason": exit_reason,
            "probe_type": probe_type,
            "raw_item": dict(item),
            "router_info": router_info,
        }
        event["remaining_qty"] = qty
        event["remaining_fee_usdt"] = fee
        event["matched_qty"] = 0.0
        return event

    def is_probe_trade_row(row):
        return (
            row.get("entry_reason") in PROBE_TYPES
            or row.get("exit_reason") in PROBE_EXIT_REASONS
            or row.get("probe_type") in PROBE_TYPES
            or row.get("probe_type") == "probe"
        )

    def record_trade_summary_row(row, lifecycle_diagnosis):
        trade_rows.append(row)
        if not is_probe_trade_row(row):
            return
        probe_trade_rows.append(row)
        lifecycle_rows.append({
            "ts_utc": first_observed(row.get("exit_ts"), row.get("entry_ts"), row.get("timestamp")),
            "run_id": row["run_id"],
            "symbol": row["symbol"],
            "probe_type": row["probe_type"],
            "entry_ts": row.get("entry_ts", not_obs),
            "entry_px": row.get("entry_px", not_obs),
            "exit_ts": row.get("exit_ts", not_obs),
            "exit_px": row.get("exit_px", not_obs),
            "exit_reason": row.get("exit_reason", not_obs),
            "gross_bps": row.get("gross_bps", not_obs),
            "net_bps": row.get("net_bps", not_obs),
            "remaining_value_usdt": row.get("remaining_value_usdt", not_obs),
            "dust_threshold_usdt": row.get("dust_threshold_usdt", not_obs),
            "state_still_present_after_close": not_obs,
            "profit_taking_state_present": str(state_present(profit_state, row["symbol"])).lower(),
            "highest_px_state_present": str(state_present(highest_state, row["symbol"])).lower(),
            "stop_loss_state_present": str(state_present(stop_state, row["symbol"])).lower(),
            "fixed_stop_loss_state_present": str(state_present(fixed_stop_state, row["symbol"])).lower(),
            "repeated_exit_signal_after_flat": "false",
            "diagnosis": lifecycle_diagnosis,
        })

    def prorate(value, part, total):
        if value is None or part is None or total is None or total <= 0:
            return None
        return value * part / total

    def dust_threshold_for_symbol(symbol):
        dust = latest_dust_by_symbol.get(symbol, {})
        observed = as_float(dust.get("dust_threshold_usdt"))
        return max(value for value in (global_dust_threshold_usdt, observed) if value is not None)

    def value_for_qty(qty, *prices):
        qty_f = as_float(qty)
        if qty_f is None:
            return None
        for price in prices:
            price_f = as_float(price)
            if price_f is not None:
                return qty_f * price_f
        return None

    def is_dust_value(value, threshold):
        value_f = as_float(value)
        threshold_f = as_float(threshold)
        return value_f is not None and threshold_f is not None and value_f < threshold_f

    def append_dust_residual_row(status, symbol, qty, residual_value, dust_threshold, diagnosis, open_event=None, close_event=None, reference_px=None):
        event = close_event or open_event or {}
        raw_payload = {}
        if open_event:
            raw_payload["entry_trade"] = sanitize_obj(open_event.get("raw_item", {}))
            raw_payload["entry_router_decision"] = sanitize_obj(open_event.get("router_info", {}))
        if close_event:
            raw_payload["exit_trade"] = sanitize_obj(close_event.get("raw_item", {}))
            raw_payload["exit_router_decision"] = sanitize_obj(close_event.get("router_info", {}))
        raw_payload["diagnosis"] = diagnosis
        entry_reason = first_observed(open_event.get("entry_reason") if open_event else not_obs, open_event.get("router_info", {}).get("reason") if open_event else not_obs)
        exit_reason = first_observed(close_event.get("exit_reason") if close_event else not_obs, close_event.get("router_info", {}).get("reason") if close_event else not_obs)
        probe_type = first_observed(
            open_event.get("probe_type") if open_event else not_obs,
            close_event.get("probe_type") if close_event else not_obs,
            probe_type_from_reason(entry_reason),
            probe_type_from_reason(exit_reason),
        )
        row = {
            "run_id": first_observed(event.get("run_id"), open_event.get("run_id") if open_event else not_obs),
            "source_file": ";".join(part for part in (open_event.get("source_file") if open_event else "", close_event.get("source_file") if close_event else "") if part),
            "row_number": ";".join(str(part) for part in (open_event.get("row_number") if open_event else "", close_event.get("row_number") if close_event else "") if part not in ("", None)),
            "timestamp": first_observed(event.get("timestamp"), open_event.get("timestamp") if open_event else not_obs),
            "symbol": symbol,
            "side": first_observed(open_event.get("side") if open_event else not_obs, close_event.get("side") if close_event else not_obs),
            "qty": fmt_num(qty, 12),
            "price": fmt_num(reference_px, 10),
            "entry_ts": open_event.get("timestamp", not_obs) if open_event else not_obs,
            "entry_px": fmt_num(open_event.get("price") if open_event else None, 10),
            "exit_ts": close_event.get("timestamp", not_obs) if close_event else not_obs,
            "exit_px": fmt_num(close_event.get("price") if close_event else None, 10),
            "entry_reason": entry_reason,
            "exit_reason": exit_reason,
            "probe_type": probe_type,
            "roundtrip_status": status,
            "gross_pnl_usdt": not_obs,
            "fee_total_usdt": not_obs,
            "net_pnl_usdt": not_obs,
            "gross_bps": not_obs,
            "net_bps": not_obs,
            "hold_minutes": not_obs,
            "remaining_value_usdt": fmt_num(residual_value, 12),
            "dust_threshold_usdt": fmt_num(dust_threshold, 12),
            "diagnosis": diagnosis,
            "raw_json": safe_json(raw_payload),
        }
        key = (row["roundtrip_status"], row["source_file"], row["row_number"], row["symbol"], row["qty"], row["remaining_value_usdt"])
        if key not in dust_residual_row_keys:
            dust_residual_row_keys.add(key)
            dust_residual_roundtrip_rows.append(row)
        if "open" in status or "position" in status:
            dust_residual_position_keys.add((symbol, row["entry_ts"], row["qty"], row["remaining_value_usdt"]))
        return row

    def matched_roundtrip_row(open_event, close_event, matched_qty, open_fee_alloc, close_fee_alloc, open_remaining_after):
        entry_px = open_event["price"]
        exit_px = close_event["price"]
        entry_notional = entry_px * matched_qty if entry_px is not None else None
        gross_pnl = (exit_px - entry_px) * matched_qty if entry_px is not None and exit_px is not None else None
        fee_total = open_fee_alloc + close_fee_alloc if open_fee_alloc is not None and close_fee_alloc is not None else None
        net_pnl = gross_pnl - fee_total if gross_pnl is not None and fee_total is not None else None
        gross_bps = gross_pnl / entry_notional * 10000.0 if gross_pnl is not None and entry_notional else None
        net_bps = net_pnl / entry_notional * 10000.0 if net_pnl is not None and entry_notional else None
        hold_minutes = None
        if open_event["ts_dt"] and close_event["ts_dt"]:
            hold_minutes = (close_event["ts_dt"] - open_event["ts_dt"]).total_seconds() / 60.0
        residual_value = open_remaining_after * exit_px if exit_px is not None and open_remaining_after is not None and open_remaining_after > 0 else 0.0
        dust = latest_dust_by_symbol.get(open_event["symbol"], {})
        dust_threshold = dust_threshold_for_symbol(open_event["symbol"])
        if residual_value == 0.0:
            raw_dust = as_float(dust.get("raw_held_value_usdt"))
            if raw_dust is not None:
                residual_value = raw_dust
        entry_reason = first_observed(open_event.get("entry_reason"), open_event.get("router_info", {}).get("reason"))
        exit_reason = first_observed(close_event.get("exit_reason"), close_event.get("router_info", {}).get("reason"))
        probe_type = first_observed(
            open_event.get("probe_type"),
            close_event.get("probe_type"),
            probe_type_from_reason(entry_reason),
            probe_type_from_reason(exit_reason),
        )
        raw_payload = {
            "entry_trade": sanitize_obj(open_event["raw_item"]),
            "exit_trade": sanitize_obj(close_event["raw_item"]),
            "entry_router_decision": sanitize_obj(open_event.get("router_info", {})),
            "exit_router_decision": sanitize_obj(close_event.get("router_info", {})),
            "matched_qty": matched_qty,
        }
        exit_router = close_event.get("router_info", {}) if isinstance(close_event.get("router_info"), dict) else {}
        hold_hours = hold_minutes / 60.0 if hold_minutes is not None else None
        exit_priority = first_observed(exit_router.get("exit_priority"), exit_priority_for_reason(exit_reason))
        min_hold_hours = first_observed(exit_router.get("min_hold_hours"), config_number("swing_min_hold_hours"), not_obs)
        exit_allowed_before_min_hold = first_observed(
            exit_router.get("exit_allowed_before_min_hold"),
            str(exit_priority == "hard").lower() if exit_priority != not_obs else not_obs,
        )
        exit_blocked_by_min_hold = first_observed(exit_router.get("exit_blocked_by_min_hold"), "false")
        min_hold_block_reason = first_observed(exit_router.get("min_hold_block_reason"), "")
        would_status, would_24h_net_bps_raw, would_24h_gross_bps_raw, would_obs = estimate_held_24h_outcome(
            open_event["symbol"],
            open_event.get("ts_dt"),
            entry_px,
            gross_bps,
            net_bps,
        )
        if would_obs:
            raw_payload["would_have_held_24h_observation"] = sanitize_obj(
                {
                    "ts_utc": would_obs.get("ts_utc"),
                    "price": would_obs.get("price"),
                    "source": would_obs.get("source"),
                    "gross_bps": would_24h_gross_bps_raw,
                }
            )
        actual_net_f = as_float(net_bps)
        early_cost_calc = (
            would_24h_net_bps_raw - actual_net_f
            if would_24h_net_bps_raw is not None and actual_net_f is not None
            else None
        )
        early_cost_bps = first_observed(exit_router.get("early_exit_opportunity_cost_bps"), early_cost_calc, not_obs)
        return {
            "run_id": close_event["run_id"],
            "source_file": f"{open_event['source_file']};{close_event['source_file']}",
            "row_number": f"{open_event['row_number']};{close_event['row_number']}",
            "timestamp": close_event["timestamp"],
            "symbol": open_event["symbol"],
            "side": "buy/sell",
            "qty": fmt_num(matched_qty, 12),
            "price": fmt_num(exit_px, 10),
            "entry_ts": open_event["timestamp"],
            "entry_px": fmt_num(entry_px, 10),
            "exit_ts": close_event["timestamp"],
            "exit_px": fmt_num(exit_px, 10),
            "entry_reason": entry_reason,
            "exit_reason": exit_reason,
            "probe_type": probe_type,
            "roundtrip_status": "closed",
            "gross_pnl_usdt": fmt_num(gross_pnl, 12),
            "fee_total_usdt": fmt_num(fee_total, 12),
            "net_pnl_usdt": fmt_num(net_pnl, 12),
            "gross_bps": fmt_num(gross_bps, 4),
            "net_bps": fmt_num(net_bps, 4),
            "hold_minutes": fmt_num(hold_minutes, 3),
            "hold_hours": fmt_num(hold_hours, 4),
            "min_hold_hours": flatten_value(min_hold_hours),
            "exit_allowed_before_min_hold": bool_text(exit_allowed_before_min_hold),
            "exit_blocked_by_min_hold": bool_text(exit_blocked_by_min_hold),
            "exit_priority": exit_priority,
            "min_hold_block_reason": flatten_value(min_hold_block_reason),
            "early_exit_opportunity_cost_bps": flatten_value(early_cost_bps),
            "would_have_held_24h_status": would_status,
            "would_have_held_24h_net_bps": fmt_num(would_24h_net_bps_raw, 4),
            "remaining_value_usdt": fmt_num(residual_value, 12),
            "dust_threshold_usdt": fmt_num(dust_threshold, 12),
            "raw_json": safe_json(raw_payload),
        }

    def open_trade_row(open_event, status):
        remaining_qty = open_event.get("remaining_qty")
        remaining_value = remaining_qty * open_event["price"] if remaining_qty is not None and open_event["price"] is not None else None
        dust_threshold = dust_threshold_for_symbol(open_event["symbol"])
        raw_payload = {
            "entry_trade": sanitize_obj(open_event["raw_item"]),
            "entry_router_decision": sanitize_obj(open_event.get("router_info", {})),
            "remaining_qty": remaining_qty,
        }
        return {
            "run_id": open_event["run_id"],
            "source_file": open_event["source_file"],
            "row_number": open_event["row_number"],
            "timestamp": open_event["timestamp"],
            "symbol": open_event["symbol"],
            "side": open_event["side"],
            "qty": fmt_num(remaining_qty, 12),
            "price": fmt_num(open_event["price"], 10),
            "entry_ts": open_event["timestamp"],
            "entry_px": fmt_num(open_event["price"], 10),
            "exit_ts": not_obs,
            "exit_px": not_obs,
            "entry_reason": first_observed(open_event.get("entry_reason"), open_event.get("router_info", {}).get("reason")),
            "exit_reason": not_obs,
            "probe_type": open_event["probe_type"],
            "roundtrip_status": status,
            "gross_pnl_usdt": not_obs,
            "fee_total_usdt": fmt_num(open_event.get("remaining_fee_usdt"), 12),
            "net_pnl_usdt": not_obs,
            "gross_bps": not_obs,
            "net_bps": not_obs,
            "hold_minutes": not_obs,
            "remaining_value_usdt": fmt_num(remaining_value, 12),
            "dust_threshold_usdt": fmt_num(dust_threshold, 12),
            "raw_json": safe_json(raw_payload),
        }

    def unmatched_close_row(close_event):
        raw_payload = {
            "exit_trade": sanitize_obj(close_event["raw_item"]),
            "exit_router_decision": sanitize_obj(close_event.get("router_info", {})),
            "remaining_qty": close_event.get("remaining_qty"),
        }
        return {
            "run_id": close_event["run_id"],
            "source_file": close_event["source_file"],
            "row_number": close_event["row_number"],
            "timestamp": close_event["timestamp"],
            "symbol": close_event["symbol"],
            "side": close_event["side"],
            "qty": fmt_num(close_event.get("remaining_qty"), 12),
            "price": fmt_num(close_event["price"], 10),
            "entry_ts": not_obs,
            "entry_px": not_obs,
            "exit_ts": close_event["timestamp"],
            "exit_px": fmt_num(close_event["price"], 10),
            "entry_reason": not_obs,
            "exit_reason": first_observed(close_event.get("exit_reason"), close_event.get("router_info", {}).get("reason")),
            "probe_type": close_event["probe_type"],
            "roundtrip_status": "unmatched_close",
            "gross_pnl_usdt": not_obs,
            "fee_total_usdt": fmt_num(close_event.get("fee_usdt"), 12),
            "net_pnl_usdt": not_obs,
            "gross_bps": not_obs,
            "net_bps": not_obs,
            "hold_minutes": not_obs,
            "remaining_value_usdt": not_obs,
            "dust_threshold_usdt": not_obs,
            "raw_json": safe_json(raw_payload),
        }

    for trade_path in trade_paths:
        run_id = trade_path.parent.name
        try:
            with trade_path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
                reader = csv.DictReader(fh)
                file_rows = 0
                counted_rows = 0
                notional_total = 0.0
                fee_total = 0.0
                slippage_total = 0.0
                trade_warnings = []
                for idx, item in enumerate(reader):
                    file_rows += 1
                    event = build_trade_event(trade_path, run_id, idx, item)
                    raw_trade_events.append(event)
                    symbol_value = first_value(item, ("symbol", "instId", "inst_id", "instrument"), event.get("symbol"))
                    fill_metrics_rows.append({
                        "run_id": csv_null(first_value(item, ("run_id",), run_id)),
                        "ts_utc": csv_null(first_value(item, ("ts_utc", "timestamp", "ts", "time"), event.get("timestamp"))),
                        "symbol": csv_null(symbol_value),
                        "normalized_symbol": csv_null(first_value(item, ("normalized_symbol",), normalize_trade_symbol_for_contract(symbol_value))),
                        "side": csv_null(first_value(item, ("side",), event.get("side"))),
                        "action": csv_null(first_value(item, ("action", "intent"), event.get("intent"))),
                        "qty": csv_null(first_value(item, ("qty", "amount", "sz"), event.get("qty"))),
                        "price": csv_null(first_value(item, ("price", "px"), event.get("price"))),
                        "notional_usdt": csv_null(first_value(item, ("notional_usdt", "notional", "cost"), event.get("notional_usdt"))),
                        "fee": csv_null(first_value(item, ("fee", "commission"), not_obs)),
                        "fee_ccy": csv_null(first_value(item, ("fee_ccy", "feeCcy", "commission_asset"), not_obs)),
                        "fee_usdt": csv_null(first_value(item, ("fee_usdt", "commission_usdt"), event.get("fee_usdt"))),
                        "slippage_usdt": csv_null(first_value(item, ("slippage_usdt", "slippage"), not_obs)),
                        "order_id": csv_null(first_value(item, ("order_id", "ord_id", "cl_ord_id"), not_obs)),
                        "trade_id": csv_null(first_value(item, ("trade_id", "fill_id"), not_obs)),
                        "strategy_id": csv_null(first_value(item, ("strategy_id",), "v5")),
                        "position_id": csv_null(first_value(item, ("position_id",), not_obs)),
                        "trade_export_schema_version": first_value(item, ("trade_export_schema_version",), TRADE_EXPORT_SCHEMA_VERSION),
                    })
                    notional = as_float(event.get("notional_usdt"))
                    if notional is None or abs(notional) <= 0.0:
                        continue
                    counted_rows += 1
                    notional_total += abs(float(notional))
                    fee_value = as_float(event.get("fee_usdt"))
                    slippage_value = as_float(first_value(item, ("slippage_usdt", "slippage"), not_obs))
                    if fee_value is None:
                        trade_warnings.append(f"trades.csv row {idx + 2} missing fee_usdt")
                    if slippage_value is None:
                        trade_warnings.append(f"trades.csv row {idx + 2} missing slippage_usdt")
                    fee_total += float(fee_value or 0.0)
                    slippage_total += float(slippage_value or 0.0)
                trade_file_stats_by_run[run_id] = {
                    "run_id": run_id,
                    "trades_file_exists": True,
                    "trades_file_rows": file_rows,
                    "trades_counted_rows": counted_rows,
                    "trades_turnover_usdt": notional_total,
                    "trades_fees_usdt_total": fee_total,
                    "trades_slippage_usdt_total": slippage_total,
                    "trades_cost_usdt_total": fee_total + slippage_total,
                    "parse_error": "",
                    "trade_metrics_warning": "; ".join(trade_warnings),
                    "trade_metrics_warning_count": len(trade_warnings),
                    "source_file": str(trade_path.relative_to(OUT)),
                }
                trade_metrics_rows.append({
                    "run_id": run_id,
                    "trades_file_exists": "true",
                    "trades_file_rows": file_rows,
                    "trades_counted_rows": counted_rows,
                    "num_trades": counted_rows,
                    "turnover_usdt": fmt_num(notional_total, 12),
                    "fees_usdt_total": fmt_num(fee_total, 12),
                    "slippage_usdt_total": fmt_num(slippage_total, 12),
                    "cost_usdt_total": fmt_num(fee_total + slippage_total, 12),
                    "fills_count_today": counted_rows,
                    "trade_metrics_warning": "; ".join(trade_warnings),
                    "trade_metrics_warning_count": len(trade_warnings),
                    "trade_export_schema_version": TRADE_EXPORT_SCHEMA_VERSION,
                    "summary_metrics_version": SUMMARY_METRICS_VERSION,
                })
        except Exception as exc:
            trade_read_errors += 1
            trade_file_stats_by_run[run_id] = {
                "run_id": run_id,
                "trades_file_exists": True,
                "trades_file_rows": 0,
                "trades_counted_rows": 0,
                "trades_turnover_usdt": 0.0,
                "trades_fees_usdt_total": 0.0,
                "trades_slippage_usdt_total": 0.0,
                "trades_cost_usdt_total": 0.0,
                "parse_error": repr(exc),
                "trade_metrics_warning": f"trades.csv parse failed: {exc!r}",
                "trade_metrics_warning_count": 1,
                "source_file": str(trade_path.relative_to(OUT)),
            }
            trade_metrics_rows.append({
                "run_id": run_id,
                "trades_file_exists": "true",
                "trades_file_rows": 0,
                "trades_counted_rows": 0,
                "num_trades": 0,
                "turnover_usdt": "null",
                "fees_usdt_total": "null",
                "slippage_usdt_total": "null",
                "cost_usdt_total": "null",
                "fills_count_today": 0,
                "trade_metrics_warning": f"trades.csv parse failed: {exc!r}",
                "trade_metrics_warning_count": 1,
                "trade_export_schema_version": TRADE_EXPORT_SCHEMA_VERSION,
                "summary_metrics_version": SUMMARY_METRICS_VERSION,
            })
            collection_errors.append({"source": str(trade_path), "error": f"trade_csv: {exc!r}"})

    for run_id, stats in sorted(trade_file_stats_by_run.items()):
        summary_path = OUT / "raw" / "recent_runs" / run_id / "summary.json"
        summary = load_json(summary_path) if summary_path.is_file() else None
        if not isinstance(summary, dict):
            summary = {}
        summary_num_trades = as_int(summary.get("num_trades"))
        summary_turnover = as_float(first_observed(summary.get("turnover_usdt"), summary.get("notional_usdt_total")))
        summary_fees = as_float(first_observed(summary.get("fees_usdt_total"), summary.get("fee_usdt_total")))
        summary_slippage = as_float(summary.get("slippage_usdt_total"))
        summary_cost = as_float(summary.get("cost_usdt_total"))
        count_mismatch = int(stats["trades_counted_rows"]) != int(summary_num_trades)
        cost_mismatch = (
            summary_cost is not None
            and abs(float(stats["trades_cost_usdt_total"]) - float(summary_cost)) > 1e-9
        )
        high_mismatch = int(stats["trades_counted_rows"]) > 0 and int(summary_num_trades) == 0
        if not (count_mismatch or cost_mismatch):
            continue
        diagnosis = "summary_trade_count_mismatch"
        if high_mismatch:
            diagnosis = "high_issue_summary_trade_count_mismatch"
        elif cost_mismatch:
            diagnosis = "summary_trade_cost_mismatch"
        row = {
            "run_id": run_id,
            "source_file": stats["source_file"],
            "trades_file_exists": str(bool(stats["trades_file_exists"])).lower(),
            "trades_file_rows": stats["trades_file_rows"],
            "trades_counted_rows": stats["trades_counted_rows"],
            "summary_num_trades": summary_num_trades if summary_path.is_file() else not_obs,
            "trades_turnover_usdt": fmt_num(stats["trades_turnover_usdt"], 12),
            "summary_turnover_usdt": fmt_num(summary_turnover, 12),
            "trades_fees_usdt_total": fmt_num(stats["trades_fees_usdt_total"], 12),
            "summary_fees_usdt_total": fmt_num(summary_fees, 12),
            "trades_slippage_usdt_total": fmt_num(stats["trades_slippage_usdt_total"], 12),
            "summary_slippage_usdt_total": fmt_num(summary_slippage, 12),
            "trades_cost_usdt_total": fmt_num(stats["trades_cost_usdt_total"], 12),
            "summary_cost_usdt_total": fmt_num(summary_cost, 12),
            "count_mismatch": str(bool(count_mismatch)).lower(),
            "cost_mismatch": str(bool(cost_mismatch)).lower(),
            "high_issue": str(bool(high_mismatch)).lower(),
            "diagnosis": diagnosis,
            "parse_error": stats.get("parse_error") or "",
            "trade_metrics_warning": stats.get("trade_metrics_warning") or "",
        }
        summary_trade_count_mismatch_rows.append(row)
        if high_mismatch:
            add_issue(
                "high",
                "summary_trade_count_mismatch",
                "trades.csv has counted fill rows but summary.json reports num_trades=0.",
                {
                    "run_id": run_id,
                    "source_file": stats["source_file"],
                    "trades_file_rows": stats["trades_file_rows"],
                    "trades_counted_rows": stats["trades_counted_rows"],
                    "summary_num_trades": summary_num_trades,
                    "summary_path": str(summary_path.relative_to(OUT)) if summary_path.exists() else not_obs,
                },
            )

    def load_order_lifecycle_rows():
        rows = []
        seen = set()
        paths = []
        reports_path = OUT / "raw" / "reports" / "order_lifecycle.csv"
        if reports_path.is_file():
            paths.append(reports_path)
        paths.extend(sorted((OUT / "raw" / "recent_runs").glob("*/order_lifecycle.csv")))
        for path in paths:
            try:
                with path.open("r", encoding="utf-8", newline="") as fh:
                    for row in csv.DictReader(fh):
                        if not row:
                            continue
                        run_id = str(row.get("run_id") or path.parent.name or "").strip()
                        lifecycle_id = str(row.get("lifecycle_id") or "").strip()
                        key = lifecycle_id or "|".join([
                            run_id,
                            str(row.get("cl_ord_id") or ""),
                            str(row.get("symbol") or ""),
                            str(row.get("decision_ts") or row.get("submit_ts") or ""),
                        ])
                        if key in seen:
                            continue
                        seen.add(key)
                        item = {field: csv_null(row.get(field, "")) for field in ORDER_LIFECYCLE_FIELDS}
                        item["run_id"] = item.get("run_id") or run_id
                        item["schema_version"] = item.get("schema_version") or "v5.order_lifecycle.v1"
                        rows.append(item)
            except Exception as exc:
                collection_errors.append({"source": str(path), "error": f"order_lifecycle_csv: {exc!r}"})
        return rows

    def lifecycle_nullish(value):
        return str(value if value is not None else "").strip().lower() in {"", "null", "none", "nan", not_obs}

    def lifecycle_identity(value):
        return "" if lifecycle_nullish(value) else str(value).strip()

    def lifecycle_field_needs_fill(row, field):
        value = row.get(field)
        if lifecycle_nullish(value):
            return True
        if field in {"fill_count", "fee", "fee_usdt", "filled_qty", "avg_fill_px", "fill_px", "notional_usdt"}:
            number = as_float(value)
            return number is not None and abs(number) <= 0.0
        return False

    def lifecycle_dedupe_fills(rows):
        out = []
        seen_keys = set()
        for row in rows:
            key = (
                lifecycle_identity(row.get("run_id")),
                lifecycle_identity(row.get("order_id")),
                lifecycle_identity(row.get("trade_id")),
                lifecycle_identity(row.get("ts_utc")),
                lifecycle_identity(row.get("symbol")),
                lifecycle_identity(row.get("qty")),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out.append(row)
        return out

    def lifecycle_same_symbol_side_intent(row, fill):
        row_symbol = normalize_trade_symbol_for_contract(first_observed(row.get("symbol"), row.get("normalized_symbol")))
        fill_symbol = normalize_trade_symbol_for_contract(first_observed(fill.get("symbol"), fill.get("normalized_symbol")))
        if row_symbol != fill_symbol:
            return False
        row_side = lifecycle_identity(row.get("side")).lower()
        fill_side = lifecycle_identity(fill.get("side")).lower()
        if row_side and fill_side and row_side != fill_side:
            return False
        row_intent = lifecycle_identity(row.get("intent")).upper()
        fill_action = lifecycle_identity(fill.get("action")).upper()
        if row_intent and fill_action and row_intent != fill_action:
            return False
        return True

    def numeric_close(left, right, tolerance):
        left_num = as_float(left)
        right_num = as_float(right)
        return left_num is not None and right_num is not None and abs(left_num - right_num) <= tolerance

    def matching_fill_rows_for_lifecycle(row, candidates):
        cl_ord_id = lifecycle_identity(row.get("cl_ord_id"))
        exchange_order_id = lifecycle_identity(row.get("exchange_order_id"))
        order_ids = {value for value in (cl_ord_id, exchange_order_id) if value}
        trade_ids = {
            part.strip()
            for part in lifecycle_identity(row.get("trade_ids")).replace(",", ";").split(";")
            if part.strip()
        }
        exact = []
        for fill in candidates:
            order_id = lifecycle_identity(fill.get("order_id"))
            trade_id = lifecycle_identity(fill.get("trade_id"))
            if order_id and order_id in order_ids:
                exact.append(fill)
            elif trade_id and trade_id in trade_ids:
                exact.append(fill)
        if exact:
            return lifecycle_dedupe_fills(exact)
        soft = [fill for fill in candidates if lifecycle_same_symbol_side_intent(row, fill)]
        if not soft:
            return []
        price_filtered = [
            fill for fill in soft
            if numeric_close(first_observed(row.get("avg_fill_px"), row.get("fill_px")), fill.get("price"), 1e-6)
        ]
        qty_filtered = [
            fill for fill in (price_filtered or soft)
            if numeric_close(row.get("filled_qty"), fill.get("qty"), 1e-12)
        ]
        for group in (qty_filtered, price_filtered, soft):
            deduped = lifecycle_dedupe_fills(group)
            if len(deduped) == 1:
                return deduped
        return []

    def aggregate_lifecycle_fill_metrics(fills):
        ordered = sorted(fills, key=lambda row: (lifecycle_identity(row.get("ts_utc")), lifecycle_identity(row.get("trade_id"))))
        ts_values = [lifecycle_identity(row.get("ts_utc")) for row in ordered if lifecycle_identity(row.get("ts_utc"))]
        qty_sum = 0.0
        px_qty_sum = 0.0
        notional_sum = 0.0
        fee_usdt_sum = 0.0
        fee_values = []
        fee_ccys = set()
        trade_ids = []
        first_px = None
        for fill in ordered:
            px = as_float(fill.get("price"))
            qty = as_float(fill.get("qty"))
            if px is not None and first_px is None:
                first_px = px
            if px is not None and qty is not None and qty > 0:
                qty_sum += qty
                px_qty_sum += px * qty
            notional = as_float(fill.get("notional_usdt"))
            if notional is not None:
                notional_sum += abs(float(notional))
            fee_usdt = as_float(fill.get("fee_usdt"))
            if fee_usdt is not None:
                fee_usdt_sum += abs(float(fee_usdt))
            fee = as_float(fill.get("fee"))
            if fee is not None:
                fee_values.append(float(fee))
            fee_ccy = lifecycle_identity(fill.get("fee_ccy")).upper()
            if fee_ccy:
                fee_ccys.add(fee_ccy)
            trade_id = lifecycle_identity(fill.get("trade_id"))
            if trade_id:
                trade_ids.append(trade_id)
        avg_px = (px_qty_sum / qty_sum) if qty_sum > 0 else first_px
        fee_ccy = next(iter(fee_ccys)) if len(fee_ccys) == 1 else ("mixed" if fee_ccys else "")
        return {
            "first_fill_ts": ts_values[0] if ts_values else "",
            "last_fill_ts": ts_values[-1] if ts_values else "",
            "fill_px": first_px,
            "avg_fill_px": avg_px,
            "filled_qty": qty_sum if qty_sum > 0 else None,
            "fee": sum(fee_values) if fee_values and fee_ccy != "mixed" else None,
            "fee_ccy": fee_ccy,
            "fee_usdt": fee_usdt_sum if fee_usdt_sum > 0 else None,
            "notional_usdt": notional_sum if notional_sum > 0 else None,
            "trade_ids": ";".join(dict.fromkeys(trade_ids)),
            "fill_count": len(ordered),
        }

    def backfill_order_lifecycle_from_fill_metrics(rows):
        fills_by_run = defaultdict(list)
        for fill in fill_metrics_rows:
            run_id = lifecycle_identity(fill.get("run_id"))
            if run_id:
                fills_by_run[run_id].append(fill)
        out = []
        for row in rows:
            enriched = dict(row)
            matches = matching_fill_rows_for_lifecycle(enriched, fills_by_run.get(lifecycle_identity(enriched.get("run_id")), []))
            if matches:
                aggregate = aggregate_lifecycle_fill_metrics(matches)
                for field, value in aggregate.items():
                    if value is not None and lifecycle_field_needs_fill(enriched, field):
                        enriched[field] = csv_null(value)
            out.append(enriched)
        return out

    order_lifecycle_rows = backfill_order_lifecycle_from_fill_metrics(load_order_lifecycle_rows())
    order_lifecycle_trade_metric_fill_count = sum(
        as_int(first_observed(row.get("trades_counted_rows"), row.get("num_trades"), row.get("fills_count_today"))) or 0
        for row in trade_metrics_rows
    )
    order_lifecycle_missing_high_issue = (
        order_lifecycle_trade_metric_fill_count > 0
        and len(order_lifecycle_rows) == 0
    )
    if order_lifecycle_missing_high_issue:
        add_issue(
            "high",
            "order_lifecycle_missing_for_trades",
            "trade_metrics has counted trades but order_lifecycle.csv is empty.",
            {"trade_metric_fill_count": order_lifecycle_trade_metric_fill_count},
        )

    raw_trade_events.sort(key=lambda event: (
        event["symbol"],
        event["ts_dt"] or dt.datetime.min.replace(tzinfo=dt.timezone.utc),
        event["source_file"],
        event["row_number"],
    ))
    open_lots = defaultdict(deque)
    qty_eps = 1e-12
    for event in raw_trade_events:
        if event["intent"] == "OPEN_LONG":
            if event["qty"] is None or event["qty"] <= qty_eps:
                record_trade_summary_row(open_trade_row(event, "open_qty_not_observable"), "probe_trade_open_qty_not_observable")
                covered_trade_event_ids.add(event["event_id"])
                continue
            open_lots[event["symbol"]].append(event)
            continue
        if event["intent"] != "CLOSE_LONG":
            record_trade_summary_row(open_trade_row(event, "trade_intent_not_observable"), "probe_trade_intent_not_observable")
            covered_trade_event_ids.add(event["event_id"])
            continue
        remaining_close_qty = event["qty"]
        if remaining_close_qty is None or remaining_close_qty <= qty_eps:
            record_trade_summary_row(unmatched_close_row(event), "probe_close_qty_not_observable")
            covered_trade_event_ids.add(event["event_id"])
            continue
        while remaining_close_qty > qty_eps and open_lots[event["symbol"]]:
            open_event = open_lots[event["symbol"]][0]
            open_remaining = open_event.get("remaining_qty")
            if open_remaining is None or open_remaining <= qty_eps:
                open_lots[event["symbol"]].popleft()
                continue
            dust_threshold = dust_threshold_for_symbol(event["symbol"])
            open_value_before = value_for_qty(open_remaining, event.get("price"), open_event.get("price"))
            if is_dust_value(open_value_before, dust_threshold):
                append_dust_residual_row(
                    "open_dust_residual_ignored",
                    event["symbol"],
                    open_remaining,
                    open_value_before,
                    dust_threshold,
                    "open_lot_value_below_dust_threshold_before_fifo_match",
                    open_event=open_event,
                    close_event=event,
                    reference_px=event.get("price") or open_event.get("price"),
                )
                covered_trade_event_ids.add(open_event["event_id"])
                open_event["remaining_qty"] = 0.0
                open_lots[event["symbol"]].popleft()
                continue
            matched_qty = min(open_remaining, remaining_close_qty)
            open_fee_alloc = prorate(open_event.get("remaining_fee_usdt"), matched_qty, open_remaining)
            close_fee_alloc = prorate(event.get("fee_usdt"), matched_qty, event["qty"])
            open_remaining_after = max(0.0, open_remaining - matched_qty)
            row = matched_roundtrip_row(open_event, event, matched_qty, open_fee_alloc, close_fee_alloc, open_remaining_after)
            matched_notional = value_for_qty(matched_qty, open_event.get("price"), event.get("price"))
            if is_dust_value(matched_notional, dust_threshold):
                append_dust_residual_row(
                    "dust_residual_roundtrip_ignored",
                    event["symbol"],
                    matched_qty,
                    matched_notional,
                    dust_threshold,
                    "matched_notional_below_dust_threshold_excluded_from_roundtrip_stats",
                    open_event=open_event,
                    close_event=event,
                    reference_px=event.get("price") or open_event.get("price"),
                )
            else:
                record_trade_summary_row(row, "probe_roundtrip_closed" if is_probe_trade_row(row) else "roundtrip_closed")
            covered_trade_event_ids.add(open_event["event_id"])
            covered_trade_event_ids.add(event["event_id"])
            open_event["matched_qty"] += matched_qty
            event["matched_qty"] += matched_qty
            open_event["remaining_qty"] = open_remaining_after
            if open_event.get("remaining_fee_usdt") is not None and open_fee_alloc is not None:
                open_event["remaining_fee_usdt"] = max(0.0, open_event["remaining_fee_usdt"] - open_fee_alloc)
            remaining_close_qty = max(0.0, remaining_close_qty - matched_qty)
            event["remaining_qty"] = remaining_close_qty
            if open_event["remaining_qty"] <= qty_eps:
                open_lots[event["symbol"]].popleft()
            else:
                residual_value = value_for_qty(open_event["remaining_qty"], event.get("price"), open_event.get("price"))
                if is_dust_value(residual_value, dust_threshold):
                    append_dust_residual_row(
                        "open_dust_residual_ignored",
                        event["symbol"],
                        open_event["remaining_qty"],
                        residual_value,
                        dust_threshold,
                        "open_lot_residual_value_below_dust_threshold_after_fifo_match",
                        open_event=open_event,
                        close_event=event,
                        reference_px=event.get("price") or open_event.get("price"),
                    )
                    open_event["remaining_qty"] = 0.0
                    open_lots[event["symbol"]].popleft()
        if remaining_close_qty > qty_eps:
            event["remaining_qty"] = remaining_close_qty
            dust_threshold = dust_threshold_for_symbol(event["symbol"])
            close_value = value_for_qty(remaining_close_qty, event.get("price"))
            if is_dust_value(close_value, dust_threshold):
                append_dust_residual_row(
                    "dust_close_ignored",
                    event["symbol"],
                    remaining_close_qty,
                    close_value,
                    dust_threshold,
                    "close_qty_value_below_dust_threshold_without_effective_open_lot",
                    close_event=event,
                    reference_px=event.get("price"),
                )
            else:
                record_trade_summary_row(unmatched_close_row(event), "probe_close_without_observable_open")
            covered_trade_event_ids.add(event["event_id"])

    for symbol, lots in open_lots.items():
        for open_event in lots:
            if open_event.get("remaining_qty") is None or open_event["remaining_qty"] <= qty_eps:
                continue
            dust_threshold = dust_threshold_for_symbol(symbol)
            remaining_value = value_for_qty(open_event["remaining_qty"], open_event.get("price"))
            if is_dust_value(remaining_value, dust_threshold):
                append_dust_residual_row(
                    "open_dust_residual_ignored",
                    symbol,
                    open_event["remaining_qty"],
                    remaining_value,
                    dust_threshold,
                    "open_lot_value_below_dust_threshold_excluded_from_open_positions",
                    open_event=open_event,
                    reference_px=open_event.get("price"),
                )
                covered_trade_event_ids.add(open_event["event_id"])
                continue
            status = "open_residual" if open_event.get("matched_qty", 0.0) > qty_eps else "open"
            record_trade_summary_row(open_trade_row(open_event, status), "probe_trade_open_residual" if status == "open_residual" else "probe_trade_open")
            covered_trade_event_ids.add(open_event["event_id"])

    for symbol, events in exit_signal_by_symbol.items():
        dust = latest_dust_by_symbol.get(symbol, {})
        raw_f = as_float(dust.get("raw_held_value_usdt"))
        eff_f = as_float(dust.get("effective_held_value_usdt"))
        dust_f = as_float(dust.get("dust_threshold_usdt"))
        dust_only = raw_f is not None and dust_f is not None and raw_f < dust_f and (eff_f is None or eff_f == 0.0)
        actual_repeated = len(events) > 1 and dust_only
        if actual_repeated:
            add_issue(
                "high",
                "repeated_probe_exit_signal_after_flat_dust_only",
                "Repeated exit signal was observed after the symbol was flat or dust-only.",
                {"symbol": symbol, "event_count": len(events), "events": events[-5:], "dust": dust},
            )
        for row in lifecycle_rows:
            if row["symbol"] == symbol and row["exit_reason"] in FLAT_EXIT_SIGNAL_REASONS:
                row["repeated_exit_signal_after_flat"] = str(actual_repeated).lower()

    for row in lifecycle_rows:
        if row["exit_reason"] not in PROBE_EXIT_REASONS:
            continue
        symbol = row["symbol"]
        dust = latest_dust_by_symbol.get(symbol, {})
        raw_f = as_float(dust.get("raw_held_value_usdt", row["remaining_value_usdt"]))
        dust_f = as_float(dust.get("dust_threshold_usdt", row["dust_threshold_usdt"]))
        state_present_any = any(state_present(state, symbol) for state in state_maps.values())
        row["state_still_present_after_close"] = str(state_present_any).lower()
        if raw_f is not None and dust_f is not None and raw_f < dust_f and state_present_any:
            row["diagnosis"] = "high_issue_probe_closed_but_active_state_remains"
            add_issue(
                "high",
                "probe_closed_but_active_state_remains",
                "Probe appears closed/dust-only but active state remains.",
                {"symbol": symbol, "run_id": row["run_id"], "exit_reason": row["exit_reason"], "remaining_value_usdt": raw_f, "dust_threshold_usdt": dust_f},
            )

    label_rows = []
    labels_path = OUT / "raw" / "reports" / "skipped_candidate_labels.jsonl"
    if labels_path.is_file():
        for line in labels_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    label_rows.append(item)
            except Exception:
                collection_errors.append({"source": str(labels_path), "error": "invalid jsonl row"})
    label_rows, label_duplicate_count = dedupe_rows_by_key(label_rows, btc_label_row_key)
    if labels_path.is_file():
        labels_text = "\n".join(json.dumps(sanitize_obj(row), ensure_ascii=False, sort_keys=True) for row in label_rows)
        write_text("raw/reports/skipped_candidate_labels.jsonl", labels_text + ("\n" if labels_text else ""))

    outcome_rows = []
    for outcomes_path in sorted((OUT / "raw" / "reports").glob("**/skipped_candidate_outcomes*.csv")):
        try:
            with outcomes_path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
                outcome_rows.extend(dict(row) for row in csv.DictReader(fh))
        except Exception as exc:
            collection_errors.append({"source": str(outcomes_path), "error": f"outcomes_csv: {exc!r}"})
    outcome_rows, outcome_duplicate_count = dedupe_rows_by_key(outcome_rows, btc_label_row_key)

    label_index = {btc_label_row_key(row): row for row in label_rows if all(part != not_obs for part in btc_label_row_key(row))}
    outcome_index = {btc_label_row_key(row): row for row in outcome_rows if all(part != not_obs for part in btc_label_row_key(row))}

    alt_impulse_shadow_label_rows = []
    alt_impulse_shadow_labels_path = OUT / "raw" / "reports" / "alt_impulse_shadow_labels.jsonl"
    if alt_impulse_shadow_labels_path.is_file():
        for line in alt_impulse_shadow_labels_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    alt_impulse_shadow_label_rows.append(item)
            except Exception:
                collection_errors.append({"source": str(alt_impulse_shadow_labels_path), "error": "invalid jsonl row"})
    alt_impulse_shadow_label_rows, alt_impulse_shadow_duplicate_count = dedupe_rows_by_key(
        alt_impulse_shadow_label_rows,
        btc_label_row_key,
    )
    if alt_impulse_shadow_labels_path.is_file():
        alt_text = "\n".join(
            json.dumps(sanitize_obj(row), ensure_ascii=False, sort_keys=True)
            for row in alt_impulse_shadow_label_rows
        )
        write_text("raw/reports/alt_impulse_shadow_labels.jsonl", alt_text + ("\n" if alt_text else ""))

    def normalize_multi_symbol_text(value):
        text = flatten_value(value)
        return text.replace("-", "/").upper() if text else ""

    def parse_symbols_list(value):
        if isinstance(value, list):
            raw = value
        else:
            text = flatten_value(value)
            if not text or text == not_obs:
                return []
            try:
                parsed = json.loads(text)
                raw = parsed if isinstance(parsed, list) else [text]
            except Exception:
                raw = [part.strip() for part in text.split(",")]
        out = []
        seen = set()
        for item in raw:
            symbol = normalize_multi_symbol_text(item)
            if symbol == "MULTI":
                continue
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            out.append(symbol)
        return out

    MULTI_SHADOW_MODE_ALL = "all_candidates"
    MULTI_SHADOW_MODE_PROTECT_RECOVERY = "protect_recovery_rules"
    MULTI_SHADOW_MODES = [MULTI_SHADOW_MODE_ALL, MULTI_SHADOW_MODE_PROTECT_RECOVERY]

    def multi_position_swing_shadow_row_key(row):
        symbols = parse_symbols_list(first_value(row, ("symbols", "symbols_json"), ""))
        return (
            flatten_value(first_value(row, ("shadow_mode",), MULTI_SHADOW_MODE_ALL)) or MULTI_SHADOW_MODE_ALL,
            flatten_value(first_value(row, ("run_id",), not_obs)) or not_obs,
            canonical_ts_utc(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs)),
            flatten_value(first_value(row, ("k", "top_k"), not_obs)) or not_obs,
            ",".join(symbols) or not_obs,
        )

    multi_position_swing_shadow_label_rows = []
    multi_position_swing_shadow_labels_path = OUT / "raw" / "reports" / "multi_position_swing_shadow_labels.jsonl"
    if multi_position_swing_shadow_labels_path.is_file():
        for line in multi_position_swing_shadow_labels_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    multi_position_swing_shadow_label_rows.append(item)
            except Exception:
                collection_errors.append({"source": str(multi_position_swing_shadow_labels_path), "error": "invalid jsonl row"})
    multi_position_swing_shadow_label_rows, multi_position_swing_shadow_duplicate_count = dedupe_rows_by_key(
        multi_position_swing_shadow_label_rows,
        multi_position_swing_shadow_row_key,
    )
    if multi_position_swing_shadow_labels_path.is_file():
        multi_shadow_text = "\n".join(
            json.dumps(sanitize_obj(row), ensure_ascii=False, sort_keys=True)
            for row in multi_position_swing_shadow_label_rows
        )
        write_text(
            "raw/reports/multi_position_swing_shadow_labels.jsonl",
            multi_shadow_text + ("\n" if multi_shadow_text else ""),
        )

    def protect_sol_exception_shadow_row_key(row):
        return (
            flatten_value(first_value(row, ("experiment_name",), "protect_sol_exception_v1")) or "protect_sol_exception_v1",
            flatten_value(first_value(row, ("run_id",), not_obs)) or not_obs,
            canonical_ts_utc(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs)),
            flatten_value(first_value(row, ("symbol",), not_obs)) or not_obs,
            flatten_value(first_value(row, ("original_block_reason", "skip_reason", "reason"), not_obs)) or not_obs,
            flatten_value(first_value(row, ("f3_weight_candidate",), not_obs)) or not_obs,
            flatten_value(first_value(row, ("f4_weight_candidate",), not_obs)) or not_obs,
        )

    protect_sol_exception_shadow_label_rows = []
    protect_sol_exception_shadow_labels_path = OUT / "raw" / "reports" / "protect_sol_exception_shadow_labels.jsonl"
    if protect_sol_exception_shadow_labels_path.is_file():
        for line in protect_sol_exception_shadow_labels_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    protect_sol_exception_shadow_label_rows.append(item)
            except Exception:
                collection_errors.append({"source": str(protect_sol_exception_shadow_labels_path), "error": "invalid jsonl row"})
    protect_sol_exception_shadow_label_rows, protect_sol_exception_shadow_duplicate_count = dedupe_rows_by_key(
        protect_sol_exception_shadow_label_rows,
        protect_sol_exception_shadow_row_key,
    )

    def protect_sol_exception_shadow_is_heartbeat(row):
        return (
            flatten_value(row.get("event_type")).lower() == "heartbeat"
            or flatten_value(row.get("label_status")).lower() == "heartbeat"
            or flatten_value(row.get("heartbeat")).lower() in {"1", "true", "yes", "y"}
        )

    protect_sol_exception_shadow_heartbeat_rows = [
        row for row in protect_sol_exception_shadow_label_rows
        if protect_sol_exception_shadow_is_heartbeat(row)
    ]
    protect_sol_exception_shadow_sample_label_rows = [
        row for row in protect_sol_exception_shadow_label_rows
        if not protect_sol_exception_shadow_is_heartbeat(row)
    ]
    if protect_sol_exception_shadow_labels_path.is_file():
        protect_sol_text = "\n".join(
            json.dumps(sanitize_obj(row), ensure_ascii=False, sort_keys=True)
            for row in protect_sol_exception_shadow_label_rows
        )
        write_text(
            "raw/reports/protect_sol_exception_shadow_labels.jsonl",
            protect_sol_text + ("\n" if protect_sol_text else ""),
        )

    def loose_label_key(row):
        return (
            flatten_value(first_value(row, ("run_id",), not_obs)) or not_obs,
            flatten_value(first_value(row, ("symbol", "instId"), not_obs)) or not_obs,
            flatten_value(first_value(row, ("skip_reason", "reason", "blocked_reason"), not_obs)) or not_obs,
        )

    def build_loose_index(rows):
        result = {}
        for row in rows:
            key = loose_label_key(row)
            if not key or any(part == not_obs for part in key):
                continue
            existing = result.get(key)
            if existing is None or status_rank(row) > status_rank(existing):
                result[key] = row
        return result

    label_loose_index = build_loose_index(label_rows)
    outcome_loose_index = build_loose_index(outcome_rows)
    def label_horizon_fields(horizons, *, include_status_reason=True):
        fields = []
        for horizon in horizons:
            h = int(horizon)
            fields.extend([
                f"label_{h}h_gross_bps",
                f"label_{h}h_net_bps",
                f"label_{h}h_would_have_won_net",
            ])
            if include_status_reason:
                fields.extend([f"label_{h}h_status", f"label_{h}h_reason"])
        return fields

    def future_price_debug_fields(horizons):
        fields = []
        for horizon in horizons:
            h = int(horizon)
            fields.extend([f"future_px_{h}h", f"future_price_source_{h}h"])
        return fields

    def aggregate_rows_by_horizon(rows, horizons, value_prefix="label_"):
        out = []
        for horizon in horizons:
            h = int(horizon)
            net_key = f"{value_prefix}{h}h_net_bps"
            status_key = f"{value_prefix}{h}h_status"
            values = [as_float(row.get(net_key)) for row in rows]
            usable = [value for value in values if value is not None]
            def row_horizon_status(row):
                status = flatten_value(row.get(status_key))
                if status in {"pending", "not_observable", "complete"}:
                    return status
                if as_float(row.get(net_key)) is not None:
                    return "complete"
                value_text = flatten_value(row.get(net_key))
                if value_text == "pending":
                    return "pending"
                if value_text == not_obs:
                    return "not_observable"
                return ""
            out.append({
                "horizon_hours": h,
                "count": len(rows),
                "pending_count": sum(1 for row in rows if row_horizon_status(row) == "pending"),
                "not_observable_count": sum(1 for row in rows if row_horizon_status(row) == "not_observable"),
                "complete_count": sum(1 for row in rows if row_horizon_status(row) == "complete"),
                "avg_net_bps": round(sum(usable) / len(usable), 6) if usable else not_obs,
                "win_rate": round(sum(1 for value in usable if value > 0) / len(usable), 6) if usable else not_obs,
            })
        return out

    def aggregate_rows_by_symbol_regime_horizon(rows, horizons):
        grouped = defaultdict(list)
        for row in rows:
            key = (
                flatten_value(first_value(row, ("symbol",), not_obs)) or not_obs,
                flatten_value(first_value(row, ("regime_state", "regime"), not_obs)) or not_obs,
            )
            grouped[key].append(row)
        out = []
        for (symbol, regime_state), group_rows in sorted(grouped.items(), key=lambda item: item[0]):
            for horizon_row in aggregate_rows_by_horizon(group_rows, horizons):
                payload = dict(horizon_row)
                payload["symbol"] = symbol
                payload["regime_state"] = regime_state
                out.append(payload)
        return out

    high_score_outcome_fields = [
        "ts_utc",
        "run_id",
        "symbol",
        "intended_side",
        "skip_reason",
        "high_score_block_category",
        "final_score",
        "selected_rank",
        "target_w",
        "trend_score",
        "trend_side",
        "alpha6_score",
        "alpha6_side",
        "f4_volume_expansion",
        "f5_rsi_trend_confirm",
        "last_exit_reason",
        "last_exit_px",
        "highest_px_before_exit",
        "elapsed_hours",
        "required_cooldown_hours",
        "breakout_exception_met",
        "entry_px",
        "rt_cost_bps",
        "current_level",
        "regime",
        *label_horizon_fields(label_horizons),
        "label_status",
        "label_not_observable_reason",
    ]

    def truthy(value):
        return str(value or "").strip().lower() in {"1", "true", "yes", "y"}

    def protect_sol_exception_shadow_outcome_row(row):
        payload = dict(row)
        payload["original_block_reason"] = first_observed(
            first_value(payload, ("original_block_reason", "skip_reason", "reason"), not_obs)
        )
        for horizon in protect_sol_exception_horizons:
            h = int(horizon)
            if as_float(payload.get(f"would_pnl_bps_{h}h")) is None:
                payload[f"would_pnl_bps_{h}h"] = flatten_value(
                    first_value(payload, (f"label_{h}h_net_bps",), not_obs)
                )
        return payload

    protect_sol_exception_shadow_rows = [
        protect_sol_exception_shadow_outcome_row(row)
        for row in protect_sol_exception_shadow_sample_label_rows
    ]

    def protect_sol_candidate_key(row):
        return (
            flatten_value(first_value(row, ("run_id",), not_obs)) or not_obs,
            canonical_ts_utc(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs)),
            flatten_value(first_value(row, ("symbol",), not_obs)) or not_obs,
            flatten_value(first_value(row, ("original_block_reason", "skip_reason", "reason"), not_obs)) or not_obs,
        )

    def aggregate_protect_sol_exception_shadow(rows, horizons, *, include_variant=False):
        min_samples = int(config_number("protect_sol_exception_min_complete_samples_warning") or 5)
        buckets = defaultdict(list)
        for row in rows:
            for horizon in horizons:
                key = [
                    flatten_value(first_value(row, ("symbol",), not_obs)) or not_obs,
                    flatten_value(first_value(row, ("original_block_reason", "skip_reason", "reason"), not_obs)) or not_obs,
                    int(horizon),
                ]
                if include_variant:
                    key.extend(
                        [
                            flatten_value(first_value(row, ("f3_weight_candidate",), not_obs)) or not_obs,
                            flatten_value(first_value(row, ("f4_weight_candidate",), not_obs)) or not_obs,
                        ]
                    )
                buckets[tuple(key)].append(row)
        out = []
        for key, bucket_rows in sorted(buckets.items(), key=lambda item: item[0]):
            horizon = int(key[2])
            net_key = f"would_pnl_bps_{horizon}h"
            status_key = f"label_{horizon}h_status"
            values = [as_float(row.get(net_key)) for row in bucket_rows]
            usable = [value for value in values if value is not None]
            unique_keys = {protect_sol_candidate_key(row) for row in bucket_rows}
            complete_unique_keys = {
                protect_sol_candidate_key(row)
                for row in bucket_rows
                if as_float(row.get(net_key)) is not None
            }
            avg_net = sum(usable) / len(usable) if usable else None
            complete_unique_count = len(complete_unique_keys)
            payload = {
                "symbol": key[0],
                "original_block_reason": key[1],
                "horizon_hours": horizon,
                "count": len(bucket_rows),
                "unique_candidate_count": len(unique_keys),
                "complete_count": len(usable),
                "complete_unique_candidate_count": complete_unique_count,
                "pending_count": sum(1 for row in bucket_rows if flatten_value(row.get(status_key)) == "pending"),
                "not_observable_count": sum(1 for row in bucket_rows if flatten_value(row.get(status_key)) == "not_observable"),
                "avg_would_pnl_bps": round(avg_net, 6) if avg_net is not None else not_obs,
                "win_rate": round(sum(1 for value in usable if value > 0) / len(usable), 6) if usable else not_obs,
                "current_strategy_net_bps": 0.0,
                "better_than_current_strategy": str(bool(avg_net is not None and avg_net > 0.0)).lower(),
                "sample_warning": (
                    f"insufficient_samples_min_{min_samples}"
                    if complete_unique_count < min_samples
                    else ""
                ),
                "live_ready_suggestion": str(
                    bool(complete_unique_count >= min_samples and avg_net is not None and avg_net > 0.0)
                ).lower(),
            }
            if include_variant:
                payload["f3_weight_candidate"] = key[3]
                payload["f4_weight_candidate"] = key[4]
            out.append(payload)
        return out

    protect_sol_exception_shadow_by_horizon = aggregate_protect_sol_exception_shadow(
        protect_sol_exception_shadow_rows,
        protect_sol_exception_horizons,
        include_variant=False,
    )
    protect_sol_exception_factor_weight_shadow_rows = aggregate_protect_sol_exception_shadow(
        protect_sol_exception_shadow_rows,
        protect_sol_exception_horizons,
        include_variant=True,
    )

    HIGH_SCORE_NON_ENTRY_MANAGEMENT_REASONS = {
        "rank_exit_target_still_positive",
        "exit_order_selected",
        "deadband",
        "active_probe_ignore_zero_target_close",
        "swing_min_hold_guard",
        "hold_current_no_valid_replacement",
    }

    def high_score_reason_text(row):
        return flatten_value(first_value(row, ("skip_reason", "router_reason", "reason", "blocked_reason"), ""))

    def is_high_score_labelable_reason(reason):
        text = str(reason or "").strip()
        if text in HIGH_SCORE_NON_ENTRY_MANAGEMENT_REASONS:
            return False
        return (
            text.startswith("protect_entry_")
            or text == "cost_aware_edge"
            or text.startswith("negative_expectancy_")
            or text == "same_symbol_reentry_cooldown"
            or text.startswith("min_notional")
            or text.startswith("insufficient_cash")
        )

    def is_high_score_blocked_outcome_source(row):
        if not is_high_score_labelable_reason(high_score_reason_text(row)):
            return False
        if truthy(row.get("high_score_blocked_target")):
            return True
        category = flatten_value(row.get("high_score_block_category"))
        return bool(category and category != not_obs and as_float(row.get("final_score")) is not None)

    high_score_outcome_by_key = {}
    for row in list(label_rows) + list(outcome_rows):
        if not is_high_score_blocked_outcome_source(row):
            continue
        key = btc_label_row_key(row)
        if not key or any(part == not_obs for part in key):
            continue
        existing = high_score_outcome_by_key.get(key)
        if existing is None or status_rank(row) > status_rank(existing):
            high_score_outcome_by_key[key] = row

    def high_score_outcome_field_value(row, field):
        value = first_value(row, (field,), not_obs)
        if field == "label_not_observable_reason" or field.endswith("h_reason"):
            return "" if value in (None, "") else flatten_value(value)
        return first_observed(value)

    high_score_blocked_outcome_rows = []
    for row in high_score_outcome_by_key.values():
        high_score_blocked_outcome_rows.append({
            field: high_score_outcome_field_value(row, field)
            for field in high_score_outcome_fields
        })

    def aggregate_high_score_outcomes(rows, key_fields):
        grouped = defaultdict(list)
        for row in rows:
            grouped[tuple(row.get(field) or not_obs for field in key_fields)].append(row)
        out = []
        for key, group_rows in sorted(grouped.items(), key=lambda item: item[0]):
            payload = {field: key[idx] for idx, field in enumerate(key_fields)}
            payload["count"] = len(group_rows)
            for horizon in label_horizons:
                values = [as_float(row.get(f"label_{horizon}h_net_bps")) for row in group_rows]
                usable = [value for value in values if value is not None]
                payload[f"avg_{horizon}h_net_bps"] = round(sum(usable) / len(usable), 6) if usable else not_obs
                payload[f"win_rate_{horizon}h"] = round(sum(1 for value in usable if value > 0) / len(usable), 6) if usable else not_obs
            out.append(payload)
        return out

    high_score_blocked_outcomes_by_symbol = aggregate_high_score_outcomes(
        high_score_blocked_outcome_rows,
        ["symbol", "skip_reason"],
    )
    high_score_blocked_outcomes_by_reason = aggregate_high_score_outcomes(
        high_score_blocked_outcome_rows,
        ["skip_reason"],
    )

    def normalize_symbol_text(value):
        text = flatten_value(value)
        return text.replace("-", "/").upper() if text else ""

    def symbol_map_get(mapping, symbol):
        if not isinstance(mapping, dict):
            return {}
        if symbol in mapping:
            return mapping.get(symbol) or {}
        wanted = normalize_symbol_text(symbol)
        for key, value in mapping.items():
            if normalize_symbol_text(key) == wanted:
                return value or {}
        return {}

    def positive_float(value):
        number = as_float(value)
        if number is None or number <= 0:
            return None
        return number

    def price_from_dict(obj, names=("latest_px", "current_px", "price", "px")):
        if not isinstance(obj, dict):
            return None
        return positive_float(first_value(obj, names, not_obs))

    def symbol_price_from_nested(obj, symbol, names=("latest_px", "current_px", "last_px", "price", "px", "close")):
        wanted = normalize_symbol_text(symbol)
        if not wanted:
            return None
        for item in iter_dicts(obj):
            item_symbol = normalize_symbol_text(first_value(item, ("symbol", "instId", "inst_id", "instrument"), ""))
            if item_symbol != wanted:
                continue
            price = price_from_dict(item, names)
            if price is not None:
                return price
        return None

    def cache_timestamp_ms(value):
        if value in (None, "", not_obs):
            return None
        if isinstance(value, (int, float)):
            raw = float(value)
            return int(raw if raw > 10_000_000_000 else raw * 1000.0)
        text = str(value).strip()
        if not text:
            return None
        if re.fullmatch(r"\d+(?:\.\d+)?", text):
            raw = float(text)
            return int(raw if raw > 10_000_000_000 else raw * 1000.0)
        parsed = parse_dt_utc(text)
        return int(parsed.timestamp() * 1000.0) if parsed else None

    def cache_file_epoch(path, prefix):
        suffix = path.stem[len(prefix):] if path.stem.startswith(prefix) else path.stem
        hourly_match = re.search(r"(20\d{6}_\d{2})$", suffix)
        if hourly_match:
            try:
                return dt.datetime.strptime(hourly_match.group(1), "%Y%m%d_%H").timestamp()
            except Exception:
                pass
        date_tokens = re.findall(r"(20\d{2}-\d{2}-\d{2}|20\d{6})", suffix)
        if date_tokens:
            token = date_tokens[-1]
            try:
                return dt.datetime.strptime(token, "%Y-%m-%d" if "-" in token else "%Y%m%d").timestamp()
            except Exception:
                pass
        try:
            return path.stat().st_mtime
        except Exception:
            return 0.0

    def cache_symbol_prefixes(symbol):
        text = flatten_value(symbol)
        variants = [
            text.replace("/", "_").replace("-", "_"),
            text.replace("/", "-"),
            text.replace("/", "").replace("-", ""),
        ]
        return [value for value in dict.fromkeys(v.strip() for v in variants) if value]

    def cache_files_for_symbol(symbol):
        cache_dir = ROOT / "data" / "cache"
        if not cache_dir.is_dir():
            return []
        files = []
        seen = set()
        for prefix in cache_symbol_prefixes(symbol):
            patterns = (
                f"{prefix}_1H_*.csv",
                f"{prefix}_1h_*.csv",
                f"{prefix}_60m_*.csv",
                f"{prefix}*1H*.csv",
                f"{prefix}*1h*.csv",
                f"{prefix}*60m*.csv",
            )
            for pattern in patterns:
                for path in cache_dir.glob(pattern):
                    if path.is_file() and path not in seen:
                        seen.add(path)
                        files.append(path)
            if not files:
                for pattern in patterns:
                    for path in cache_dir.rglob(pattern):
                        if path.is_file() and path not in seen:
                            seen.add(path)
                            files.append(path)
        return sorted(files, key=lambda path: cache_file_epoch(path, cache_symbol_prefixes(symbol)[0] if cache_symbol_prefixes(symbol) else ""))

    def row_get_ci(row, names):
        lowered = {str(key).strip().lower(): value for key, value in row.items()}
        for name in names:
            if name in lowered:
                return lowered[name]
        return None

    cache_candles_by_symbol = {}

    def load_cache_candles(symbol):
        cache_key = normalize_symbol_text(symbol)
        if cache_key in cache_candles_by_symbol:
            return cache_candles_by_symbol[cache_key]
        candles = {}
        for path in cache_files_for_symbol(symbol):
            try:
                with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        ts_ms = cache_timestamp_ms(row_get_ci(row, ("timestamp", "timestamp_ms", "ts", "time", "datetime", "date")))
                        close = positive_float(row_get_ci(row, ("close", "c", "last", "price", "px")))
                        if ts_ms is not None and close is not None:
                            candles[int(ts_ms)] = close
            except Exception as exc:
                collection_errors.append({"source": str(path), "error": f"alt_impulse_shadow_cache_read: {exc!r}"})
        rows = sorted(candles.items())
        cache_candles_by_symbol[cache_key] = rows
        return rows

    def cache_price_at_or_after(symbol, when_dt):
        if when_dt is None:
            return None, "missing_market_data"
        candles = load_cache_candles(symbol)
        if not candles:
            return None, "missing_market_data"
        target_ms = int(when_dt.timestamp() * 1000.0)
        for ts_ms, close in candles:
            if ts_ms >= target_ms:
                return close, ""
        return None, "missing_future_px"

    def price_point_at_or_near(points, when_dt, *, before_tolerance_seconds=300, after_tolerance_seconds=7200):
        if when_dt is None:
            return None
        if not points:
            return None
        target_ms = int(when_dt.timestamp() * 1000.0)
        before_limit = target_ms - int(before_tolerance_seconds * 1000.0)
        after_limit = target_ms + int(after_tolerance_seconds * 1000.0)
        candidates = []
        for ts_ms, close, source in points:
            try:
                ts_int = int(ts_ms)
                close_value = float(close)
            except Exception:
                continue
            if close_value <= 0.0:
                continue
            if before_limit <= ts_int <= after_limit:
                candidates.append((abs(ts_int - target_ms), 0 if ts_int >= target_ms else 1, ts_int, close_value, source))
        if not candidates:
            return None
        _, _, ts_int, close_value, source = sorted(candidates)[0]
        return close_value, source, ts_int

    def cache_future_price(symbol, when_dt):
        if when_dt is None:
            return None, "", "missing_market_data"
        candles = load_cache_candles(symbol)
        if not candles:
            return None, "", "missing_market_data"
        points = [(ts_ms, close, "data_cache_1h") for ts_ms, close in candles]
        match = price_point_at_or_near(points, when_dt)
        if match is None:
            return None, "", "missing_future_px"
        price, source, _ts_ms = match
        return price, source, ""

    run_market_price_points_by_symbol = defaultdict(list)
    for run_id, audit in audit_by_run.items():
        audit_dt = parse_dt_utc(run_ts(run_id, audit)) or parse_run_time(run_id)
        if audit_dt is None:
            continue
        ts_ms = int(audit_dt.timestamp() * 1000.0)
        for item in iter_dicts(audit):
            symbol = normalize_symbol_text(first_value(item, ("symbol", "instId", "inst_id", "instrument"), ""))
            if not symbol:
                continue
            price = price_from_dict(item, ("latest_px", "current_px", "last_px", "price", "px", "close"))
            if price is None:
                continue
            run_market_price_points_by_symbol[symbol].append((ts_ms, price, f"recent_run_decision_audit:{run_id}"))
    for symbol in list(run_market_price_points_by_symbol.keys()):
        run_market_price_points_by_symbol[symbol] = sorted(run_market_price_points_by_symbol[symbol], key=lambda row: row[0])

    skipped_label_entry_price_points_by_symbol = defaultdict(list)
    skipped_label_future_price_points_by_symbol = defaultdict(list)

    def row_entry_dt(row):
        ts_value = first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs))
        parsed = parse_dt_utc(ts_value)
        if parsed is not None:
            return parsed
        entry_ts_ms = first_value(row, ("entry_ts_ms",), not_obs)
        parsed = parse_dt_utc(entry_ts_ms)
        return parsed

    for provider_name, provider_rows in (
        ("skipped_candidate_label_provider", label_rows),
        ("skipped_candidate_outcome_provider", outcome_rows),
    ):
        for provider_row in provider_rows:
            symbol = normalize_symbol_text(first_value(provider_row, ("symbol", "instId"), ""))
            entry_px_for_provider = positive_float(first_value(provider_row, ("entry_px",), not_obs))
            entry_dt_for_provider = row_entry_dt(provider_row)
            if not symbol or entry_px_for_provider is None or entry_dt_for_provider is None:
                continue
            skipped_label_entry_price_points_by_symbol[symbol].append(
                (int(entry_dt_for_provider.timestamp() * 1000.0), entry_px_for_provider, f"{provider_name}_entry_px")
            )
            for horizon in label_horizons:
                status = flatten_value(first_value(provider_row, (f"label_{int(horizon)}h_status",), ""))
                gross_bps = as_float(first_value(provider_row, (f"label_{int(horizon)}h_gross_bps",), not_obs))
                if status != "complete" or gross_bps is None:
                    continue
                future_dt = entry_dt_for_provider + dt.timedelta(hours=int(horizon))
                future_px = entry_px_for_provider * (1.0 + gross_bps / 10000.0)
                skipped_label_future_price_points_by_symbol[symbol].append(
                    (int(future_dt.timestamp() * 1000.0), future_px, provider_name)
                )
    for symbol in list(skipped_label_entry_price_points_by_symbol.keys()):
        skipped_label_entry_price_points_by_symbol[symbol] = sorted(
            skipped_label_entry_price_points_by_symbol[symbol],
            key=lambda row: row[0],
        )
    for symbol in list(skipped_label_future_price_points_by_symbol.keys()):
        skipped_label_future_price_points_by_symbol[symbol] = sorted(
            skipped_label_future_price_points_by_symbol[symbol],
            key=lambda row: row[0],
        )

    def provider_entry_price_for_symbol(symbol, when_dt):
        normalized = normalize_symbol_text(symbol)
        match = price_point_at_or_near(
            skipped_label_entry_price_points_by_symbol.get(normalized),
            when_dt,
            before_tolerance_seconds=7200,
            after_tolerance_seconds=7200,
        )
        if match is None:
            return None, ""
        price, source, _ts_ms = match
        return price, source

    def future_price_for_symbol(symbol, when_dt):
        cache_px, cache_source, cache_reason = cache_future_price(symbol, when_dt)
        if cache_px is not None:
            return cache_px, cache_source, ""
        normalized = normalize_symbol_text(symbol)
        run_match = price_point_at_or_near(run_market_price_points_by_symbol.get(normalized), when_dt)
        if run_match is not None:
            price, source, _ts_ms = run_match
            return price, source, ""
        label_match = price_point_at_or_near(skipped_label_future_price_points_by_symbol.get(normalized), when_dt)
        if label_match is not None:
            price, source, _ts_ms = label_match
            return price, source, ""
        if cache_reason == "missing_market_data" and not run_market_price_points_by_symbol.get(normalized) and not skipped_label_future_price_points_by_symbol.get(normalized):
            return None, "", "missing_market_data"
        return None, "", "missing_future_px"

    def resolve_alt_shadow_entry_px(row):
        symbol = first_observed(first_value(row, ("symbol", "instId"), not_obs))
        run_id = first_observed(first_value(row, ("run_id",), not_obs))
        ts_utc = first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs))
        audit = audit_by_run.get(run_id, {}) if run_id != not_obs else {}

        target_item = symbol_map_get(target_explain_by_symbol(audit), symbol)
        price = price_from_dict(target_item, ("latest_px", "current_px", "price", "px"))
        if price is not None:
            return price, "", "target_execution_explain"

        router_item = symbol_map_get(router_decision_by_symbol(audit), symbol)
        price = price_from_dict(router_item, ("latest_px", "current_px", "price", "px"))
        if price is not None:
            return price, "", "router_decisions"

        price = symbol_price_from_nested(audit, symbol)
        if price is not None:
            return price, "", "decision_audit_market_data"

        event_price = price_from_dict(symbol_map_get(event_candidate_price_by_symbol, symbol), ("latest_px", "current_px", "price", "px"))
        if event_price is not None:
            return event_price, "", "event_candidates"

        cache_price, cache_reason = cache_price_at_or_after(symbol, parse_dt_utc(ts_utc))
        if cache_price is not None:
            return cache_price, "", "data_cache_1h"

        existing_price = price_from_dict(row, ("entry_px", "latest_px", "current_px", "price", "px"))
        if existing_price is not None:
            return existing_price, "", "label_row"

        return None, "missing_entry_px", cache_reason or "missing_entry_px"

    def alt_shadow_not_observable_reason(reasons):
        reasons = [flatten_value(reason).strip() for reason in reasons if flatten_value(reason).strip()]
        for preferred in ("missing_entry_px", "missing_market_data", "missing_future_px"):
            if preferred in reasons:
                return preferred
        return first_observed(*reasons) if reasons else ""

    def alt_impulse_shadow_decision_for_row(row):
        explicit = flatten_value(first_value(row, ("shadow_decision", "alpha_discovery_board_status"), "")).strip().upper()
        if explicit in {"REGIME_SHADOW", "KEEP_SHADOW"}:
            return explicit
        complete_count = as_float(first_value(row, ("complete_count",), not_obs))
        if complete_count is not None:
            return "REGIME_SHADOW" if complete_count > 0 else "KEEP_SHADOW"
        label_status = flatten_value(first_value(row, ("label_status",), "")).strip().lower()
        if label_status == "complete":
            return "REGIME_SHADOW"
        if label_status in {"pending", "not_observable", "heartbeat"}:
            return "KEEP_SHADOW"
        has_observable_avg = any(as_float(first_value(row, (f"avg_{int(h)}h_net_bps",), not_obs)) is not None for h in label_horizons)
        if has_observable_avg:
            return "REGIME_SHADOW"
        return "KEEP_SHADOW"

    def decorate_alt_impulse_shadow_status(row):
        row = dict(row)
        decision = alt_impulse_shadow_decision_for_row(row)
        row["shadow_decision"] = decision
        row["alpha_discovery_board_status"] = decision
        row["paper_ready_allowed"] = "false"
        row["live_ready_allowed"] = "false"
        row["shadow_decision_reason"] = "alt_impulse_regime_dependent_shadow_only"
        return row

    def decorate_alt_impulse_shadow_rows(rows):
        return [decorate_alt_impulse_shadow_status(row) for row in rows]

    def build_alt_impulse_shadow_row(row):
        out = {
            field: first_observed(first_value(row, (field,), not_obs))
            for field in alt_impulse_shadow_fields
        }
        out = decorate_alt_impulse_shadow_status(out)
        if out.get("regime_state") in (None, "", not_obs):
            out["regime_state"] = first_observed(first_value(row, ("regime",), not_obs))
        if out.get("risk_level") in (None, "", not_obs):
            out["risk_level"] = first_observed(first_value(row, ("current_level",), not_obs))
        if out.get("broad_market_positive_count") in (None, "", not_obs):
            out["broad_market_positive_count"] = first_observed(first_value(row, ("whitelist_positive_4h_count",), not_obs))
        if out.get("btc_trend_state") in (None, "", not_obs):
            btc_ret = as_float(first_value(row, ("btc_4h_ret_bps",), not_obs))
            if btc_ret is None:
                out["btc_trend_state"] = not_obs
            elif btc_ret > 0:
                out["btc_trend_state"] = "positive_4h"
            elif btc_ret < 0:
                out["btc_trend_state"] = "negative_4h"
            else:
                out["btc_trend_state"] = "flat_4h"
        entry_px, entry_reason, _entry_source = resolve_alt_shadow_entry_px(row)
        entry_dt = parse_dt_utc(first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs)))
        rt_cost_bps = as_float(first_value(row, ("rt_cost_bps",), not_obs))
        if rt_cost_bps is None:
            rt_cost_bps = 0.0

        horizon_statuses = []
        not_observable_reasons = []
        out["entry_px"] = fmt_num(entry_px, 10) if entry_px is not None else not_obs
        if entry_reason:
            not_observable_reasons.append(entry_reason)

        for horizon in label_horizons:
            gross_field = f"label_{horizon}h_gross_bps"
            net_field = f"label_{horizon}h_net_bps"
            win_field = f"label_{horizon}h_would_have_won_net"
            status_field = f"label_{horizon}h_status"
            reason_field = f"label_{horizon}h_reason"
            future_px_field = f"future_px_{horizon}h"
            future_source_field = f"future_price_source_{horizon}h"
            existing_value = first_value(row, (net_field,), not_obs)
            if as_float(existing_value) is not None:
                out[gross_field] = first_observed(first_value(row, (gross_field,), not_obs))
                out[net_field] = first_observed(existing_value)
                out[win_field] = first_observed(first_value(row, (win_field,), str(as_float(existing_value) > 0)))
                out[status_field] = "complete"
                out[reason_field] = ""
                existing_gross = as_float(first_value(row, (gross_field,), not_obs))
                if entry_px is not None and existing_gross is not None:
                    out[future_px_field] = fmt_num(entry_px * (1.0 + existing_gross / 10000.0), 10)
                    out[future_source_field] = "existing_label_gross_bps"
                else:
                    out[future_px_field] = first_observed(first_value(row, (future_px_field,), not_obs))
                    out[future_source_field] = first_observed(first_value(row, (future_source_field,), not_obs))
                horizon_statuses.append(out[status_field] if out[status_field] in ("pending", "not_observable", "complete") else "complete")
                continue
            if entry_px is None or entry_dt is None:
                out[gross_field] = not_obs
                out[net_field] = not_obs
                out[win_field] = not_obs
                out[status_field] = "not_observable"
                out[reason_field] = entry_reason or "missing_entry_px"
                out[future_px_field] = not_obs
                out[future_source_field] = not_obs
                horizon_statuses.append("not_observable")
                if entry_reason:
                    not_observable_reasons.append(entry_reason)
                else:
                    not_observable_reasons.append("missing_entry_px")
                continue
            horizon_dt = entry_dt + dt.timedelta(hours=horizon)
            if NOW < horizon_dt:
                out[gross_field] = "pending"
                out[net_field] = "pending"
                out[win_field] = "pending"
                out[status_field] = "pending"
                out[reason_field] = f"awaiting_horizon_until_{horizon_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                out[future_px_field] = "pending"
                out[future_source_field] = "pending"
                horizon_statuses.append("pending")
                continue
            future_px, future_source, future_reason = future_price_for_symbol(out["symbol"], horizon_dt)
            if future_px is None:
                out[gross_field] = not_obs
                out[net_field] = not_obs
                out[win_field] = not_obs
                out[status_field] = "not_observable"
                out[reason_field] = future_reason or "missing_future_px"
                out[future_px_field] = not_obs
                out[future_source_field] = future_reason or "missing_future_px"
                horizon_statuses.append("not_observable")
                not_observable_reasons.append(future_reason or "missing_future_px")
                continue
            gross_bps = ((future_px / entry_px) - 1.0) * 10000.0
            net_bps = gross_bps - rt_cost_bps
            out[gross_field] = fmt_num(gross_bps, 6)
            out[net_field] = fmt_num(net_bps, 6)
            out[win_field] = str(net_bps > 0).lower()
            out[status_field] = "complete"
            out[reason_field] = ""
            out[future_px_field] = fmt_num(future_px, 10)
            out[future_source_field] = future_source or "not_observable"
            horizon_statuses.append("complete")

        if any(status == "complete" for status in horizon_statuses):
            out["label_status"] = "complete"
        elif any(status == "pending" for status in horizon_statuses):
            out["label_status"] = "pending"
        elif horizon_statuses and all(status == "not_observable" for status in horizon_statuses):
            out["label_status"] = "not_observable"
        elif any(status == "not_observable" for status in horizon_statuses):
            out["label_status"] = "not_observable"
        else:
            out["label_status"] = first_observed(first_value(row, ("label_status",), not_obs))
        if entry_px is not None:
            not_observable_reasons = [reason for reason in not_observable_reasons if flatten_value(reason).strip() != "missing_entry_px"]
        if out["label_status"] == "complete":
            out["label_not_observable_reason"] = ""
        elif out["label_status"] == "not_observable":
            out["label_not_observable_reason"] = alt_shadow_not_observable_reason(not_observable_reasons)
        else:
            out["label_not_observable_reason"] = ""
        return out

    def build_high_score_blocked_outcome_row(row):
        out = {
            field: first_observed(first_value(row, (field,), not_obs))
            for field in high_score_outcome_fields
        }
        out["intended_side"] = first_observed(out.get("intended_side"), "buy")
        out["skip_reason"] = high_score_reason_text(row)
        out["rt_cost_bps"] = first_observed(out.get("rt_cost_bps"), "30")

        entry_px, entry_reason, _entry_source = resolve_alt_shadow_entry_px(row)
        entry_dt = parse_dt_utc(first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs)))
        rt_cost_bps = as_float(first_value(out, ("rt_cost_bps",), not_obs))
        if rt_cost_bps is None:
            rt_cost_bps = 30.0

        horizon_statuses = []
        not_observable_reasons = []
        out["entry_px"] = fmt_num(entry_px, 10) if entry_px is not None else not_obs
        if entry_reason:
            not_observable_reasons.append(entry_reason)

        for horizon in label_horizons:
            h = int(horizon)
            gross_field = f"label_{h}h_gross_bps"
            net_field = f"label_{h}h_net_bps"
            win_field = f"label_{h}h_would_have_won_net"
            status_field = f"label_{h}h_status"
            reason_field = f"label_{h}h_reason"
            existing_value = first_value(row, (net_field,), not_obs)
            if as_float(existing_value) is not None:
                out[gross_field] = first_observed(first_value(row, (gross_field,), not_obs))
                out[net_field] = first_observed(existing_value)
                out[win_field] = first_observed(first_value(row, (win_field,), str(as_float(existing_value) > 0).lower()))
                out[status_field] = "complete"
                out[reason_field] = ""
                horizon_statuses.append("complete")
                continue
            if entry_px is None or entry_dt is None:
                out[gross_field] = not_obs
                out[net_field] = not_obs
                out[win_field] = not_obs
                out[status_field] = "not_observable"
                out[reason_field] = entry_reason or "missing_entry_px"
                horizon_statuses.append("not_observable")
                not_observable_reasons.append(entry_reason or "missing_entry_px")
                continue
            horizon_dt = entry_dt + dt.timedelta(hours=h)
            if NOW < horizon_dt:
                out[gross_field] = "pending"
                out[net_field] = "pending"
                out[win_field] = "pending"
                out[status_field] = "pending"
                out[reason_field] = f"awaiting_horizon_until_{horizon_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                horizon_statuses.append("pending")
                continue
            future_px, _future_source, future_reason = future_price_for_symbol(out["symbol"], horizon_dt)
            if future_px is None:
                out[gross_field] = not_obs
                out[net_field] = not_obs
                out[win_field] = not_obs
                out[status_field] = "not_observable"
                out[reason_field] = future_reason or "missing_future_px"
                horizon_statuses.append("not_observable")
                not_observable_reasons.append(future_reason or "missing_future_px")
                continue
            gross_bps = ((future_px / entry_px) - 1.0) * 10000.0
            net_bps = gross_bps - rt_cost_bps
            out[gross_field] = fmt_num(gross_bps, 6)
            out[net_field] = fmt_num(net_bps, 6)
            out[win_field] = str(net_bps > 0).lower()
            out[status_field] = "complete"
            out[reason_field] = ""
            horizon_statuses.append("complete")

        if any(status == "complete" for status in horizon_statuses):
            out["label_status"] = "complete"
            out["label_not_observable_reason"] = ""
        elif any(status == "pending" for status in horizon_statuses):
            out["label_status"] = "pending"
            out["label_not_observable_reason"] = ""
        elif horizon_statuses:
            out["label_status"] = "not_observable"
            out["label_not_observable_reason"] = alt_shadow_not_observable_reason(not_observable_reasons)
        else:
            out["label_status"] = not_obs
            out["label_not_observable_reason"] = not_obs
        return out

    for row in high_score_blocked_rows:
        if not is_high_score_labelable_reason(high_score_reason_text(row)):
            continue
        key = btc_label_key(
            row.get("run_id"),
            row.get("ts_utc"),
            row.get("symbol"),
            high_score_reason_text(row),
        )
        if not key or any(part == not_obs for part in key):
            continue
        reason = high_score_reason_text(row)
        synthesized = build_high_score_blocked_outcome_row(row)
        if (
            reason != "same_symbol_reentry_cooldown"
            and as_float(synthesized.get("entry_px")) is None
            and synthesized.get("label_status") == "not_observable"
        ):
            continue
        existing = high_score_outcome_by_key.get(key)
        if existing is None or status_rank(synthesized) > status_rank(existing):
            high_score_outcome_by_key[key] = synthesized

    high_score_blocked_outcome_rows = []
    for row in high_score_outcome_by_key.values():
        high_score_blocked_outcome_rows.append({
            field: high_score_outcome_field_value(row, field)
            for field in high_score_outcome_fields
        })
    high_score_blocked_outcomes_by_symbol = aggregate_high_score_outcomes(
        high_score_blocked_outcome_rows,
        ["symbol", "skip_reason"],
    )
    high_score_blocked_outcomes_by_reason = aggregate_high_score_outcomes(
        high_score_blocked_outcome_rows,
        ["skip_reason"],
    )
    high_score_outcome_loose_index = build_loose_index(high_score_blocked_outcome_rows)

    alt_impulse_shadow_fields = [
        "ts_utc",
        "run_id",
        "symbol",
        "entry_px",
        "final_score",
        "trend_score",
        "trend_side",
        "alpha6_score",
        "alpha6_side",
        "f4_volume_expansion",
        "f5_rsi_trend_confirm",
        "skip_reason",
        "btc_4h_ret_bps",
        "btc_trend_state",
        "whitelist_positive_4h_count",
        "broad_market_positive_count",
        "regime",
        "regime_state",
        "current_level",
        "risk_level",
        "funding_state",
        "volatility_bucket",
        "shadow_decision",
        "alpha_discovery_board_status",
        "paper_ready_allowed",
        "live_ready_allowed",
        "shadow_decision_reason",
        "rt_cost_bps",
        *label_horizon_fields(label_horizons),
        *future_price_debug_fields(label_horizons),
        "label_status",
        "label_not_observable_reason",
    ]
    alt_impulse_shadow_rows = []
    for row in alt_impulse_shadow_label_rows:
        alt_impulse_shadow_rows.append(build_alt_impulse_shadow_row(row))
    alt_impulse_shadow_entry_px_not_observable_count = sum(
        1 for row in alt_impulse_shadow_rows
        if as_float(row.get("entry_px")) is None
    )
    if alt_impulse_shadow_rows and alt_impulse_shadow_entry_px_not_observable_count == len(alt_impulse_shadow_rows):
        add_issue(
            "medium",
            "alt_impulse_shadow_entry_px_not_observable",
            "ALT impulse shadow labels exist but every sample has entry_px not_observable, so forward labels cannot be trusted.",
            {
                "alt_impulse_shadow_label_count": len(alt_impulse_shadow_rows),
                "entry_px_not_observable_count": alt_impulse_shadow_entry_px_not_observable_count,
            },
        )
    alt_impulse_shadow_matured_horizon_count = 0
    alt_impulse_shadow_missing_future_px_count = 0
    for row in alt_impulse_shadow_rows:
        entry_px_value = as_float(row.get("entry_px"))
        entry_dt = parse_dt_utc(first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs)))
        if entry_px_value is None or entry_dt is None:
            continue
        for horizon in label_horizons:
            horizon_dt = entry_dt + dt.timedelta(hours=int(horizon))
            if NOW < horizon_dt:
                continue
            alt_impulse_shadow_matured_horizon_count += 1
            status = flatten_value(row.get(f"label_{int(horizon)}h_status"))
            reason = flatten_value(row.get(f"label_{int(horizon)}h_reason"))
            if status == "not_observable" and reason == "missing_future_px":
                alt_impulse_shadow_missing_future_px_count += 1
    if (
        alt_impulse_shadow_rows
        and alt_impulse_shadow_matured_horizon_count > 0
        and alt_impulse_shadow_missing_future_px_count == alt_impulse_shadow_matured_horizon_count
    ):
        add_issue(
            "medium",
            "alt_impulse_shadow_future_px_not_observable",
            "ALT impulse shadow labels have entry_px, but every matured horizon is missing future_px.",
            {
                "alt_impulse_shadow_label_count": len(alt_impulse_shadow_rows),
                "matured_horizon_count": alt_impulse_shadow_matured_horizon_count,
                "missing_future_px_count": alt_impulse_shadow_missing_future_px_count,
            },
        )
    alt_impulse_shadow_by_symbol = decorate_alt_impulse_shadow_rows(aggregate_high_score_outcomes(
        alt_impulse_shadow_rows,
        ["symbol", "skip_reason"],
    ))
    alt_impulse_shadow_by_reason = decorate_alt_impulse_shadow_rows(aggregate_high_score_outcomes(
        alt_impulse_shadow_rows,
        ["skip_reason"],
    ))
    alt_impulse_shadow_by_regime = decorate_alt_impulse_shadow_rows(aggregate_high_score_outcomes(
        alt_impulse_shadow_rows,
        ["regime_state"],
    ))
    alt_impulse_shadow_by_symbol_regime_horizon = decorate_alt_impulse_shadow_rows(aggregate_rows_by_symbol_regime_horizon(
        alt_impulse_shadow_rows,
        label_horizons,
    ))
    high_score_blocked_outcomes_by_horizon = aggregate_rows_by_horizon(high_score_blocked_outcome_rows, label_horizons)
    alt_impulse_shadow_by_horizon = decorate_alt_impulse_shadow_rows(aggregate_rows_by_horizon(alt_impulse_shadow_rows, label_horizons))

    alt_impulse_readiness_fields = [
        "symbol",
        "ready_for_live_probe",
        "blocking_reasons",
        "sample_count",
        "recent_sample_count",
        "avg_24h_net_bps",
        "avg_48h_net_bps",
        "win_rate_24h",
        "win_rate_48h",
        "bnb_high_score_blocked_24h_avg_net_bps",
        "bnb_negative_expectancy_bps",
    ]
    alt_impulse_readiness_thresholds = {
        "min_sample_count": 30,
        "min_recent_7d_sample_count": 10,
        "min_avg_24h_net_bps": 80.0,
        "min_win_rate_24h": 0.60,
        "min_avg_48h_net_bps": 50.0,
        "recent_window_days": 7,
    }

    def alt_impulse_readiness_values(rows, horizon):
        values = []
        key = f"label_{int(horizon)}h_net_bps"
        for row in rows:
            value = as_float(first_value(row, (key,), not_obs))
            if value is not None:
                values.append(value)
        return values

    def alt_impulse_readiness_avg(values):
        return round(sum(values) / len(values), 6) if values else None

    def alt_impulse_readiness_win_rate(values):
        return round(sum(1 for value in values if value > 0.0) / len(values), 6) if values else None

    def alt_impulse_readiness_entry_dt(row):
        return parse_dt_utc(first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs)))

    def alt_impulse_negative_expectancy_bps(symbol):
        if not isinstance(negative_expectancy_state, dict):
            return None
        wanted = normalize_symbol_text(symbol)
        for section_name in ("stats", "symbols"):
            section = negative_expectancy_state.get(section_name)
            if not isinstance(section, dict):
                continue
            for raw_symbol, entry in section.items():
                if normalize_symbol_text(raw_symbol) == wanted and isinstance(entry, dict):
                    return as_float(first_value(entry, ("net_expectancy_bps", "expectancy_bps"), not_obs))
        for raw_symbol, entry in negative_expectancy_state.items():
            if normalize_symbol_text(raw_symbol) == wanted and isinstance(entry, dict):
                return as_float(first_value(entry, ("net_expectancy_bps", "expectancy_bps"), not_obs))
        return None

    def alt_impulse_high_score_24h_avg(symbol):
        wanted = normalize_symbol_text(symbol)
        values = [
            as_float(first_value(row, ("label_24h_net_bps",), not_obs))
            for row in high_score_blocked_outcome_rows
            if normalize_symbol_text(first_value(row, ("symbol",), "")) == wanted
        ]
        usable = [value for value in values if value is not None]
        return alt_impulse_readiness_avg(usable)

    def build_alt_impulse_readiness(rows):
        recent_cutoff = NOW - dt.timedelta(days=int(alt_impulse_readiness_thresholds["recent_window_days"]))
        symbols = sorted({normalize_symbol_text(first_value(row, ("symbol",), "")) for row in rows if normalize_symbol_text(first_value(row, ("symbol",), ""))})
        by_symbol = []
        for symbol in symbols:
            symbol_rows = [row for row in rows if normalize_symbol_text(first_value(row, ("symbol",), "")) == symbol]
            recent_rows = [
                row for row in symbol_rows
                if (alt_impulse_readiness_entry_dt(row) is not None and alt_impulse_readiness_entry_dt(row) >= recent_cutoff)
            ]
            net_24h = alt_impulse_readiness_values(symbol_rows, 24)
            net_48h = alt_impulse_readiness_values(symbol_rows, 48)
            avg_24h = alt_impulse_readiness_avg(net_24h)
            avg_48h = alt_impulse_readiness_avg(net_48h)
            win_24h = alt_impulse_readiness_win_rate(net_24h)
            win_48h = alt_impulse_readiness_win_rate(net_48h)
            blocking = []
            if len(symbol_rows) < int(alt_impulse_readiness_thresholds["min_sample_count"]):
                blocking.append("sample_count_lt_30")
            if len(recent_rows) < int(alt_impulse_readiness_thresholds["min_recent_7d_sample_count"]):
                blocking.append("recent_7d_sample_count_lt_10")
            if avg_24h is None:
                blocking.append("avg_24h_not_observable")
            elif avg_24h <= float(alt_impulse_readiness_thresholds["min_avg_24h_net_bps"]):
                blocking.append("avg_24h_net_bps_lte_80")
            if win_24h is None:
                blocking.append("win_rate_24h_not_observable")
            elif win_24h <= float(alt_impulse_readiness_thresholds["min_win_rate_24h"]):
                blocking.append("win_rate_24h_lte_0_60")
            if avg_48h is None:
                blocking.append("avg_48h_not_observable")
            elif avg_48h <= float(alt_impulse_readiness_thresholds["min_avg_48h_net_bps"]):
                blocking.append("avg_48h_net_bps_lte_50")

            bnb_high_score_avg = None
            bnb_negexp = None
            if symbol == "BNB/USDT":
                bnb_high_score_avg = alt_impulse_high_score_24h_avg(symbol)
                bnb_negexp = alt_impulse_negative_expectancy_bps(symbol)
                if bnb_high_score_avg is None:
                    blocking.append("bnb_high_score_blocked_24h_not_observable")
                elif bnb_high_score_avg <= 0.0:
                    blocking.append("bnb_high_score_blocked_24h_avg_lte_0")
                if bnb_negexp is None:
                    blocking.append("bnb_negative_expectancy_not_observable")
                elif bnb_negexp < 0.0:
                    blocking.append("bnb_negative_expectancy_lt_0")

            by_symbol.append({
                "symbol": symbol,
                "ready_for_live_probe": not blocking,
                "blocking_reasons": ",".join(blocking),
                "sample_count": len(symbol_rows),
                "recent_sample_count": len(recent_rows),
                "avg_24h_net_bps": avg_24h if avg_24h is not None else not_obs,
                "avg_48h_net_bps": avg_48h if avg_48h is not None else not_obs,
                "win_rate_24h": win_24h if win_24h is not None else not_obs,
                "win_rate_48h": win_48h if win_48h is not None else not_obs,
                "bnb_high_score_blocked_24h_avg_net_bps": bnb_high_score_avg if bnb_high_score_avg is not None else not_obs,
                "bnb_negative_expectancy_bps": bnb_negexp if bnb_negexp is not None else not_obs,
            })

        overall_recent = [
            row for row in rows
            if (alt_impulse_readiness_entry_dt(row) is not None and alt_impulse_readiness_entry_dt(row) >= recent_cutoff)
        ]
        overall_24h = alt_impulse_readiness_values(rows, 24)
        overall_48h = alt_impulse_readiness_values(rows, 48)
        ready_symbols = [row["symbol"] for row in by_symbol if bool(row.get("ready_for_live_probe"))]
        overall_blocking = [] if ready_symbols else ["no_symbol_ready_for_live_probe"]
        if len(rows) < int(alt_impulse_readiness_thresholds["min_sample_count"]):
            overall_blocking.append("overall_sample_count_lt_30")
        if len(overall_recent) < int(alt_impulse_readiness_thresholds["min_recent_7d_sample_count"]):
            overall_blocking.append("overall_recent_7d_sample_count_lt_10")
        summary = {
            "schema_version": "v5.alt_impulse_shadow_readiness.v1",
            "generated_ts_utc": NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ready_for_live_probe": bool(ready_symbols),
            "ready_symbols": ready_symbols,
            "blocking_reasons": overall_blocking,
            "sample_count": len(rows),
            "recent_sample_count": len(overall_recent),
            "avg_24h_net_bps": alt_impulse_readiness_avg(overall_24h) if overall_24h else not_obs,
            "avg_48h_net_bps": alt_impulse_readiness_avg(overall_48h) if overall_48h else not_obs,
            "win_rate_24h": alt_impulse_readiness_win_rate(overall_24h) if overall_24h else not_obs,
            "win_rate_48h": alt_impulse_readiness_win_rate(overall_48h) if overall_48h else not_obs,
            "thresholds": dict(alt_impulse_readiness_thresholds),
            "by_symbol": by_symbol,
        }
        return summary, by_symbol

    alt_impulse_shadow_readiness, alt_impulse_shadow_readiness_by_symbol = build_alt_impulse_readiness(alt_impulse_shadow_rows)
    skipped_candidate_outcomes_by_horizon = aggregate_rows_by_horizon(outcome_rows, label_horizons)
    skipped_candidate_outcomes_by_symbol = aggregate_high_score_outcomes(outcome_rows, ["symbol", "skip_reason"])
    skipped_candidate_outcomes_by_reason = aggregate_high_score_outcomes(outcome_rows, ["skip_reason"])
    multi_position_swing_horizons = normalize_horizon_list(
        config_int_list("multi_position_swing_shadow_horizons_hours"),
        [24, 48, 72],
    )

    def parse_json_obj(value, default):
        if isinstance(value, (dict, list)):
            return value
        text = flatten_value(value)
        if not text or text == not_obs:
            return default
        try:
            return json.loads(text)
        except Exception:
            return default

    MULTI_POSITION_NEGATIVE_EXPECTANCY_HARD_REASONS = {
        "negative_expectancy_cooldown",
        "negative_expectancy_open_block",
        "negative_expectancy_fast_fail_open_block",
        "protect_negative_expectancy_short_cycle_block",
    }

    def audit_is_risk_off(audit):
        for value in (audit.get("regime"), audit.get("current_level"), audit.get("risk_level"), audit.get("market_regime")):
            text = flatten_value(value).strip().lower().replace("_", "-")
            if text in {"risk-off", "riskoff"}:
                return True
        for item in audit.get("target_execution_explain") or []:
            if not isinstance(item, dict):
                continue
            for value in (item.get("regime"), item.get("current_level")):
                text = flatten_value(value).strip().lower().replace("_", "-")
                if text in {"risk-off", "riskoff"}:
                    return True
        return False

    def target_weight_from_targets(targets, symbol):
        if not isinstance(targets, dict):
            return None
        wanted = normalize_symbol_text(symbol)
        for key, value in targets.items():
            if normalize_symbol_text(key) == wanted:
                return as_float(value)
        return None

    def router_reasons_for_symbol(audit, symbol):
        wanted = normalize_symbol_text(symbol)
        reasons = set()
        for item in audit.get("router_decisions") or []:
            if not isinstance(item, dict):
                continue
            if normalize_symbol_text(item.get("symbol")) != wanted:
                continue
            reason = flatten_value(first_value(item, ("reason", "source_reason"), ""))
            if reason:
                reasons.add(reason)
        return reasons

    def multi_shadow_candidate_entry_px(symbol, audit, audit_ts):
        target_item = symbol_map_get(target_explain_by_symbol(audit), symbol)
        price = price_from_dict(target_item, ("entry_px", "latest_px", "current_px", "price", "px"))
        if price is not None:
            return price, "target_execution_explain"
        router_item = symbol_map_get(router_decision_by_symbol(audit), symbol)
        price = price_from_dict(router_item, ("entry_px", "latest_px", "current_px", "price", "px"))
        if price is not None:
            return price, "router_decisions"
        price = symbol_price_from_nested(audit, symbol)
        if price is not None:
            return price, "decision_audit_market_data"
        cache_price, cache_reason = cache_price_at_or_after(symbol, audit_ts)
        if cache_price is not None:
            return cache_price, "data_cache_1h"
        provider_price, provider_source = provider_entry_price_for_symbol(symbol, audit_ts)
        if provider_price is not None:
            return provider_price, provider_source
        return None, cache_reason or "missing_entry_px"

    def multi_shadow_protect_recovery_allowed_symbols():
        return list(
            dict.fromkeys(
                normalize_symbol_text(symbol)
                for symbol in config_string_list(
                    "protect_recovery_allowed_symbols",
                    ["BTC/USDT", "SOL/USDT", "ETH/USDT"],
                )
                if normalize_symbol_text(symbol)
            )
        )

    def multi_shadow_negative_expectancy_entry(symbol):
        if not isinstance(negative_expectancy_state, dict):
            return {}
        wanted = normalize_symbol_text(symbol)
        for section_name in ("stats", "symbols"):
            section = negative_expectancy_state.get(section_name)
            if not isinstance(section, dict):
                continue
            for raw_symbol, entry in section.items():
                if normalize_symbol_text(raw_symbol) == wanted and isinstance(entry, dict):
                    return entry
        for raw_symbol, entry in negative_expectancy_state.items():
            if normalize_symbol_text(raw_symbol) == wanted and isinstance(entry, dict):
                return entry
        return {}

    def multi_shadow_symbol_has_negative_expectancy(symbol, router_reasons):
        if router_reasons & MULTI_POSITION_NEGATIVE_EXPECTANCY_HARD_REASONS:
            return True
        entry = multi_shadow_negative_expectancy_entry(symbol)
        if not isinstance(entry, dict) or not entry:
            return False
        closed_cycles = as_float(first_value(entry, ("closed_cycles",), 0)) or 0.0
        fast_fail_cycles = as_float(first_value(entry, ("fast_fail_closed_cycles",), 0)) or 0.0
        net_bps = as_float(first_value(entry, ("net_expectancy_bps", "expectancy_bps"), not_obs))
        fast_fail_bps = as_float(first_value(entry, ("fast_fail_net_expectancy_bps", "fast_fail_expectancy_bps"), not_obs))
        return bool(
            (closed_cycles > 0 and net_bps is not None and net_bps < 0)
            or (fast_fail_cycles > 0 and fast_fail_bps is not None and fast_fail_bps < 0)
        )

    def multi_shadow_alpha6_confirmed_for_swing(explain):
        alpha6_side = flatten_value(first_value(explain, ("alpha6_side",), "")).lower()
        alpha6_score = as_float(first_value(explain, ("alpha6_score",), not_obs))
        f4 = as_float(first_value(explain, ("f4_volume_expansion",), not_obs))
        f5 = as_float(first_value(explain, ("f5_rsi_trend_confirm",), not_obs))
        min_alpha6 = config_number("swing_min_alpha6_score")
        min_f5 = config_number("swing_min_f5_rsi")
        min_f4 = config_number("swing_min_f4_volume")
        min_alpha6 = 0.50 if min_alpha6 is None else float(min_alpha6)
        min_f5 = 0.30 if min_f5 is None else float(min_f5)
        min_f4 = 0.0 if min_f4 is None else float(min_f4)
        return bool(
            alpha6_side == "buy"
            and alpha6_score is not None and alpha6_score >= min_alpha6
            and f4 is not None and f4 >= min_f4
            and f5 is not None and f5 >= min_f5
        )

    def multi_shadow_return_bps(symbol, when_dt, hours=4):
        if when_dt is None:
            return None
        start_px = cache_price_at_or_after(symbol, when_dt - dt.timedelta(hours=int(hours)))[0]
        end_px = cache_price_at_or_after(symbol, when_dt)[0]
        if start_px is None or end_px is None or start_px <= 0 or end_px <= 0:
            return None
        return ((end_px / start_px) - 1.0) * 10000.0

    def multi_shadow_collect_candidates_for_audit(run_id, audit, shadow_mode=MULTI_SHADOW_MODE_ALL):
        if not bool(config_bool("multi_position_swing_shadow_enabled", True)):
            return [], "disabled"
        if audit_is_risk_off(audit):
            return [], "risk_off"
        if shadow_mode == MULTI_SHADOW_MODE_PROTECT_RECOVERY:
            allowed_symbols = set(multi_shadow_protect_recovery_allowed_symbols())
            min_positive = config_number("protect_recovery_min_positive_whitelist_4h_count")
            min_positive = 3 if min_positive is None else int(min_positive)
            require_market_context = config_bool("protect_recovery_require_market_context", True)
            positive_count = 0
            audit_ts_for_context = parse_dt_utc(run_ts(run_id, audit)) or parse_run_time(run_id)
            for symbol in allowed_symbols:
                ret_bps = multi_shadow_return_bps(symbol, audit_ts_for_context, hours=4)
                if ret_bps is not None and ret_bps > 0:
                    positive_count += 1
            if require_market_context and positive_count < min_positive:
                return [], f"protect_recovery_market_context_not_met:{positive_count}/{min_positive}"
        else:
            allowed_symbols = {
                normalize_symbol_text(symbol)
                for symbol in config_string_list(
                    "multi_position_swing_shadow_symbols",
                    ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"],
                )
            }
        min_score = config_number("multi_position_swing_shadow_min_final_score")
        if min_score is None:
            min_score = 0.30
        audit_ts = parse_dt_utc(run_ts(run_id, audit)) or parse_run_time(run_id)
        targets = audit.get("targets_post_risk") if isinstance(audit.get("targets_post_risk"), dict) else {}
        explain_map = target_explain_by_symbol(audit)
        ordered = []
        seen = set()

        for idx, row in enumerate(audit.get("top_scores") or [], start=1):
            if not isinstance(row, dict):
                continue
            symbol = normalize_symbol_text(first_value(row, ("symbol", "instId"), ""))
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            ordered.append((symbol, row, idx))

        for symbol in targets.keys():
            normalized = normalize_symbol_text(symbol)
            if normalized and normalized not in seen:
                seen.add(normalized)
                ordered.append((normalized, symbol_map_get(explain_map, normalized), len(ordered) + 1))

        candidates = []
        debug_reasons = []
        for symbol, row, ordinal in ordered:
            if symbol not in allowed_symbols:
                debug_reasons.append(f"{symbol}:not_allowed")
                continue
            explain = symbol_map_get(explain_map, symbol)
            final_score = first_observed(
                top_score_value(row),
                first_value(explain, ("final_score", "score", "display_score"), not_obs),
            )
            score_value = as_float(final_score)
            if score_value is None or score_value < float(min_score):
                debug_reasons.append(f"{symbol}:score_below_min")
                continue
            reasons = router_reasons_for_symbol(audit, symbol)
            has_negative_expectancy = multi_shadow_symbol_has_negative_expectancy(symbol, reasons)
            if shadow_mode == MULTI_SHADOW_MODE_ALL and reasons & MULTI_POSITION_NEGATIVE_EXPECTANCY_HARD_REASONS:
                debug_reasons.append(f"{symbol}:negative_expectancy_hard_cooldown")
                continue
            if (
                shadow_mode == MULTI_SHADOW_MODE_PROTECT_RECOVERY
                and config_bool("protect_recovery_disallow_symbols_with_negative_expectancy", True)
                and has_negative_expectancy
            ):
                debug_reasons.append(f"{symbol}:protect_recovery_negative_expectancy_excluded")
                continue
            entry_px, entry_source = multi_shadow_candidate_entry_px(symbol, audit, audit_ts)
            selected_rank = first_observed(
                first_value(row, ("rank", "base_rank", "selected_rank"), not_obs),
                first_value(explain, ("selected_rank", "rank"), ordinal),
            )
            entry_support = "alpha6_confirmed" if multi_shadow_alpha6_confirmed_for_swing(explain) else "score"
            candidates.append(
                {
                    "symbol": symbol,
                    "final_score": float(score_value),
                    "selected_rank": as_int(selected_rank) or ordinal,
                    "target_w": target_weight_from_targets(targets, symbol),
                    "entry_px": entry_px,
                    "entry_px_source": entry_source,
                    "router_action": first_observed(first_value(explain, ("router_action",), not_obs)),
                    "router_reason": first_observed(first_value(explain, ("router_reason", "blocked_reason"), not_obs)),
                    "entry_support": entry_support,
                    "negative_expectancy_excluded": has_negative_expectancy,
                }
            )
        if shadow_mode == MULTI_SHADOW_MODE_PROTECT_RECOVERY:
            candidates.sort(key=lambda item: (0 if item.get("entry_support") == "alpha6_confirmed" else 1, -float(item.get("final_score") or 0.0), int(item.get("selected_rank") or 999)))
        else:
            candidates.sort(key=lambda item: (-float(item.get("final_score") or 0.0), int(item.get("selected_rank") or 999)))
        return candidates, ";".join(debug_reasons)

    def generate_multi_position_swing_shadow_label_rows():
        generated = []
        debug_rows = []
        rt_cost_bps = config_number("multi_position_swing_shadow_rt_cost_bps")
        if rt_cost_bps is None:
            rt_cost_bps = 30.0
        for run_id, audit in sorted(audit_by_run.items()):
            raw_qualified_count = 0
            min_score = config_number("multi_position_swing_shadow_min_final_score")
            if min_score is None:
                min_score = 0.30
            allowed = {
                normalize_symbol_text(symbol)
                for symbol in config_string_list(
                    "multi_position_swing_shadow_symbols",
                    ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"],
                )
            }
            for row in audit.get("top_scores") or []:
                if not isinstance(row, dict):
                    continue
                symbol = normalize_symbol_text(first_value(row, ("symbol", "instId"), ""))
                score = as_float(top_score_value(row))
                if symbol in allowed and score is not None and score >= float(min_score):
                    raw_qualified_count += 1
            audit_ts_text = canonical_ts_utc(run_ts(run_id, audit))
            for shadow_mode in MULTI_SHADOW_MODES:
                candidates, debug_reason = multi_shadow_collect_candidates_for_audit(run_id, audit, shadow_mode=shadow_mode)
                if raw_qualified_count > 0 and not candidates:
                    debug_rows.append(
                        {
                            "ts_utc": audit_ts_text,
                            "run_id": run_id,
                            "shadow_mode": shadow_mode,
                            "qualified_candidate_count": raw_qualified_count,
                            "debug_reason": debug_reason or "no_candidates_after_filter",
                        }
                    )
                    continue
                if not candidates:
                    continue
                for k in range(1, min(3, len(candidates)) + 1):
                    selected = candidates[:k]
                    symbols = [item["symbol"] for item in selected]
                    generated.append(
                        {
                            "ts_utc": audit_ts_text,
                            "run_id": run_id,
                            "shadow_mode": shadow_mode,
                            "k": k,
                            "symbols": symbols,
                            "equal_weight": round(1.0 / float(k), 8),
                            "entry_px": {item["symbol"]: item.get("entry_px") for item in selected},
                            "entry_px_by_symbol": {item["symbol"]: item.get("entry_px") for item in selected},
                            "entry_px_source": {item["symbol"]: item.get("entry_px_source") for item in selected},
                            "final_score": {item["symbol"]: item.get("final_score") for item in selected},
                            "final_score_by_symbol": {item["symbol"]: item.get("final_score") for item in selected},
                            "selected_rank": {item["symbol"]: item.get("selected_rank") for item in selected},
                            "entry_support": {item["symbol"]: item.get("entry_support") for item in selected},
                            "target_w": {item["symbol"]: item.get("target_w") for item in selected},
                            "rt_cost_bps": rt_cost_bps,
                            "label_status": "pending",
                            "debug_reason": f"generated_from_decision_audit:{shadow_mode}",
                        }
                    )
        return generated, debug_rows

    generated_multi_shadow_rows, multi_position_swing_shadow_debug_rows = generate_multi_position_swing_shadow_label_rows()
    if generated_multi_shadow_rows:
        multi_position_swing_shadow_label_rows.extend(generated_multi_shadow_rows)
        multi_position_swing_shadow_label_rows, generated_duplicate_count = dedupe_rows_by_key(
            multi_position_swing_shadow_label_rows,
            multi_position_swing_shadow_row_key,
        )
        multi_position_swing_shadow_duplicate_count += generated_duplicate_count
        multi_shadow_text = "\n".join(
            json.dumps(sanitize_obj(row), ensure_ascii=False, sort_keys=True)
            for row in multi_position_swing_shadow_label_rows
        )
        write_text(
            "raw/reports/multi_position_swing_shadow_labels.jsonl",
            multi_shadow_text + ("\n" if multi_shadow_text else ""),
        )
        if "reports/multi_position_swing_shadow_labels.jsonl" in missing_paths:
            missing_paths.remove("reports/multi_position_swing_shadow_labels.jsonl")

    def multi_shadow_entry_px_map(row, symbols):
        raw = first_value(row, ("entry_px_by_symbol", "entry_px"), {})
        parsed = parse_json_obj(raw, {})
        out = {}
        if isinstance(parsed, dict):
            for symbol in symbols:
                value = positive_float(first_value(parsed, (symbol, symbol.replace("/", "-")), not_obs))
                if value is not None:
                    out[symbol] = value
        for symbol in symbols:
            if symbol in out:
                continue
            audit = audit_by_run.get(first_observed(first_value(row, ("run_id",), not_obs)), {})
            target_item = symbol_map_get(target_explain_by_symbol(audit), symbol)
            price = price_from_dict(target_item, ("entry_px", "latest_px", "current_px", "price", "px"))
            if price is None:
                price = symbol_price_from_nested(audit, symbol)
            if price is None:
                price = cache_price_at_or_after(symbol, parse_dt_utc(first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs))))[0]
            if price is None:
                price = provider_entry_price_for_symbol(
                    symbol,
                    parse_dt_utc(first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs))),
                )[0]
            if price is not None:
                out[symbol] = price
        return out

    def build_multi_position_swing_shadow_row(row):
        symbols = parse_symbols_list(first_value(row, ("symbols", "symbols_json"), ""))
        entry_dt = parse_dt_utc(first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs)))
        entry_px_map = multi_shadow_entry_px_map(row, symbols)
        rt_cost_bps = as_float(first_value(row, ("rt_cost_bps",), not_obs))
        if rt_cost_bps is None:
            rt_cost_bps = 0.0
        out = {
            "ts_utc": first_observed(first_value(row, ("ts_utc", "entry_ts", "timestamp", "ts"), not_obs)),
            "run_id": first_observed(first_value(row, ("run_id",), not_obs)),
            "shadow_mode": first_observed(first_value(row, ("shadow_mode",), MULTI_SHADOW_MODE_ALL)),
            "k": first_observed(first_value(row, ("k", "top_k"), len(symbols) if symbols else not_obs)),
            "symbols": json.dumps(symbols, ensure_ascii=False),
            "equal_weight": first_observed(first_value(row, ("equal_weight",), (1.0 / len(symbols)) if symbols else not_obs)),
            "entry_px": json.dumps(entry_px_map, ensure_ascii=False, sort_keys=True) if entry_px_map else not_obs,
            "entry_px_by_symbol": json.dumps(entry_px_map, ensure_ascii=False, sort_keys=True) if entry_px_map else not_obs,
            "final_score": json.dumps(parse_json_obj(first_value(row, ("final_score", "final_scores"), {}), {}), ensure_ascii=False, sort_keys=True),
            "final_score_by_symbol": json.dumps(parse_json_obj(first_value(row, ("final_score_by_symbol", "final_score", "final_scores"), {}), {}), ensure_ascii=False, sort_keys=True),
            "selected_rank": json.dumps(parse_json_obj(first_value(row, ("selected_rank", "selected_ranks"), {}), {}), ensure_ascii=False, sort_keys=True),
            "entry_support": json.dumps(parse_json_obj(first_value(row, ("entry_support",), {}), {}), ensure_ascii=False, sort_keys=True),
            "rt_cost_bps": fmt_num(rt_cost_bps, 6),
            "debug_reason": first_observed(first_value(row, ("debug_reason",), "")),
        }
        horizon_statuses = []
        for horizon in multi_position_swing_horizons:
            status_field = f"label_{horizon}h_status"
            avg_field = f"label_{horizon}h_portfolio_avg_net_bps"
            worst_field = f"label_{horizon}h_worst_symbol_net_bps"
            win_field = f"label_{horizon}h_win_count"
            symbol_field = f"label_{horizon}h_symbol_net_bps"
            reason_field = f"label_{horizon}h_reason"

            existing_avg = first_value(row, (avg_field, f"portfolio_avg_{horizon}h_net_bps"), not_obs)
            if as_float(existing_avg) is not None:
                out[avg_field] = first_observed(existing_avg)
                out[f"label_{horizon}h_net_bps"] = first_observed(existing_avg)
                out[worst_field] = first_observed(first_value(row, (worst_field, f"worst_symbol_{horizon}h_net_bps"), not_obs))
                out[win_field] = first_observed(first_value(row, (win_field, f"win_count_{horizon}h"), not_obs))
                out[symbol_field] = first_observed(first_value(row, (symbol_field,), not_obs))
                out[status_field] = first_observed(first_value(row, (status_field,), "complete"))
                out[reason_field] = first_observed(first_value(row, (reason_field,), ""))
                horizon_statuses.append(out[status_field] if out[status_field] in ("pending", "not_observable", "complete") else "complete")
                continue

            if entry_dt is None or not symbols or len(entry_px_map) != len(symbols):
                missing_symbols = [symbol for symbol in symbols if symbol not in entry_px_map]
                out[avg_field] = not_obs
                out[f"label_{horizon}h_net_bps"] = not_obs
                out[worst_field] = not_obs
                out[win_field] = not_obs
                out[symbol_field] = not_obs
                out[status_field] = "not_observable"
                out[reason_field] = "missing_entry_px" if missing_symbols else "missing_entry_ts"
                horizon_statuses.append("not_observable")
                continue

            horizon_dt = entry_dt + dt.timedelta(hours=int(horizon))
            if NOW < horizon_dt:
                out[avg_field] = "pending"
                out[f"label_{horizon}h_net_bps"] = "pending"
                out[worst_field] = "pending"
                out[win_field] = "pending"
                out[symbol_field] = "pending"
                out[status_field] = "pending"
                out[reason_field] = f"awaiting_horizon_until_{horizon_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                horizon_statuses.append("pending")
                continue

            symbol_net = {}
            missing = []
            for symbol in symbols:
                future_px, future_source, future_reason = future_price_for_symbol(symbol, horizon_dt)
                entry_px = as_float(entry_px_map.get(symbol))
                if entry_px is None or entry_px <= 0:
                    missing.append(f"{symbol}:missing_entry_px")
                    continue
                if future_px is None:
                    missing.append(f"{symbol}:{future_reason or 'missing_future_px'}")
                    continue
                symbol_net[symbol] = round(((future_px / entry_px) - 1.0) * 10000.0 - rt_cost_bps, 6)
            if missing or len(symbol_net) != len(symbols):
                out[avg_field] = not_obs
                out[f"label_{horizon}h_net_bps"] = not_obs
                out[worst_field] = not_obs
                out[win_field] = not_obs
                out[symbol_field] = json.dumps(symbol_net, ensure_ascii=False, sort_keys=True) if symbol_net else not_obs
                out[status_field] = "not_observable"
                out[reason_field] = ";".join(missing) or "missing_market_data"
                horizon_statuses.append("not_observable")
                continue
            values = list(symbol_net.values())
            out[avg_field] = fmt_num(sum(values) / len(values), 6)
            out[f"label_{horizon}h_net_bps"] = out[avg_field]
            out[worst_field] = fmt_num(min(values), 6)
            out[win_field] = str(sum(1 for value in values if value > 0))
            out[symbol_field] = json.dumps(symbol_net, ensure_ascii=False, sort_keys=True)
            out[status_field] = "complete"
            out[reason_field] = ""
            horizon_statuses.append("complete")

        if any(status == "complete" for status in horizon_statuses):
            out["label_status"] = "complete"
        elif any(status == "not_observable" for status in horizon_statuses):
            out["label_status"] = "not_observable"
        elif any(status == "pending" for status in horizon_statuses):
            out["label_status"] = "pending"
        else:
            out["label_status"] = first_observed(first_value(row, ("label_status",), not_obs))
        return out

    def aggregate_multi_position_by_k(rows):
        grouped = defaultdict(list)
        for row in rows:
            grouped[(
                flatten_value(row.get("shadow_mode") or MULTI_SHADOW_MODE_ALL),
                flatten_value(row.get("k") or not_obs),
            )].append(row)
        out = []
        for (shadow_mode, k), group_rows in sorted(grouped.items()):
            payload = {"shadow_mode": shadow_mode, "k": k, "count": len(group_rows)}
            for horizon in multi_position_swing_horizons:
                values = [as_float(row.get(f"label_{horizon}h_portfolio_avg_net_bps")) for row in group_rows]
                usable = [value for value in values if value is not None]
                payload[f"avg_{horizon}h_net_bps"] = round(sum(usable) / len(usable), 6) if usable else not_obs
            values_24h = [as_float(row.get("label_24h_portfolio_avg_net_bps")) for row in group_rows]
            usable_24h = [value for value in values_24h if value is not None]
            worst_24h = [as_float(row.get("label_24h_worst_symbol_net_bps")) for row in group_rows]
            usable_worst_24h = [value for value in worst_24h if value is not None]
            payload["win_rate"] = round(sum(1 for value in usable_24h if value > 0) / len(usable_24h), 6) if usable_24h else not_obs
            payload["worst_avg"] = round(sum(usable_worst_24h) / len(usable_worst_24h), 6) if usable_worst_24h else not_obs
            out.append(payload)
        return out

    def aggregate_multi_position_by_symbol(rows):
        grouped = defaultdict(list)
        for row in rows:
            for symbol in parse_symbols_list(row.get("symbols")):
                grouped[(flatten_value(row.get("shadow_mode") or MULTI_SHADOW_MODE_ALL), symbol)].append(row)
        out = []
        for (shadow_mode, symbol), group_rows in sorted(grouped.items()):
            payload = {"shadow_mode": shadow_mode, "symbol": symbol, "count": len(group_rows)}
            for horizon in multi_position_swing_horizons:
                values = []
                for row in group_rows:
                    per_symbol = parse_json_obj(row.get(f"label_{horizon}h_symbol_net_bps"), {})
                    if isinstance(per_symbol, dict):
                        value = as_float(first_value(per_symbol, (symbol, symbol.replace("/", "-")), not_obs))
                        if value is not None:
                            values.append(value)
                payload[f"avg_{horizon}h_net_bps"] = round(sum(values) / len(values), 6) if values else not_obs
                payload[f"win_rate_{horizon}h"] = round(sum(1 for value in values if value > 0) / len(values), 6) if values else not_obs
            out.append(payload)
        return out

    multi_position_swing_shadow_rows = [
        build_multi_position_swing_shadow_row(row)
        for row in multi_position_swing_shadow_label_rows
    ]
    multi_position_swing_shadow_by_k = aggregate_multi_position_by_k(multi_position_swing_shadow_rows)
    multi_position_swing_shadow_by_symbol = aggregate_multi_position_by_symbol(multi_position_swing_shadow_rows)

    def factor_forward_value(row, horizon):
        key = btc_label_key(
            row.get("run_id"),
            row.get("ts_utc"),
            row.get("symbol"),
            row.get("router_reason"),
        )
        label = label_index.get(key) if key and all(part != not_obs for part in key) else None
        outcome = outcome_index.get(key) if key and all(part != not_obs for part in key) else None
        if not (label or outcome) and key and all(part != not_obs for part in key):
            loose_key = (key[0], key[2], key[3])
            label = label_loose_index.get(loose_key)
            outcome = outcome_loose_index.get(loose_key)
        src = outcome or label or {}
        if src:
            value = first_value(src, (f"forward_{horizon}h_net_bps", f"label_{horizon}h_net_bps"), not_obs)
            if value != not_obs:
                return flatten_value(value)
            status = flatten_value(first_value(src, (f"label_{horizon}h_status", "label_status", "label_24h_status"), ""))
            if status == "pending":
                return "pending"
            if status == "not_observable":
                return not_obs
        age_hours = parse_time_to_hours_ago(row.get("ts_utc"))
        if age_hours is not None and age_hours < float(horizon):
            return "pending"
        return not_obs

    for row in factor_contribution_rows:
        for horizon in label_horizons:
            row[f"forward_{horizon}h_net_bps"] = factor_forward_value(row, horizon)

    f3_dominant_swing_guard_outcomes = [dict(row) for row in f3_dominant_swing_guard_cases]
    for row in f3_dominant_swing_guard_outcomes:
        for horizon in label_horizons:
            row[f"forward_{horizon}h_net_bps"] = factor_forward_value(row, horizon)

    def aggregate_factor_contribution(rows):
        grouped = defaultdict(list)
        for row in rows:
            factor = row.get("dominant_factor") or not_obs
            if factor == not_obs:
                continue
            grouped[factor].append(row)
        out = []
        for factor, group_rows in sorted(grouped.items()):
            payload = {"dominant_factor": factor, "count": len(group_rows)}
            for horizon in label_horizons:
                values = [as_float(row.get(f"forward_{horizon}h_net_bps")) for row in group_rows]
                usable = [value for value in values if value is not None]
                payload[f"avg_{horizon}h_net_bps"] = round(sum(usable) / len(usable), 6) if usable else not_obs
                payload[f"win_rate_{horizon}h"] = round(sum(1 for value in usable if value > 0) / len(usable), 6) if usable else not_obs
            out.append(payload)
        return out

    factor_contribution_outcomes_by_factor = aggregate_factor_contribution(factor_contribution_rows)
    f3_dominant_row = next(
        (
            row for row in factor_contribution_outcomes_by_factor
            if row.get("dominant_factor") == "f3_vol_adj_ret"
        ),
        {},
    )
    f3_dominant_count = as_int(f3_dominant_row.get("count")) if f3_dominant_row else 0
    f3_dominant_avg_4h_net_bps = as_float(f3_dominant_row.get("avg_4h_net_bps")) if f3_dominant_row else None
    f3_dominant_avg_8h_net_bps = as_float(f3_dominant_row.get("avg_8h_net_bps")) if f3_dominant_row else None
    f3_dominant_avg_12h_net_bps = as_float(f3_dominant_row.get("avg_12h_net_bps")) if f3_dominant_row else None
    f3_dominant_avg_24h_net_bps = as_float(f3_dominant_row.get("avg_24h_net_bps")) if f3_dominant_row else None
    f3_dominant_win_rate_24h = as_float(f3_dominant_row.get("win_rate_24h")) if f3_dominant_row else None
    f3_dominant_swing_guard_candidate_count = len(f3_dominant_swing_guard_cases)
    f3_dominant_swing_guard_blocked_count = sum(
        1
        for row in f3_dominant_swing_guard_cases
        if row.get("swing_f3_dominant_blocked") == "true"
        or row.get("swing_hold_block_reason") == "swing_f3_dominant_not_qualified"
    )
    f3_dominant_swing_guard_still_swing_count = sum(
        1
        for row in f3_dominant_swing_guard_cases
        if row.get("dominant_factor") == "f3_vol_adj_ret" and row.get("swing_hold_position") == "true"
    )
    f3_dominant_negative_evidence = (
        f3_dominant_count >= 20
        and f3_dominant_avg_24h_net_bps is not None
        and f3_dominant_avg_24h_net_bps < -50.0
        and f3_dominant_win_rate_24h is not None
        and f3_dominant_win_rate_24h < 0.3
    )
    if f3_dominant_negative_evidence:
        add_issue(
            "medium",
            "f3_dominant_negative_evidence",
            "F3-vol-adjusted-return dominant candidates show materially negative 24h forward outcomes; monitor before considering any trading guard.",
            {
                "f3_dominant_count": f3_dominant_count,
                "avg_4h_net_bps": f3_dominant_avg_4h_net_bps,
                "avg_8h_net_bps": f3_dominant_avg_8h_net_bps,
                "avg_12h_net_bps": f3_dominant_avg_12h_net_bps,
                "avg_24h_net_bps": f3_dominant_avg_24h_net_bps,
                "win_rate_24h": f3_dominant_win_rate_24h,
            },
        )

    high_score_pending_count = 0
    high_score_matured_unlabeled_count = 0
    high_score_labelable_rows = [
        row for row in high_score_blocked_rows
        if is_high_score_labelable_reason(high_score_reason_text(row))
    ]
    high_score_non_entry_management_rows = [
        row for row in high_score_blocked_rows
        if not is_high_score_labelable_reason(high_score_reason_text(row))
    ]
    if audit_high_score_but_not_executed_count and not high_score_blocked_rows:
        add_issue(
            "medium",
            "high_score_blocked_targets_summary_missing",
            "Decision audit contains high_score_but_not_executed=true but high_score_blocked_targets.csv would be empty.",
            {"audit_high_score_but_not_executed_count": audit_high_score_but_not_executed_count},
        )

    for row in high_score_labelable_rows:
        key = btc_label_key(
            row.get("run_id"),
            row.get("ts_utc"),
            row.get("symbol"),
            row.get("router_reason"),
        )
        if not key or any(part == not_obs for part in key):
            continue
        loose_key = (key[0], key[2], key[3])
        label = label_index.get(key) or label_loose_index.get(loose_key)
        outcome = (
            outcome_index.get(key)
            or high_score_outcome_by_key.get(key)
            or outcome_loose_index.get(loose_key)
            or high_score_outcome_loose_index.get(loose_key)
        )
        src = outcome or label or {}
        label_status = flatten_value(first_value(src, ("label_status", "label_24h_status"), not_obs))
        if label_status == not_obs and not (label or outcome):
            label_status = "unlabeled"
        if label_status == "pending":
            high_score_pending_count += 1
        age_hours = parse_time_to_hours_ago(row.get("ts_utc"))
        if age_hours is None or age_hours < 24:
            continue
        evidence = {
            "run_id": row.get("run_id"),
            "ts_utc": row.get("ts_utc"),
            "symbol": row.get("symbol"),
            "skip_reason": row.get("router_reason"),
            "age_hours": age_hours,
            "unique_key": btc_label_key_text(key),
        }
        if not label and not outcome:
            high_score_matured_unlabeled_count += 1
            add_issue(
                "high",
                "high_score_blocked_matured_without_label",
                "High-score blocked target is mature but was not found in skipped candidate labels or outcomes.",
                evidence,
            )
        elif label_status == "pending":
            add_issue(
                "high",
                "high_score_blocked_matured_label_pending",
                "High-score blocked target is mature but its skipped candidate label is still pending.",
                evidence,
            )

    def explicit_entry_px(src, item):
        return first_observed(
            first_value(src or {}, ("entry_px", "price", "px"), not_obs),
            first_value(item or {}, ("entry_px", "price", "px"), not_obs),
        )

    def display_entry_px(src, item):
        return first_observed(
            explicit_entry_px(src, item),
            first_value(item or {}, ("latest_px", "last_px"), not_obs),
        )

    def not_observable_reason_for(skip_reason, src, item):
        if skip_reason == "btc_leadership_probe_not_flat" and explicit_entry_px(src, item) == not_obs:
            return "not_flat"
        if skip_reason == "btc_leadership_probe_cooldown" and explicit_entry_px(src, item) == not_obs:
            return "cooldown"
        if skip_reason in BTC_LEADERSHIP_LABELABLE_REASONS and explicit_entry_px(src, item) == not_obs:
            return "entry_px_missing"
        return flatten_value(first_value(src or {}, ("label_not_observable_reason", "not_observable_reason"), ""))

    btc_skip_decisions = list(btc_skip_decisions_by_key.values())
    labeled_complete_count = 0
    pending_count = 0
    not_observable_count = 0
    unlabeled_high_issue_count = 0
    duplicated_removed = btc_skip_decision_duplicates_removed + label_duplicate_count + outcome_duplicate_count

    for decision in btc_skip_decisions:
        item = decision["item"]
        symbol = flatten_value(item.get("symbol")) or not_obs
        reason = flatten_value(item.get("reason")) or not_obs
        decision_key = decision["unique_key"]
        label = label_index.get(decision_key)
        outcome = outcome_index.get(decision_key)
        src = outcome or label or {}
        label_present = label is not None
        outcome_present = outcome is not None
        not_observable_reason = not_observable_reason_for(reason, src, item)
        label_status = flatten_value(first_value(src, ("label_status", "label_24h_status"), not_obs))
        if not_observable_reason:
            label_status = "not_observable"
        elif label_status == not_obs and not (label_present or outcome_present):
            label_status = "unlabeled"
        age_hours = parse_time_to_hours_ago(first_observed(first_value(src, ("ts_utc", "entry_ts", "entry_ts_ms"), not_obs), decision["ts_utc"]))
        age_text = f"{age_hours:.3f}" if age_hours is not None else not_obs
        missing_label = not label_present and not outcome_present and not not_observable_reason
        if missing_label:
            unlabeled_high_issue_count += 1
            add_issue(
                "high",
                "btc_leadership_blocked_cases_not_labeled",
                "BTC leadership probe skip decision was not found in skipped candidate labels or outcomes.",
                {"run_id": decision["run_id"], "ts_utc": decision["ts_utc"], "symbol": symbol, "skip_reason": reason, "unique_key": btc_label_key_text(decision_key)},
            )
        if label_status == "pending":
            pending_count += 1
        if label_status == "complete":
            labeled_complete_count += 1
        if label_status == "not_observable":
            not_observable_count += 1
        if label_status == "pending" and age_hours is not None and age_hours >= 24:
            add_issue(
                "high",
                "matured_skipped_candidates_still_pending",
                "Skipped candidate is old enough to mature but still pending.",
                {"run_id": decision["run_id"], "ts_utc": decision["ts_utc"], "symbol": symbol, "skip_reason": reason, "age_hours": age_hours, "unique_key": btc_label_key_text(decision_key)},
            )
        maturity_rows.append({
            "ts_utc": decision["ts_utc"],
            "run_id": decision["run_id"],
            "symbol": symbol,
            "skip_reason": reason,
            "action": flatten_value(item.get("action")),
            "label_present": str(label_present).lower(),
            "outcome_present": str(outcome_present).lower(),
            "label_status": label_status,
            "not_observable_reason": not_observable_reason,
            "age_hours": age_text,
            "maturity_issue": "not_observable" if not_observable_reason else ("missing_label_or_outcome" if missing_label else ("pending_after_maturity" if label_status == "pending" and age_hours is not None and age_hours >= 24 else "")),
            "raw_json": safe_json(item),
        })
        btc_blocked_rows.append({
            "ts_utc": decision["ts_utc"],
            "run_id": decision["run_id"],
            "symbol": symbol,
            "skip_reason": reason,
            "entry_px": not_obs if not_observable_reason else display_entry_px(src, item),
            "age_hours": age_text,
            "label_4h_net_bps": flatten_value(first_value(src, ("label_4h_net_bps",), not_obs)),
            "label_8h_net_bps": flatten_value(first_value(src, ("label_8h_net_bps",), not_obs)),
            "label_12h_net_bps": flatten_value(first_value(src, ("label_12h_net_bps",), not_obs)),
            "label_24h_net_bps": flatten_value(first_value(src, ("label_24h_net_bps",), not_obs)),
            "label_status": label_status,
            "not_observable_reason": not_observable_reason,
            "alpha6_score": flatten_value(first_value(src, ("alpha6_score",), item.get("alpha6_score", not_obs))),
            "f4_volume_expansion": flatten_value(first_value(src, ("f4_volume_expansion",), item.get("f4_volume_expansion", not_obs))),
            "f5_rsi_trend_confirm": flatten_value(first_value(src, ("f5_rsi_trend_confirm",), item.get("f5_rsi_trend_confirm", not_obs))),
            "rolling_high": flatten_value(first_value(src, ("rolling_high",), item.get("rolling_high", not_obs))),
            "breakout_met": flatten_value(first_value(src, ("breakout_met",), item.get("breakout_met", not_obs))),
            "net_expectancy_bps": flatten_value(first_value(src, ("net_expectancy_bps",), item.get("net_expectancy_bps", not_obs))),
            "closed_cycles": flatten_value(first_value(src, ("closed_cycles",), item.get("closed_cycles", not_obs))),
        })

    btc_blocked_labeler_summary = {
        "total_blocked": len(btc_skip_decisions),
        "labeled_complete": labeled_complete_count,
        "pending": pending_count,
        "not_observable": not_observable_count,
        "duplicated_removed": duplicated_removed,
        "unlabeled_high_issue_count": unlabeled_high_issue_count,
    }

    live_text = (OUT / "raw" / "config_live_prod.yaml").read_text(encoding="utf-8", errors="replace") if (OUT / "raw" / "config_live_prod.yaml").is_file() else ""
    effective_path = OUT / "raw" / "reports" / "effective_live_config.json"
    effective_data = load_json(effective_path) if effective_path.is_file() else {}
    effective_keys = collect_config_keys_from_json(effective_data)
    live_missing = [key for key in BTC_LEADERSHIP_CONFIG_KEYS if key not in live_text]
    effective_missing = [key for key in BTC_LEADERSHIP_CONFIG_KEYS if key not in effective_keys]
    btc_config_audit = {
        "seen_in_decision_audit": bool(btc_seen_in_decision_audit),
        "present_in_live_prod_yaml": any(key in live_text for key in BTC_LEADERSHIP_CONFIG_KEYS),
        "present_in_effective_config": any(key in effective_keys for key in BTC_LEADERSHIP_CONFIG_KEYS),
        "missing_keys": [f"live_prod_yaml:{key}" for key in live_missing] + [f"effective_config:{key}" for key in effective_missing],
        "recommendation": "not_needed" if not btc_seen_in_decision_audit or (not live_missing and not effective_missing) else "make btc_leadership_probe_* keys explicit in live_prod.yaml and effective_live_config.json for observability",
    }
    write_text("summaries/btc_leadership_config_audit.json", json.dumps(btc_config_audit, ensure_ascii=False, indent=2) + "\n")
    if btc_seen_in_decision_audit and (live_missing or effective_missing):
        severity = "high" if len(live_missing) == len(BTC_LEADERSHIP_CONFIG_KEYS) or len(effective_missing) == len(BTC_LEADERSHIP_CONFIG_KEYS) else "medium"
        add_issue(
            severity,
            "btc_leadership_probe_missing_effective_config",
            "BTC leadership probe appears in decision audits but key config entries are missing from live or effective config artifacts.",
            {"missing_keys": btc_config_audit["missing_keys"]},
        )

    probe_exit_config_missing = []
    for key in PROBE_EXIT_CONFIG_KEYS:
        if key not in live_text and key not in effective_keys:
            probe_exit_config_missing.append(key)
    if probe_trade_rows and probe_exit_config_missing:
        add_issue(
            "medium",
            "probe_exit_policy_config_missing_while_probe_trades_exist",
            "Probe trades exist but probe exit policy keys are not observable in config artifacts.",
            {"missing_keys": probe_exit_config_missing},
        )
    probe_dust_residual_count = sum(1 for row in dust_residual_roundtrip_rows if is_probe_trade_row(row))
    if probe_counts["market_impulse_probe_open_count"] > 0 and not lifecycle_rows and not probe_dust_residual_count:
        add_issue(
            "high",
            "market_impulse_probe_opened_but_no_lifecycle_audit_row",
            "market_impulse_probe opened but no lifecycle audit row was observable.",
            {},
        )

    raw_trade_file_rows = len(raw_trade_events)
    has_trade_data = bool(trade_paths) and trade_read_errors == 0
    if not has_trade_data:
        trade_observation_status = "not_observable"
    elif raw_trade_file_rows == 0 and len(trade_rows) == 0:
        trade_observation_status = "no_trades"
    else:
        trade_observation_status = "observed"

    def symbol_asset(symbol):
        text = flatten_value(symbol)
        return text.split("/", 1)[0] if "/" in text else text

    def numeric_first(*values):
        for value in values:
            number = as_float(value)
            if number is not None:
                return number
        return None

    def text_first(*values):
        for value in values:
            text = flatten_value(value)
            if text not in ("", not_obs):
                return text
        return not_obs

    def bool_value(value):
        if isinstance(value, bool):
            return value
        text = flatten_value(value).strip().lower()
        return text in {"true", "1", "yes", "y", "on"}

    def position_entry_for_symbol(symbol):
        for source in (positions_state, ledger_state):
            for item in all_dicts(source):
                item_symbol = flatten_value(first_value(item, ("symbol", "instId", "instrument"), ""))
                if item_symbol == symbol:
                    return item
        return {}

    def ledger_qty_for_symbol(symbol):
        asset = symbol_asset(symbol)
        balances = ledger_state.get("balances") if isinstance(ledger_state, dict) else {}
        if isinstance(balances, dict):
            qty = as_float(balances.get(asset))
            if qty is not None:
                return qty
        for item in all_dicts(ledger_state):
            item_symbol = flatten_value(first_value(item, ("symbol", "instId", "instrument"), ""))
            item_asset = flatten_value(first_value(item, ("asset", "currency", "coin"), ""))
            if item_symbol == symbol or item_asset == asset:
                qty = numeric_first(first_value(item, ("qty", "amount", "available", "balance", "total"), not_obs))
                if qty is not None:
                    return qty
        return None

    def ledger_value_for_symbol(symbol):
        asset = symbol_asset(symbol)
        for item in all_dicts(ledger_state):
            item_symbol = flatten_value(first_value(item, ("symbol", "instId", "instrument"), ""))
            item_asset = flatten_value(first_value(item, ("asset", "currency", "ccy", "coin"), ""))
            if item_symbol == symbol or item_asset == asset:
                value = numeric_first(
                    first_value(item, ("eqUsd", "eq_usd", "value_usdt", "usd_value", "notional_usdt", "current_value_usdt"), not_obs)
                )
                if value is not None:
                    return value
        return None

    def max_numeric(*values):
        nums = [as_float(value) for value in values]
        nums = [value for value in nums if value is not None]
        return max(nums) if nums else None

    def append_dust_position_from_roundtrip_row(row, current_value, dust_threshold, diagnosis):
        key = (row.get("symbol"), row.get("entry_ts"), row.get("qty"), fmt_num(current_value, 12))
        dust_residual_position_keys.add(key)
        csv_key = ("open_dust_residual_ignored", row.get("source_file"), row.get("row_number"), row.get("symbol"), row.get("qty"), fmt_num(current_value, 12))
        if csv_key in dust_residual_row_keys:
            return
        dust_residual_row_keys.add(csv_key)
        dust_residual_roundtrip_rows.append({
            "run_id": row.get("run_id", not_obs),
            "source_file": row.get("source_file", not_obs),
            "row_number": row.get("row_number", not_obs),
            "timestamp": row.get("timestamp", not_obs),
            "symbol": row.get("symbol", not_obs),
            "side": row.get("side", not_obs),
            "qty": row.get("qty", not_obs),
            "price": row.get("price", not_obs),
            "entry_ts": row.get("entry_ts", not_obs),
            "entry_px": row.get("entry_px", not_obs),
            "exit_ts": row.get("exit_ts", not_obs),
            "exit_px": row.get("exit_px", not_obs),
            "entry_reason": row.get("entry_reason", not_obs),
            "exit_reason": row.get("exit_reason", not_obs),
            "probe_type": row.get("probe_type", not_obs),
            "roundtrip_status": "open_dust_residual_ignored",
            "gross_pnl_usdt": not_obs,
            "fee_total_usdt": row.get("fee_total_usdt", not_obs),
            "net_pnl_usdt": not_obs,
            "gross_bps": not_obs,
            "net_bps": not_obs,
            "hold_minutes": not_obs,
            "remaining_value_usdt": fmt_num(current_value, 12),
            "dust_threshold_usdt": fmt_num(dust_threshold, 12),
            "diagnosis": diagnosis,
            "raw_json": row.get("raw_json", "{}"),
        })

    def open_position_row_from_trade(row):
        symbol = row.get("symbol", not_obs)
        if symbol in ("", not_obs):
            return None
        pos_entry = position_entry_for_symbol(symbol)
        profit_entry = state_entry(profit_state, symbol) or {}
        highest_entry = state_entry(highest_state, symbol) or {}
        stop_entry = state_entry(stop_state, symbol) or {}
        fixed_stop_entry = state_entry(fixed_stop_state, symbol) or {}
        context = latest_symbol_context.get(symbol, {})

        qty = numeric_first(row.get("qty"), first_value(pos_entry, ("qty", "amount", "size", "position", "position_qty"), not_obs), ledger_qty_for_symbol(symbol))
        entry_px = numeric_first(row.get("entry_px"), row.get("price"), first_value(pos_entry, ("entry_px", "entry_price", "avg_entry_px", "avg_px"), not_obs), first_value(profit_entry, ("entry_px", "entry_price"), not_obs), first_value(stop_entry, ("entry_px", "entry_price"), not_obs), first_value(highest_entry, ("entry_px", "entry_price"), not_obs))
        current_px = numeric_first(context.get("current_px"), first_value(pos_entry, ("current_px", "mark_px", "mark_price", "last_px", "price"), not_obs), first_value(profit_entry, ("current_px", "current_price", "last_px"), not_obs))
        notional_entry = entry_px * qty if entry_px is not None and qty is not None else numeric_first(row.get("remaining_value_usdt"), first_value(pos_entry, ("notional_entry_usdt", "entry_notional_usdt"), not_obs))
        current_value = current_px * qty if current_px is not None and qty is not None else numeric_first(first_value(pos_entry, ("current_value_usdt", "value_usdt", "notional_usdt"), not_obs))
        ledger_current_value = ledger_value_for_symbol(symbol)
        effective_current_value = ledger_current_value if ledger_current_value is not None else current_value
        dust_threshold = dust_threshold_for_symbol(symbol)
        if is_dust_value(effective_current_value, dust_threshold):
            append_dust_position_from_roundtrip_row(
                row,
                effective_current_value,
                dust_threshold,
                "open_position_value_below_dust_threshold_excluded_from_effective_positions",
            )
            return None
        entry_fee = as_float(row.get("fee_total_usdt"))
        fee_rate = entry_fee / notional_entry if entry_fee is not None and notional_entry else None
        estimated_exit_fee = current_value * fee_rate if current_value is not None and fee_rate is not None else None
        gross_usdt = current_value - notional_entry if current_value is not None and notional_entry is not None else None
        net_usdt = gross_usdt - entry_fee - estimated_exit_fee if gross_usdt is not None and entry_fee is not None and estimated_exit_fee is not None else None
        gross_bps = gross_usdt / notional_entry * 10000.0 if gross_usdt is not None and notional_entry else None
        net_bps = net_usdt / notional_entry * 10000.0 if net_usdt is not None and notional_entry else None

        current_stop_px = max_numeric(
            first_value(profit_entry, ("current_stop", "current_stop_px", "current_stop_price", "stop_px", "stop_price"), not_obs),
            first_value(stop_entry, ("current_stop_price", "current_stop", "current_stop_px", "stop_px", "stop_price"), not_obs),
            first_value(fixed_stop_entry, ("current_stop_price", "current_stop", "stop_px", "stop_price"), not_obs),
        )
        highest_px = max_numeric(
            first_value(highest_entry, ("highest_px", "highest_price"), not_obs),
            first_value(profit_entry, ("highest_price", "highest_px"), not_obs),
            first_value(stop_entry, ("highest_price", "highest_px"), not_obs),
            current_px,
        )
        entry_reason = text_first(row.get("entry_reason"), first_value(pos_entry, ("entry_reason",), not_obs), first_value(profit_entry, ("entry_reason",), not_obs))
        probe_type = text_first(row.get("probe_type"), first_value(pos_entry, ("probe_type",), not_obs), first_value(profit_entry, ("probe_type",), not_obs))
        is_probe = entry_reason in PROBE_TYPES or probe_type in PROBE_TYPES or probe_type == "probe"
        stop_type = text_first(first_value(stop_entry, ("current_stop_type", "stop_type"), ""), first_value(profit_entry, ("current_action",), ""))
        profit_lock_active = bool(current_stop_px is not None and entry_px is not None and current_stop_px >= entry_px)
        trailing_active = bool_value(first_value(stop_entry, ("is_trailing", "trailing_active"), False)) or "trailing" in stop_type.lower()
        open_row = {
            "symbol": symbol,
            "entry_ts": row.get("entry_ts", not_obs),
            "entry_px": fmt_num(entry_px, 10),
            "qty": fmt_num(qty, 12),
            "current_px": fmt_num(current_px, 10),
            "current_value_usdt": fmt_num(current_value, 12),
            "notional_entry_usdt": fmt_num(notional_entry, 12),
            "unrealized_gross_bps": fmt_num(gross_bps, 4),
            "unrealized_net_bps": fmt_num(net_bps, 4),
            "unrealized_net_usdt": fmt_num(net_usdt, 12),
            "entry_reason": entry_reason,
            "probe_type": probe_type,
            "current_stop_px": fmt_num(current_stop_px, 10),
            "highest_px": fmt_num(highest_px, 10),
            "current_level": text_first(context.get("current_level"), first_value(pos_entry, ("current_level", "risk_level"), not_obs)),
            "regime": text_first(context.get("regime"), first_value(pos_entry, ("regime", "market_regime"), not_obs)),
            "is_probe": str(is_probe).lower(),
            "profit_lock_active": str(profit_lock_active).lower(),
            "trailing_active": str(trailing_active).lower(),
        }
        if net_bps is not None and net_bps > 100 and not (profit_lock_active or trailing_active):
            add_issue(
                "medium",
                "open_profit_without_profit_lock",
                "Open position has unrealized net profit above 100 bps but no observable profit lock or trailing protection.",
                {
                    "symbol": symbol,
                    "entry_px": open_row["entry_px"],
                    "current_px": open_row["current_px"],
                    "unrealized_net_bps": open_row["unrealized_net_bps"],
                    "current_stop_px": open_row["current_stop_px"],
                    "profit_lock_active": open_row["profit_lock_active"],
                    "trailing_active": open_row["trailing_active"],
                },
            )
        return open_row

    for row in trade_rows:
        if row.get("roundtrip_status") in {"open", "open_residual"}:
            open_position = open_position_row_from_trade(row)
            if open_position:
                open_position_rows.append(open_position)

    closed_roundtrip_rows = [row for row in trade_rows if row.get("roundtrip_status") == "closed"]

    def probe_config_number(key, default):
        value = config_number(key)
        return float(value) if value is not None else float(default)

    probe_take_profit_net_bps_cfg = probe_config_number("probe_take_profit_net_bps", 80.0)
    probe_stop_loss_net_bps_cfg = probe_config_number("probe_stop_loss_net_bps", -50.0)
    probe_trailing_enable_after_net_bps_cfg = probe_config_number("probe_trailing_enable_after_net_bps", 50.0)
    probe_trailing_gap_bps_cfg = probe_config_number("probe_trailing_gap_bps", 25.0)
    probe_time_stop_hours_cfg = probe_config_number("probe_time_stop_hours", 8.0)
    probe_time_stop_min_net_bps_cfg = probe_config_number("probe_time_stop_min_net_bps", 10.0)
    active_probe_ignore_counts_by_symbol = Counter(
        row.get("symbol") or not_obs
        for row in router_rows
        if row.get("reason") == "active_probe_ignore_zero_target_close"
    )
    open_position_by_symbol = {row.get("symbol"): row for row in open_position_rows}

    def active_probe_state_present(symbol):
        return any(state_present(state, symbol) for state in (profit_state, highest_state, stop_state, fixed_stop_state))

    def probe_next_exit_condition(net_bps, highest_net_bps, hold_hours):
        parts = []
        if net_bps is None:
            parts.append("net_bps_not_observable")
        elif net_bps <= probe_stop_loss_net_bps_cfg:
            parts.append("probe_stop_loss_now")
        else:
            parts.append(f"stop_loss_buffer_bps={fmt_num(net_bps - probe_stop_loss_net_bps_cfg, 4)}")

        if net_bps is not None and net_bps >= probe_take_profit_net_bps_cfg:
            parts.append("probe_take_profit_now")
        elif net_bps is not None:
            parts.append(f"take_profit_remaining_bps={fmt_num(probe_take_profit_net_bps_cfg - net_bps, 4)}")

        if highest_net_bps is None:
            parts.append("trailing_high_not_observable")
        elif highest_net_bps < probe_trailing_enable_after_net_bps_cfg:
            parts.append(f"trailing_enable_remaining_bps={fmt_num(probe_trailing_enable_after_net_bps_cfg - highest_net_bps, 4)}")
        elif net_bps is None:
            parts.append("trailing_drawdown_not_observable")
        else:
            drawdown = highest_net_bps - net_bps
            if drawdown >= probe_trailing_gap_bps_cfg:
                parts.append("probe_trailing_stop_now")
            else:
                parts.append(f"trailing_gap_remaining_bps={fmt_num(probe_trailing_gap_bps_cfg - drawdown, 4)}")

        if hold_hours is None:
            parts.append("time_stop_hold_not_observable")
        elif hold_hours >= probe_time_stop_hours_cfg and (net_bps is None or net_bps < probe_time_stop_min_net_bps_cfg):
            parts.append("probe_time_stop_now")
        elif hold_hours < probe_time_stop_hours_cfg:
            parts.append(f"time_stop_remaining_hours={fmt_num(probe_time_stop_hours_cfg - hold_hours, 4)}")
        else:
            parts.append(f"time_stop_armed_requires_net_below_bps={fmt_num(probe_time_stop_min_net_bps_cfg, 4)}")
        return ";".join(parts)

    for trade_row in trade_rows:
        if trade_row.get("roundtrip_status") not in {"open", "open_residual"}:
            continue
        entry_reason = flatten_value(trade_row.get("entry_reason"))
        probe_type = first_observed(trade_row.get("probe_type"), probe_type_from_reason(entry_reason))
        if entry_reason not in PROBE_TYPES and probe_type not in PROBE_TYPES:
            continue
        symbol = trade_row.get("symbol") or not_obs
        open_position = open_position_by_symbol.get(symbol, {})
        profit_entry = state_entry(profit_state, symbol) or {}
        stop_entry = state_entry(stop_state, symbol) or {}
        entry_dt = parse_dt_utc(trade_row.get("entry_ts"))
        hold_hours = (NOW - entry_dt).total_seconds() / 3600.0 if entry_dt else None
        entry_px = as_float(first_observed(trade_row.get("entry_px"), open_position.get("entry_px")))
        current_px = as_float(open_position.get("current_px"))
        gross_bps = as_float(open_position.get("unrealized_gross_bps"))
        net_bps = as_float(open_position.get("unrealized_net_bps"))
        highest_net_bps = numeric_first(
            first_value(profit_entry, ("highest_net_bps",), not_obs),
            first_value(stop_entry, ("highest_net_bps",), not_obs),
            net_bps,
        )
        state_present_any = active_probe_state_present(symbol)
        ignore_count = int(active_probe_ignore_counts_by_symbol.get(symbol, 0))
        past_time_stop = bool(
            hold_hours is not None
            and hold_hours > probe_time_stop_hours_cfg + 1.0
        )
        if past_time_stop:
            diagnosis = "medium_issue_active_probe_past_time_stop"
            add_issue(
                "medium",
                "active_probe_past_time_stop",
                "Active probe is still open more than one hour past configured probe_time_stop_hours.",
                {
                    "symbol": symbol,
                    "probe_type": probe_type,
                    "entry_ts": trade_row.get("entry_ts", not_obs),
                    "hold_hours": fmt_num(hold_hours, 4),
                    "probe_time_stop_hours": fmt_num(probe_time_stop_hours_cfg, 4),
                    "unrealized_net_bps": fmt_num(net_bps, 4),
                },
            )
        elif state_present_any:
            diagnosis = "active_probe_zero_target_protected" if ignore_count else "active_probe_state_present"
        else:
            diagnosis = "active_probe_state_not_observable"
        open_probe_watch_rows.append({
            "symbol": symbol,
            "probe_type": probe_type,
            "entry_ts": trade_row.get("entry_ts", not_obs),
            "entry_px": fmt_num(entry_px, 10),
            "current_px": fmt_num(current_px, 10),
            "hold_hours": fmt_num(hold_hours, 4),
            "unrealized_gross_bps": fmt_num(gross_bps, 4),
            "unrealized_net_bps": fmt_num(net_bps, 4),
            "highest_net_bps": fmt_num(highest_net_bps, 4),
            "probe_take_profit_net_bps": fmt_num(probe_take_profit_net_bps_cfg, 4),
            "probe_stop_loss_net_bps": fmt_num(probe_stop_loss_net_bps_cfg, 4),
            "probe_trailing_enable_after_net_bps": fmt_num(probe_trailing_enable_after_net_bps_cfg, 4),
            "probe_trailing_gap_bps": fmt_num(probe_trailing_gap_bps_cfg, 4),
            "probe_time_stop_hours": fmt_num(probe_time_stop_hours_cfg, 4),
            "probe_time_stop_min_net_bps": fmt_num(probe_time_stop_min_net_bps_cfg, 4),
            "next_expected_exit_condition": probe_next_exit_condition(net_bps, highest_net_bps, hold_hours),
            "active_probe_ignore_zero_target_close_count": str(ignore_count),
            "state_present": str(state_present_any).lower(),
            "diagnosis": diagnosis,
        })

    for row in closed_roundtrip_rows:
        exit_reason = flatten_value(row.get("exit_reason"))
        entry_reason = flatten_value(row.get("entry_reason"))
        probe_type = first_observed(row.get("probe_type"), probe_type_from_reason(entry_reason))
        if (
            (entry_reason in PROBE_TYPES or probe_type in PROBE_TYPES)
            and exit_reason in {"zero_target_close", "normal_zero_target_close"}
        ):
            add_issue(
                "high",
                "active_probe_closed_by_zero_target_close",
                "Probe position was closed by normal zero_target_close instead of probe exit policy.",
                {
                    "symbol": row.get("symbol", not_obs),
                    "probe_type": probe_type,
                    "entry_ts": row.get("entry_ts", not_obs),
                    "exit_ts": row.get("exit_ts", not_obs),
                    "exit_reason": exit_reason,
                },
            )

    configured_swing_min_hold_hours = config_number("swing_min_hold_hours")
    if configured_swing_min_hold_hours is None:
        configured_swing_min_hold_hours = 24.0
    for row in closed_roundtrip_rows:
        hold_hours = as_float(row.get("hold_hours"))
        if hold_hours is None:
            hold_minutes = as_float(row.get("hold_minutes"))
            hold_hours = hold_minutes / 60.0 if hold_minutes is not None else None
        exit_reason = flatten_value(row.get("exit_reason"))
        exit_priority = first_observed(row.get("exit_priority"), exit_priority_for_reason(exit_reason))
        min_hold_hours = first_observed(row.get("min_hold_hours"), configured_swing_min_hold_hours)
        min_hold_f = as_float(min_hold_hours)
        is_early_soft_exit = (
            exit_priority == "soft"
            and hold_hours is not None
            and min_hold_f is not None
            and hold_hours < min_hold_f
        )
        if is_early_soft_exit:
            early_exit_rows.append({
                "ts_utc": first_observed(row.get("exit_ts"), row.get("timestamp")),
                "run_id": row.get("run_id", not_obs),
                "symbol": row.get("symbol", not_obs),
                "event_type": "closed_soft_exit_before_min_hold",
                "exit_reason": exit_reason,
                "exit_priority": exit_priority,
                "hold_hours": fmt_num(hold_hours, 4),
                "min_hold_hours": flatten_value(min_hold_hours),
                "exit_allowed_before_min_hold": bool_text(row.get("exit_allowed_before_min_hold")),
                "exit_blocked_by_min_hold": bool_text(row.get("exit_blocked_by_min_hold")),
                "min_hold_block_reason": row.get("min_hold_block_reason", ""),
                "actual_net_bps": row.get("net_bps", not_obs),
                "would_have_held_24h_status": row.get("would_have_held_24h_status", "not_observable_no_24h_price"),
                "would_have_held_24h_net_bps": row.get("would_have_held_24h_net_bps", not_obs),
                "early_exit_opportunity_cost_bps": row.get("early_exit_opportunity_cost_bps", not_obs),
                "diagnosis": "soft_exit_violated_swing_min_hold",
                "raw_json": row.get("raw_json", "{}"),
            })

    for row in router_rows:
        if row.get("reason") != "swing_min_hold_exit_block":
            continue
        try:
            raw = json.loads(row.get("raw_json") or "{}")
        except Exception:
            raw = {}
        early_exit_rows.append({
            "ts_utc": row.get("audit_timestamp", not_obs),
            "run_id": row.get("run_id", not_obs),
            "symbol": row.get("symbol", not_obs),
            "event_type": "pending_soft_exit_blocked_by_min_hold",
            "exit_reason": first_observed(row.get("source_reason"), first_value(raw, ("source_reason",), not_obs)),
            "exit_priority": first_observed(first_value(raw, ("exit_priority",), not_obs), "soft"),
            "hold_hours": flatten_value(first_value(raw, ("hold_hours",), not_obs)),
            "min_hold_hours": flatten_value(first_value(raw, ("min_hold_hours",), configured_swing_min_hold_hours)),
            "exit_allowed_before_min_hold": bool_text(first_value(raw, ("exit_allowed_before_min_hold",), False)),
            "exit_blocked_by_min_hold": bool_text(first_value(raw, ("exit_blocked_by_min_hold",), True)),
            "min_hold_block_reason": flatten_value(first_value(raw, ("min_hold_block_reason",), "soft_exit_before_swing_min_hold")),
            "actual_net_bps": not_obs,
            "would_have_held_24h_status": "pending_not_matured_or_no_24h_price",
            "would_have_held_24h_net_bps": not_obs,
            "early_exit_opportunity_cost_bps": flatten_value(first_value(raw, ("early_exit_opportunity_cost_bps",), not_obs)),
            "diagnosis": "soft_exit_blocked_by_swing_min_hold",
            "raw_json": row.get("raw_json", "{}"),
        })

    def entry_run_id_from_roundtrip_source(source_file):
        text = flatten_value(source_file).replace("\\", "/")
        first_part = text.split(";", 1)[0]
        match = re.search(r"(?:^|/)raw/recent_runs/([^/]+)/trades\.csv$", first_part)
        if match:
            return match.group(1)
        parts = [part for part in first_part.split("/") if part]
        for idx, part in enumerate(parts):
            if part == "recent_runs" and idx + 1 < len(parts):
                return parts[idx + 1]
        return not_obs

    def is_normal_non_probe_entry(row):
        entry_reason = flatten_value(row.get("entry_reason")).strip().lower()
        probe_type = flatten_value(row.get("probe_type")).strip()
        return (
            entry_reason in {"ok", "normal"}
            and row.get("entry_reason") not in PROBE_TYPES
            and row.get("exit_reason") not in PROBE_EXIT_REASONS
            and probe_type not in PROBE_TYPES
            and probe_type != "probe"
        )

    def result_bucket_for_net_bps(net_bps):
        value = as_float(net_bps)
        if value is None:
            return not_obs
        if value > 0:
            return "win"
        if value <= -100:
            return "loss_le_-100bps"
        if value < 0:
            return "loss"
        return "flat"

    def protect_sideways_context_for_roundtrip(row):
        symbol = row.get("symbol", not_obs)
        entry_run_id = entry_run_id_from_roundtrip_source(row.get("source_file"))
        return (
            entry_context_by_run_symbol.get((entry_run_id, symbol))
            or entry_context_by_run_symbol.get((row.get("run_id"), symbol))
            or {}
        )

    protect_sideways_normal_entry_rows = []
    for row in closed_roundtrip_rows:
        if not is_normal_non_probe_entry(row):
            continue
        context = protect_sideways_context_for_roundtrip(row)
        current_level = flatten_value(context.get("current_level", not_obs)).upper()
        regime = flatten_value(context.get("regime", not_obs)).lower()
        if current_level != "PROTECT" or regime != "sideways":
            continue
        protect_sideways_normal_entry_rows.append({
            "entry_ts": row.get("entry_ts", not_obs),
            "symbol": row.get("symbol", not_obs),
            "entry_px": row.get("entry_px", not_obs),
            "exit_ts": row.get("exit_ts", not_obs),
            "exit_px": row.get("exit_px", not_obs),
            "hold_minutes": row.get("hold_minutes", not_obs),
            "net_bps": row.get("net_bps", not_obs),
            "alpha6_score_at_entry": first_observed(context.get("alpha6_score")),
            "f4_at_entry": first_observed(context.get("f4_volume_expansion")),
            "f5_at_entry": first_observed(context.get("f5_rsi_trend_confirm")),
            "trend_score_at_entry": first_observed(context.get("trend_score")),
            "exit_reason": row.get("exit_reason", not_obs),
            "result_bucket": result_bucket_for_net_bps(row.get("net_bps")),
        })

    def aggregate_protect_sideways_rows(rows):
        grouped = defaultdict(list)
        for row in rows:
            grouped[row.get("symbol") or not_obs].append(row)
        out = []
        for symbol, group_rows in sorted(grouped.items()):
            net_values = [as_float(row.get("net_bps")) for row in group_rows]
            net_values = [value for value in net_values if value is not None]
            hold_values = [as_float(row.get("hold_minutes")) for row in group_rows]
            hold_values = [value for value in hold_values if value is not None]
            out.append({
                "symbol": symbol,
                "count": len(group_rows),
                "avg_net_bps": round(sum(net_values) / len(net_values), 6) if net_values else not_obs,
                "win_rate": round(sum(1 for value in net_values if value > 0) / len(net_values), 6) if net_values else not_obs,
                "avg_hold_minutes": round(sum(hold_values) / len(hold_values), 6) if hold_values else not_obs,
            })
        return out

    protect_sideways_normal_entry_by_symbol = aggregate_protect_sideways_rows(protect_sideways_normal_entry_rows)
    protect_sideways_net_values = [as_float(row.get("net_bps")) for row in protect_sideways_normal_entry_rows]
    protect_sideways_net_values = [value for value in protect_sideways_net_values if value is not None]
    protect_sideways_avg_net_bps = (
        sum(protect_sideways_net_values) / len(protect_sideways_net_values)
        if protect_sideways_net_values
        else None
    )
    if len(protect_sideways_normal_entry_rows) >= 5 and protect_sideways_avg_net_bps is not None and protect_sideways_avg_net_bps < -30.0:
        add_issue(
            "medium",
            "protect_sideways_normal_entry_negative",
            "PROTECT + Sideways normal non-probe entries have negative average realized net bps in the bundle window.",
            {
                "sample_count": len(protect_sideways_normal_entry_rows),
                "avg_net_bps": fmt_num(protect_sideways_avg_net_bps, 6),
            },
        )

    SWING_EARLY_EXIT_REASONS = {
        "atr_trailing",
        "zero_target_close",
        "rank_exit",
        "regime_exit",
    }

    def roundtrip_payload(row):
        payload = parse_json_obj(row.get("raw_json"), {})
        return payload if isinstance(payload, dict) else {}

    def iter_embedded_router_payloads(row):
        payload = roundtrip_payload(row)
        for section in ("entry_router_decision", "exit_router_decision"):
            router_payload = payload.get(section)
            if not isinstance(router_payload, dict):
                continue
            yield router_payload
            embedded = parse_json_obj(router_payload.get("raw_json"), {})
            if isinstance(embedded, dict):
                yield embedded
                embedded_inner = parse_json_obj(embedded.get("raw_json"), {})
                if isinstance(embedded_inner, dict):
                    yield embedded_inner

    def iter_roundtrip_payload_dicts(row):
        def walk(obj, depth=0):
            if depth > 8:
                return
            if isinstance(obj, dict):
                yield obj
                for value in obj.values():
                    yield from walk(value, depth + 1)
            elif isinstance(obj, list):
                for item in obj:
                    yield from walk(item, depth + 1)
            elif isinstance(obj, str):
                parsed = parse_json_obj(obj, None)
                if isinstance(parsed, (dict, list)):
                    yield from walk(parsed, depth + 1)

        for item in iter_embedded_router_payloads(row):
            yield item
        yield from walk(roundtrip_payload(row))

    def row_has_truthy_key(row, key):
        if truthy(row.get(key)):
            return True
        payload = roundtrip_payload(row)
        for item in iter_roundtrip_payload_dicts(row):
            if key in item and truthy(item.get(key)):
                return True
            for meta_key in ("meta_json", "raw_meta", "meta", "metadata", "order_meta"):
                if meta_key not in item:
                    continue
                meta = parse_json_obj(item.get(meta_key), {})
                if isinstance(meta, dict) and truthy(meta.get(key)):
                    return True
        text = flatten_value(payload).lower()
        return (
            f'"{key}": true' in text
            or f'\\"{key}\\": true' in text
            or re.search(rf"{re.escape(key)}\s*[:=]\s*(true|1|yes)", text) is not None
        )

    def swing_required_hold_hours(row):
        value = as_float(first_value(row, ("swing_min_hold_hours", "required_hold_hours", "min_hold_hours"), not_obs))
        if value is not None and value > 0:
            return value
        payload = roundtrip_payload(row)
        for item in iter_roundtrip_payload_dicts(row):
            value = as_float(first_value(item, ("swing_min_hold_hours", "required_hold_hours", "min_hold_hours"), not_obs))
            if value is not None and value > 0:
                return value
            for meta_key in ("meta_json", "raw_meta", "meta", "metadata", "order_meta"):
                meta = parse_json_obj(item.get(meta_key), {}) if isinstance(item, dict) else {}
                if not isinstance(meta, dict):
                    continue
                value = as_float(first_value(meta, ("swing_min_hold_hours", "required_hold_hours", "min_hold_hours"), not_obs))
                if value is not None and value > 0:
                    return value
        configured = config_number("swing_min_hold_hours")
        return configured if configured is not None and configured > 0 else 24.0

    def is_swing_normal_non_probe_roundtrip(row):
        if row.get("roundtrip_status") != "closed":
            return False
        if is_probe_trade_row(row):
            return False
        return row_has_truthy_key(row, "swing_hold_position")

    def swing_early_exit_reason(exit_reason):
        text = flatten_value(exit_reason).strip().lower()
        if text.startswith("stop_loss") or text.startswith("fixed_stop_loss"):
            return False
        return text in SWING_EARLY_EXIT_REASONS or text.startswith("rank_exit")

    def configured_roundtrip_cost_bps():
        configured = config_number("cost_aware_roundtrip_cost_bps")
        if configured is not None:
            return configured
        fee = config_number("fee_bps")
        slippage = config_number("slippage_bps")
        if fee is None:
            fee = 0.0
        if slippage is None:
            slippage = 0.0
        return 2.0 * (fee + slippage)

    def forward_net_bps(symbol, base_dt, base_px, horizon_hours, rt_cost_bps):
        base_price = as_float(base_px)
        if base_dt is None or base_price is None or base_price <= 0:
            return not_obs
        horizon_dt = base_dt + dt.timedelta(hours=int(horizon_hours))
        if horizon_dt > NOW:
            return "pending"
        future_px, _future_source, _future_reason = future_price_for_symbol(symbol, horizon_dt)
        if future_px is None:
            return not_obs
        return fmt_num(((future_px / base_price) - 1.0) * 10000.0 - rt_cost_bps, 6)

    def better_to_hold_text(future_net_bps, realized_net_bps):
        if future_net_bps == "pending":
            return "pending"
        future_value = as_float(future_net_bps)
        realized_value = as_float(realized_net_bps)
        if future_value is None or realized_value is None:
            return not_obs
        return str(future_value > realized_value).lower()

    def config_text_value(key):
        value = find_config_value(effective_data, key)
        if value not in (None, "", not_obs):
            return flatten_value(value)
        match = re.search(rf"(?m)^\s*{re.escape(key)}\s*:\s*([^#\n]+)", live_config_text or "")
        if not match:
            return not_obs
        return match.group(1).strip().strip("\"'")

    def nested_obj_has_key(obj, key, depth=0):
        if depth > 8:
            return False
        if isinstance(obj, dict):
            if key in obj:
                return True
            return any(nested_obj_has_key(value, key, depth + 1) for value in obj.values())
        if isinstance(obj, list):
            return any(nested_obj_has_key(value, key, depth + 1) for value in obj)
        if isinstance(obj, str):
            parsed = parse_json_obj(obj, None)
            if isinstance(parsed, (dict, list)):
                return nested_obj_has_key(parsed, key, depth + 1)
        return False

    def first_nested_value_from_obj(obj, keys):
        for key in keys:
            found = find_config_value(obj, key)
            if found is not None and flatten_value(found) not in ("", not_obs):
                return found
        return not_obs

    def first_nested_value_from_row(row, keys):
        for key in keys:
            if key in row and flatten_value(row.get(key)) not in ("", not_obs):
                return row.get(key)
        for item in iter_roundtrip_payload_dicts(row):
            if not isinstance(item, dict):
                continue
            for key in keys:
                if key in item and flatten_value(item.get(key)) not in ("", not_obs):
                    return item.get(key)
        return not_obs

    def row_payload_has_key(row, key):
        if key in row:
            return True
        return any(isinstance(item, dict) and key in item for item in iter_roundtrip_payload_dicts(row))

    def observed_bool_or_not(value):
        text = bool_observed(value)
        return text if text in {"true", "false"} else not_obs

    swing_guard_effective_ts = first_observed(
        config_text_value("swing_atr_early_exit_guard_effective_ts"),
        config_text_value("swing_atr_early_exit_guard_config_effective_ts"),
        config_text_value("swing_atr_early_exit_guard_enabled_ts"),
        not_obs,
    )
    swing_guard_effective_dt = parse_dt_utc(swing_guard_effective_ts)

    def swing_guard_context_at_exit(row, exit_dt, early_exit):
        exit_audit = audit_by_run.get(row.get("run_id"), {})
        guard_keys = (
            "swing_atr_early_exit_guard_enabled",
            "swing_atr_early_exit_guard_active",
            "swing_atr_early_exit_guard_blocked",
        )
        explicit_enabled = first_observed(
            first_nested_value_from_row(row, ("swing_atr_early_exit_guard_enabled",)),
            first_nested_value_from_obj(exit_audit, ("swing_atr_early_exit_guard_enabled",)),
            not_obs,
        )
        active_value = first_observed(
            first_nested_value_from_row(row, ("swing_atr_early_exit_guard_active",)),
            first_nested_value_from_obj(exit_audit, ("swing_atr_early_exit_guard_active",)),
            not_obs,
        )
        guard_config_seen = any(row_payload_has_key(row, key) or nested_obj_has_key(exit_audit, key) for key in guard_keys)
        if not guard_config_seen and swing_guard_effective_dt is not None and exit_dt is not None:
            guard_config_seen = exit_dt >= swing_guard_effective_dt
        if explicit_enabled != not_obs:
            guard_enabled_at_exit = observed_bool_or_not(explicit_enabled)
        elif active_value != not_obs:
            guard_enabled_at_exit = observed_bool_or_not(active_value)
        elif guard_config_seen and swing_guard_effective_dt is not None and exit_dt is not None and exit_dt >= swing_guard_effective_dt:
            guard_enabled_at_exit = str(config_bool("swing_atr_early_exit_guard_enabled", False)).lower()
        else:
            guard_enabled_at_exit = not_obs
        fingerprint = first_observed(
            first_nested_value_from_row(row, ("config_fingerprint", "effective_config_hash", "effective_live_config_hash", "code_version", "git_commit")),
            first_nested_value_from_obj(exit_audit, ("config_fingerprint", "effective_config_hash", "effective_live_config_hash", "code_version", "git_commit")),
            not_obs,
        )
        if fingerprint == not_obs and guard_config_seen and swing_guard_effective_dt is not None:
            fingerprint = first_observed(
                provenance_meta.get("effective_live_config_hash"),
                provenance_meta.get("git_commit"),
                not_obs,
            )
        if not early_exit:
            is_post_fix_sample = "false"
            diagnosis = "not_early_soft_exit"
        elif guard_config_seen and guard_enabled_at_exit == "true":
            is_post_fix_sample = "true"
            diagnosis = "post_fix_soft_exit_before_min_hold"
        elif guard_enabled_at_exit == "false":
            is_post_fix_sample = "false"
            diagnosis = "guard_disabled_at_exit"
        else:
            is_post_fix_sample = not_obs
            diagnosis = "historical_or_unknown_fix_state"
        return {
            "guard_enabled_at_exit": guard_enabled_at_exit,
            "guard_config_seen_at_exit": str(bool(guard_config_seen)).lower(),
            "code_version_or_config_fingerprint_at_exit": flatten_value(fingerprint),
            "is_post_fix_sample": is_post_fix_sample,
            "diagnosis": diagnosis,
        }

    swing_rt_cost_bps = configured_roundtrip_cost_bps()
    swing_early_exit_rows = []
    for row in closed_roundtrip_rows:
        if not is_swing_normal_non_probe_roundtrip(row):
            continue
        symbol = row.get("symbol", not_obs)
        entry_dt = parse_dt_utc(row.get("entry_ts"))
        exit_dt = parse_dt_utc(row.get("exit_ts"))
        hold_hours = as_float(row.get("hold_minutes"))
        if hold_hours is not None:
            hold_hours = hold_hours / 60.0
        elif entry_dt is not None and exit_dt is not None:
            hold_hours = (exit_dt - entry_dt).total_seconds() / 3600.0
        required_hold = swing_required_hold_hours(row)
        exit_reason = flatten_value(row.get("exit_reason")).strip()
        exit_priority = first_observed(row.get("exit_priority"), exit_priority_for_reason(exit_reason))
        exit_allowed_before_min_hold = first_observed(
            row.get("exit_allowed_before_min_hold"),
            str(exit_priority == "hard").lower() if exit_priority != not_obs else not_obs,
        )
        exit_blocked_by_min_hold = first_observed(row.get("exit_blocked_by_min_hold"), "false")
        min_hold_block_reason = first_observed(row.get("min_hold_block_reason"), "")
        before_min_hold = hold_hours is not None and required_hold is not None and hold_hours < required_hold
        early_exit = before_min_hold and swing_early_exit_reason(exit_reason)
        future_24_entry = forward_net_bps(symbol, entry_dt, row.get("entry_px"), 24, swing_rt_cost_bps)
        future_48_entry = forward_net_bps(symbol, entry_dt, row.get("entry_px"), 48, swing_rt_cost_bps)
        future_72_entry = forward_net_bps(symbol, entry_dt, row.get("entry_px"), 72, swing_rt_cost_bps)
        future_24_after_exit = forward_net_bps(symbol, exit_dt, row.get("exit_px"), 24, swing_rt_cost_bps)
        future_48_after_exit = forward_net_bps(symbol, exit_dt, row.get("exit_px"), 48, swing_rt_cost_bps)
        guard_context = swing_guard_context_at_exit(row, exit_dt, early_exit)
        swing_early_exit_rows.append({
            "symbol": symbol,
            "entry_ts": row.get("entry_ts", not_obs),
            "exit_ts": row.get("exit_ts", not_obs),
            "entry_px": row.get("entry_px", not_obs),
            "exit_px": row.get("exit_px", not_obs),
            "exit_reason": exit_reason or not_obs,
            "hold_hours": fmt_num(hold_hours, 6),
            "required_hold_hours": fmt_num(required_hold, 6),
            "exited_before_min_hold": str(bool(early_exit)).lower(),
            "exit_priority": exit_priority,
            "exit_allowed_before_min_hold": bool_text(exit_allowed_before_min_hold),
            "exit_blocked_by_min_hold": bool_text(exit_blocked_by_min_hold),
            "min_hold_block_reason": flatten_value(min_hold_block_reason),
            "net_bps_at_exit": row.get("net_bps", not_obs),
            "future_24h_net_bps_from_entry": future_24_entry,
            "future_48h_net_bps_from_entry": future_48_entry,
            "future_72h_net_bps_from_entry": future_72_entry,
            "future_24h_net_bps_after_exit": future_24_after_exit,
            "future_48h_net_bps_after_exit": future_48_after_exit,
            "would_have_been_better_to_hold_24h": better_to_hold_text(future_24_entry, row.get("net_bps")),
            "would_have_been_better_to_hold_48h": better_to_hold_text(future_48_entry, row.get("net_bps")),
            **guard_context,
        })

    def swing_avg_numeric_field(rows, field):
        values = [as_float(row.get(field)) for row in rows]
        values = [value for value in values if value is not None]
        return (sum(values) / len(values)) if values else None

    def aggregate_swing_early_exit_by_reason(rows):
        grouped = defaultdict(list)
        for row in rows:
            grouped[row.get("exit_reason") or not_obs].append(row)
        out = []
        for reason, group_rows in sorted(grouped.items()):
            early_rows = [row for row in group_rows if row.get("exited_before_min_hold") == "true"]
            better_24 = [row for row in early_rows if row.get("would_have_been_better_to_hold_24h") in {"true", "false"}]
            better_48 = [row for row in early_rows if row.get("would_have_been_better_to_hold_48h") in {"true", "false"}]
            out.append({
                "exit_reason": reason,
                "count": len(group_rows),
                "early_exit_count": len(early_rows),
                "avg_net_bps_at_exit": fmt_num(swing_avg_numeric_field(group_rows, "net_bps_at_exit"), 6),
                "avg_future_24h_net_bps_from_entry": fmt_num(swing_avg_numeric_field(group_rows, "future_24h_net_bps_from_entry"), 6),
                "avg_future_48h_net_bps_from_entry": fmt_num(swing_avg_numeric_field(group_rows, "future_48h_net_bps_from_entry"), 6),
                "better_to_hold_24h_count": sum(1 for row in better_24 if row.get("would_have_been_better_to_hold_24h") == "true"),
                "better_to_hold_24h_rate": fmt_num(
                    sum(1 for row in better_24 if row.get("would_have_been_better_to_hold_24h") == "true") / len(better_24)
                    if better_24
                    else None,
                    6,
                ),
                "better_to_hold_48h_count": sum(1 for row in better_48 if row.get("would_have_been_better_to_hold_48h") == "true"),
                "better_to_hold_48h_rate": fmt_num(
                    sum(1 for row in better_48 if row.get("would_have_been_better_to_hold_48h") == "true") / len(better_48)
                    if better_48
                    else None,
                    6,
                ),
            })
        return out

    swing_early_exit_by_reason = aggregate_swing_early_exit_by_reason(swing_early_exit_rows)
    swing_early_exit_sample_rows = [row for row in swing_early_exit_rows if row.get("exited_before_min_hold") == "true"]
    swing_early_exit_better_24_rows = [
        row for row in swing_early_exit_sample_rows
        if row.get("would_have_been_better_to_hold_24h") in {"true", "false"}
    ]
    swing_early_exit_better_24_count = sum(
        1 for row in swing_early_exit_better_24_rows
        if row.get("would_have_been_better_to_hold_24h") == "true"
    )
    swing_early_exit_better_24_rate = (
        swing_early_exit_better_24_count / len(swing_early_exit_better_24_rows)
        if swing_early_exit_better_24_rows
        else None
    )
    swing_filled_soft_exit_before_min_hold_count = len(swing_early_exit_sample_rows)
    swing_post_fix_early_exit_sample_rows = [
        row for row in swing_early_exit_sample_rows
        if row.get("is_post_fix_sample") == "true"
    ]
    swing_historical_or_unknown_early_exit_rows = [
        row for row in swing_early_exit_sample_rows
        if row.get("is_post_fix_sample") != "true"
    ]
    swing_blocked_by_min_hold_count = sum(
        1 for row in early_exit_rows
        if row.get("event_type") == "pending_soft_exit_blocked_by_min_hold"
        or row.get("exit_blocked_by_min_hold") == "true"
    )
    if swing_post_fix_early_exit_sample_rows:
        add_issue(
            "high",
            "swing_soft_exit_before_min_hold_filled",
            "A post-fix soft swing exit filled before min-hold while the swing ATR early-exit guard was observable at exit.",
            {
                "post_fix_filled_soft_exit_before_min_hold_count": len(swing_post_fix_early_exit_sample_rows),
                "blocked_by_min_hold_count": swing_blocked_by_min_hold_count,
                "sample_rows": swing_post_fix_early_exit_sample_rows[:10],
            },
        )
    if swing_historical_or_unknown_early_exit_rows:
        add_issue(
            "medium",
            "swing_soft_exit_before_min_hold_historical_or_unknown",
            "Soft swing exits before min-hold were observed, but the bundle cannot confirm the swing ATR early-exit guard was active at those exit times.",
            {
                "historical_or_unknown_count": len(swing_historical_or_unknown_early_exit_rows),
                "blocked_by_min_hold_count": swing_blocked_by_min_hold_count,
                "sample_rows": swing_historical_or_unknown_early_exit_rows[:10],
            },
        )
    if (
        len(swing_early_exit_sample_rows) >= 3
        and len(swing_early_exit_better_24_rows) >= 3
        and swing_early_exit_better_24_rate is not None
        and swing_early_exit_better_24_rate > 0.6
    ):
        add_issue(
            "medium",
            "swing_early_exit_premature",
            "Swing-hold positions exited before min-hold and most observable 24h hold outcomes would have been better.",
            {
                "sample_count": len(swing_early_exit_sample_rows),
                "observable_24h_count": len(swing_early_exit_better_24_rows),
                "would_have_been_better_to_hold_24h_rate": fmt_num(swing_early_exit_better_24_rate, 6),
                "by_reason": swing_early_exit_by_reason,
            },
        )

    POST_MIN_HOLD_ATR_HORIZONS = (6, 12, 24)

    def post_min_hold_hold_hours(row, entry_dt=None, exit_dt=None):
        hold_hours = as_float(row.get("hold_hours"))
        if hold_hours is not None:
            return hold_hours
        hold_minutes = as_float(row.get("hold_minutes"))
        if hold_minutes is not None:
            return hold_minutes / 60.0
        if entry_dt is not None and exit_dt is not None:
            return (exit_dt - entry_dt).total_seconds() / 3600.0
        return None

    def price_and_net_if_held_after_exit(symbol, entry_px, exit_dt, horizon_hours, rt_cost_bps):
        entry_price = as_float(entry_px)
        if exit_dt is None or entry_price is None or entry_price <= 0:
            return not_obs, not_obs
        horizon_dt = exit_dt + dt.timedelta(hours=int(horizon_hours))
        if horizon_dt > NOW:
            return "pending", "pending"
        future_px, _future_source, _future_reason = future_price_for_symbol(symbol, horizon_dt)
        if future_px is None:
            return not_obs, not_obs
        net_bps = ((future_px / entry_price) - 1.0) * 10000.0 - rt_cost_bps
        return fmt_num(future_px, 10), fmt_num(net_bps, 6)

    def post_min_hold_atr_diagnosis(row):
        observable = [
            row.get(f"would_have_been_better_{horizon}h")
            for horizon in POST_MIN_HOLD_ATR_HORIZONS
            if row.get(f"would_have_been_better_{horizon}h") in {"true", "false"}
        ]
        if not observable:
            return "post_min_hold_atr_exit_not_observable"
        if any(value == "true" for value in observable):
            return "post_min_hold_atr_exit_better_to_hold"
        return "post_min_hold_atr_exit_saved_loss"

    post_min_hold_atr_exit_rows = []
    for row in closed_roundtrip_rows:
        symbol = row.get("symbol", not_obs)
        exit_reason = flatten_value(row.get("exit_reason")).strip().lower()
        if exit_reason != "atr_trailing":
            continue
        if not row_has_truthy_key(row, "swing_hold_position"):
            continue
        entry_dt = parse_dt_utc(row.get("entry_ts"))
        exit_dt = parse_dt_utc(row.get("exit_ts"))
        hold_hours = post_min_hold_hold_hours(row, entry_dt, exit_dt)
        min_hold_hours = swing_required_hold_hours(row)
        if hold_hours is None or min_hold_hours is None:
            continue
        hours_after_min_hold = hold_hours - min_hold_hours
        if hold_hours < min_hold_hours or hours_after_min_hold > 6.0:
            continue
        realized_net_bps = first_observed(row.get("net_bps"), row.get("realized_net_bps"), not_obs)
        price_after = {}
        net_if_held = {}
        better_if_held = {}
        for horizon in POST_MIN_HOLD_ATR_HORIZONS:
            price_value, net_value = price_and_net_if_held_after_exit(
                symbol,
                row.get("entry_px"),
                exit_dt,
                horizon,
                swing_rt_cost_bps,
            )
            price_after[horizon] = price_value
            net_if_held[horizon] = net_value
            better_if_held[horizon] = better_to_hold_text(net_value, realized_net_bps)
        post_row = {
            "symbol": symbol,
            "entry_ts": row.get("entry_ts", not_obs),
            "exit_ts": row.get("exit_ts", not_obs),
            "entry_px": row.get("entry_px", not_obs),
            "exit_px": row.get("exit_px", not_obs),
            "exit_reason": exit_reason,
            "hold_hours": fmt_num(hold_hours, 6),
            "min_hold_hours": fmt_num(min_hold_hours, 6),
            "hours_after_min_hold": fmt_num(hours_after_min_hold, 6),
            "realized_net_bps": flatten_value(realized_net_bps),
            "realized_net_pnl_usdt": first_observed(row.get("net_pnl_usdt"), row.get("realized_net_pnl_usdt"), not_obs),
            "price_at_exit": row.get("exit_px", not_obs),
            "price_after_6h": price_after[6],
            "price_after_12h": price_after[12],
            "price_after_24h": price_after[24],
            "net_bps_if_held_6h_after_exit": net_if_held[6],
            "net_bps_if_held_12h_after_exit": net_if_held[12],
            "net_bps_if_held_24h_after_exit": net_if_held[24],
            "would_have_been_better_6h": better_if_held[6],
            "would_have_been_better_12h": better_if_held[12],
            "would_have_been_better_24h": better_if_held[24],
            "f4_at_entry": flatten_value(first_nested_value_from_row(row, ("f4_volume_expansion", "f4"))),
            "f5_at_entry": flatten_value(first_nested_value_from_row(row, ("f5_rsi_trend_confirm", "f5"))),
            "dominant_factor": flatten_value(first_nested_value_from_row(row, ("dominant_factor",))),
            "dominant_factor_contribution_pct": flatten_value(first_nested_value_from_row(row, ("dominant_factor_contribution_pct", "contribution_pct", "dominant_contribution_pct"))),
        }
        post_row["diagnosis"] = post_min_hold_atr_diagnosis(post_row)
        post_min_hold_atr_exit_rows.append(post_row)

    def post_min_hold_atr_aggregate_by_symbol(rows):
        grouped = defaultdict(list)
        for row in rows:
            grouped[row.get("symbol") or not_obs].append(row)
        out = []
        for symbol, group_rows in sorted(grouped.items()):
            payload = {
                "symbol": symbol,
                "sample_count": len(group_rows),
                "avg_realized_net_bps": fmt_num(swing_avg_numeric_field(group_rows, "realized_net_bps"), 6),
                "diagnosis_mix": json.dumps(dict(sorted(Counter(row.get("diagnosis") or not_obs for row in group_rows).items())), ensure_ascii=False, sort_keys=True),
            }
            for horizon in POST_MIN_HOLD_ATR_HORIZONS:
                better_rows = [
                    row for row in group_rows
                    if row.get(f"would_have_been_better_{horizon}h") in {"true", "false"}
                ]
                better_count = sum(
                    1 for row in better_rows
                    if row.get(f"would_have_been_better_{horizon}h") == "true"
                )
                payload[f"observable_{horizon}h_count"] = len(better_rows)
                payload[f"better_to_hold_{horizon}h_count"] = better_count
                payload[f"better_to_hold_{horizon}h_rate"] = fmt_num(
                    better_count / len(better_rows) if better_rows else None,
                    6,
                )
                payload[f"avg_net_bps_if_held_{horizon}h_after_exit"] = fmt_num(
                    swing_avg_numeric_field(group_rows, f"net_bps_if_held_{horizon}h_after_exit"),
                    6,
                )
            out.append(payload)
        return out

    post_min_hold_atr_exit_outcomes_by_symbol = post_min_hold_atr_aggregate_by_symbol(post_min_hold_atr_exit_rows)
    post_min_hold_atr_better_12_rows = [
        row for row in post_min_hold_atr_exit_rows
        if row.get("would_have_been_better_12h") in {"true", "false"}
    ]
    post_min_hold_atr_better_12_count = sum(
        1 for row in post_min_hold_atr_better_12_rows
        if row.get("would_have_been_better_12h") == "true"
    )
    post_min_hold_atr_better_12_rate = (
        post_min_hold_atr_better_12_count / len(post_min_hold_atr_better_12_rows)
        if post_min_hold_atr_better_12_rows
        else None
    )

    SWING_ATR_SOFT_EXIT_MIN_SAMPLE_COUNT = 3
    SWING_ATR_SOFT_EXIT_MIN_OBSERVABLE_12H_COUNT = 3
    SWING_ATR_SOFT_EXIT_MIN_BETTER_12H_RATE = 0.60
    SWING_ATR_SOFT_EXIT_MIN_IMPROVEMENT_BPS = 50.0

    def swing_atr_soft_exit_readiness_reasons(
        sample_count,
        observable_12h_count,
        better_to_hold_12h_rate,
        improvement_bps,
    ):
        reasons = []
        if sample_count < SWING_ATR_SOFT_EXIT_MIN_SAMPLE_COUNT:
            reasons.append("sample_count_lt_3")
        if observable_12h_count < SWING_ATR_SOFT_EXIT_MIN_OBSERVABLE_12H_COUNT:
            reasons.append("observable_12h_count_lt_3")
        if better_to_hold_12h_rate is None or better_to_hold_12h_rate < SWING_ATR_SOFT_EXIT_MIN_BETTER_12H_RATE:
            reasons.append("better_to_hold_12h_rate_lt_0_60")
        if improvement_bps is None or improvement_bps < SWING_ATR_SOFT_EXIT_MIN_IMPROVEMENT_BPS:
            reasons.append("improvement_bps_lt_50")
        return reasons

    def build_swing_atr_soft_exit_readiness_by_symbol(rows):
        out = []
        for row in rows:
            sample_count = as_int(row.get("sample_count"))
            observable_12h_count = as_int(row.get("observable_12h_count"))
            better_rate = as_float(row.get("better_to_hold_12h_rate"))
            avg_realized = as_float(row.get("avg_realized_net_bps"))
            avg_delayed_12h = as_float(row.get("avg_net_bps_if_held_12h_after_exit"))
            improvement = (
                avg_delayed_12h - avg_realized
                if avg_delayed_12h is not None and avg_realized is not None
                else None
            )
            blocking_reasons = swing_atr_soft_exit_readiness_reasons(
                sample_count,
                observable_12h_count,
                better_rate,
                improvement,
            )
            out.append({
                "symbol": row.get("symbol", not_obs),
                "ready_for_live_guard": str(not blocking_reasons).lower(),
                "blocking_reasons": ";".join(blocking_reasons),
                "sample_count": sample_count,
                "observable_12h_count": observable_12h_count,
                "better_to_hold_12h_rate": fmt_num(better_rate, 6),
                "avg_realized_net_bps": fmt_num(avg_realized, 6),
                "avg_delayed_12h_net_bps": fmt_num(avg_delayed_12h, 6),
                "improvement_bps": fmt_num(improvement, 6),
            })
        return out

    swing_atr_soft_exit_readiness_by_symbol = build_swing_atr_soft_exit_readiness_by_symbol(
        post_min_hold_atr_exit_outcomes_by_symbol
    )
    swing_atr_soft_exit_ready_symbols = [
        row for row in swing_atr_soft_exit_readiness_by_symbol
        if row.get("ready_for_live_guard") == "true"
    ]
    swing_atr_soft_exit_avg_realized = swing_avg_numeric_field(
        post_min_hold_atr_exit_rows,
        "realized_net_bps",
    )
    swing_atr_soft_exit_avg_delayed_12h = swing_avg_numeric_field(
        post_min_hold_atr_exit_rows,
        "net_bps_if_held_12h_after_exit",
    )
    swing_atr_soft_exit_improvement_bps = (
        swing_atr_soft_exit_avg_delayed_12h - swing_atr_soft_exit_avg_realized
        if swing_atr_soft_exit_avg_delayed_12h is not None and swing_atr_soft_exit_avg_realized is not None
        else None
    )
    swing_atr_soft_exit_readiness_blocking_reasons = swing_atr_soft_exit_readiness_reasons(
        len(post_min_hold_atr_exit_rows),
        len(post_min_hold_atr_better_12_rows),
        post_min_hold_atr_better_12_rate,
        swing_atr_soft_exit_improvement_bps,
    )
    if not swing_atr_soft_exit_readiness_blocking_reasons and not swing_atr_soft_exit_ready_symbols:
        swing_atr_soft_exit_readiness_blocking_reasons.append("no_symbol_ready_for_live_guard")
    swing_atr_soft_exit_readiness = {
        "schema_version": "v5.swing_atr_soft_exit_readiness.v1",
        "ready_for_live_guard": not bool(swing_atr_soft_exit_readiness_blocking_reasons),
        "blocking_reasons": list(swing_atr_soft_exit_readiness_blocking_reasons),
        "sample_count": len(post_min_hold_atr_exit_rows),
        "observable_12h_count": len(post_min_hold_atr_better_12_rows),
        "better_to_hold_12h_rate": post_min_hold_atr_better_12_rate if post_min_hold_atr_better_12_rate is not None else not_obs,
        "avg_realized_net_bps": swing_atr_soft_exit_avg_realized if swing_atr_soft_exit_avg_realized is not None else not_obs,
        "avg_delayed_12h_net_bps": swing_atr_soft_exit_avg_delayed_12h if swing_atr_soft_exit_avg_delayed_12h is not None else not_obs,
        "improvement_bps": swing_atr_soft_exit_improvement_bps if swing_atr_soft_exit_improvement_bps is not None else not_obs,
        "ready_symbols": [row.get("symbol", not_obs) for row in swing_atr_soft_exit_ready_symbols],
        "thresholds": {
            "min_sample_count": SWING_ATR_SOFT_EXIT_MIN_SAMPLE_COUNT,
            "min_observable_12h_count": SWING_ATR_SOFT_EXIT_MIN_OBSERVABLE_12H_COUNT,
            "min_better_to_hold_12h_rate": SWING_ATR_SOFT_EXIT_MIN_BETTER_12H_RATE,
            "min_improvement_bps": SWING_ATR_SOFT_EXIT_MIN_IMPROVEMENT_BPS,
        },
        "by_symbol": swing_atr_soft_exit_readiness_by_symbol,
    }

    if (
        len(post_min_hold_atr_exit_rows) >= 3
        and len(post_min_hold_atr_better_12_rows) >= 3
        and post_min_hold_atr_better_12_rate is not None
        and post_min_hold_atr_better_12_rate > 0.6
    ):
        add_issue(
            "medium",
            "post_min_hold_atr_exit_may_be_premature",
            "Swing positions exited by ATR shortly after min-hold and most observable 12h hold outcomes would have been better.",
            {
                "sample_count": len(post_min_hold_atr_exit_rows),
                "observable_12h_count": len(post_min_hold_atr_better_12_rows),
                "would_have_been_better_12h_rate": fmt_num(post_min_hold_atr_better_12_rate, 6),
                "by_symbol": post_min_hold_atr_exit_outcomes_by_symbol,
            },
        )

    swing_atr_soft_exit_shadow_enabled = config_bool("swing_atr_soft_exit_shadow_enabled", True)
    swing_atr_soft_exit_shadow_grace_hours = [
        horizon for horizon in normalize_horizon_list(
            config_int_list("swing_atr_soft_exit_shadow_grace_hours"),
            [3, 6, 12],
        )
        if horizon in {3, 6, 12}
    ] or [3, 6, 12]
    swing_atr_soft_exit_shadow_min_net_bps_hard_exit = (
        config_number("swing_atr_soft_exit_shadow_min_net_bps_hard_exit")
    )
    if swing_atr_soft_exit_shadow_min_net_bps_hard_exit is None:
        swing_atr_soft_exit_shadow_min_net_bps_hard_exit = -180.0
    swing_atr_soft_exit_shadow_require_f5_breakdown = (
        config_number("swing_atr_soft_exit_shadow_require_f5_breakdown")
    )
    if swing_atr_soft_exit_shadow_require_f5_breakdown is None:
        swing_atr_soft_exit_shadow_require_f5_breakdown = -0.30

    def swing_atr_soft_exit_hard_reasons(net_bps, f5_value):
        reasons = []
        net_value = as_float(net_bps)
        f5_float = as_float(f5_value)
        if (
            net_value is not None
            and net_value <= swing_atr_soft_exit_shadow_min_net_bps_hard_exit
        ):
            reasons.append("net_bps_hard_exit")
        if (
            f5_float is not None
            and f5_float <= swing_atr_soft_exit_shadow_require_f5_breakdown
        ):
            reasons.append("f5_momentum_breakdown")
        return reasons

    swing_atr_soft_exit_shadow_rows = []
    if swing_atr_soft_exit_shadow_enabled:
        for row in closed_roundtrip_rows:
            symbol = row.get("symbol", not_obs)
            exit_reason = flatten_value(row.get("exit_reason")).strip().lower()
            if exit_reason != "atr_trailing":
                continue
            if not row_has_truthy_key(row, "swing_hold_position"):
                continue
            exit_dt = parse_dt_utc(row.get("exit_ts"))
            net_bps_at_exit = first_observed(row.get("net_bps"), row.get("realized_net_bps"), not_obs)
            f5_value = flatten_value(first_nested_value_from_row(row, ("f5_rsi_trend_confirm", "f5")))
            hard_reasons = swing_atr_soft_exit_hard_reasons(net_bps_at_exit, f5_value)
            would_delay = not hard_reasons
            shadow_row = {
                "symbol": symbol,
                "exit_ts": row.get("exit_ts", not_obs),
                "exit_px": row.get("exit_px", not_obs),
                "net_bps_at_exit": flatten_value(net_bps_at_exit),
                "f5_rsi_trend_confirm": f5_value,
                "would_delay_exit_if_enabled": str(bool(would_delay)).lower(),
                "hard_exit_reason": ";".join(hard_reasons) if hard_reasons else "none",
            }
            for horizon in (3, 6, 12):
                if horizon not in swing_atr_soft_exit_shadow_grace_hours:
                    delayed_net = not_obs
                else:
                    _delayed_px, delayed_net = price_and_net_if_held_after_exit(
                        symbol,
                        row.get("entry_px"),
                        exit_dt,
                        horizon,
                        swing_rt_cost_bps,
                    )
                shadow_row[f"net_bps_if_delayed_{horizon}h"] = delayed_net
                shadow_row[f"better_to_delay_{horizon}h"] = better_to_hold_text(
                    delayed_net,
                    net_bps_at_exit,
                )
            swing_atr_soft_exit_shadow_rows.append(shadow_row)

    BNB_PROFIT_LOCK_SYMBOL = "BNB/USDT"
    BNB_PROFIT_LOCK_THRESHOLDS = (30, 50)
    BNB_PROFIT_LOCK_DELAY_HOURS = (6, 12, 24)

    def cache_max_close_between(symbol, start_dt, end_dt):
        if start_dt is None or end_dt is None:
            return None
        start_ms = int(start_dt.timestamp() * 1000.0)
        end_ms = int(end_dt.timestamp() * 1000.0)
        if end_ms < start_ms:
            return None
        values = [
            close
            for ts_ms, close in load_cache_candles(symbol)
            if start_ms <= int(ts_ms) <= end_ms
        ]
        return max(values) if values else None

    def bnb_profit_lock_max_unrealized_bps(row, entry_dt, exit_dt):
        direct_bps = max_numeric(
            first_nested_value_from_row(row, ("max_unrealized_bps",)),
            first_nested_value_from_row(row, ("max_unrealized_net_bps",)),
            first_nested_value_from_row(row, ("highest_unrealized_net_bps",)),
            first_nested_value_from_row(row, ("highest_net_bps",)),
            first_nested_value_from_row(row, ("mfe_bps",)),
            first_nested_value_from_row(row, ("max_favorable_excursion_bps",)),
        )
        if direct_bps is not None:
            return direct_bps
        entry_px = as_float(row.get("entry_px"))
        if entry_px is None or entry_px <= 0:
            return None
        highest_px = max_numeric(
            first_nested_value_from_row(row, ("highest_px_before_exit",)),
            first_nested_value_from_row(row, ("highest_px",)),
            first_nested_value_from_row(row, ("highest_price",)),
            first_nested_value_from_row(row, ("max_px",)),
            first_nested_value_from_row(row, ("max_price",)),
            first_nested_value_from_row(row, ("high_px",)),
            cache_max_close_between(row.get("symbol"), entry_dt, exit_dt),
            row.get("entry_px"),
            row.get("exit_px"),
        )
        if highest_px is None or highest_px <= 0:
            return None
        return ((highest_px / entry_px) - 1.0) * 10000.0 - swing_rt_cost_bps

    def profit_lock_shadow_exit_value(max_unrealized_bps, threshold_bps):
        max_value = as_float(max_unrealized_bps)
        if max_value is None:
            return not_obs
        if max_value >= float(threshold_bps):
            return fmt_num(float(threshold_bps), 6)
        return "not_triggered"

    def best_bnb_shadow_exit_policy(row):
        candidates = [("actual_exit", row.get("actual_exit_net_bps"))]
        for threshold in BNB_PROFIT_LOCK_THRESHOLDS:
            candidates.append((f"profit_lock_{threshold}bps_exit", row.get(f"profit_lock_{threshold}bps_exit")))
        for horizon in BNB_PROFIT_LOCK_DELAY_HOURS:
            candidates.append((f"delayed_exit_{horizon}h", row.get(f"delayed_exit_{horizon}h")))
        numeric_candidates = [
            (name, as_float(value))
            for name, value in candidates
            if as_float(value) is not None
        ]
        if not numeric_candidates:
            return not_obs
        best_name, _best_value = max(numeric_candidates, key=lambda item: item[1])
        return best_name

    def bnb_profit_lock_diagnosis(row):
        best_policy = row.get("best_shadow_exit_policy")
        if best_policy == not_obs:
            return "bnb_profit_lock_shadow_not_observable"
        if best_policy == "actual_exit":
            return "actual_atr_exit_best_or_tied"
        if best_policy.startswith("profit_lock_"):
            return "profit_lock_would_have_helped"
        if best_policy.startswith("delayed_exit_"):
            return "atr_trailing_delay_would_have_helped"
        return "bnb_profit_lock_shadow_observed"

    bnb_profit_lock_shadow_rows = []
    for row in closed_roundtrip_rows:
        if normalize_symbol_text(row.get("symbol")) != BNB_PROFIT_LOCK_SYMBOL:
            continue
        if not row_has_truthy_key(row, "swing_hold_position"):
            continue
        entry_dt = parse_dt_utc(row.get("entry_ts"))
        exit_dt = parse_dt_utc(row.get("exit_ts"))
        actual_exit_net_bps = first_observed(row.get("net_bps"), row.get("realized_net_bps"), not_obs)
        max_unrealized_bps = bnb_profit_lock_max_unrealized_bps(row, entry_dt, exit_dt)
        shadow_row = {
            "symbol": row.get("symbol", BNB_PROFIT_LOCK_SYMBOL),
            "entry_ts": row.get("entry_ts", not_obs),
            "exit_ts": row.get("exit_ts", not_obs),
            "entry_px": row.get("entry_px", not_obs),
            "exit_px": row.get("exit_px", not_obs),
            "exit_reason": row.get("exit_reason", not_obs),
            "max_unrealized_bps": fmt_num(max_unrealized_bps, 6),
            "actual_exit_net_bps": flatten_value(actual_exit_net_bps),
        }
        for threshold in BNB_PROFIT_LOCK_THRESHOLDS:
            shadow_row[f"profit_lock_{threshold}bps_exit"] = profit_lock_shadow_exit_value(
                max_unrealized_bps,
                threshold,
            )
        for horizon in BNB_PROFIT_LOCK_DELAY_HOURS:
            _delayed_px, delayed_net = price_and_net_if_held_after_exit(
                row.get("symbol"),
                row.get("entry_px"),
                exit_dt,
                horizon,
                swing_rt_cost_bps,
            )
            shadow_row[f"delayed_exit_{horizon}h"] = delayed_net
        shadow_row["best_shadow_exit_policy"] = best_bnb_shadow_exit_policy(shadow_row)
        shadow_row["diagnosis"] = bnb_profit_lock_diagnosis(shadow_row)
        bnb_profit_lock_shadow_rows.append(shadow_row)

    SOL_SWING_SYMBOL = "SOL/USDT"

    def is_sol_symbol(value):
        return normalize_symbol_text(value) == SOL_SWING_SYMBOL

    def numeric_values(rows, field):
        values = [as_float(row.get(field)) for row in rows]
        return [value for value in values if value is not None]

    def avg_field(rows, field):
        values = numeric_values(rows, field)
        return (sum(values) / len(values)) if values else None

    def sum_field(rows, field):
        values = numeric_values(rows, field)
        return sum(values) if values else None

    def is_sol_swing_roundtrip(row):
        if not is_sol_symbol(row.get("symbol")) or is_probe_trade_row(row):
            return False
        entry_reason = flatten_value(row.get("entry_reason")).strip().lower()
        exit_reason = flatten_value(row.get("exit_reason")).strip().lower()
        raw_json = flatten_value(row.get("raw_json")).lower()
        return (
            entry_reason in {"ok", "normal", "normal_entry", "protect_recovery", "protect_recovery_swing"}
            or exit_reason.startswith("protect_profit_lock")
            or "swing_hold_position" in raw_json
        )

    sol_real_swing_roundtrips = [row for row in closed_roundtrip_rows if is_sol_swing_roundtrip(row)]
    sol_high_score_target_rows = [row for row in high_score_blocked_rows if is_sol_symbol(row.get("symbol"))]
    sol_high_score_outcome_rows = [row for row in high_score_blocked_outcome_rows if is_sol_symbol(row.get("symbol"))]

    def sol_multi_position_shadow_row():
        rows = [row for row in multi_position_swing_shadow_by_symbol if is_sol_symbol(row.get("symbol"))]
        for mode in (MULTI_SHADOW_MODE_PROTECT_RECOVERY, MULTI_SHADOW_MODE_ALL):
            for row in rows:
                if flatten_value(row.get("shadow_mode") or MULTI_SHADOW_MODE_ALL) == mode:
                    return row
        return rows[0] if rows else {}

    def latest_sol_selected_and_reasons():
        selected_count = 0
        reasons = Counter()
        for run_id, audit in audit_by_run.items():
            if not isinstance(audit, dict):
                continue
            audit_dt = parse_dt_utc(first_observed(first_value(audit, ("now_ts", "window_end_ts", "ts_utc", "timestamp"), not_obs)))
            if audit_dt is None:
                audit_dt = parse_run_time(run_id)
            if audit_dt is None or audit_dt.timestamp() < RECENT_24H:
                continue
            selected = False
            target_w = target_weight_from_targets(audit.get("targets_post_risk"), SOL_SWING_SYMBOL)
            if target_w is not None and target_w > 0:
                selected = True
            for item in audit.get("target_execution_explain") or []:
                if not isinstance(item, dict) or not is_sol_symbol(item.get("symbol")):
                    continue
                explain_target_w = as_float(first_value(item, ("target_w", "effective_target_w", "target_weight"), not_obs))
                if explain_target_w is not None and explain_target_w > 0:
                    selected = True
                action = flatten_value(first_value(item, ("router_action", "action"), "")).lower()
                reason = flatten_value(first_value(item, ("router_reason", "blocked_reason", "reason"), ""))
                if action == "skip" and reason:
                    reasons[reason] += 1
            for item in audit.get("router_decisions") or []:
                if not isinstance(item, dict) or not is_sol_symbol(item.get("symbol")):
                    continue
                action = flatten_value(item.get("action")).lower()
                reason = flatten_value(first_value(item, ("reason", "source_reason"), ""))
                if action == "skip" and reason:
                    reasons[reason] += 1
            if selected:
                selected_count += 1
        return selected_count, ";".join(f"{reason}:{count}" for reason, count in reasons.most_common()) or not_obs

    latest_sol_selected_count, latest_sol_block_reasons = latest_sol_selected_and_reasons()
    sol_multi_shadow = sol_multi_position_shadow_row()
    sol_swing_performance_rows = [{
        "window": "last_72h",
        "real_roundtrip_count": len(sol_real_swing_roundtrips),
        "real_net_bps_avg": fmt_num(avg_field(sol_real_swing_roundtrips, "net_bps"), 6),
        "real_net_pnl_usdt": fmt_num(sum_field(sol_real_swing_roundtrips, "net_pnl_usdt") if sol_real_swing_roundtrips else 0.0, 12),
        "high_score_blocked_count": len(sol_high_score_target_rows) if sol_high_score_target_rows else len(sol_high_score_outcome_rows),
        "high_score_blocked_24h_avg": fmt_num(avg_field(sol_high_score_outcome_rows, "label_24h_net_bps"), 6),
        "high_score_blocked_48h_avg": fmt_num(avg_field(sol_high_score_outcome_rows, "label_48h_net_bps"), 6),
        "high_score_blocked_72h_avg": fmt_num(avg_field(sol_high_score_outcome_rows, "label_72h_net_bps"), 6),
        "multi_position_shadow_24h_avg": first_observed(first_value(sol_multi_shadow, ("avg_24h_net_bps",), not_obs)),
        "multi_position_shadow_48h_avg": first_observed(first_value(sol_multi_shadow, ("avg_48h_net_bps",), not_obs)),
        "multi_position_shadow_72h_avg": first_observed(first_value(sol_multi_shadow, ("avg_72h_net_bps",), not_obs)),
        "latest_selected_count": latest_sol_selected_count,
        "latest_block_reasons": latest_sol_block_reasons,
    }]

    def roundtrip_entry_notional(row):
        qty = as_float(row.get("qty"))
        entry_px = as_float(row.get("entry_px"))
        if qty is not None and entry_px is not None:
            return abs(qty * entry_px)
        net_pnl = as_float(row.get("net_pnl_usdt"))
        net_bps = as_float(row.get("net_bps"))
        if net_pnl is not None and net_bps not in (None, 0.0):
            return abs(net_pnl * 10000.0 / net_bps)
        return None

    def negative_expectancy_entries_by_symbol():
        entries = {}
        if not isinstance(negative_expectancy_state, dict):
            return entries
        for section_name in ("stats", "symbols"):
            section = negative_expectancy_state.get(section_name)
            if isinstance(section, dict):
                for symbol, entry in section.items():
                    if isinstance(entry, dict):
                        entries[flatten_value(symbol)] = entry
        for symbol, entry in negative_expectancy_state.items():
            if isinstance(entry, dict) and "/" in flatten_value(symbol):
                entries.setdefault(flatten_value(symbol), entry)
        return entries

    roundtrip_by_symbol = defaultdict(lambda: {"count": 0, "net_pnl_sum": 0.0, "has_net_pnl": False, "entry_notional_sum": 0.0})
    for row in closed_roundtrip_rows:
        symbol = row.get("symbol", not_obs)
        if symbol in ("", not_obs):
            continue
        stats = roundtrip_by_symbol[symbol]
        stats["count"] += 1
        net_pnl = as_float(row.get("net_pnl_usdt"))
        if net_pnl is not None:
            stats["net_pnl_sum"] += net_pnl
            stats["has_net_pnl"] = True
        entry_notional = roundtrip_entry_notional(row)
        if entry_notional is not None:
            stats["entry_notional_sum"] += entry_notional

    negative_entries = negative_expectancy_entries_by_symbol()
    negative_consistency_rows = []
    for symbol in sorted(set(roundtrip_by_symbol.keys()) | set(negative_entries.keys())):
        rt = roundtrip_by_symbol.get(symbol, {"count": 0, "net_pnl_sum": 0.0, "has_net_pnl": False, "entry_notional_sum": 0.0})
        neg = negative_entries.get(symbol, {})
        rt_net_pnl = rt["net_pnl_sum"] if rt.get("has_net_pnl") else None
        rt_weighted_bps = (rt_net_pnl / rt["entry_notional_sum"] * 10000.0) if rt_net_pnl is not None and rt.get("entry_notional_sum", 0.0) > 0 else None
        neg_closed_cycles = as_float(first_value(neg, ("closed_cycles",), not_obs))
        neg_net_pnl = as_float(first_value(neg, ("net_pnl_sum_usdt",), not_obs))
        neg_net_bps = as_float(first_value(neg, ("net_expectancy_bps",), not_obs))
        neg_fast_fail_net_bps = as_float(first_value(neg, ("fast_fail_net_expectancy_bps", "fast_fail_expectancy_bps"), not_obs))
        pnl_mismatch = (rt_net_pnl - neg_net_pnl) if rt_net_pnl is not None and neg_net_pnl is not None else None
        bps_mismatch = abs(rt_weighted_bps - neg_net_bps) if rt_weighted_bps is not None and neg_net_bps is not None else None
        pnl_sign_mismatch = bool(
            rt_net_pnl is not None
            and neg_net_pnl is not None
            and rt_net_pnl > 0
            and neg_net_pnl < 0
            and abs(pnl_mismatch or 0.0) > 0.05
        )
        bps_large_mismatch = bool(bps_mismatch is not None and bps_mismatch > 50.0)
        mismatch_suspected = bool(pnl_sign_mismatch or bps_large_mismatch)
        if mismatch_suspected:
            diagnosis = "high_issue_negative_expectancy_roundtrip_mismatch"
            add_issue(
                "high",
                "negative_expectancy_roundtrip_mismatch",
                "Roundtrip summary and negative expectancy state disagree for the same symbol.",
                {
                    "symbol": symbol,
                    "roundtrip_net_pnl_sum_usdt": fmt_num(rt_net_pnl, 12),
                    "roundtrip_weighted_net_bps": fmt_num(rt_weighted_bps, 4),
                    "negexp_net_pnl_sum_usdt": fmt_num(neg_net_pnl, 12),
                    "negexp_net_expectancy_bps": fmt_num(neg_net_bps, 4),
                    "negexp_fast_fail_net_expectancy_bps": fmt_num(neg_fast_fail_net_bps, 4),
                    "pnl_mismatch_usdt": fmt_num(pnl_mismatch, 12),
                    "bps_mismatch": fmt_num(bps_mismatch, 4),
                    "roundtrip_closed_count": int(rt["count"]),
                    "negexp_closed_cycles": fmt_num(neg_closed_cycles, 0),
                },
            )
        elif not neg:
            diagnosis = "not_observable_negative_expectancy_symbol_missing"
            if int(rt.get("count") or 0) > 0:
                add_issue(
                    "medium",
                    "negative_expectancy_symbol_missing",
                    "Roundtrip summary has closed cycles for a symbol that is absent from negative expectancy state.",
                    {
                        "symbol": symbol,
                        "roundtrip_closed_count": int(rt["count"]),
                        "roundtrip_net_pnl_sum_usdt": fmt_num(rt_net_pnl, 12),
                        "roundtrip_weighted_net_bps": fmt_num(rt_weighted_bps, 4),
                    },
                )
        elif rt["count"] == 0:
            diagnosis = "not_observable_no_closed_roundtrips"
        elif rt_net_pnl is None or neg_net_pnl is None or rt_weighted_bps is None or neg_net_bps is None:
            diagnosis = "not_observable_pnl_or_bps"
        else:
            diagnosis = "ok"
        negative_consistency_rows.append({
            "symbol": symbol,
            "roundtrip_closed_count": int(rt["count"]),
            "roundtrip_net_pnl_sum_usdt": fmt_num(rt_net_pnl, 12),
            "roundtrip_weighted_net_bps": fmt_num(rt_weighted_bps, 4),
            "negexp_closed_cycles": fmt_num(neg_closed_cycles, 0),
            "negexp_net_pnl_sum_usdt": fmt_num(neg_net_pnl, 12),
            "negexp_net_expectancy_bps": fmt_num(neg_net_bps, 4),
            "negexp_fast_fail_net_expectancy_bps": fmt_num(neg_fast_fail_net_bps, 4),
            "pnl_mismatch_usdt": fmt_num(pnl_mismatch, 12),
            "bps_mismatch": fmt_num(bps_mismatch, 4),
            "mismatch_suspected": str(mismatch_suspected).lower(),
            "diagnosis": diagnosis,
        })

    BNB_RISK_SYMBOL = "BNB/USDT"

    def json_number_or_not_obs(value, digits=6):
        number = as_float(value)
        if number is None:
            return not_obs
        text = fmt_num(number, digits)
        try:
            return float(text)
        except (TypeError, ValueError):
            return not_obs

    def latest_closed_roundtrip_for_symbol(symbol):
        wanted = normalize_symbol_text(symbol)
        rows = [
            row for row in closed_roundtrip_rows
            if normalize_symbol_text(row.get("symbol")) == wanted
        ]
        if not rows:
            return {}
        return sorted(
            rows,
            key=lambda row: (
                parse_dt_utc(first_observed(row.get("exit_ts"), row.get("timestamp"), row.get("entry_ts")))
                or dt.datetime.min.replace(tzinfo=dt.timezone.utc)
            ),
        )[-1]

    def latest_observed_current_price(symbol):
        context_price = price_from_dict(symbol_map_get(latest_symbol_context, symbol), ("current_px", "latest_px", "last_px", "price", "px"))
        if context_price is not None:
            return context_price
        event_price = price_from_dict(symbol_map_get(event_candidate_price_by_symbol, symbol), ("current_px", "latest_px", "last_px", "price", "px"))
        if event_price is not None:
            return event_price
        observations = sorted(
            price_observations_by_symbol.get(normalize_symbol_text(symbol), []) + price_observations_by_symbol.get(symbol, []),
            key=lambda item: item.get("ts_dt") or dt.datetime.min.replace(tzinfo=dt.timezone.utc),
        )
        for obs in reversed(observations):
            price = as_float(obs.get("price"))
            if price is not None and price > 0:
                return price
        future_px, _source, _reason = future_price_for_symbol(symbol, NOW)
        return future_px

    def roundtrip_cost_bps_from_row(row):
        gross = as_float(row.get("gross_bps"))
        net = as_float(row.get("net_bps"))
        if gross is not None and net is not None:
            return max(0.0, gross - net)
        fee_total = as_float(row.get("fee_total_usdt"))
        entry_notional = roundtrip_entry_notional(row)
        if fee_total is not None and entry_notional and entry_notional > 0:
            return abs(fee_total) / entry_notional * 10000.0
        return configured_roundtrip_cost_bps()

    def if_held_to_current_net_bps(row, current_px):
        entry_px = as_float(row.get("entry_px"))
        if entry_px is None or entry_px <= 0 or current_px is None or current_px <= 0:
            return None
        return ((current_px / entry_px) - 1.0) * 10000.0 - roundtrip_cost_bps_from_row(row)

    def protect_alt_short_cycle_guard_active_for_bnb(entry):
        enabled = config_bool("protect_alt_short_cycle_guard_enabled", True)
        symbols = {
            normalize_symbol_text(symbol)
            for symbol in config_string_list("protect_alt_short_cycle_symbols", ["BNB/USDT", "ETH/USDT"])
        }
        if not enabled or normalize_symbol_text(BNB_RISK_SYMBOL) not in symbols or not isinstance(entry, dict) or not entry:
            return False
        closed_cycles = as_float(first_value(entry, ("closed_cycles",), 0)) or 0.0
        fast_fail_cycles = as_float(first_value(entry, ("fast_fail_closed_cycles",), 0)) or 0.0
        min_cycles = config_number("protect_alt_short_cycle_min_cycles")
        min_cycles = 2.0 if min_cycles is None else float(min_cycles)
        net_floor = config_number("protect_alt_short_cycle_net_floor_bps")
        net_floor = -20.0 if net_floor is None else float(net_floor)
        fast_fail_floor = config_number("protect_alt_short_cycle_fast_fail_floor_bps")
        fast_fail_floor = -30.0 if fast_fail_floor is None else float(fast_fail_floor)
        net_bps = as_float(first_value(entry, ("net_expectancy_bps", "expectancy_bps"), not_obs))
        fast_fail_bps = as_float(first_value(entry, ("fast_fail_net_expectancy_bps", "fast_fail_expectancy_bps"), not_obs))
        return bool(
            (closed_cycles >= min_cycles and net_bps is not None and net_bps <= net_floor)
            or (fast_fail_cycles >= min_cycles and fast_fail_bps is not None and fast_fail_bps <= fast_fail_floor)
        )

    def bnb_risk_recommendation(entry, guard_active):
        if not isinstance(entry, dict) or not entry:
            return "shadow_only"
        net_bps = as_float(first_value(entry, ("net_expectancy_bps", "expectancy_bps"), not_obs))
        fast_fail_bps = as_float(first_value(entry, ("fast_fail_net_expectancy_bps", "fast_fail_expectancy_bps"), not_obs))
        closed_cycles = as_float(first_value(entry, ("closed_cycles",), 0)) or 0.0
        if guard_active or (closed_cycles > 0 and ((net_bps is not None and net_bps < 0) or (fast_fail_bps is not None and fast_fail_bps < 0))):
            return "keep_blocked"
        if closed_cycles > 0 and (net_bps is not None and net_bps >= 0) and (fast_fail_bps is None or fast_fail_bps >= 0):
            return "eligible_for_review"
        return "shadow_only"

    bnb_negative_entry = negative_entries.get(BNB_RISK_SYMBOL) or multi_shadow_negative_expectancy_entry(BNB_RISK_SYMBOL)
    latest_bnb_roundtrip = latest_closed_roundtrip_for_symbol(BNB_RISK_SYMBOL)
    latest_bnb_current_px = latest_observed_current_price(BNB_RISK_SYMBOL)
    bnb_if_held_current_net_bps = (
        if_held_to_current_net_bps(latest_bnb_roundtrip, latest_bnb_current_px)
        if latest_bnb_roundtrip
        else None
    )
    bnb_guard_active = protect_alt_short_cycle_guard_active_for_bnb(bnb_negative_entry)
    bnb_risk_summary = {
        "closed_cycles": json_number_or_not_obs(first_value(bnb_negative_entry, ("closed_cycles",), not_obs), 0),
        "net_expectancy_bps": json_number_or_not_obs(first_value(bnb_negative_entry, ("net_expectancy_bps", "expectancy_bps"), not_obs), 6),
        "fast_fail_net_expectancy_bps": json_number_or_not_obs(first_value(bnb_negative_entry, ("fast_fail_net_expectancy_bps", "fast_fail_expectancy_bps"), not_obs), 6),
        "latest_roundtrip_net_bps": json_number_or_not_obs(first_observed(latest_bnb_roundtrip.get("net_bps"), latest_bnb_roundtrip.get("realized_net_bps"), not_obs), 6) if latest_bnb_roundtrip else not_obs,
        "latest_roundtrip_exit_reason": first_observed(latest_bnb_roundtrip.get("exit_reason"), not_obs) if latest_bnb_roundtrip else not_obs,
        "latest_roundtrip_if_held_current_net_bps": json_number_or_not_obs(bnb_if_held_current_net_bps, 6),
        "protect_alt_short_cycle_guard_active": bool(bnb_guard_active),
        "recommendation": bnb_risk_recommendation(bnb_negative_entry, bnb_guard_active),
    }
    if bnb_risk_summary["recommendation"] == "keep_blocked":
        add_issue(
            "warning",
            "bnb_negative_expectancy_keep_blocked",
            "BNB remains a negative-expectancy symbol; a single if-held recovery observation must not relax protect recovery or f3/f4/f5 weak-entry guards.",
            {
                "closed_cycles": bnb_risk_summary["closed_cycles"],
                "net_expectancy_bps": bnb_risk_summary["net_expectancy_bps"],
                "fast_fail_net_expectancy_bps": bnb_risk_summary["fast_fail_net_expectancy_bps"],
                "latest_roundtrip_net_bps": bnb_risk_summary["latest_roundtrip_net_bps"],
                "latest_roundtrip_if_held_current_net_bps": bnb_risk_summary["latest_roundtrip_if_held_current_net_bps"],
                "protect_alt_short_cycle_guard_active": bnb_risk_summary["protect_alt_short_cycle_guard_active"],
                "recommendation": bnb_risk_summary["recommendation"],
                "protect_recovery_allowed_symbols": multi_shadow_protect_recovery_allowed_symbols(),
                "diagnostic_only": True,
            },
        )

    def is_rank_exit_reason(value):
        return flatten_value(value).startswith("rank_exit")

    def first_rank_exit_reason(*values):
        for value in values:
            text = flatten_value(value)
            if text.startswith("rank_exit"):
                return text
        return ""

    def audit_notes_for_rank(audit):
        notes = audit.get("notes") if isinstance(audit, dict) else []
        if not isinstance(notes, list):
            notes = [notes] if notes else []
        return [flatten_value(note) for note in notes]

    def note_for_symbol(notes, symbol, marker):
        for note in notes:
            if marker in note and symbol in note:
                return note
        return ""

    def rank_from_note(note):
        if not note:
            return None
        match = re.search(r"(?:rank=|rank\s+)(\d+)", note)
        if match:
            return as_int(match.group(1))
        return None

    def target_w_from_audit(audit, symbol):
        explain_rows = audit.get("target_execution_explain") if isinstance(audit.get("target_execution_explain"), list) else []
        for item in explain_rows:
            if isinstance(item, dict) and flatten_value(item.get("symbol")) == symbol:
                value = first_value(item, ("target_w", "effective_target_w", "target_weight"), not_obs)
                if as_float(value) is not None:
                    return as_float(value)
        targets = audit.get("targets_post_risk") if isinstance(audit.get("targets_post_risk"), dict) else {}
        value = targets.get(symbol)
        if isinstance(value, dict):
            value = first_value(value, ("target_w", "weight", "w"), not_obs)
        return as_float(value)

    def rank_from_audit(audit, symbol, note):
        note_rank = rank_from_note(note)
        if note_rank:
            return note_rank
        explain_rows = audit.get("target_execution_explain") if isinstance(audit.get("target_execution_explain"), list) else []
        for item in explain_rows:
            if isinstance(item, dict) and flatten_value(item.get("symbol")) == symbol:
                rank = as_int(first_value(item, ("selected_rank", "rank"), not_obs))
                if rank:
                    return rank
        return None

    def has_rank_exit_signal(audit, symbol, exit_reason):
        signals = audit.get("exit_signals") if isinstance(audit.get("exit_signals"), list) else []
        for item in signals:
            if not isinstance(item, dict):
                continue
            if flatten_value(item.get("symbol")) != symbol:
                continue
            reason = first_rank_exit_reason(item.get("reason"), item.get("exit_reason"), item.get("source_reason"))
            if reason and (reason == exit_reason or reason.startswith("rank_exit")):
                return True
        return False

    def has_rank_exit_router_close_create(audit, symbol, exit_reason):
        decisions = audit.get("router_decisions") if isinstance(audit.get("router_decisions"), list) else []
        for item in decisions:
            if not isinstance(item, dict):
                continue
            if flatten_value(item.get("symbol")) != symbol:
                continue
            action = flatten_value(item.get("action")).lower()
            side = flatten_value(item.get("side")).lower()
            intent = normalize_trade_intent(item)
            reason = first_rank_exit_reason(item.get("reason"), item.get("source_reason"), item.get("exit_reason"))
            if action == "create" and (side == "sell" or intent == "CLOSE_LONG") and reason:
                if reason == exit_reason or reason.startswith("rank_exit"):
                    return True
        return False

    def symbol_from_inst_id(inst_id):
        text = flatten_value(inst_id).strip()
        if "/" in text:
            return text
        if "-" in text:
            base, quote = text.split("-", 1)
            return f"{base}/{quote}"
        return text or not_obs

    def log_line_ts_utc(line):
        match = re.search(
            r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)",
            line,
        )
        if not match:
            return not_obs, None
        raw = match.group(1).replace(",", ".")
        parsed = parse_dt_utc(raw)
        if parsed is None:
            return not_obs, None
        return parsed.strftime("%Y-%m-%dT%H:%M:%SZ"), parsed

    def in_current_72h_window(value):
        parsed = value if isinstance(value, dt.datetime) else parse_dt_utc(value)
        if parsed is None:
            return False
        parsed = parsed.astimezone(dt.timezone.utc)
        return WINDOW_72H_START <= parsed <= WINDOW_72H_END

    def audit_window_for_run(run_id, audit):
        if not isinstance(audit, dict):
            return None, None, parse_run_time(run_id)
        audit_dt = parse_dt_utc(run_ts(run_id, audit)) or parse_run_time(run_id)
        start_dt = parse_dt_utc(first_value(audit, ("window_start_ts", "start_ts"), not_obs))
        end_dt = parse_dt_utc(first_value(audit, ("window_end_ts", "end_ts"), not_obs))
        if start_dt is None and end_dt is not None:
            start_dt = end_dt - dt.timedelta(hours=1)
        if start_dt is None:
            run_dt = parse_run_time(run_id)
            if run_dt is not None:
                start_dt = run_dt
        if end_dt is None and start_dt is not None:
            end_dt = start_dt + dt.timedelta(hours=1)
        return start_dt, end_dt, audit_dt

    def run_id_for_log_event(event_dt):
        if event_dt is None:
            return not_obs
        matches = []
        tolerance = dt.timedelta(minutes=90)
        for run_id, audit in audit_by_run.items():
            start_dt, end_dt, audit_dt = audit_window_for_run(run_id, audit)
            reference_dt = end_dt or audit_dt or parse_run_time(run_id)
            if reference_dt is None:
                continue
            delta = abs(event_dt - reference_dt)
            if delta > tolerance:
                continue
            in_window = bool(start_dt is not None and end_dt is not None and start_dt <= event_dt < end_dt)
            matches.append((0 if in_window else 1, delta.total_seconds(), run_id))
        if matches:
            return sorted(matches)[0][2]
        return not_obs

    def parse_trade_safety_rank_exit_line(line, source):
        if "TRADE_SAFETY:" not in line:
            return None
        match = re.search(r"TRADE_SAFETY:\s*(?P<side>\w+)\s+(?P<inst>[A-Z0-9][A-Z0-9/-]*)(?:,\s*(?P<rest>.*))?$", line)
        if not match:
            return None
        side = flatten_value(match.group("side")).lower()
        inst_id = flatten_value(match.group("inst"))
        rest = flatten_value(match.group("rest"))
        fields = {
            key: value.strip()
            for key, value in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)=([^,]+)", rest)
        }
        intent = flatten_value(fields.get("intent")).upper()
        reason = flatten_value(fields.get("reason"))
        if side != "sell" or intent != "CLOSE_LONG" or not reason.startswith("rank_exit"):
            return None
        ts_utc, event_dt = log_line_ts_utc(line)
        current_window = in_current_72h_window(event_dt)
        return {
            "ts_utc": ts_utc,
            "run_id": run_id_for_log_event(event_dt) if current_window else not_obs,
            "symbol": symbol_from_inst_id(inst_id),
            "exit_reason": reason,
            "side": side,
            "intent": intent,
            "notional": first_observed(fields.get("notional")),
            "source": source,
            "raw_json": safe_json({"line": line.strip()}),
            "_event_dt": event_dt,
            "_current_window": current_window,
        }

    def rank_exit_events_from_trades():
        rows = []
        seen = set()

        def add(row):
            key = (row.get("ts_utc"), row.get("run_id"), row.get("symbol"), row.get("exit_reason"))
            if key in seen:
                return
            seen.add(key)
            rows.append(row)

        for event in raw_trade_events:
            intent = event.get("intent")
            side = flatten_value(event.get("side")).lower()
            raw_item = event.get("raw_item") if isinstance(event.get("raw_item"), dict) else {}
            router = event.get("router_info") if isinstance(event.get("router_info"), dict) else {}
            reason = first_rank_exit_reason(
                event.get("exit_reason"),
                raw_item.get("exit_reason"),
                raw_item.get("reason"),
                raw_item.get("source_reason"),
                router.get("reason"),
                router.get("source_reason"),
            )
            if reason and (intent == "CLOSE_LONG" or side == "sell"):
                add({
                    "ts_utc": event.get("timestamp", not_obs),
                    "run_id": event.get("run_id", not_obs),
                    "symbol": event.get("symbol", not_obs),
                    "exit_reason": reason,
                    "source": f"trades:{event.get('source_file', not_obs)}",
                })
        for row in trade_rows:
            reason = first_rank_exit_reason(row.get("exit_reason"), row.get("raw_json"))
            if not reason:
                continue
            side = flatten_value(row.get("side")).lower()
            if "sell" not in side and row.get("roundtrip_status") != "closed":
                continue
            add({
                "ts_utc": first_observed(row.get("exit_ts"), row.get("timestamp"), row.get("entry_ts")),
                "run_id": row.get("run_id", not_obs),
                "symbol": row.get("symbol", not_obs),
                "exit_reason": reason,
                "source": f"trades_roundtrips:{row.get('source_file', not_obs)}",
            })
        return rows

    legacy_rank_exit_event_rows = []

    def rank_exit_events_from_logs():
        rows = []
        for log_path in sorted((OUT / "raw" / "logs").glob("*")):
            source = f"log:{log_path.relative_to(OUT).as_posix()}"
            try:
                with log_path.open("r", encoding="utf-8", errors="replace") as fh:
                    for line in fh:
                        event = parse_trade_safety_rank_exit_line(line, source)
                        if event:
                            if event.get("_current_window"):
                                rows.append(event)
                            else:
                                legacy_rank_exit_event_rows.append({
                                    "ts_utc": event.get("ts_utc", not_obs),
                                    "run_id": not_obs,
                                    "symbol": event.get("symbol", not_obs),
                                    "exit_reason": event.get("exit_reason", not_obs),
                                    "source": event.get("source", not_obs),
                                    "notional": event.get("notional", not_obs),
                                    "diagnosis": "legacy_rank_exit_event_outside_current_window",
                                })
            except Exception as exc:
                collection_errors.append({"source": str(log_path), "error": f"rank_exit_trade_safety_scan: {exc!r}"})
        return rows

    def rank_exit_events():
        rows = []
        seen = set()
        for event in rank_exit_events_from_trades() + rank_exit_events_from_logs():
            key = (
                event.get("ts_utc", not_obs),
                event.get("run_id", not_obs),
                event.get("symbol", not_obs),
                event.get("exit_reason", not_obs),
                event.get("source", not_obs),
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(event)
        return rows

    def rank_exit_log_target_positive_symbols():
        symbols = set()
        for log_path in sorted((OUT / "raw" / "logs").glob("*")):
            try:
                with log_path.open("r", encoding="utf-8", errors="replace") as fh:
                    for line in fh:
                        if "rank_exit_target_still_positive" not in line:
                            continue
                        _, line_dt = log_line_ts_utc(line)
                        if not in_current_72h_window(line_dt):
                            continue
                        match = re.search(r"([A-Z0-9]+/[A-Z0-9]+)", line)
                        if match:
                            symbols.add(match.group(1))
            except Exception as exc:
                collection_errors.append({"source": str(log_path), "error": f"rank_exit_log_scan: {exc!r}"})
        return symbols

    def build_rank_exit_consistency_rows():
        rows = []
        log_target_positive_symbols = rank_exit_log_target_positive_symbols()
        close_only_weight_eps = config_number("close_only_weight_eps")
        if close_only_weight_eps is None:
            close_only_weight_eps = 0.001
        for event in rank_exit_events():
            run_id = event.get("run_id", not_obs)
            symbol = event.get("symbol", not_obs)
            exit_reason = event.get("exit_reason", not_obs)
            reliable_run = bool(run_id not in (None, "", not_obs) and run_id in audit_by_run)
            audit = audit_by_run.get(run_id, {}) if reliable_run else {}
            notes = audit_notes_for_rank(audit)
            target_positive_note = note_for_symbol(notes, symbol, "rank_exit_target_still_positive")
            has_target_still_positive_note = bool(reliable_run and (target_positive_note or symbol in log_target_positive_symbols))
            target_w = target_w_from_audit(audit, symbol) if reliable_run else None
            target_positive = bool(target_w is not None and target_w > close_only_weight_eps)
            has_exit_signal = has_rank_exit_signal(audit, symbol, exit_reason) if reliable_run else False
            has_router_close_create = has_rank_exit_router_close_create(audit, symbol, exit_reason) if reliable_run else False
            conflict_suspected = bool(
                is_rank_exit_reason(exit_reason)
                and reliable_run
                and (has_target_still_positive_note or target_positive)
                and (not has_exit_signal or not has_router_close_create)
            )
            missing_bits = []
            if not has_exit_signal:
                missing_bits.append("missing_exit_signal")
            if not has_router_close_create:
                missing_bits.append("missing_router_close_create")
            if conflict_suspected:
                diagnosis = "high_issue_rank_exit_target_positive_execution_conflict:" + ",".join(missing_bits)
                add_issue(
                    "high",
                    "rank_exit_target_positive_execution_conflict",
                    "rank_exit sell was observed while target remained positive or target-positive note was present, without complete exit_signal/router close-create evidence.",
                    {
                        "run_id": run_id,
                        "symbol": symbol,
                        "exit_reason": exit_reason,
                        "source": event.get("source", not_obs),
                        "target_w": fmt_num(target_w, 8),
                        "target_positive": target_positive,
                        "has_target_still_positive_note": has_target_still_positive_note,
                        "has_exit_signal": has_exit_signal,
                        "has_router_close_create": has_router_close_create,
                    },
                )
            elif is_rank_exit_reason(exit_reason) and not reliable_run:
                diagnosis = "rank_exit_event_unmatched_to_run"
            elif is_rank_exit_reason(exit_reason):
                diagnosis = "ok"
            else:
                diagnosis = not_obs
            rows.append({
                "ts_utc": event.get("ts_utc", not_obs),
                "run_id": run_id,
                "symbol": symbol,
                "exit_reason": exit_reason,
                "source": event.get("source", not_obs),
                "target_w": fmt_num(target_w, 8),
                "rank": fmt_num(rank_from_audit(audit, symbol, target_positive_note), 0),
                "close_only_weight_eps": fmt_num(close_only_weight_eps, 8),
                "has_exit_signal": str(has_exit_signal).lower(),
                "has_router_close_create": str(has_router_close_create).lower(),
                "has_target_still_positive_note": str(has_target_still_positive_note).lower(),
                "target_positive": str(target_positive).lower(),
                "conflict_suspected": str(conflict_suspected).lower(),
                "diagnosis": diagnosis,
            })
        return rows

    rank_exit_consistency_rows = build_rank_exit_consistency_rows()

    uncovered_trade_events = sorted({event["event_id"] for event in raw_trade_events} - covered_trade_event_ids)
    roundtrip_warning = bool(uncovered_trade_events)
    if roundtrip_warning:
        add_issue(
            "high",
            "trades_exist_but_roundtrip_summary_missing",
            "Raw trades exist but roundtrip/open trade summary rows are missing.",
            {"raw_trade_rows": raw_trade_file_rows, "roundtrip_rows": len(trade_rows), "uncovered_trade_events": uncovered_trade_events[:20]},
        )

    for log_path in sorted((OUT / "raw" / "logs").glob("*")):
        try:
            with log_path.open("r", encoding="utf-8", errors="replace") as fh:
                for idx, line in enumerate(fh):
                    lower = line.lower()
                    if any(term in lower for term in DUST_TERMS):
                        dust_rows.append({
                            "source": str(log_path.relative_to(OUT)),
                            "run_id": not_obs,
                            "ts_utc": not_obs,
                            "symbol": not_obs,
                            "raw_held_value_usdt": not_obs,
                            "effective_held_value_usdt": not_obs,
                            "dust_threshold_usdt": not_obs,
                            "reason": "log_line",
                            "anti_chase_triggered": str("anti_chase" in lower or "anti-chase" in lower).lower(),
                            "dust_position_ignored_for_add_size": not_obs,
                            "bug_suspected": not_obs,
                            "diagnosis": f"log_line:{idx + 1}",
                            "raw_json": sanitize_text(line.strip())[:2000],
                        })
        except Exception as exc:
            collection_errors.append({"source": str(log_path), "error": f"log_scan: {exc!r}"})

    for row in load_jsonl(OUT / "raw" / "reports" / "quant_lab_usage.jsonl"):
        if not isinstance(row, dict):
            continue
        event_type = str(row.get("event_type") or not_obs)
        event_kind = quant_lab_event_kind(row)
        source = "reports/quant_lab_usage.jsonl"
        if event_kind in {"permission", "order_filter", "run_summary", "live_permission", "filter_order", "final_permission"}:
            raw_permission_decision = first_observed(
                row.get("raw_permission_decision"),
                row.get("quant_lab_permission"),
                row.get("permission"),
                row.get("quant_lab_decision"),
                not_obs,
            )
            effective_permission_decision = first_observed(
                row.get("effective_permission_decision"),
                row.get("final_permission"),
                row.get("effective_decision"),
                not_obs,
            )
            quant_lab_compliance_rows.append({
                "source": source,
                "run_id": flatten_value(row.get("run_id") or not_obs),
                "ts_utc": flatten_value(row.get("ts") or not_obs),
                "event_type": "permission_audit" if event_type == "permission_audit" else event_type,
                "event_id": flatten_value(row.get("event_id") or not_obs),
                "request_id": flatten_value(row.get("request_id") or not_obs),
                "original_request_id": flatten_value(row.get("original_request_id") or not_obs),
                "original_event_id": flatten_value(row.get("original_event_id") or not_obs),
                "endpoint_path": flatten_value(first_observed(row.get("endpoint_path"), row.get("endpoint"), not_obs)),
                "status_code": flatten_value(row.get("status_code") if row.get("status_code") is not None else not_obs),
                "success": bool_observed(row.get("success")),
                "latency_ms": flatten_value(row.get("latency_ms") if row.get("latency_ms") is not None else not_obs),
                "error_type": flatten_value(row.get("error_type") or not_obs),
                "error_message_short": flatten_value(first_observed(row.get("error_message_short"), row.get("error_message_sanitized"), not_obs)),
                "mode": flatten_value(row.get("mode") or not_obs),
                "local_mode": flatten_value(first_observed(row.get("local_mode"), row.get("mode"), not_obs)),
                "mode_source": flatten_value(row.get("mode_source") or not_obs),
                "quant_lab_requested_mode": flatten_value(first_observed(row.get("quant_lab_requested_mode"), row.get("requested_mode"), not_obs)),
                "quant_lab_effective_mode": flatten_value(first_observed(row.get("quant_lab_effective_mode"), row.get("effective_mode"), row.get("mode"), not_obs)),
                "called_api": bool_observed(row.get("called_api")),
                "apply_permission_gate": bool_observed(row.get("apply_permission_gate")),
                "apply_cost_gate": bool_observed(row.get("apply_cost_gate")),
                "permission_gate_enforced": bool_observed(row.get("permission_gate_enforced")),
                "cost_gate_enforced": bool_observed(row.get("cost_gate_enforced")),
                "enforce_readiness_status": flatten_value(row.get("enforce_readiness_status") or not_obs),
                "enforce_blocked_reasons": flatten_value(first_observed(row.get("enforce_blocked_reasons"), row.get("enforce_blocked_reason"), not_obs)),
                "enforce_blocked_reason": flatten_value(row.get("enforce_blocked_reason") or not_obs),
                "contract_version_match": bool_observed(row.get("contract_version_match")),
                "telemetry_schema_version_match": bool_observed(row.get("telemetry_schema_version_match")),
                "raw_permission_decision": flatten_value(raw_permission_decision),
                "raw_permission_status": flatten_value(row.get("raw_permission_status") or not_obs),
                "raw_permission_enforceable": bool_observed(row.get("raw_permission_enforceable")),
                "effective_permission_decision": flatten_value(effective_permission_decision),
                "would_block_if_enforced": bool_observed(row.get("would_block_if_enforced")),
                "shadow_override_reason": flatten_value(row.get("shadow_override_reason") or not_obs),
                "fallback_reason": flatten_value(row.get("fallback_reason") or not_obs),
                "remote_permission_as_of_ts": flatten_value(row.get("remote_permission_as_of_ts") or not_obs),
                "remote_permission_expires_at": flatten_value(row.get("remote_permission_expires_at") or not_obs),
                "remote_permission_status": flatten_value(row.get("remote_permission_status") or not_obs),
                "remote_permission_source_bundle_ts": flatten_value(row.get("remote_permission_source_bundle_ts") or not_obs),
                "remote_permission_telemetry_latest_ts": flatten_value(row.get("remote_permission_telemetry_latest_ts") or not_obs),
                "remote_permission_contract_version": flatten_value(first_observed(row.get("remote_permission_contract_version"), row.get("contract_version"), not_obs)),
                "permission_contract_violation": bool_observed(row.get("permission_contract_violation")),
                "contract_version": flatten_value(row.get("contract_version") or row.get("remote_permission_contract_version") or not_obs),
                "permission_decision": flatten_value(first_observed(row.get("permission_decision"), raw_permission_decision, not_obs)),
                "effective_decision": flatten_value(first_observed(row.get("effective_decision"), effective_permission_decision, not_obs)),
                "order_decision": flatten_value(row.get("order_decision") or not_obs),
                "fail_policy": flatten_value(row.get("fail_policy") or not_obs),
                "fallback_used": bool_observed(row.get("fallback_used")),
                "symbol": flatten_value(row.get("symbol") or not_obs),
                "side": flatten_value(row.get("side") or not_obs),
                "intent": flatten_value(row.get("intent") or not_obs),
                "orders_before": flatten_value(row.get("orders_before") if row.get("orders_before") is not None else not_obs),
                "orders_after": flatten_value(row.get("orders_after") if row.get("orders_after") is not None else not_obs),
                "orders_filtered": flatten_value(row.get("orders_filtered") if row.get("orders_filtered") is not None else not_obs),
                "buy_orders_filtered": flatten_value(row.get("buy_orders_filtered") if row.get("buy_orders_filtered") is not None else not_obs),
                "filtered": str(bool(row.get("filtered"))).lower() if "filtered" in row else not_obs,
                "filter_reason": flatten_value(row.get("filter_reason") or not_obs),
                "diagnosis": "filtered" if row.get("filtered") else ("fallback_policy_applied" if row.get("fallback_used") else "ok"),
                "raw_json": safe_json(row),
            })
        if event_kind == "cost_estimate":
            required_edge = first_observed(row.get("required_edge_bps"), row.get("min_required_edge_bps"), not_obs)
            cost_source = first_observed(row.get("cost_source"), row.get("source"), row.get("local_cost_source"), not_obs)
            fallback_level = first_observed(row.get("fallback_level"), not_obs)
            cost_model_version_value = flatten_value(row.get("cost_model_version") or not_obs).strip().lower()
            degraded_cost = (
                flatten_value(cost_source).strip().lower() == "global_default"
                or flatten_value(fallback_level).strip().upper() == "GLOBAL_DEFAULT"
                or cost_model_version_value == "global_default_v0"
            )
            cost_diagnosis = "global_default_cost" if degraded_cost else flatten_value(row.get("diagnosis") or ("fallback_cost" if row.get("fallback_used") else "ok"))
            quant_lab_cost_usage_rows.append({
                "source": source,
                "run_id": flatten_value(row.get("run_id") or not_obs),
                "ts_utc": flatten_value(row.get("ts") or not_obs),
                "event_type": "cost_usage",
                "schema_version": flatten_value(row.get("schema_version") or not_obs),
                "contract_version": flatten_value(first_observed(row.get("contract_version"), row.get("cost_contract_version"), not_obs)),
                "event_id_generation_version": flatten_value(row.get("event_id_generation_version") or not_obs),
                "source_snapshot_hash": flatten_value(row.get("source_snapshot_hash") or not_obs),
                "event_id": flatten_value(row.get("event_id") or not_obs),
                "request_id": flatten_value(row.get("request_id") or not_obs),
                "endpoint_path": flatten_value(first_observed(row.get("endpoint_path"), row.get("endpoint"), "/v1/costs/estimate")),
                "status_code": flatten_value(row.get("status_code") if row.get("status_code") is not None else not_obs),
                "success": bool_observed(first_observed(row.get("success"), True)),
                "latency_ms": flatten_value(row.get("latency_ms") if row.get("latency_ms") is not None else not_obs),
                "error_type": flatten_value(row.get("error_type") or not_obs),
                "error_message_short": flatten_value(first_observed(row.get("error_message_short"), row.get("error_message_sanitized"), not_obs)),
                "mode": flatten_value(row.get("mode") or not_obs),
                "mode_source": flatten_value(row.get("mode_source") or not_obs),
                "quant_lab_requested_mode": flatten_value(first_observed(row.get("quant_lab_requested_mode"), row.get("requested_mode"), not_obs)),
                "quant_lab_effective_mode": flatten_value(first_observed(row.get("quant_lab_effective_mode"), row.get("effective_mode"), row.get("mode"), not_obs)),
                "called_api": bool_observed(row.get("called_api")),
                "apply_permission_gate": bool_observed(row.get("apply_permission_gate")),
                "apply_cost_gate": bool_observed(row.get("apply_cost_gate")),
                "permission_gate_enforced": bool_observed(row.get("permission_gate_enforced")),
                "enforce_readiness_status": flatten_value(row.get("enforce_readiness_status") or not_obs),
                "enforce_blocked_reasons": flatten_value(first_observed(row.get("enforce_blocked_reasons"), row.get("enforce_blocked_reason"), not_obs)),
                "enforce_blocked_reason": flatten_value(row.get("enforce_blocked_reason") or not_obs),
                "contract_version_match": bool_observed(row.get("contract_version_match")),
                "telemetry_schema_version_match": bool_observed(row.get("telemetry_schema_version_match")),
                "symbol": flatten_value(row.get("symbol") or not_obs),
                "request_symbol": flatten_value(first_observed(row.get("request_symbol"), row.get("symbol"), not_obs)),
                "normalized_symbol": flatten_value(row.get("normalized_symbol") or not_obs),
                "response_symbol": flatten_value(first_observed(row.get("response_symbol"), row.get("normalized_symbol"), row.get("symbol"), not_obs)),
                "venue": flatten_value(row.get("venue") or not_obs),
                "instrument_type": flatten_value(row.get("instrument_type") or not_obs),
                "side": flatten_value(row.get("side") or not_obs),
                "intent": flatten_value(row.get("intent") or not_obs),
                "notional_usdt": flatten_value(row.get("notional_usdt") if row.get("notional_usdt") is not None else not_obs),
                "quantile": flatten_value(row.get("quantile") or not_obs),
                "requested_quantile": flatten_value(first_observed(row.get("requested_quantile"), row.get("quantile"), not_obs)),
                "strategy_id": flatten_value(first_observed(row.get("strategy_id"), row.get("alpha_id"), not_obs)),
                "request_id": flatten_value(row.get("request_id") or not_obs),
                "requested_regime": flatten_value(first_observed(row.get("requested_regime"), row.get("regime"), not_obs)),
                "matched_regime": flatten_value(first_observed(row.get("matched_regime"), row.get("regime"), not_obs)),
                "alpha_id": flatten_value(row.get("alpha_id") or not_obs),
                "cost_bps": flatten_value(first_observed(row.get("cost_bps"), row.get("total_cost_bps"), row.get("effective_total_cost_bps"))),
                "cost_usdt": flatten_value(row.get("cost_usdt") if row.get("cost_usdt") is not None else not_obs),
                "cost_source": flatten_value(cost_source),
                "cost_model_version": flatten_value(row.get("cost_model_version") or not_obs),
                "cost_contract_version": flatten_value(first_observed(row.get("cost_contract_version"), row.get("contract_version"), QUANT_LAB_CONTRACT_VERSION)),
                "as_of_ts": flatten_value(first_observed(row.get("as_of_ts"), row.get("response_ts"), not_obs)),
                "fallback_level": flatten_value(fallback_level),
                "sample_count": flatten_value(row.get("sample_count") if row.get("sample_count") is not None else not_obs),
                "total_cost_bps": flatten_value(row.get("total_cost_bps") if row.get("total_cost_bps") is not None else not_obs),
                "effective_total_cost_bps": flatten_value(row.get("effective_total_cost_bps") if row.get("effective_total_cost_bps") is not None else not_obs),
                "selected_total_cost_bps": flatten_value(row.get("selected_total_cost_bps") if row.get("selected_total_cost_bps") is not None else first_observed(row.get("total_cost_bps"), not_obs)),
                "total_cost_bps_p50": flatten_value(row.get("total_cost_bps_p50") if row.get("total_cost_bps_p50") is not None else not_obs),
                "total_cost_bps_p75": flatten_value(row.get("total_cost_bps_p75") if row.get("total_cost_bps_p75") is not None else not_obs),
                "total_cost_bps_p90": flatten_value(row.get("total_cost_bps_p90") if row.get("total_cost_bps_p90") is not None else not_obs),
                "required_edge_bps": flatten_value(required_edge),
                "expected_edge_bps": flatten_value(row.get("expected_edge_bps") if row.get("expected_edge_bps") is not None else not_obs),
                "expected_edge_source": flatten_value(first_observed(row.get("expected_edge_source"), row.get("proxy_source"))),
                "min_required_edge_bps": flatten_value(row.get("min_required_edge_bps") if row.get("min_required_edge_bps") is not None else not_obs),
                "would_filter_by_cost": bool_observed(first_observed(row.get("would_filter_by_cost"), row.get("would_filter"))),
                "would_block_by_cost": bool_observed(first_observed(row.get("would_block_by_cost"), row.get("would_filter_by_cost"), row.get("would_filter"))),
                "actually_filtered": bool_observed(first_observed(row.get("actually_filtered"), row.get("order_filtered"))),
                "cost_gate_enforced": bool_observed(row.get("cost_gate_enforced")),
                "quant_lab_decision": flatten_value(row.get("quant_lab_decision") or not_obs),
                "fallback_used": bool_observed(row.get("fallback_used")),
                "fallback_used_for_cost_model": str(bool(truthy_observed(row.get("fallback_used")) or degraded_cost)).lower(),
                "fallback_reason": flatten_value(row.get("fallback_reason") or not_obs),
                "degraded_cost_model": str(bool(degraded_cost)).lower(),
                "filtered": str(bool(row.get("filtered"))).lower() if "filtered" in row else not_obs,
                "filter_reason": flatten_value(row.get("filter_reason") or not_obs),
                "warning": flatten_value(row.get("warning") or not_obs),
                "cost_gate_verified": bool_observed(row.get("cost_gate_verified")),
                "diagnosis": cost_diagnosis,
                "raw_json": safe_json(row),
            })
        if event_type == "live_guard_impact" or event_kind == "live_guard_impact":
            live_guard_impact_rows.append({
                "run_id": flatten_value(row.get("run_id") or not_obs),
                "ts_utc": flatten_value(first_observed(row.get("ts_utc"), row.get("ts"), not_obs)),
                "symbol": flatten_value(row.get("symbol") or not_obs),
                "strategy_candidate": flatten_value(row.get("strategy_candidate") or not_obs),
                "intent": flatten_value(row.get("intent") or not_obs),
                "would_have_opened_live": bool_observed(row.get("would_have_opened_live")),
                "would_be_blocked_by_quant_lab_no_live_modes": bool_observed(first_observed(row.get("would_be_blocked_by_quant_lab_no_live_modes"), row.get("blocked_by_quant_lab_no_live_modes"))),
                "would_be_blocked_by_cost_trust_guard": bool_observed(first_observed(row.get("would_be_blocked_by_cost_trust_guard"), row.get("would_block_by_cost_trust_guard"))),
                "would_be_blocked_by_shadow_live_whitelist": bool_observed(first_observed(row.get("would_be_blocked_by_shadow_live_whitelist"), row.get("blocked_by_shadow_live_whitelist"))),
                "cost_quality": flatten_value(row.get("cost_quality") or not_obs),
                "cost_trusted_for_live": bool_observed(row.get("cost_trusted_for_live")),
                "cost_trust_level": flatten_value(row.get("cost_trust_level") or not_obs),
                "raw_permission_decision": flatten_value(row.get("raw_permission_decision") or not_obs),
                "allowed_live_modes": flatten_value(row.get("allowed_live_modes") or not_obs),
                "final_decision_actual": flatten_value(first_observed(row.get("final_decision_actual"), row.get("final_decision_after_guard"), not_obs)),
                "guard_enforced": "false",
            })
        if quant_lab_is_fallback(row):
            quant_lab_fallback_rows.append({
                "source": source,
                "run_id": flatten_value(row.get("run_id") or not_obs),
                "ts_utc": flatten_value(row.get("ts") or not_obs),
                "event_type": "fallback",
                "event_id": flatten_value(row.get("event_id") or not_obs),
                "request_id": flatten_value(row.get("request_id") or not_obs),
                "original_request_id": flatten_value(first_observed(row.get("original_request_id"), row.get("request_id"), not_obs)),
                "original_event_id": flatten_value(first_observed(row.get("original_event_id"), row.get("event_id"), not_obs)),
                "endpoint": flatten_value(first_observed(row.get("endpoint"), row.get("endpoint_path"), row.get("path"))),
                "endpoint_path": flatten_value(first_observed(row.get("endpoint_path"), row.get("endpoint"), row.get("path"))),
                "status_code": flatten_value(row.get("status_code") if row.get("status_code") is not None else not_obs),
                "success": bool_observed(row.get("success")),
                "latency_ms": flatten_value(row.get("latency_ms") if row.get("latency_ms") is not None else not_obs),
                "symbol": flatten_value(row.get("symbol") or not_obs),
                "side": flatten_value(row.get("side") or not_obs),
                "intent": flatten_value(row.get("intent") or not_obs),
                "fail_policy": flatten_value(row.get("fail_policy") or not_obs),
                "effective_decision": flatten_value(row.get("effective_decision") or row.get("order_decision") or not_obs),
                "fallback_used": str(quant_lab_is_fallback(row)).lower(),
                "error": flatten_value(first_observed(row.get("error"), row.get("error_type"))),
                "error_type": flatten_value(first_observed(row.get("error_type"), row.get("error"), not_obs)),
                "error_message_short": flatten_value(first_observed(row.get("error_message_short"), row.get("error_message_sanitized"), row.get("error"), not_obs)),
                "diagnosis": flatten_value(row.get("fallback_reason") or row.get("filter_reason") or row.get("action_taken") or "fallback_policy_applied"),
                "raw_json": safe_json(row),
            })

    for row in load_jsonl(OUT / "raw" / "reports" / "quant_lab_requests.jsonl"):
        if not isinstance(row, dict):
            continue
        request_success = quant_lab_request_success(row)
        if request_success:
            quant_lab_request_success_count += 1
        else:
            quant_lab_request_error_count += 1
        if quant_lab_is_fallback(row):
            quant_lab_fallback_rows.append({
                "source": "reports/quant_lab_requests.jsonl",
                "run_id": flatten_value(row.get("run_id") or not_obs),
                "ts_utc": flatten_value(row.get("ts") or not_obs),
                "event_type": "fallback",
                "event_id": flatten_value(row.get("event_id") or not_obs),
                "request_id": flatten_value(row.get("request_id") or not_obs),
                "original_request_id": flatten_value(first_observed(row.get("original_request_id"), row.get("request_id"), not_obs)),
                "original_event_id": flatten_value(first_observed(row.get("original_event_id"), row.get("event_id"), not_obs)),
                "endpoint": flatten_value(first_observed(row.get("endpoint"), row.get("endpoint_path"), row.get("path"))),
                "endpoint_path": flatten_value(first_observed(row.get("endpoint_path"), row.get("endpoint"), row.get("path"))),
                "status_code": flatten_value(row.get("status_code") if row.get("status_code") is not None else not_obs),
                "success": bool_observed(row.get("success")),
                "latency_ms": flatten_value(row.get("latency_ms") if row.get("latency_ms") is not None else not_obs),
                "symbol": flatten_value((row.get("params") or {}).get("symbol") if isinstance(row.get("params"), dict) else not_obs),
                "side": flatten_value((row.get("params") or {}).get("side") if isinstance(row.get("params"), dict) else not_obs),
                "intent": not_obs,
                "fail_policy": not_obs,
                "effective_decision": not_obs,
                "fallback_used": str(quant_lab_is_fallback(row)).lower(),
                "error": flatten_value(first_observed(row.get("error"), row.get("error_type"), f"http_{row.get('status_code')}" if not request_success and row.get("status_code") else not_obs)),
                "error_type": flatten_value(first_observed(row.get("error_type"), row.get("error"), f"http_{row.get('status_code')}" if not request_success and row.get("status_code") else not_obs)),
                "error_message_short": flatten_value(first_observed(row.get("error_message_short"), row.get("error_message_sanitized"), row.get("error"), not_obs)),
                "diagnosis": flatten_value(row.get("fallback_reason") or row.get("action_taken") or "fallback_request"),
                "raw_json": safe_json(row),
            })

    quant_lab_permission_audit_rows = list(quant_lab_compliance_rows)
    def quant_lab_mode_audit_row(row):
        requested_mode = first_observed(row.get("quant_lab_requested_mode"), row.get("requested_mode"), row.get("mode"), not_obs)
        effective_mode = first_observed(row.get("quant_lab_effective_mode"), row.get("effective_mode"), row.get("mode"), not_obs)
        blocked_reasons = first_observed(row.get("enforce_blocked_reasons"), row.get("enforce_blocked_reason"), not_obs)
        return {
            "source": flatten_value(row.get("source") or not_obs),
            "run_id": flatten_value(row.get("run_id") or not_obs),
            "ts_utc": flatten_value(row.get("ts_utc") or row.get("ts") or not_obs),
            "event_type": flatten_value(row.get("event_type") or not_obs),
            "event_id": flatten_value(row.get("event_id") or not_obs),
            "request_id": flatten_value(row.get("request_id") or not_obs),
            "mode": flatten_value(row.get("mode") or not_obs),
            "mode_source": flatten_value(row.get("mode_source") or not_obs),
            "quant_lab_requested_mode": flatten_value(requested_mode),
            "quant_lab_effective_mode": flatten_value(effective_mode),
            "called_api": bool_observed(row.get("called_api")),
            "apply_permission_gate": bool_observed(row.get("apply_permission_gate")),
            "apply_cost_gate": bool_observed(row.get("apply_cost_gate")),
            "permission_gate_enforced": bool_observed(row.get("permission_gate_enforced")),
            "cost_gate_enforced": bool_observed(row.get("cost_gate_enforced")),
            "enforce_readiness_status": flatten_value(row.get("enforce_readiness_status") or not_obs),
            "enforce_blocked_reasons": flatten_value(blocked_reasons),
            "enforce_blocked_reason": flatten_value(row.get("enforce_blocked_reason") or not_obs),
            "contract_version_match": bool_observed(row.get("contract_version_match")),
            "telemetry_schema_version_match": bool_observed(row.get("telemetry_schema_version_match")),
            "raw_permission_decision": flatten_value(row.get("raw_permission_decision") or row.get("permission_decision") or not_obs),
            "effective_permission_decision": flatten_value(row.get("effective_permission_decision") or row.get("effective_decision") or not_obs),
            "would_block_if_enforced": bool_observed(row.get("would_block_if_enforced")),
            "fallback_used": bool_observed(row.get("fallback_used")),
            "fallback_reason": flatten_value(row.get("fallback_reason") or not_obs),
        }

    for row in quant_lab_compliance_rows + quant_lab_cost_usage_rows + quant_lab_fallback_rows:
        if any(
            first_observed(row.get(field), not_obs) != not_obs
            for field in (
                "mode",
                "mode_source",
                "quant_lab_requested_mode",
                "quant_lab_effective_mode",
                "enforce_readiness_status",
                "enforce_blocked_reasons",
                "enforce_blocked_reason",
            )
        ):
            quant_lab_mode_audit_rows.append(quant_lab_mode_audit_row(row))

    def normalize_shadow_side(value, intent):
        text = flatten_value(value).strip().lower()
        intent_text = flatten_value(intent).strip().upper()
        if text in {"buy", "sell"}:
            return text
        if intent_text == "OPEN_LONG":
            return "buy"
        if intent_text == "CLOSE_LONG":
            return "sell"
        return text or not_obs

    def normalize_shadow_intent(value, side):
        text = flatten_value(value).strip().upper()
        side_text = flatten_value(side).strip().lower()
        if text:
            return text
        if side_text == "buy":
            return "OPEN_LONG"
        if side_text == "sell":
            return "CLOSE_LONG"
        return not_obs

    def shadow_roundtrip_key(run_id, symbol, side="buy", intent="OPEN_LONG"):
        return (
            flatten_value(run_id),
            flatten_value(symbol),
            normalize_shadow_side(side, intent),
            normalize_shadow_intent(intent, side),
        )

    roundtrip_rows_by_entry_key = defaultdict(list)
    roundtrip_rows_by_entry_run = defaultdict(list)
    for row in trade_rows:
        symbol = flatten_value(row.get("symbol"))
        entry_ts = flatten_value(row.get("entry_ts"))
        if symbol in ("", not_obs) or entry_ts in ("", not_obs):
            continue
        entry_run_id = entry_run_id_from_roundtrip_source(row.get("source_file"))
        if entry_run_id in ("", not_obs):
            entry_run_id = flatten_value(row.get("run_id"))
        key = shadow_roundtrip_key(entry_run_id, symbol, "buy", "OPEN_LONG")
        roundtrip_rows_by_entry_key[key].append(row)
        roundtrip_rows_by_entry_run[entry_run_id].append(row)

    def choose_shadow_roundtrip(rows):
        if not rows:
            return None
        closed_rows = [row for row in rows if row.get("roundtrip_status") == "closed"]
        if closed_rows:
            return closed_rows[0]
        return rows[0]

    def quant_lab_shadow_permission(row):
        return flatten_value(first_observed(
            row.get("raw_permission_decision"),
            row.get("permission_decision"),
            row.get("quant_lab_permission"),
            row.get("permission"),
            not_obs,
        ))

    def quant_lab_shadow_final_permission(row):
        return flatten_value(first_observed(
            row.get("effective_permission_decision"),
            row.get("effective_decision"),
            row.get("final_permission"),
            not_obs,
        ))

    def quant_lab_shadow_outcome_bucket(actual_executed, matched_row):
        if not actual_executed:
            return "not_executed_or_no_matching_roundtrip"
        status = flatten_value(matched_row.get("roundtrip_status") if matched_row else not_obs)
        if status != "closed":
            return "executed_roundtrip_pending"
        net_bps = as_float(matched_row.get("net_bps"))
        if net_bps is None:
            return "executed_closed_outcome_not_observable"
        if net_bps > 0:
            return "profitable_blocked_by_shadow"
        if net_bps < 0:
            return "losing_blocked_by_shadow"
        return "flat_blocked_by_shadow"

    shadow_permission_candidates = []
    shadow_permission_seen = set()
    for row in quant_lab_permission_audit_rows:
        if not truthy_observed(row.get("would_block_if_enforced")):
            continue
        raw_run_id = flatten_value(row.get("run_id") or not_obs)
        raw_symbol = flatten_value(row.get("symbol") or not_obs)
        side = normalize_shadow_side(row.get("side"), row.get("intent"))
        intent = normalize_shadow_intent(row.get("intent"), side)
        inferred_roundtrip = None
        if raw_symbol in ("", not_obs):
            run_rows = roundtrip_rows_by_entry_run.get(raw_run_id, [])
            observed_symbols = {flatten_value(item.get("symbol")) for item in run_rows if flatten_value(item.get("symbol")) not in ("", not_obs)}
            if len(observed_symbols) == 1:
                inferred_roundtrip = choose_shadow_roundtrip(run_rows)
                raw_symbol = next(iter(observed_symbols))
                side = "buy"
                intent = "OPEN_LONG"
        if raw_run_id in ("", not_obs) or raw_symbol in ("", not_obs):
            continue
        if side not in {"buy", not_obs} and intent != "OPEN_LONG":
            continue
        if intent not in {"OPEN_LONG", not_obs} and side != "buy":
            continue
        side = "buy" if side == not_obs else side
        intent = "OPEN_LONG" if intent == not_obs else intent
        dedupe_key = (raw_run_id, raw_symbol, side, intent)
        if dedupe_key in shadow_permission_seen:
            continue
        shadow_permission_seen.add(dedupe_key)
        shadow_permission_candidates.append((row, raw_run_id, raw_symbol, side, intent, inferred_roundtrip))

    for row, run_id, symbol, side, intent, inferred_roundtrip in shadow_permission_candidates:
        matched = inferred_roundtrip or choose_shadow_roundtrip(roundtrip_rows_by_entry_key.get(shadow_roundtrip_key(run_id, symbol, side, intent), []))
        actual_executed = matched is not None and flatten_value(matched.get("roundtrip_status")) in {"closed", "open", "open_residual", "open_dust_residual_ignored"}
        quant_lab_shadow_outcome_rows.append({
            "run_id": run_id,
            "symbol": symbol,
            "side": side,
            "intent": intent,
            "entry_ts": matched.get("entry_ts", not_obs) if matched else flatten_value(row.get("ts_utc") or row.get("ts") or not_obs),
            "exit_ts": matched.get("exit_ts", not_obs) if matched else not_obs,
            "quant_lab_permission": quant_lab_shadow_permission(row),
            "final_permission": quant_lab_shadow_final_permission(row),
            "would_block_if_enforced": "true",
            "actual_executed": str(bool(actual_executed)).lower(),
            "roundtrip_status": matched.get("roundtrip_status", "not_matched") if matched else "not_matched",
            "net_bps": matched.get("net_bps", not_obs) if matched else not_obs,
            "net_pnl_usdt": matched.get("net_pnl_usdt", not_obs) if matched else not_obs,
            "exit_reason": matched.get("exit_reason", not_obs) if matched else not_obs,
            "outcome_bucket": quant_lab_shadow_outcome_bucket(actual_executed, matched),
        })

    shadow_rows_by_permission = defaultdict(list)
    for row in quant_lab_shadow_outcome_rows:
        shadow_rows_by_permission[row.get("quant_lab_permission") or not_obs].append(row)
    for permission, rows in sorted(shadow_rows_by_permission.items()):
        numeric_net = [as_float(row.get("net_bps")) for row in rows if row.get("actual_executed") == "true"]
        numeric_net = [value for value in numeric_net if value is not None]
        numeric_pnl = [as_float(row.get("net_pnl_usdt")) for row in rows if row.get("actual_executed") == "true"]
        numeric_pnl = [value for value in numeric_pnl if value is not None]
        quant_lab_shadow_outcomes_by_permission.append({
            "permission": permission,
            "would_block_count": len(rows),
            "executed_count": sum(1 for row in rows if row.get("actual_executed") == "true"),
            "avg_net_bps": fmt_num(sum(numeric_net) / len(numeric_net), 6) if numeric_net else not_obs,
            "win_rate": fmt_num(sum(1 for value in numeric_net if value > 0) / len(numeric_net), 6) if numeric_net else not_obs,
            "net_pnl_sum_usdt": fmt_num(sum(numeric_pnl), 12) if numeric_pnl else not_obs,
        })

    config_runtime_consumption_rows = build_config_runtime_consumption_audit()
    config_runtime_not_consumed_count = sum(
        1 for row in config_runtime_consumption_rows
        if row.get("present_in_live_prod") == "true" and row.get("consumed_in_runtime_code") != "true"
        and row.get("diagnosis") not in {"intentionally_inactive", "legacy_execution_quant_lab_inactive_top_level_authoritative"}
    )

    write_csv(
        "summaries/router_decisions.csv",
        router_rows,
        ["run_id", "audit_timestamp", "index", "symbol", "action", "reason", "source_reason", "stage", "side", "drift", "deadband", "hold_hours", "min_hold_hours", "exit_allowed_before_min_hold", "exit_blocked_by_min_hold", "exit_priority", "min_hold_block_reason", "early_exit_opportunity_cost_bps", "raw_json"],
    )
    write_csv(
        "summaries/trades_roundtrips.csv",
        trade_rows,
        ["run_id", "source_file", "row_number", "timestamp", "symbol", "side", "qty", "price", "entry_ts", "entry_px", "exit_ts", "exit_px", "entry_reason", "exit_reason", "probe_type", "roundtrip_status", "gross_pnl_usdt", "fee_total_usdt", "net_pnl_usdt", "gross_bps", "net_bps", "hold_minutes", "hold_hours", "min_hold_hours", "exit_allowed_before_min_hold", "exit_blocked_by_min_hold", "exit_priority", "min_hold_block_reason", "early_exit_opportunity_cost_bps", "would_have_held_24h_status", "would_have_held_24h_net_bps", "remaining_value_usdt", "dust_threshold_usdt", "raw_json"],
    )
    write_csv(
        "summaries/early_exit_cases.csv",
        early_exit_rows,
        ["ts_utc", "run_id", "symbol", "event_type", "exit_reason", "exit_priority", "hold_hours", "min_hold_hours", "exit_allowed_before_min_hold", "exit_blocked_by_min_hold", "min_hold_block_reason", "actual_net_bps", "would_have_held_24h_status", "would_have_held_24h_net_bps", "early_exit_opportunity_cost_bps", "diagnosis", "raw_json"],
    )
    write_csv(
        "summaries/dust_residual_roundtrips.csv",
        dust_residual_roundtrip_rows,
        ["run_id", "source_file", "row_number", "timestamp", "symbol", "side", "qty", "price", "entry_ts", "entry_px", "exit_ts", "exit_px", "entry_reason", "exit_reason", "probe_type", "roundtrip_status", "gross_pnl_usdt", "fee_total_usdt", "net_pnl_usdt", "gross_bps", "net_bps", "hold_minutes", "remaining_value_usdt", "dust_threshold_usdt", "diagnosis", "raw_json"],
    )
    write_csv(
        "summaries/open_positions.csv",
        open_position_rows,
        ["symbol", "entry_ts", "entry_px", "qty", "current_px", "current_value_usdt", "notional_entry_usdt", "unrealized_gross_bps", "unrealized_net_bps", "unrealized_net_usdt", "entry_reason", "probe_type", "current_stop_px", "highest_px", "current_level", "regime", "is_probe", "profit_lock_active", "trailing_active"],
    )
    write_csv(
        "summaries/open_probe_watch.csv",
        open_probe_watch_rows,
        ["symbol", "probe_type", "entry_ts", "entry_px", "current_px", "hold_hours", "unrealized_gross_bps", "unrealized_net_bps", "highest_net_bps", "probe_take_profit_net_bps", "probe_stop_loss_net_bps", "probe_trailing_enable_after_net_bps", "probe_trailing_gap_bps", "probe_time_stop_hours", "probe_time_stop_min_net_bps", "next_expected_exit_condition", "active_probe_ignore_zero_target_close_count", "state_present", "diagnosis"],
    )
    write_csv(
        "summaries/probe_diagnostics.csv",
        probe_rows,
        ["source", "run_id", "ts_utc", "symbol", "probe_type", "event_type", "action", "reason", "status", "alpha6_score", "f4_volume_expansion", "f5_rsi_trend_confirm", "rolling_high", "breakout_met", "net_expectancy_bps", "raw_json"],
    )
    write_csv(
        "summaries/dust_anti_chase_cases.csv",
        dust_rows,
        ["source", "run_id", "ts_utc", "symbol", "raw_held_value_usdt", "effective_held_value_usdt", "dust_threshold_usdt", "reason", "anti_chase_triggered", "dust_position_ignored_for_add_size", "bug_suspected", "diagnosis", "raw_json"],
    )
    write_csv(
        "summaries/probe_lifecycle_audit.csv",
        lifecycle_rows,
        ["ts_utc", "run_id", "symbol", "probe_type", "entry_ts", "entry_px", "exit_ts", "exit_px", "exit_reason", "gross_bps", "net_bps", "remaining_value_usdt", "dust_threshold_usdt", "state_still_present_after_close", "profit_taking_state_present", "highest_px_state_present", "stop_loss_state_present", "fixed_stop_loss_state_present", "repeated_exit_signal_after_flat", "diagnosis"],
    )
    write_csv(
        "summaries/skipped_candidate_maturity_audit.csv",
        maturity_rows,
        ["ts_utc", "run_id", "symbol", "skip_reason", "action", "label_present", "outcome_present", "label_status", "not_observable_reason", "age_hours", "maturity_issue", "raw_json"],
    )
    write_csv(
        "summaries/btc_leadership_probe_blocked_outcomes.csv",
        btc_blocked_rows,
        ["ts_utc", "run_id", "symbol", "skip_reason", "entry_px", "age_hours", *[f"label_{int(h)}h_net_bps" for h in label_horizons], "label_status", "not_observable_reason", "alpha6_score", "f4_volume_expansion", "f5_rsi_trend_confirm", "rolling_high", "breakout_met", "net_expectancy_bps", "closed_cycles"],
    )
    write_csv(
        "summaries/negative_expectancy_consistency.csv",
        negative_consistency_rows,
        ["symbol", "roundtrip_closed_count", "roundtrip_net_pnl_sum_usdt", "roundtrip_weighted_net_bps", "negexp_closed_cycles", "negexp_net_pnl_sum_usdt", "negexp_net_expectancy_bps", "negexp_fast_fail_net_expectancy_bps", "pnl_mismatch_usdt", "bps_mismatch", "mismatch_suspected", "diagnosis"],
    )
    write_text(
        "summaries/bnb_risk_summary.json",
        json.dumps(bnb_risk_summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    write_csv(
        "summaries/summary_trade_count_mismatch.csv",
        summary_trade_count_mismatch_rows,
        ["run_id", "source_file", "trades_file_exists", "trades_file_rows", "trades_counted_rows", "summary_num_trades", "trades_turnover_usdt", "summary_turnover_usdt", "trades_fees_usdt_total", "summary_fees_usdt_total", "trades_slippage_usdt_total", "summary_slippage_usdt_total", "trades_cost_usdt_total", "summary_cost_usdt_total", "count_mismatch", "cost_mismatch", "high_issue", "diagnosis", "parse_error", "trade_metrics_warning"],
    )
    write_csv(
        "reports/summary_trade_count_mismatch.csv",
        summary_trade_count_mismatch_rows,
        ["run_id", "source_file", "trades_file_exists", "trades_file_rows", "trades_counted_rows", "summary_num_trades", "trades_turnover_usdt", "summary_turnover_usdt", "trades_fees_usdt_total", "summary_fees_usdt_total", "trades_slippage_usdt_total", "summary_slippage_usdt_total", "trades_cost_usdt_total", "summary_cost_usdt_total", "count_mismatch", "cost_mismatch", "high_issue", "diagnosis", "parse_error", "trade_metrics_warning"],
    )
    write_csv(
        "summaries/trade_metrics.csv",
        trade_metrics_rows,
        ["run_id", "trades_file_exists", "trades_file_rows", "trades_counted_rows", "num_trades", "turnover_usdt", "fees_usdt_total", "slippage_usdt_total", "cost_usdt_total", "fills_count_today", "trade_metrics_warning", "trade_metrics_warning_count", "trade_export_schema_version", "summary_metrics_version"],
    )
    write_csv(
        "summaries/fill_metrics.csv",
        fill_metrics_rows,
        ["run_id", "ts_utc", "symbol", "normalized_symbol", "side", "action", "qty", "price", "notional_usdt", "fee", "fee_ccy", "fee_usdt", "slippage_usdt", "order_id", "trade_id", "strategy_id", "position_id", "trade_export_schema_version"],
    )
    write_csv(
        "summaries/order_lifecycle.csv",
        order_lifecycle_rows,
        ORDER_LIFECYCLE_FIELDS,
    )
    write_csv(
        "summaries/config_runtime_consumption_audit.csv",
        config_runtime_consumption_rows,
        ["config_key", "defined_in_schema", "present_in_live_prod", "present_in_effective_config", "consumed_in_runtime_code", "consumer_category", "consumer_files", "diagnosis"],
    )
    write_csv(
        "summaries/quant_lab_compliance.csv",
        quant_lab_compliance_rows,
        ["source", "run_id", "ts_utc", "event_type", "event_id", "request_id", "original_request_id", "original_event_id", "endpoint_path", "status_code", "success", "latency_ms", "error_type", "error_message_short", "mode", "local_mode", "permission_gate_enforced", "cost_gate_enforced", "raw_permission_decision", "raw_permission_status", "raw_permission_enforceable", "effective_permission_decision", "would_block_if_enforced", "shadow_override_reason", "fallback_used", "fallback_reason", "remote_permission_as_of_ts", "remote_permission_expires_at", "remote_permission_status", "remote_permission_source_bundle_ts", "remote_permission_telemetry_latest_ts", "remote_permission_contract_version", "permission_contract_violation", "contract_version", "permission_decision", "effective_decision", "order_decision", "fail_policy", "symbol", "side", "intent", "orders_before", "orders_after", "orders_filtered", "buy_orders_filtered", "filtered", "filter_reason", "diagnosis", "raw_json"],
    )
    write_csv(
        "summaries/quant_lab_permission_audit.csv",
        quant_lab_permission_audit_rows,
        ["source", "run_id", "ts_utc", "event_type", "event_id", "request_id", "original_request_id", "original_event_id", "endpoint_path", "status_code", "success", "latency_ms", "error_type", "error_message_short", "mode", "local_mode", "permission_gate_enforced", "raw_permission_decision", "raw_permission_status", "raw_permission_enforceable", "effective_permission_decision", "would_block_if_enforced", "shadow_override_reason", "fallback_used", "fallback_reason", "remote_permission_as_of_ts", "remote_permission_expires_at", "remote_permission_status", "remote_permission_source_bundle_ts", "remote_permission_telemetry_latest_ts", "remote_permission_contract_version", "permission_contract_violation", "contract_version", "symbol", "side", "intent", "filtered", "filter_reason", "diagnosis", "raw_json"],
    )
    write_csv(
        "summaries/quant_lab_mode_audit.csv",
        quant_lab_mode_audit_rows,
        ["source", "run_id", "ts_utc", "event_type", "event_id", "request_id", "mode", "mode_source", "quant_lab_requested_mode", "quant_lab_effective_mode", "called_api", "apply_permission_gate", "apply_cost_gate", "permission_gate_enforced", "cost_gate_enforced", "enforce_readiness_status", "enforce_blocked_reasons", "enforce_blocked_reason", "contract_version_match", "telemetry_schema_version_match", "raw_permission_decision", "effective_permission_decision", "would_block_if_enforced", "fallback_used", "fallback_reason"],
    )
    write_csv(
        "summaries/quant_lab_cost_usage.csv",
        quant_lab_cost_usage_rows,
        ["source", "run_id", "ts_utc", "event_type", "schema_version", "contract_version", "event_id_generation_version", "source_snapshot_hash", "event_id", "request_id", "endpoint_path", "status_code", "success", "latency_ms", "error_type", "error_message_short", "mode", "symbol", "request_symbol", "normalized_symbol", "response_symbol", "venue", "instrument_type", "side", "intent", "notional_usdt", "quantile", "requested_quantile", "strategy_id", "requested_regime", "matched_regime", "alpha_id", "cost_bps", "cost_usdt", "cost_source", "fallback_level", "cost_model_version", "cost_contract_version", "as_of_ts", "sample_count", "selected_total_cost_bps", "total_cost_bps", "effective_total_cost_bps", "total_cost_bps_p50", "total_cost_bps_p75", "total_cost_bps_p90", "required_edge_bps", "expected_edge_bps", "expected_edge_source", "min_required_edge_bps", "would_filter_by_cost", "would_block_by_cost", "actually_filtered", "cost_gate_enforced", "quant_lab_decision", "fallback_used", "fallback_used_for_cost_model", "fallback_reason", "degraded_cost_model", "filtered", "filter_reason", "warning", "cost_gate_verified", "diagnosis", "raw_json"],
    )
    write_csv(
        "summaries/live_guard_impact.csv",
        live_guard_impact_rows,
        ["run_id", "ts_utc", "symbol", "strategy_candidate", "intent", "would_have_opened_live", "would_be_blocked_by_quant_lab_no_live_modes", "would_be_blocked_by_cost_trust_guard", "would_be_blocked_by_shadow_live_whitelist", "cost_quality", "cost_trusted_for_live", "cost_trust_level", "raw_permission_decision", "allowed_live_modes", "final_decision_actual", "guard_enforced"],
    )
    write_csv(
        "summaries/quant_lab_fallbacks.csv",
        quant_lab_fallback_rows,
        ["source", "run_id", "ts_utc", "event_type", "event_id", "request_id", "original_request_id", "original_event_id", "endpoint", "endpoint_path", "status_code", "success", "latency_ms", "symbol", "side", "intent", "fail_policy", "effective_decision", "fallback_used", "error", "error_type", "error_message_short", "diagnosis", "raw_json"],
    )
    write_csv(
        "summaries/quant_lab_shadow_outcomes.csv",
        quant_lab_shadow_outcome_rows,
        ["run_id", "symbol", "side", "intent", "entry_ts", "exit_ts", "quant_lab_permission", "final_permission", "would_block_if_enforced", "actual_executed", "roundtrip_status", "net_bps", "net_pnl_usdt", "exit_reason", "outcome_bucket"],
    )
    write_csv(
        "summaries/quant_lab_shadow_outcomes_by_permission.csv",
        quant_lab_shadow_outcomes_by_permission,
        ["permission", "would_block_count", "executed_count", "avg_net_bps", "win_rate", "net_pnl_sum_usdt"],
    )
    write_csv(
        "summaries/rank_exit_consistency.csv",
        rank_exit_consistency_rows,
        ["ts_utc", "run_id", "symbol", "exit_reason", "source", "target_w", "rank", "close_only_weight_eps", "has_exit_signal", "has_router_close_create", "has_target_still_positive_note", "target_positive", "conflict_suspected", "diagnosis"],
    )
    write_csv(
        "summaries/legacy_rank_exit_events.csv",
        legacy_rank_exit_event_rows,
        ["ts_utc", "run_id", "symbol", "exit_reason", "source", "notional", "diagnosis"],
    )
    write_csv(
        "summaries/protect_sideways_normal_entry_outcomes.csv",
        protect_sideways_normal_entry_rows,
        ["entry_ts", "symbol", "entry_px", "exit_ts", "exit_px", "hold_minutes", "net_bps", "alpha6_score_at_entry", "f4_at_entry", "f5_at_entry", "trend_score_at_entry", "exit_reason", "result_bucket"],
    )
    write_csv(
        "summaries/protect_sideways_normal_entry_outcomes_by_symbol.csv",
        protect_sideways_normal_entry_by_symbol,
        ["symbol", "count", "avg_net_bps", "win_rate", "avg_hold_minutes"],
    )
    write_csv(
        "summaries/swing_early_exit_audit.csv",
        swing_early_exit_rows,
        ["symbol", "entry_ts", "exit_ts", "entry_px", "exit_px", "exit_reason", "hold_hours", "required_hold_hours", "exited_before_min_hold", "exit_priority", "exit_allowed_before_min_hold", "exit_blocked_by_min_hold", "min_hold_block_reason", "guard_enabled_at_exit", "guard_config_seen_at_exit", "code_version_or_config_fingerprint_at_exit", "is_post_fix_sample", "diagnosis", "net_bps_at_exit", "future_24h_net_bps_from_entry", "future_48h_net_bps_from_entry", "future_72h_net_bps_from_entry", "future_24h_net_bps_after_exit", "future_48h_net_bps_after_exit", "would_have_been_better_to_hold_24h", "would_have_been_better_to_hold_48h"],
    )
    write_csv(
        "summaries/swing_early_exit_outcomes_by_reason.csv",
        swing_early_exit_by_reason,
        ["exit_reason", "count", "early_exit_count", "avg_net_bps_at_exit", "avg_future_24h_net_bps_from_entry", "avg_future_48h_net_bps_from_entry", "better_to_hold_24h_count", "better_to_hold_24h_rate", "better_to_hold_48h_count", "better_to_hold_48h_rate"],
    )
    write_csv(
        "summaries/post_min_hold_atr_exit_audit.csv",
        post_min_hold_atr_exit_rows,
        ["symbol", "entry_ts", "exit_ts", "entry_px", "exit_px", "exit_reason", "hold_hours", "min_hold_hours", "hours_after_min_hold", "realized_net_bps", "realized_net_pnl_usdt", "price_at_exit", "price_after_6h", "price_after_12h", "price_after_24h", "net_bps_if_held_6h_after_exit", "net_bps_if_held_12h_after_exit", "net_bps_if_held_24h_after_exit", "would_have_been_better_6h", "would_have_been_better_12h", "would_have_been_better_24h", "f4_at_entry", "f5_at_entry", "dominant_factor", "dominant_factor_contribution_pct", "diagnosis"],
    )
    write_csv(
        "summaries/post_min_hold_atr_exit_outcomes_by_symbol.csv",
        post_min_hold_atr_exit_outcomes_by_symbol,
        ["symbol", "sample_count", "avg_realized_net_bps", "avg_net_bps_if_held_6h_after_exit", "avg_net_bps_if_held_12h_after_exit", "avg_net_bps_if_held_24h_after_exit", "observable_6h_count", "better_to_hold_6h_count", "better_to_hold_6h_rate", "observable_12h_count", "better_to_hold_12h_count", "better_to_hold_12h_rate", "observable_24h_count", "better_to_hold_24h_count", "better_to_hold_24h_rate", "diagnosis_mix"],
    )
    write_text(
        "summaries/swing_atr_soft_exit_readiness.json",
        json.dumps(swing_atr_soft_exit_readiness, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    write_csv(
        "summaries/swing_atr_soft_exit_readiness_by_symbol.csv",
        swing_atr_soft_exit_readiness_by_symbol,
        ["symbol", "ready_for_live_guard", "blocking_reasons", "sample_count", "observable_12h_count", "better_to_hold_12h_rate", "avg_realized_net_bps", "avg_delayed_12h_net_bps", "improvement_bps"],
    )
    write_csv(
        "summaries/swing_atr_soft_exit_shadow.csv",
        swing_atr_soft_exit_shadow_rows,
        ["symbol", "exit_ts", "exit_px", "net_bps_at_exit", "f5_rsi_trend_confirm", "would_delay_exit_if_enabled", "hard_exit_reason", "net_bps_if_delayed_3h", "net_bps_if_delayed_6h", "net_bps_if_delayed_12h", "better_to_delay_3h", "better_to_delay_6h", "better_to_delay_12h"],
    )
    write_csv(
        "summaries/bnb_profit_lock_shadow.csv",
        bnb_profit_lock_shadow_rows,
        ["symbol", "entry_ts", "exit_ts", "entry_px", "exit_px", "exit_reason", "max_unrealized_bps", "profit_lock_30bps_exit", "profit_lock_50bps_exit", "delayed_exit_6h", "delayed_exit_12h", "delayed_exit_24h", "actual_exit_net_bps", "best_shadow_exit_policy", "diagnosis"],
    )
    write_csv(
        "summaries/factor_contribution_audit.csv",
        factor_contribution_rows,
        ["ts_utc", "run_id", "symbol", "final_score", "alpha6_score", "raw_factors", "z_factors", "effective_factor_weights", "contribution_f1_mom_5d", "contribution_f2_mom_20d", "contribution_f3_vol_adj_ret", "contribution_f4_volume_expansion", "contribution_f5_rsi_trend_confirm", "dominant_factor", "dominant_factor_contribution_pct", "router_action", "router_reason", *[f"forward_{int(h)}h_net_bps" for h in label_horizons]],
    )
    write_csv(
        "summaries/factor_contribution_outcomes_by_factor.csv",
        factor_contribution_outcomes_by_factor,
        ["dominant_factor", "count", *[f"avg_{int(h)}h_net_bps" for h in label_horizons], *[f"win_rate_{int(h)}h" for h in label_horizons]],
    )
    write_csv(
        "summaries/f3_dominant_swing_guard_cases.csv",
        f3_dominant_swing_guard_cases,
        ["ts_utc", "run_id", "symbol", "action", "side", "intent", "reason", "router_reason", "entry_reason", "dominant_factor", "dominant_factor_contribution_pct", "swing_f3_dominant_blocked", "swing_hold_position", "f4_volume_expansion", "f5_rsi_trend_confirm", "swing_hold_block_reason", "factor_contribution_source"],
    )
    write_csv(
        "summaries/f3_dominant_swing_guard_outcomes.csv",
        f3_dominant_swing_guard_outcomes,
        ["ts_utc", "run_id", "symbol", "action", "side", "intent", "reason", "router_reason", "entry_reason", "dominant_factor", "dominant_factor_contribution_pct", "swing_f3_dominant_blocked", "swing_hold_position", "f4_volume_expansion", "f5_rsi_trend_confirm", "swing_hold_block_reason", "factor_contribution_source", *[f"forward_{int(h)}h_net_bps" for h in label_horizons]],
    )
    write_csv(
        "summaries/high_score_blocked_targets.csv",
        high_score_blocked_rows,
        ["ts_utc", "run_id", "symbol", "final_score", "selected_rank", "target_w", "router_action", "router_reason", "high_score_block_category", "trend_score", "trend_side", "alpha6_score", "alpha6_side", "f4_volume_expansion", "f5_rsi_trend_confirm", "last_exit_reason", "last_exit_px", "highest_px_before_exit", "elapsed_hours", "required_cooldown_hours", "breakout_exception_met", "entry_px", "current_level", "regime"],
    )
    write_csv(
        "summaries/high_score_blocked_outcomes.csv",
        high_score_blocked_outcome_rows,
        high_score_outcome_fields,
    )
    write_csv(
        "summaries/high_score_blocked_outcomes_by_symbol.csv",
        high_score_blocked_outcomes_by_symbol,
        ["symbol", "skip_reason", "count", *[f"avg_{int(h)}h_net_bps" for h in label_horizons], *[f"win_rate_{int(h)}h" for h in label_horizons]],
    )
    write_csv(
        "summaries/high_score_blocked_outcomes_by_reason.csv",
        high_score_blocked_outcomes_by_reason,
        ["skip_reason", "count", *[f"avg_{int(h)}h_net_bps" for h in label_horizons], *[f"win_rate_{int(h)}h" for h in label_horizons]],
    )
    write_csv(
        "summaries/high_score_blocked_outcomes_by_horizon.csv",
        high_score_blocked_outcomes_by_horizon,
        ["horizon_hours", "count", "pending_count", "not_observable_count", "complete_count", "avg_net_bps", "win_rate"],
    )
    protect_sol_exception_horizon_fields = []
    for horizon in protect_sol_exception_horizons:
        h = int(horizon)
        protect_sol_exception_horizon_fields.extend(
            [
                f"would_pnl_bps_{h}h",
                f"label_{h}h_gross_bps",
                f"label_{h}h_net_bps",
                f"label_{h}h_would_have_won_net",
                f"label_{h}h_status",
                f"label_{h}h_reason",
            ]
        )
    write_csv(
        "summaries/protect_sol_exception_shadow_outcomes.csv",
        protect_sol_exception_shadow_rows,
        [
            "experiment_name",
            "enabled_shadow_only",
            "shadow_only",
            "enable_live_experiment",
            "ts_utc",
            "run_id",
            "symbol",
            "intended_side",
            "alpha6_side",
            "would_enter",
            "would_target_w",
            "would_size_notional",
            "would_exit_time",
            "entry_px",
            "original_block_reason",
            "experiment_reason",
            "final_score",
            "target_w",
            "alpha6_score",
            "trend_score",
            "f3_vol_adj_ret",
            "f4_volume_expansion",
            "f5_rsi_trend_confirm",
            "f3_weight_candidate",
            "f4_weight_candidate",
            "f3_z_factor",
            "f4_z_factor",
            "shadow_alpha6_score_candidate",
            "shadow_alpha6_score_delta",
            "btc_leadership_relax_allowed",
            "alt_impulse_relax_allowed",
            "eth_relax_allowed",
            "current_level",
            "regime",
            "rt_cost_bps",
            *protect_sol_exception_horizon_fields,
            "label_status",
            "label_not_observable_reason",
        ],
    )
    write_csv(
        "summaries/protect_sol_exception_shadow_outcomes_by_symbol_reason_horizon.csv",
        protect_sol_exception_shadow_by_horizon,
        [
            "symbol",
            "original_block_reason",
            "horizon_hours",
            "count",
            "unique_candidate_count",
            "complete_count",
            "complete_unique_candidate_count",
            "pending_count",
            "not_observable_count",
            "avg_would_pnl_bps",
            "win_rate",
            "current_strategy_net_bps",
            "better_than_current_strategy",
            "sample_warning",
            "live_ready_suggestion",
        ],
    )
    write_csv(
        "summaries/protect_sol_exception_factor_weight_shadow.csv",
        protect_sol_exception_factor_weight_shadow_rows,
        [
            "symbol",
            "original_block_reason",
            "horizon_hours",
            "f3_weight_candidate",
            "f4_weight_candidate",
            "count",
            "unique_candidate_count",
            "complete_count",
            "complete_unique_candidate_count",
            "pending_count",
            "not_observable_count",
            "avg_would_pnl_bps",
            "win_rate",
            "current_strategy_net_bps",
            "better_than_current_strategy",
            "sample_warning",
            "live_ready_suggestion",
        ],
    )
    write_csv(
        "summaries/alt_impulse_shadow_outcomes.csv",
        alt_impulse_shadow_rows,
        alt_impulse_shadow_fields,
    )
    write_csv(
        "summaries/alt_impulse_shadow_outcomes_by_symbol.csv",
        alt_impulse_shadow_by_symbol,
        ["symbol", "skip_reason", "shadow_decision", "alpha_discovery_board_status", "paper_ready_allowed", "live_ready_allowed", "shadow_decision_reason", "count", *[f"avg_{int(h)}h_net_bps" for h in label_horizons], *[f"win_rate_{int(h)}h" for h in label_horizons]],
    )
    write_csv(
        "summaries/alt_impulse_shadow_outcomes_by_reason.csv",
        alt_impulse_shadow_by_reason,
        ["skip_reason", "shadow_decision", "alpha_discovery_board_status", "paper_ready_allowed", "live_ready_allowed", "shadow_decision_reason", "count", *[f"avg_{int(h)}h_net_bps" for h in label_horizons], *[f"win_rate_{int(h)}h" for h in label_horizons]],
    )
    write_csv(
        "summaries/alt_impulse_shadow_outcomes_by_horizon.csv",
        alt_impulse_shadow_by_horizon,
        ["horizon_hours", "shadow_decision", "alpha_discovery_board_status", "paper_ready_allowed", "live_ready_allowed", "shadow_decision_reason", "count", "pending_count", "not_observable_count", "complete_count", "avg_net_bps", "win_rate"],
    )
    write_csv(
        "summaries/alt_impulse_shadow_by_regime.csv",
        alt_impulse_shadow_by_regime,
        ["regime_state", "shadow_decision", "alpha_discovery_board_status", "paper_ready_allowed", "live_ready_allowed", "shadow_decision_reason", "count", *[f"avg_{int(h)}h_net_bps" for h in label_horizons], *[f"win_rate_{int(h)}h" for h in label_horizons]],
    )
    write_csv(
        "summaries/alt_impulse_shadow_by_symbol_regime_horizon.csv",
        alt_impulse_shadow_by_symbol_regime_horizon,
        ["symbol", "regime_state", "horizon_hours", "shadow_decision", "alpha_discovery_board_status", "paper_ready_allowed", "live_ready_allowed", "shadow_decision_reason", "count", "pending_count", "not_observable_count", "complete_count", "avg_net_bps", "win_rate"],
    )
    write_text(
        "summaries/alt_impulse_shadow_readiness.json",
        json.dumps(alt_impulse_shadow_readiness, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    write_csv(
        "summaries/alt_impulse_shadow_readiness_by_symbol.csv",
        alt_impulse_shadow_readiness_by_symbol,
        alt_impulse_readiness_fields,
    )
    multi_position_swing_fields = [
        "ts_utc",
        "run_id",
        "shadow_mode",
        "k",
        "symbols",
        "equal_weight",
        "entry_px",
        "entry_px_by_symbol",
        "final_score",
        "final_score_by_symbol",
        "selected_rank",
        "entry_support",
        "rt_cost_bps",
        "debug_reason",
    ]
    for horizon in multi_position_swing_horizons:
        h = int(horizon)
        multi_position_swing_fields.extend([
            f"label_{h}h_status",
            f"label_{h}h_net_bps",
            f"label_{h}h_portfolio_avg_net_bps",
            f"label_{h}h_worst_symbol_net_bps",
            f"label_{h}h_win_count",
            f"label_{h}h_symbol_net_bps",
            f"label_{h}h_reason",
        ])
    multi_position_swing_fields.append("label_status")
    write_csv(
        "summaries/multi_position_swing_shadow_outcomes.csv",
        multi_position_swing_shadow_rows,
        multi_position_swing_fields,
    )
    write_csv(
        "summaries/multi_position_swing_shadow_debug.csv",
        multi_position_swing_shadow_debug_rows,
        ["ts_utc", "run_id", "shadow_mode", "qualified_candidate_count", "debug_reason"],
    )
    write_csv(
        "summaries/multi_position_swing_shadow_by_k.csv",
        multi_position_swing_shadow_by_k,
        ["shadow_mode", "k", "count", "avg_24h_net_bps", "avg_48h_net_bps", "avg_72h_net_bps", "win_rate", "worst_avg"],
    )
    write_csv(
        "summaries/multi_position_swing_shadow_by_symbol.csv",
        multi_position_swing_shadow_by_symbol,
        ["shadow_mode", "symbol", "count", "avg_24h_net_bps", "avg_48h_net_bps", "avg_72h_net_bps", "win_rate_24h", "win_rate_48h", "win_rate_72h"],
    )
    write_csv(
        "summaries/sol_swing_performance.csv",
        sol_swing_performance_rows,
        ["window", "real_roundtrip_count", "real_net_bps_avg", "real_net_pnl_usdt", "high_score_blocked_count", "high_score_blocked_24h_avg", "high_score_blocked_48h_avg", "high_score_blocked_72h_avg", "multi_position_shadow_24h_avg", "multi_position_shadow_48h_avg", "multi_position_shadow_72h_avg", "latest_selected_count", "latest_block_reasons"],
    )
    write_csv(
        "summaries/skipped_candidate_outcomes_by_horizon.csv",
        skipped_candidate_outcomes_by_horizon,
        ["horizon_hours", "count", "pending_count", "not_observable_count", "complete_count", "avg_net_bps", "win_rate"],
    )
    write_csv(
        "summaries/market_impulse_selection_shadow.csv",
        market_impulse_selection_shadow_rows,
        ["ts_utc", "run_id", "active", "trend_buy_count", "btc_trend_score", "selected_live", "selected_by_priority", "selected_by_trend_score", "selected_by_alpha6_confirmed", "selected_by_expected_net_shadow", "candidates_json"],
    )

    entry_quality_dir = OUT / "raw" / "reports" / "entry_quality"
    missed_low_path = entry_quality_dir / "missed_low_audit.csv"
    missed_low_by_symbol_path = entry_quality_dir / "missed_low_by_symbol.csv"
    late_entry_chase_path = entry_quality_dir / "late_entry_chase_shadow.csv"
    late_entry_chase_sensitivity_path = entry_quality_dir / "late_entry_chase_threshold_sensitivity.csv"
    pullback_reversal_path = entry_quality_dir / "pullback_reversal_shadow_outcomes.csv"
    late_entry_chase_advisory_path = entry_quality_dir / "late_entry_chase_threshold_advisory.json"
    pullback_reversal_readiness_path = entry_quality_dir / "pullback_reversal_readiness.json"
    entry_quality_summary_path = entry_quality_dir / "entry_quality_summary.md"
    missed_low_present = missed_low_path.is_file()
    missed_low_by_symbol_present = missed_low_by_symbol_path.is_file()
    late_entry_chase_present = late_entry_chase_path.is_file()
    late_entry_chase_sensitivity_present = late_entry_chase_sensitivity_path.is_file()
    pullback_reversal_present = pullback_reversal_path.is_file()
    late_entry_chase_advisory_present = late_entry_chase_advisory_path.is_file()
    pullback_reversal_readiness_present = pullback_reversal_readiness_path.is_file()
    entry_quality_summary_present = entry_quality_summary_path.is_file()
    missed_low_rows = load_csv_dicts(missed_low_path)
    missed_low_by_symbol_rows = load_csv_dicts(missed_low_by_symbol_path)
    late_entry_chase_rows = load_csv_dicts(late_entry_chase_path)
    late_entry_chase_sensitivity_rows = load_csv_dicts(late_entry_chase_sensitivity_path)
    pullback_reversal_rows = load_csv_dicts(pullback_reversal_path)
    late_entry_chase_advisory = load_json(late_entry_chase_advisory_path) if late_entry_chase_advisory_present else {}
    pullback_reversal_readiness = load_json(pullback_reversal_readiness_path) if pullback_reversal_readiness_present else {}
    if not isinstance(late_entry_chase_advisory, dict):
        late_entry_chase_advisory = {}
    if not isinstance(pullback_reversal_readiness, dict):
        pullback_reversal_readiness = {}

    def first_json_value(obj, keys, default=not_obs):
        if not isinstance(obj, dict):
            if isinstance(obj, list):
                for item in obj:
                    found = first_json_value(item, keys, None)
                    if found not in (None, ""):
                        return found
            return default
        for key in keys:
            if key in obj and obj.get(key) not in (None, ""):
                return obj.get(key)
        for value in obj.values():
            if isinstance(value, (dict, list)):
                found = first_json_value(value, keys, None)
                if found not in (None, ""):
                    return found
        return default

    late_chase_loss_count = sum(
        1 for row in missed_low_rows
        if flatten_value(first_observed(row.get("diagnosis"), row.get("bucket"), "")).strip() == "late_chase_loss"
    )
    if late_chase_loss_count == 0:
        json_count = as_int(first_json_value(late_entry_chase_advisory, ("late_chase_loss_count", "missed_low_late_chase_loss_count"), 0))
        if json_count:
            late_chase_loss_count = json_count
    late_entry_chase_ready_for_live_guard = flatten_value(first_json_value(
        late_entry_chase_advisory,
        ("ready_for_live_guard", "late_entry_chase_ready_for_live_guard", "ready_for_live"),
    )).lower()
    if late_entry_chase_ready_for_live_guard in {"", "none", "null"}:
        late_entry_chase_ready_for_live_guard = not_obs
    pullback_reversal_ready_for_paper = flatten_value(first_json_value(
        pullback_reversal_readiness,
        ("ready_for_paper", "pullback_reversal_ready_for_paper"),
    )).lower()
    if pullback_reversal_ready_for_paper in {"", "none", "null"}:
        pullback_reversal_ready_for_paper = not_obs
    pullback_reversal_ready_for_live_probe = flatten_value(first_json_value(
        pullback_reversal_readiness,
        ("ready_for_live_probe", "pullback_reversal_ready_for_live_probe", "ready_for_live"),
    )).lower()
    if pullback_reversal_ready_for_live_probe in {"", "none", "null"}:
        pullback_reversal_ready_for_live_probe = not_obs
    late_entry_chase_guard_enabled = config_bool("late_entry_chase_guard_enabled", False)
    pullback_reversal_live_enabled = config_bool("pullback_reversal_live_enabled", False)
    entry_quality_available = bool(
        missed_low_present
        or missed_low_by_symbol_present
        or late_entry_chase_present
        or late_entry_chase_sensitivity_present
        or pullback_reversal_present
        or late_entry_chase_advisory_present
        or pullback_reversal_readiness_present
        or entry_quality_summary_present
    )

    def entry_quality_strategy_key(row):
        values = [
            flatten_value(row.get("strategy_candidate")),
            flatten_value(row.get("advisory_strategy_candidate")),
            flatten_value(row.get("strategy_id")),
            flatten_value(row.get("advisory_strategy_id")),
            flatten_value(row.get("experiment_name")),
        ]
        normalized = [value.strip().lower() for value in values if value and value != not_obs]
        for value in normalized:
            if value in ENTRY_QUALITY_STRATEGY_CANDIDATES:
                return value
        return ""

    def risk_on_multi_buy_strategy_key(row):
        values = [
            flatten_value(row.get("strategy_candidate")),
            flatten_value(row.get("advisory_strategy_candidate")),
            flatten_value(row.get("strategy_id")),
            flatten_value(row.get("advisory_strategy_id")),
            flatten_value(row.get("experiment_name")),
        ]
        normalized = [value.strip().lower() for value in values if value and value != not_obs]
        for value in normalized:
            if value in RISK_ON_MULTI_BUY_SHADOW_CANDIDATES:
                return value
        return ""

    def entry_quality_strategy_module(key):
        if "missed_low" in key:
            return "missed_low"
        if "late_entry_chase" in key:
            return "late_entry_chase"
        if "pullback_reversal" in key:
            return "pullback_reversal"
        return "entry_quality"

    def parse_yaml_scalar(section_text, key, default=""):
        match = re.search(rf"(?m)^\s*{re.escape(key)}\s*:\s*([^#\n]+)", section_text or "")
        if not match:
            return default
        return match.group(1).strip().strip("\"'")

    def parse_bool_text(value, default=False):
        text = str(value or "").strip().lower()
        if text in {"true", "yes", "1", "on"}:
            return True
        if text in {"false", "no", "0", "off"}:
            return False
        return default

    def quant_lab_token_from_env_file(path_text, token_env):
        if not path_text:
            return ""
        path = Path(str(path_text)).expanduser()
        if not path.is_file():
            return ""
        try:
            for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export "):].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key.strip() == token_env:
                    return value.strip().strip("\"'")
        except Exception as exc:
            collection_errors.append({"source": str(path), "error": f"entry_quality_api_env_read: {exc!r}"})
        return ""

    def extract_strategy_advisory_items(payload):
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []
        for key in ("strategy_opportunity_advisory", "advisory", "advisories", "rows", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                nested = extract_strategy_advisory_items(value)
                if nested:
                    return nested
        return []

    def advisory_reference_dt(row):
        values = [
            parse_dt_utc(row.get("as_of_ts")),
            parse_dt_utc(row.get("generated_at")),
        ]
        values = [value for value in values if value is not None]
        return max(values) if values else None

    def assess_api_advisory_row(row):
        max_age_minutes = config_number("quant_lab_strategy_opportunity_advisory_max_age_minutes") or 90.0
        require_contract = config_bool("quant_lab_strategy_opportunity_advisory_require_contract_version", True)
        reference_dt = advisory_reference_dt(row)
        expires_dt = parse_dt_utc(row.get("expires_at"))
        age_sec = (NOW - reference_dt).total_seconds() if reference_dt else None
        age_ok = bool(age_sec is not None and age_sec <= max_age_minutes * 60.0)
        expires_ok = expires_dt is None or NOW <= expires_dt
        contract = flatten_value(row.get("contract_version")).strip()
        contract_match = (not require_contract) or (contract == QUANT_LAB_CONTRACT_VERSION)
        fresh = bool(reference_dt is not None and age_ok and expires_ok and contract_match)
        return fresh, age_sec, contract_match

    def normalize_strategy_advisory_api_row(row, endpoint):
        fresh, age_sec, contract_match = assess_api_advisory_row(row)
        return {
            "source_path": f"api:{endpoint}",
            "advisory_source": "api",
            "advisory_fresh": str(bool(fresh)),
            "advisory_age_sec": fmt_num(age_sec, 3) if age_sec is not None else not_obs,
            "advisory_contract_match": str(bool(contract_match)),
            "stale_advisory_used": str(not bool(fresh)).lower(),
            "api_fallback_attempted": "true",
            "api_fallback_success": "true",
            "as_of_ts": flatten_value(row.get("as_of_ts")),
            "generated_at": flatten_value(row.get("generated_at")),
            "expires_at": flatten_value(row.get("expires_at")),
            "contract_version": flatten_value(row.get("contract_version")),
            "quant_lab_git_commit": flatten_value(first_observed(row.get("quant_lab_git_commit"), row.get("git_commit"), row.get("source_git_commit"), "")),
            "source_version": flatten_value(first_observed(row.get("source_version"), row.get("version"), "")),
            "strategy_id": flatten_value(first_observed(row.get("strategy_id"), row.get("strategy"), row.get("strategy_name"), row.get("alpha_id"), row.get("proposal_id"), "")),
            "strategy_candidate": flatten_value(first_observed(row.get("strategy_candidate"), row.get("candidate"), row.get("candidate_name"), row.get("source_strategy_candidate"), "")),
            "experiment_name": flatten_value(first_observed(row.get("experiment_name"), row.get("alpha_name"), row.get("strategy_family"), "")),
            "symbol": flatten_value(first_observed(row.get("symbol"), row.get("instId"), row.get("instrument"), row.get("normalized_symbol"), "")),
            "decision": flatten_value(first_observed(row.get("decision"), row.get("readiness_status"), row.get("board_decision"), row.get("status"), "")),
            "recommended_mode": flatten_value(first_observed(row.get("recommended_mode"), row.get("mode"), row.get("target_mode"), "")).strip().lower().replace("-", "_"),
            "horizon_hours": flatten_value(first_observed(row.get("horizon_hours"), row.get("suggested_horizon"), "")),
            "sample_count": flatten_value(row.get("sample_count")),
            "complete_sample_count": flatten_value(row.get("complete_sample_count")),
            "advisory_status": flatten_value(first_observed(row.get("status"), row.get("readiness_status"), row.get("decision"), "")),
            "advisory_reason": flatten_value(first_observed(row.get("reason"), row.get("block_reason"), row.get("live_block_reason"), row.get("live_block_reasons"), row.get("notes"), "")),
            "max_paper_notional_usdt": flatten_value(row.get("max_paper_notional_usdt")),
            "max_live_notional_usdt": "0" if not fresh else flatten_value(row.get("max_live_notional_usdt")),
            "live_block_reasons": flatten_value(first_observed(row.get("live_block_reasons"), row.get("live_block_reason"), "")),
            "would_block_if_enabled": flatten_value(first_observed(row.get("would_block_if_enabled"), row.get("would_block_if_enforced"), row.get("would_block"), row.get("would_filter"), "")),
            "would_enter": flatten_value(first_observed(row.get("would_enter"), row.get("would_enter_if_enabled"), "")),
            "current_regime": flatten_value(first_observed(row.get("current_regime"), row.get("regime_state"), row.get("market_regime"), row.get("risk_regime"), "")),
            "top_k": flatten_value(first_observed(row.get("top_k"), row.get("k"), row.get("rank_k"), "")),
            "selected_symbols": flatten_value(first_observed(row.get("selected_symbols"), row.get("selected_symbol_list"), row.get("top_symbols"), row.get("top_n_symbols"), row.get("symbols"), "")),
            "would_buy_symbols": flatten_value(first_observed(row.get("would_buy_symbols"), row.get("would_enter_symbols"), row.get("would_open_symbols"), row.get("paper_buy_symbols"), "")),
            "no_sample_reason": flatten_value(first_observed(row.get("no_sample_reason"), row.get("no_entry_reason"), row.get("not_observable_reason"), "")),
        }

    def entry_quality_api_fallback_rows():
        if not config_bool("quant_lab_strategy_opportunity_advisory_api_enabled", True):
            return []
        quant_lab_section = live_section_text("quant_lab")
        if not parse_bool_text(parse_yaml_scalar(quant_lab_section, "enabled", "false"), False):
            return []
        base_url = parse_yaml_scalar(quant_lab_section, "base_url", "http://qyun2.hrhome.top:8027").rstrip("/")
        token_env = parse_yaml_scalar(quant_lab_section, "api_token_env", "QUANT_LAB_API_TOKEN") or "QUANT_LAB_API_TOKEN"
        token = os.environ.get(token_env, "").strip()
        if not token:
            token = quant_lab_token_from_env_file(parse_yaml_scalar(quant_lab_section, "api_env_path", ""), token_env)
        if not token:
            collection_errors.append({"source": "entry_quality_api_fallback", "error": "token_missing"})
            return []
        out_rows = []
        for endpoint in ("/v1/strategy-opportunity-advisory", "/v1/strategy_opportunity_advisory"):
            url = f"{base_url}{endpoint}?{urllib.parse.urlencode({'format': 'json'})}"
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=5) as response:
                    payload = json.loads(response.read().decode("utf-8", errors="replace"))
            except Exception as exc:
                collection_errors.append({"source": f"entry_quality_api_fallback:{endpoint}", "error": repr(exc)})
                continue
            for item in extract_strategy_advisory_items(payload):
                normalized = normalize_strategy_advisory_api_row(item, endpoint)
                if entry_quality_strategy_key(normalized) or risk_on_multi_buy_strategy_key(normalized):
                    out_rows.append(normalized)
            if out_rows:
                break
        deduped = []
        seen = set()
        for row in out_rows:
            key = tuple(flatten_value(row.get(field)) for field in ("strategy_id", "strategy_candidate", "symbol", "decision", "recommended_mode", "horizon_hours", "source_path"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    def strategy_advisory_summary_rows():
        rows = load_csv_dicts(OUT / "summaries" / "strategy_opportunity_advisory_reader.csv")
        if rows and any(entry_quality_strategy_key(row) or risk_on_multi_buy_strategy_key(row) for row in rows):
            return rows
        raw_paths = [
            OUT / "raw" / "reports" / "strategy_opportunity_advisory.csv",
            OUT / "raw" / "reports" / "quant_lab" / "strategy_opportunity_advisory.csv",
            OUT / "raw" / "reports" / "quant_lab_latest" / "strategy_opportunity_advisory.csv",
            OUT / "raw" / "reports" / "quant_lab" / "latest" / "reports" / "strategy_opportunity_advisory.csv",
        ]
        raw_rows = []
        for path in raw_paths:
            for row in load_csv_dicts(path):
                payload = dict(row)
                payload.setdefault("source_path", path.as_posix())
                payload.setdefault("advisory_source", "local")
                raw_rows.append(payload)
        if raw_rows and any(entry_quality_strategy_key(row) or risk_on_multi_buy_strategy_key(row) for row in raw_rows):
            return raw_rows
        api_rows = entry_quality_api_fallback_rows()
        if api_rows:
            return api_rows
        if rows:
            return rows
        return raw_rows

    def advisory_value(row, *names, default=not_obs):
        for name in names:
            value = row.get(name)
            if value not in (None, "", not_obs):
                return value
        return default

    def advisory_mode(row):
        return flatten_value(advisory_value(row, "recommended_mode", "advisory_recommended_mode", default="")).strip().lower().replace("-", "_")

    def advisory_decision(row):
        return flatten_value(advisory_value(row, "decision", "advisory_decision", default="")).strip().upper()

    def advisory_response_action(row):
        existing = flatten_value(advisory_value(row, "response_action", "advisory_response_action", default=""))
        if existing:
            return existing
        decision = advisory_decision(row)
        mode = advisory_mode(row)
        fresh_text = flatten_value(advisory_value(row, "advisory_fresh", default="true")).strip().lower()
        fresh = fresh_text not in {"false", "0", "no"}
        if decision == "KILL":
            return "negative_advisory"
        if mode == "research":
            return "research_display_only"
        if mode == "shadow":
            return "shadow_tracking"
        if mode == "paper":
            return "paper_tracking"
        if decision == "LIVE_SMALL_READY" and not fresh:
            return "stale_advisory_live_disabled"
        if decision == "LIVE_SMALL_READY":
            return "ignored_live_small_disabled"
        return "display_only"

    def advisory_no_sample_reason(row, module_name):
        reason = flatten_value(advisory_value(
            row,
            "no_sample_reason",
            "advisory_reason",
            "live_block_reasons",
            "block_reason",
            default="",
        )).strip()
        if reason:
            return reason
        action = advisory_response_action(row)
        if action == "negative_advisory":
            return "negative_advisory"
        if action == "research_display_only":
            return "research_display_only"
        if module_name == "late_entry_chase":
            return "late_entry_chase_shadow_only"
        if module_name == "pullback_reversal":
            return "pullback_reversal_shadow_only"
        return "entry_quality_advisory_only"

    strategy_advisory_rows_for_modules = list(strategy_advisory_summary_rows())
    entry_quality_strategy_rows = []
    for advisory_row in strategy_advisory_rows_for_modules:
        strategy_key = entry_quality_strategy_key(advisory_row)
        if not strategy_key:
            continue
        module_name = entry_quality_strategy_module(strategy_key)
        fresh = flatten_value(advisory_value(advisory_row, "advisory_fresh", default=not_obs))
        max_live_notional = advisory_value(advisory_row, "max_live_notional_usdt", "advisory_max_live_notional_usdt")
        if fresh.strip().lower() in {"false", "0", "no"}:
            max_live_notional = "0"
        entry_quality_strategy_rows.append({
            "advisory_name": "strategy_opportunity_advisory",
            "source_file": flatten_value(advisory_value(advisory_row, "source_path", "advisory_source_path")),
            "available": "true",
            "row_count": 1,
            "late_chase_loss_count": late_chase_loss_count if module_name == "late_entry_chase" else not_obs,
            "ready_for_live_guard": late_entry_chase_ready_for_live_guard if module_name == "late_entry_chase" else not_obs,
            "ready_for_paper": pullback_reversal_ready_for_paper if module_name == "pullback_reversal" else not_obs,
            "ready_for_live_probe": pullback_reversal_ready_for_live_probe if module_name == "pullback_reversal" else not_obs,
            "strategy_candidate": strategy_key,
            "symbol": flatten_value(advisory_value(advisory_row, "symbol")),
            "decision": advisory_decision(advisory_row) or not_obs,
            "recommended_mode": advisory_mode(advisory_row) or not_obs,
            "advisory_source": flatten_value(advisory_value(advisory_row, "advisory_source")),
            "advisory_fresh": fresh,
            "stale_advisory_used": flatten_value(advisory_value(advisory_row, "stale_advisory_used")),
            "api_fallback_attempted": flatten_value(advisory_value(advisory_row, "api_fallback_attempted")),
            "api_fallback_success": flatten_value(advisory_value(advisory_row, "api_fallback_success")),
            "would_block_if_enabled": flatten_value(advisory_value(advisory_row, "would_block_if_enabled", "would_block_if_enforced")),
            "would_enter": flatten_value(advisory_value(advisory_row, "would_enter")),
            "no_sample_reason": advisory_no_sample_reason(advisory_row, module_name),
            "max_live_notional_usdt": flatten_value(max_live_notional),
            "max_live_notional_usdt_ignored": "true",
            "live_block_reasons": flatten_value(advisory_value(advisory_row, "live_block_reasons", "advisory_live_block_reasons")),
            "late_entry_chase_guard_enabled": str(late_entry_chase_guard_enabled).lower(),
            "pullback_reversal_live_enabled": str(pullback_reversal_live_enabled).lower(),
            "live_order_effect": "read_only_no_hard_block",
            "status": advisory_response_action(advisory_row),
            "raw_json": safe_json(advisory_row),
        })
    if entry_quality_strategy_rows:
        entry_quality_available = True
    entry_quality_advisory_rows = [
        {
            "advisory_name": "missed_low",
            "source_file": "raw/reports/entry_quality/missed_low_audit.csv",
            "available": str(missed_low_present).lower(),
            "row_count": len(missed_low_rows),
            "late_chase_loss_count": late_chase_loss_count,
            "ready_for_live_guard": not_obs,
            "ready_for_paper": not_obs,
            "ready_for_live_probe": not_obs,
            "late_entry_chase_guard_enabled": str(late_entry_chase_guard_enabled).lower(),
            "pullback_reversal_live_enabled": str(pullback_reversal_live_enabled).lower(),
            "live_order_effect": "read_only_no_hard_block",
            "status": "available" if missed_low_present else "missing_or_empty",
            "raw_json": not_obs,
        },
        {
            "advisory_name": "missed_low_by_symbol",
            "source_file": "raw/reports/entry_quality/missed_low_by_symbol.csv",
            "available": str(missed_low_by_symbol_present).lower(),
            "row_count": len(missed_low_by_symbol_rows),
            "late_chase_loss_count": late_chase_loss_count,
            "ready_for_live_guard": not_obs,
            "ready_for_paper": not_obs,
            "ready_for_live_probe": not_obs,
            "late_entry_chase_guard_enabled": str(late_entry_chase_guard_enabled).lower(),
            "pullback_reversal_live_enabled": str(pullback_reversal_live_enabled).lower(),
            "live_order_effect": "read_only_no_hard_block",
            "status": "available" if missed_low_by_symbol_present else "missing_or_empty",
            "raw_json": not_obs,
        },
        {
            "advisory_name": "late_entry_chase",
            "source_file": "raw/reports/entry_quality/late_entry_chase_threshold_advisory.json",
            "available": str(late_entry_chase_present or late_entry_chase_advisory_present).lower(),
            "row_count": len(late_entry_chase_rows),
            "late_chase_loss_count": late_chase_loss_count,
            "ready_for_live_guard": late_entry_chase_ready_for_live_guard,
            "ready_for_paper": not_obs,
            "ready_for_live_probe": not_obs,
            "late_entry_chase_guard_enabled": str(late_entry_chase_guard_enabled).lower(),
            "pullback_reversal_live_enabled": str(pullback_reversal_live_enabled).lower(),
            "live_order_effect": "read_only_no_hard_block",
            "status": "available" if late_entry_chase_present or late_entry_chase_advisory_present else "missing_or_empty",
            "raw_json": safe_json(late_entry_chase_advisory) if late_entry_chase_advisory else not_obs,
        },
        {
            "advisory_name": "late_entry_chase_threshold_sensitivity",
            "source_file": "raw/reports/entry_quality/late_entry_chase_threshold_sensitivity.csv",
            "available": str(late_entry_chase_sensitivity_present).lower(),
            "row_count": len(late_entry_chase_sensitivity_rows),
            "late_chase_loss_count": late_chase_loss_count,
            "ready_for_live_guard": late_entry_chase_ready_for_live_guard,
            "ready_for_paper": not_obs,
            "ready_for_live_probe": not_obs,
            "late_entry_chase_guard_enabled": str(late_entry_chase_guard_enabled).lower(),
            "pullback_reversal_live_enabled": str(pullback_reversal_live_enabled).lower(),
            "live_order_effect": "read_only_no_hard_block",
            "status": "available" if late_entry_chase_sensitivity_present else "missing_or_empty",
            "raw_json": not_obs,
        },
        {
            "advisory_name": "pullback_reversal",
            "source_file": "raw/reports/entry_quality/pullback_reversal_readiness.json",
            "available": str(pullback_reversal_present or pullback_reversal_readiness_present).lower(),
            "row_count": len(pullback_reversal_rows),
            "late_chase_loss_count": not_obs,
            "ready_for_live_guard": not_obs,
            "ready_for_paper": pullback_reversal_ready_for_paper,
            "ready_for_live_probe": pullback_reversal_ready_for_live_probe,
            "late_entry_chase_guard_enabled": str(late_entry_chase_guard_enabled).lower(),
            "pullback_reversal_live_enabled": str(pullback_reversal_live_enabled).lower(),
            "live_order_effect": "read_only_no_hard_block",
            "status": "available" if pullback_reversal_present or pullback_reversal_readiness_present else "missing_or_empty",
            "raw_json": safe_json(pullback_reversal_readiness) if pullback_reversal_readiness else not_obs,
        },
        {
            "advisory_name": "entry_quality_summary",
            "source_file": "raw/reports/entry_quality/entry_quality_summary.md",
            "available": str(entry_quality_summary_present).lower(),
            "row_count": 1 if entry_quality_summary_present else 0,
            "late_chase_loss_count": not_obs,
            "ready_for_live_guard": not_obs,
            "ready_for_paper": not_obs,
            "ready_for_live_probe": not_obs,
            "late_entry_chase_guard_enabled": str(late_entry_chase_guard_enabled).lower(),
            "pullback_reversal_live_enabled": str(pullback_reversal_live_enabled).lower(),
            "live_order_effect": "read_only_no_hard_block",
            "status": "available" if entry_quality_summary_present else "missing_or_empty",
            "raw_json": not_obs,
        },
    ]
    if not entry_quality_available:
        entry_quality_advisory_rows.append({
            "advisory_name": "entry_quality",
            "source_file": not_obs,
            "available": "false",
            "row_count": 0,
            "late_chase_loss_count": not_obs,
            "ready_for_live_guard": not_obs,
            "ready_for_paper": not_obs,
            "ready_for_live_probe": not_obs,
            "late_entry_chase_guard_enabled": str(late_entry_chase_guard_enabled).lower(),
            "pullback_reversal_live_enabled": str(pullback_reversal_live_enabled).lower(),
            "live_order_effect": "read_only_no_hard_block",
            "status": "quant_lab_entry_quality_unavailable",
            "raw_json": not_obs,
        })
        add_issue(
            "warning",
            "quant_lab_entry_quality_unavailable",
            "quant-lab entry-quality advisory reports were not available; live trading is unchanged.",
            {"live_order_effect": "read_only_no_hard_block"},
        )
    entry_quality_default_fields = {
        "strategy_candidate": not_obs,
        "symbol": not_obs,
        "decision": not_obs,
        "recommended_mode": not_obs,
        "advisory_source": not_obs,
        "advisory_fresh": not_obs,
        "stale_advisory_used": not_obs,
        "api_fallback_attempted": not_obs,
        "api_fallback_success": not_obs,
        "would_block_if_enabled": not_obs,
        "would_enter": not_obs,
        "no_sample_reason": not_obs,
        "max_live_notional_usdt": not_obs,
        "max_live_notional_usdt_ignored": "true",
        "live_block_reasons": not_obs,
    }
    for row in entry_quality_advisory_rows:
        for key, value in entry_quality_default_fields.items():
            row.setdefault(key, value)
    entry_quality_advisory_rows.extend(entry_quality_strategy_rows)
    entry_quality_would_block_count = sum(
        1 for row in entry_quality_strategy_rows
        if truthy_text(row.get("would_block_if_enabled"))
    )
    entry_quality_would_enter_count = sum(
        1 for row in entry_quality_strategy_rows
        if truthy_text(row.get("would_enter"))
    )
    entry_quality_no_sample_reasons = dict(
        sorted(
            Counter(
                flatten_value(row.get("no_sample_reason") or not_obs)
                for row in entry_quality_strategy_rows
            ).items()
        )
    )
    write_csv(
        "summaries/entry_quality_advisory_reader.csv",
        entry_quality_advisory_rows,
        [
            "advisory_name",
            "source_file",
            "available",
            "row_count",
            "late_chase_loss_count",
            "ready_for_live_guard",
            "ready_for_paper",
            "ready_for_live_probe",
            "strategy_candidate",
            "symbol",
            "decision",
            "recommended_mode",
            "advisory_source",
            "advisory_fresh",
            "stale_advisory_used",
            "api_fallback_attempted",
            "api_fallback_success",
            "would_block_if_enabled",
            "would_enter",
            "no_sample_reason",
            "max_live_notional_usdt",
            "max_live_notional_usdt_ignored",
            "live_block_reasons",
            "late_entry_chase_guard_enabled",
            "pullback_reversal_live_enabled",
            "live_order_effect",
            "status",
            "raw_json",
        ],
    )

    def risk_on_symbol_list(value):
        text = flatten_value(value)
        if not text or text == not_obs:
            return []
        try:
            parsed = json.loads(text)
            raw = parsed if isinstance(parsed, list) else [text]
        except Exception:
            cleaned = text.strip("[]")
            for old, new in ((";", ","), ("|", ","), ("\n", ",")):
                cleaned = cleaned.replace(old, new)
            raw = [part.strip().strip("'\"") for part in cleaned.split(",")]
        out = []
        seen = set()
        for item in raw:
            symbol = normalize_multi_symbol_text(item)
            if symbol == "MULTI":
                continue
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            out.append(symbol)
        return out

    def risk_on_symbols_from_row(row, names):
        for name in names:
            symbols = risk_on_symbol_list(row.get(name))
            if symbols:
                return symbols
        return []

    def risk_on_top_k(row, strategy_key):
        explicit = as_int(advisory_value(row, "top_k", "k", "rank_k", default=""))
        if explicit is not None and explicit > 0:
            return explicit
        match = re.search(r"risk_on_multi_buy_top(\d+)", flatten_value(strategy_key).lower())
        return int(match.group(1)) if match else None

    def risk_on_actual_bought_symbols():
        bought = set()
        for event in raw_trade_events:
            intent = flatten_value(event.get("intent")).strip().upper()
            side = flatten_value(event.get("side")).strip().lower()
            if intent in {"OPEN_LONG", "BUY", "OPEN"} or (not intent and side == "buy"):
                symbol = normalize_multi_symbol_text(event.get("symbol"))
                if symbol:
                    bought.add(symbol)
        for row in candidate_snapshot_rows:
            decision = flatten_value(row.get("final_decision")).strip().upper()
            if decision in {"OPEN_LONG", "BUY", "ALLOW_OPEN_LONG"}:
                symbol = normalize_multi_symbol_text(row.get("symbol"))
                if symbol:
                    bought.add(symbol)
        return sorted(bought)

    def risk_on_source_advisory_rows():
        raw_paths = [
            OUT / "raw" / "reports" / "strategy_opportunity_advisory.csv",
            OUT / "raw" / "reports" / "quant_lab" / "strategy_opportunity_advisory.csv",
            OUT / "raw" / "reports" / "quant_lab_latest" / "strategy_opportunity_advisory.csv",
            OUT / "raw" / "reports" / "quant_lab" / "latest" / "reports" / "strategy_opportunity_advisory.csv",
        ]
        raw_rows = []
        for path in raw_paths:
            for row in load_csv_dicts(path):
                if not risk_on_multi_buy_strategy_key(row):
                    continue
                payload = dict(row)
                payload.setdefault("source_path", path.as_posix())
                raw_rows.append(payload)
        if raw_rows:
            return raw_rows
        quant_lab_section = live_section_text("quant_lab")
        token_env = parse_yaml_scalar(quant_lab_section, "api_token_env", "QUANT_LAB_API_TOKEN") or "QUANT_LAB_API_TOKEN"
        token = os.environ.get(token_env, "").strip()
        if not token:
            token = quant_lab_token_from_env_file(parse_yaml_scalar(quant_lab_section, "api_env_path", ""), token_env)
        api_rows = [row for row in entry_quality_api_fallback_rows() if risk_on_multi_buy_strategy_key(row)] if token else []
        if api_rows:
            return api_rows
        return [row for row in strategy_advisory_rows_for_modules if risk_on_multi_buy_strategy_key(row)]

    def risk_on_detail_rows():
        detail_paths = [
            OUT / "raw" / "reports" / "risk_on_multi_buy_shadow.csv",
            ROOT / "raw" / "reports" / "risk_on_multi_buy_shadow.csv",
            ROOT / "reports" / "risk_on_multi_buy_shadow.csv",
        ]
        rows = []
        seen_paths = set()
        for path in detail_paths:
            key = path.as_posix()
            if key in seen_paths:
                continue
            seen_paths.add(key)
            for row in load_csv_dicts(path):
                payload = dict(row)
                payload.setdefault("source_path", key)
                rows.append(payload)
        return rows

    def risk_on_detail_for(advisory_row, detail_rows, top_k):
        strategy_key = risk_on_multi_buy_strategy_key(advisory_row)
        if strategy_key:
            for row in detail_rows:
                if risk_on_multi_buy_strategy_key(row) == strategy_key:
                    return row
        if top_k is not None:
            for row in detail_rows:
                detail_top_k = risk_on_top_k(row, risk_on_multi_buy_strategy_key(row))
                if detail_top_k == top_k:
                    return row
        return None

    def risk_on_symbols_csv_value(symbols):
        return json.dumps(symbols, ensure_ascii=False) if symbols else not_obs

    existing_risk_on_rows = load_csv_dicts(OUT / "summaries" / "risk_on_multi_buy_shadow.csv")
    risk_on_source_rows = risk_on_source_advisory_rows()
    risk_on_detail_source_rows = risk_on_detail_rows()
    risk_on_rows = []
    if risk_on_source_rows:
        actual_bought_symbols = risk_on_actual_bought_symbols()
        default_run_id = copied_runs[-1] if copied_runs else f"bundle_{STAMP}"
        for advisory_row in risk_on_source_rows:
            strategy_key = risk_on_multi_buy_strategy_key(advisory_row)
            top_k = risk_on_top_k(advisory_row, strategy_key)
            detail_row = risk_on_detail_for(advisory_row, risk_on_detail_source_rows, top_k)
            source_row = detail_row or advisory_row
            selected_symbols = risk_on_symbols_from_row(
                source_row,
                (
                    "selected_symbols",
                    "selected_symbol_list",
                    "top_symbols",
                    "top_n_symbols",
                    "symbols",
                    "would_buy_symbols",
                    "would_buy_symbol",
                    "symbol",
                ),
            )
            would_buy_symbols = risk_on_symbols_from_row(
                source_row,
                (
                    "would_buy_symbols",
                    "would_buy_symbol",
                    "would_enter_symbols",
                    "would_open_symbols",
                    "paper_buy_symbols",
                    "selected_symbols",
                    "symbol",
                ),
            )
            if not selected_symbols and would_buy_symbols:
                selected_symbols = list(would_buy_symbols)
            if not would_buy_symbols and selected_symbols:
                would_buy_symbols = list(selected_symbols)
            if top_k is not None and top_k > 0:
                selected_symbols = selected_symbols[:top_k]
                would_buy_symbols = would_buy_symbols[:top_k]
            missed_symbols = [symbol for symbol in would_buy_symbols if symbol not in set(actual_bought_symbols)]
            risk_on_rows.append({
                "run_id": flatten_value(advisory_value(advisory_row, "run_id", default=default_run_id)),
                "ts_utc": flatten_value(advisory_value(advisory_row, "ts_utc", "generated_at", "as_of_ts", default=NOW.strftime("%Y-%m-%dT%H:%M:%SZ"))),
                "current_regime": flatten_value(advisory_value(advisory_row, "current_regime", "regime_state", "market_regime", "risk_regime", default=not_obs)),
                "top_k": top_k if top_k is not None else not_obs,
                "selected_symbols": risk_on_symbols_csv_value(selected_symbols),
                "would_buy_symbols": risk_on_symbols_csv_value(would_buy_symbols),
                "actual_bought_symbols": json.dumps(actual_bought_symbols, ensure_ascii=False),
                "missed_symbols": json.dumps(missed_symbols, ensure_ascii=False),
                "source_detail_available": str(detail_row is not None).lower(),
                "response_action": "shadow_tracking",
                "live_order_effect": "read_only_no_live_order",
            })
    else:
        risk_on_rows = existing_risk_on_rows
        for row in risk_on_rows:
            row.setdefault("source_detail_available", "false")
            selected_symbols = risk_on_symbol_list(row.get("selected_symbols"))
            if not selected_symbols and flatten_value(row.get("selected_symbols")) != not_obs:
                row["selected_symbols"] = not_obs
            would_buy_symbols = risk_on_symbol_list(row.get("would_buy_symbols"))
            if not would_buy_symbols and flatten_value(row.get("would_buy_symbols")) != not_obs:
                row["would_buy_symbols"] = not_obs
    if risk_on_rows:
        write_csv("summaries/risk_on_multi_buy_shadow.csv", risk_on_rows, RISK_ON_MULTI_BUY_SHADOW_FIELDS)

    risk_on_latest_selected_symbols = not_obs
    risk_on_source_detail_available = False
    if risk_on_rows:
        latest_ts = max(flatten_value(row.get("ts_utc")) for row in risk_on_rows)
        latest_rows = [row for row in risk_on_rows if flatten_value(row.get("ts_utc")) == latest_ts]
        latest_payload = {
            flatten_value(row.get("top_k")) or str(index + 1): flatten_value(row.get("selected_symbols")) or not_obs
            for index, row in enumerate(latest_rows)
        }
        risk_on_latest_selected_symbols = json.dumps(latest_payload, ensure_ascii=False, sort_keys=True)
        risk_on_source_detail_available = any(truthy_text(row.get("source_detail_available")) for row in latest_rows)

    high_count = sum(1 for item in issues if item.get("severity") == "high")
    medium_count = sum(1 for item in issues if item.get("severity") == "medium")
    warning_count = sum(1 for item in issues if item.get("severity") == "warning")
    latest_24h_trade_count = sum(
        1 for event in raw_trade_events
        if event.get("ts_dt") is not None and event["ts_dt"].timestamp() >= RECENT_24H
    )

    def trade_row_dt(row):
        return parse_dt_utc(first_observed(row.get("exit_ts"), row.get("timestamp"), row.get("entry_ts")))

    latest_24h_roundtrip_count = sum(
        1 for row in closed_roundtrip_rows
        if (trade_row_dt(row) is not None and trade_row_dt(row).timestamp() >= RECENT_24H)
    )
    last_72h_trade_count = raw_trade_file_rows
    last_72h_roundtrip_count = len(closed_roundtrip_rows)
    gross_values = [as_float(row.get("gross_bps")) for row in lifecycle_rows]
    net_values = [as_float(row.get("net_bps")) for row in lifecycle_rows]
    gross_values = [v for v in gross_values if v is not None]
    net_values = [v for v in net_values if v is not None]
    closed_gross_values = [as_float(row.get("gross_bps")) for row in closed_roundtrip_rows]
    closed_net_values = [as_float(row.get("net_bps")) for row in closed_roundtrip_rows]
    closed_gross_values = [v for v in closed_gross_values if v is not None]
    closed_net_values = [v for v in closed_net_values if v is not None]
    probe_exit_count = sum(probe_counts[field] for field in ("probe_take_profit_count", "probe_stop_loss_count", "probe_trailing_stop_count", "probe_time_stop_count"))
    probe_exit_count += sum(1 for row in lifecycle_rows if row.get("exit_reason") in PROBE_EXIT_REASONS)
    dust_only_count = sum(1 for row in dust_rows if row.get("reason") == "dust_residual_no_close_order")
    stale_state_issues = sum(1 for item in issues if item.get("code") == "probe_closed_but_active_state_remains")
    repeated_exit_issues = sum(1 for item in issues if item.get("code") == "repeated_probe_exit_signal_after_flat_dust_only")
    open_net_values = [as_float(row.get("unrealized_net_bps")) for row in open_position_rows]
    open_net_values = [value for value in open_net_values if value is not None]
    open_probe_net_values = [as_float(row.get("unrealized_net_bps")) for row in open_probe_watch_rows]
    open_probe_net_values = [value for value in open_probe_net_values if value is not None]
    dust_residual_position_count = len(dust_residual_position_keys)
    dust_residual_roundtrip_count = len(dust_residual_roundtrip_rows)
    effective_open_position_count = len(open_position_rows)
    active_probe_ignore_zero_target_close_total = sum(
        as_int(row.get("active_probe_ignore_zero_target_close_count"))
        for row in open_probe_watch_rows
    )
    active_probe_past_time_stop_count = sum(
        1 for row in open_probe_watch_rows
        if row.get("diagnosis") == "medium_issue_active_probe_past_time_stop"
    )
    active_probe_zero_target_close_issue_count = sum(
        1 for item in issues
        if item.get("code") == "active_probe_closed_by_zero_target_close"
    )
    negative_expectancy_mismatch_count = sum(1 for row in negative_consistency_rows if row.get("mismatch_suspected") == "true")
    rank_exit_conflict_count = sum(1 for row in rank_exit_consistency_rows if row.get("conflict_suspected") == "true")
    rank_exit_target_positive_sell_count = sum(1 for row in rank_exit_consistency_rows if row.get("target_positive") == "true" or row.get("has_target_still_positive_note") == "true")
    protect_sideways_win_rate = (
        sum(1 for value in protect_sideways_net_values if value > 0) / len(protect_sideways_net_values)
        if protect_sideways_net_values
        else None
    )
    protect_sideways_medium_issue_present = any(item.get("code") == "protect_sideways_normal_entry_negative" for item in issues)
    swing_early_exit_count = len(swing_early_exit_sample_rows)
    swing_early_exit_atr_trailing_count = sum(
        1 for row in swing_early_exit_sample_rows
        if flatten_value(row.get("exit_reason")).strip().lower() == "atr_trailing"
    )
    swing_early_exit_medium_issue_present = any(item.get("code") == "swing_early_exit_premature" for item in issues)
    swing_post_fix_early_exit_count = len(swing_post_fix_early_exit_sample_rows)
    swing_historical_or_unknown_early_exit_count = len(swing_historical_or_unknown_early_exit_rows)
    swing_early_exit_historical_or_unknown_issue_present = any(
        item.get("code") == "swing_soft_exit_before_min_hold_historical_or_unknown"
        for item in issues
    )
    post_min_hold_atr_exit_count = len(post_min_hold_atr_exit_rows)
    post_min_hold_atr_better_6_count = sum(
        1 for row in post_min_hold_atr_exit_rows
        if row.get("would_have_been_better_6h") == "true"
    )
    post_min_hold_atr_better_24_rows = [
        row for row in post_min_hold_atr_exit_rows
        if row.get("would_have_been_better_24h") in {"true", "false"}
    ]
    post_min_hold_atr_better_24_count = sum(
        1 for row in post_min_hold_atr_better_24_rows
        if row.get("would_have_been_better_24h") == "true"
    )
    post_min_hold_atr_better_6_text = (
        f"yes / {post_min_hold_atr_better_6_count}"
        if post_min_hold_atr_better_6_count
        else "no"
    )
    post_min_hold_atr_better_12_text = (
        f"{fmt_num(post_min_hold_atr_better_12_rate, 6)} ({post_min_hold_atr_better_12_count}/{len(post_min_hold_atr_better_12_rows)})"
        if post_min_hold_atr_better_12_rate is not None
        else not_obs
    )
    post_min_hold_atr_better_24_text = (
        f"{fmt_num(post_min_hold_atr_better_24_count / len(post_min_hold_atr_better_24_rows), 6)} ({post_min_hold_atr_better_24_count}/{len(post_min_hold_atr_better_24_rows)})"
        if post_min_hold_atr_better_24_rows
        else not_obs
    )
    post_min_hold_atr_by_symbol_text = (
        "; ".join(
            f"{row.get('symbol')}: count={row.get('sample_count')}, better_12h_rate={row.get('better_to_hold_12h_rate')}, avg_realized_net_bps={row.get('avg_realized_net_bps')}"
            for row in post_min_hold_atr_exit_outcomes_by_symbol[:12]
        )
        if post_min_hold_atr_exit_outcomes_by_symbol
        else not_obs
    )
    post_min_hold_atr_medium_issue_present = any(
        item.get("code") == "post_min_hold_atr_exit_may_be_premature"
        for item in issues
    )
    bnb_profit_lock_shadow_sample_count = len(bnb_profit_lock_shadow_rows)
    bnb_profit_lock_shadow_sample_gate_met = bnb_profit_lock_shadow_sample_count >= 10
    bnb_profit_lock_shadow_best_policy_mix = dict(sorted(Counter(
        row.get("best_shadow_exit_policy") or not_obs
        for row in bnb_profit_lock_shadow_rows
    ).items()))
    bnb_profit_lock_shadow_latest = (
        sorted(
            bnb_profit_lock_shadow_rows,
            key=lambda row: parse_dt_utc(row.get("exit_ts")) or dt.datetime.min.replace(tzinfo=dt.timezone.utc),
            reverse=True,
        )[0]
        if bnb_profit_lock_shadow_rows
        else {}
    )
    high_score_block_category_counts = dict(sorted(Counter(row.get("high_score_block_category") or not_obs for row in high_score_blocked_rows).items()))
    high_score_recent_24h_rows = [
        row for row in high_score_blocked_rows
        if parse_dt_utc(row.get("ts_utc")) is not None and parse_dt_utc(row.get("ts_utc")).timestamp() >= RECENT_24H
    ]
    multi_position_swing_status_counts = Counter(row.get("label_status") or not_obs for row in multi_position_swing_shadow_rows)
    protect_sol_exception_status_counts = Counter(row.get("label_status") or not_obs for row in protect_sol_exception_shadow_rows)
    protect_sol_exception_sample_warning_count = sum(
        1 for row in protect_sol_exception_shadow_by_horizon
        if flatten_value(row.get("sample_warning"))
    )
    def quant_lab_cost_row_degraded(row):
        cost_source = flatten_value(first_observed(row.get("cost_source"), row.get("source"), not_obs)).strip().lower()
        fallback_level = flatten_value(row.get("fallback_level") or "").strip().upper()
        cost_model_version_value = flatten_value(row.get("cost_model_version") or "").strip().lower()
        return (
            truthy_observed(row.get("degraded_cost_model"))
            or cost_source == "global_default"
            or fallback_level == "GLOBAL_DEFAULT"
            or cost_model_version_value == "global_default_v0"
        )

    def quant_lab_cost_row_global_default(row):
        cost_source = flatten_value(first_observed(row.get("cost_source"), row.get("source"), not_obs)).strip().lower()
        fallback_level = flatten_value(row.get("fallback_level") or "").strip().upper()
        cost_model_version_value = flatten_value(row.get("cost_model_version") or "").strip().lower()
        return (
            cost_source == "global_default"
            or fallback_level == "GLOBAL_DEFAULT"
            or cost_model_version_value == "global_default_v0"
        )

    def quant_lab_cost_row_current_contract(row):
        schema_version = flatten_value(row.get("schema_version") or "").strip()
        contract_version = flatten_value(first_observed(row.get("cost_contract_version"), row.get("contract_version"), "")).strip()
        event_generation = flatten_value(row.get("event_id_generation_version") or "").strip()
        return (
            schema_version == QUANT_LAB_SCHEMA_VERSION
            and contract_version == QUANT_LAB_CONTRACT_VERSION
            and event_generation == QUANT_LAB_EVENT_ID_GENERATION_VERSION
        )

    def quant_lab_cost_row_source_hash(row):
        value = flatten_value(first_observed(
            row.get("source_snapshot_hash"),
            row.get("deployment_source_snapshot_hash"),
            row.get("source_generation_hash"),
            "",
        )).strip()
        return "" if value in ("", not_obs, "null") else value

    def quant_lab_symbol_cost_hit(row):
        if quant_lab_cost_row_degraded(row):
            return False
        normalized = flatten_value(row.get("normalized_symbol") or "").strip().upper()
        response_symbol = flatten_value(first_observed(row.get("response_symbol"), row.get("symbol"), "")).strip().upper()
        if normalized and response_symbol and normalized != response_symbol:
            return False
        sample_count = as_float(row.get("sample_count"))
        if sample_count is not None and sample_count <= 0:
            return False
        return bool(normalized or response_symbol)

    current_contract_cost_rows = [row for row in quant_lab_cost_usage_rows if quant_lab_cost_row_current_contract(row)]
    legacy_cost_rows = [row for row in quant_lab_cost_usage_rows if not quant_lab_cost_row_current_contract(row)]
    latest_24h_cost_rows = [
        row for row in quant_lab_cost_usage_rows
        if parse_dt_utc(row.get("ts_utc")) is not None and parse_dt_utc(row.get("ts_utc")).timestamp() >= RECENT_24H
    ]
    current_source_hash = flatten_value(provenance_meta.get("source_snapshot_hash") or not_obs)
    current_source_hash_observable = current_source_hash not in ("", not_obs, "null")
    current_rows_with_hash = [row for row in current_contract_cost_rows if quant_lab_cost_row_source_hash(row)]
    if current_source_hash_observable and current_rows_with_hash:
        post_deployment_cost_rows = [
            row for row in current_contract_cost_rows
            if quant_lab_cost_row_source_hash(row) == current_source_hash
        ]
        post_deployment_scope = "source_snapshot_hash"
    else:
        post_deployment_cost_rows = current_contract_cost_rows
        post_deployment_scope = "current_contract_schema_event_generation"
    post_deployment_ts_values = [
        parse_dt_utc(row.get("ts_utc")) for row in post_deployment_cost_rows
        if parse_dt_utc(row.get("ts_utc")) is not None
    ]
    post_deployment_start_utc = (
        min(post_deployment_ts_values).strftime("%Y-%m-%dT%H:%M:%SZ")
        if post_deployment_ts_values else not_obs
    )
    cost_degraded_count = sum(1 for row in quant_lab_cost_usage_rows if quant_lab_cost_row_degraded(row))
    current_contract_cost_degraded_count = sum(1 for row in current_contract_cost_rows if quant_lab_cost_row_degraded(row))
    latest_24h_cost_degraded_count = sum(1 for row in latest_24h_cost_rows if quant_lab_cost_row_degraded(row))
    post_deployment_cost_degraded_count = sum(1 for row in post_deployment_cost_rows if quant_lab_cost_row_degraded(row))
    global_default_cost_count = sum(1 for row in quant_lab_cost_usage_rows if quant_lab_cost_row_global_default(row))
    legacy_global_default_cost_count = sum(1 for row in legacy_cost_rows if quant_lab_cost_row_global_default(row))
    current_contract_global_default_cost_count = sum(1 for row in current_contract_cost_rows if quant_lab_cost_row_global_default(row))
    latest_24h_global_default_cost_count = sum(1 for row in latest_24h_cost_rows if quant_lab_cost_row_global_default(row))
    post_deployment_global_default_cost_count = sum(1 for row in post_deployment_cost_rows if quant_lab_cost_row_global_default(row))
    symbol_cost_hit_count = sum(1 for row in quant_lab_cost_usage_rows if quant_lab_symbol_cost_hit(row))
    cost_contract_version = next(
        (
            flatten_value(first_observed(row.get("cost_contract_version"), row.get("contract_version"), ""))
            for row in reversed(quant_lab_cost_usage_rows)
            if first_observed(row.get("cost_contract_version"), row.get("contract_version"), "") not in (None, "")
        ),
        QUANT_LAB_CONTRACT_VERSION,
    )
    def quant_lab_permission_status_stale(row):
        status = flatten_value(first_observed(row.get("remote_permission_status"), row.get("raw_permission_status"), "")).strip().upper()
        return status.startswith("STALE") or status.startswith("EXPIRED") or status == "NO_FRESH_PERMISSION"

    would_block_if_enforced_count = sum(
        1 for row in quant_lab_permission_audit_rows
        if row.get("event_type") in {"filter_order", "order_filter"} and truthy_observed(row.get("would_block_if_enforced"))
    )
    if would_block_if_enforced_count == 0:
        would_block_if_enforced_count = sum(
            1 for row in quant_lab_permission_audit_rows
            if truthy_observed(row.get("would_block_if_enforced"))
        )
    effective_block_count = sum(
        1 for row in quant_lab_permission_audit_rows
        if truthy_observed(row.get("permission_gate_enforced")) and truthy_observed(row.get("filtered"))
    )
    permission_contract_violation_count = sum(
        1 for row in quant_lab_permission_audit_rows
        if truthy_observed(row.get("permission_contract_violation"))
    )
    stale_permission_count = sum(1 for row in quant_lab_permission_audit_rows if quant_lab_permission_status_stale(row))
    latest_quant_lab_mode_row = next(
        (row for row in reversed(quant_lab_mode_audit_rows) if row.get("mode") not in (None, "", not_obs)),
        quant_lab_mode_audit_rows[-1] if quant_lab_mode_audit_rows else {},
    )
    quant_lab_shadow_executed_count = sum(1 for row in quant_lab_shadow_outcome_rows if row.get("actual_executed") == "true")
    quant_lab_shadow_profitable_blocked_count = sum(
        1 for row in quant_lab_shadow_outcome_rows
        if row.get("outcome_bucket") == "profitable_blocked_by_shadow"
    )
    quant_lab_shadow_net_values = [
        value for value in (as_float(row.get("net_bps")) for row in quant_lab_shadow_outcome_rows)
        if value is not None
    ]
    quant_lab_shadow_net_pnl_values = [
        value for value in (as_float(row.get("net_pnl_usdt")) for row in quant_lab_shadow_outcome_rows)
        if value is not None
    ]
    quant_lab_shadow_avg_net_bps = (
        sum(quant_lab_shadow_net_values) / len(quant_lab_shadow_net_values)
        if quant_lab_shadow_net_values else None
    )
    quant_lab_shadow_win_rate = (
        sum(1 for value in quant_lab_shadow_net_values if value > 0) / len(quant_lab_shadow_net_values)
        if quant_lab_shadow_net_values else None
    )
    quant_lab_shadow_net_pnl_sum_usdt = (
        sum(quant_lab_shadow_net_pnl_values) if quant_lab_shadow_net_pnl_values else None
    )

    effective_config_for_ml = load_json(OUT / "raw" / "reports" / "effective_live_config.json")
    if not isinstance(effective_config_for_ml, dict):
        effective_config_for_ml = {}

    def nested_value(obj, *path):
        current = obj
        for key in path:
            if not isinstance(current, dict) or key not in current:
                return not_obs
            current = current.get(key)
        return current if current not in (None, "") else not_obs

    ml_factor_enabled = bool_text(
        first_observed(
            nested_value(effective_config_for_ml, "ml_factor_enabled"),
            nested_value(effective_config_for_ml, "alpha", "ml_factor_enabled"),
            nested_value(effective_config_for_ml, "alpha", "ml_factor", "enabled"),
            not_obs,
        )
    )
    collect_ml_training_data = bool_text(
        first_observed(
            nested_value(effective_config_for_ml, "collect_ml_training_data"),
            nested_value(effective_config_for_ml, "execution", "collect_ml_training_data"),
            not_obs,
        )
    )
    ml_research_use_stable_universe = bool_text(
        first_observed(
            nested_value(effective_config_for_ml, "ml_research_use_stable_universe"),
            nested_value(effective_config_for_ml, "execution", "ml_research_use_stable_universe"),
            not_obs,
        )
    )
    latest_ml_signal_overview = {}
    for audit in reversed(recent_24_decisions or []):
        overview = audit.get("ml_signal_overview") if isinstance(audit, dict) else None
        if isinstance(overview, dict) and overview:
            latest_ml_signal_overview = overview
            break
    if ml_factor_enabled == "false":
        ml_live_overlay_status = "disabled_in_live_prod"
    elif latest_ml_signal_overview:
        if bool_text(latest_ml_signal_overview.get("live_active")) == "true":
            ml_live_overlay_status = "active"
        else:
            ml_live_overlay_status = flatten_value(
                first_observed(
                    latest_ml_signal_overview.get("reason"),
                    latest_ml_signal_overview.get("overlay_mode"),
                    not_obs,
                )
            )
    else:
        ml_live_overlay_status = "not_observable"

    advisory_source_health_rows = load_csv_dicts(OUT / "summaries" / "strategy_opportunity_advisory_source_health.csv")
    advisory_source_health = advisory_source_health_rows[-1] if advisory_source_health_rows else {}

    data_quality_warnings = []
    if provenance_meta.get("code_provenance") == "degraded":
        data_quality_warnings.append(f"dirty_worktree: {provenance_meta.get('provenance_status', not_obs)}")

    def live_guard_would_block(row):
        return (
            truthy_observed(row.get("would_be_blocked_by_quant_lab_no_live_modes"))
            or truthy_observed(row.get("would_be_blocked_by_cost_trust_guard"))
            or truthy_observed(row.get("would_be_blocked_by_shadow_live_whitelist"))
        )

    live_guard_would_block_rows = [row for row in live_guard_impact_rows if live_guard_would_block(row)]
    live_guard_would_block_strategy_mix = Counter()
    live_guard_would_block_symbol_mix = Counter()
    for row in live_guard_impact_rows:
        if not live_guard_would_block(row):
            continue
        live_guard_would_block_strategy_mix[flatten_value(row.get("strategy_candidate") or not_obs)] += 1
        live_guard_would_block_symbol_mix[flatten_value(row.get("symbol") or not_obs)] += 1
    profitable_open_symbols = {
        flatten_value(row.get("symbol") or "")
        for row in open_position_rows
        if as_float(row.get("unrealized_net_bps")) is not None and as_float(row.get("unrealized_net_bps")) > 0
    }
    live_guard_profitable_blocked_keys = {
        (
            flatten_value(row.get("symbol") or not_obs),
            flatten_value(row.get("strategy_candidate") or not_obs),
        )
        for row in live_guard_would_block_rows
        if flatten_value(row.get("symbol") or "") in profitable_open_symbols
    }

    window_summary = {
        "sampled_at_utc": NOW.isoformat(),
        "window_hours": 72,
        "last_72h_start_utc": WINDOW_72H_START.isoformat(),
        "last_72h_end_utc": WINDOW_72H_END.isoformat(),
        "remote_root": str(ROOT),
        "run_count": len(copied_runs),
        "recent_24h_decision_audit_count": len(recent_24_decisions),
        "log_file_count": len(copied_logs),
        "candidate_snapshot_rows": len(candidate_snapshot_rows),
        "candidate_cost_source_coverage": candidate_cost_source_coverage_value,
        "router_decision_rows": len(router_rows),
        "has_trade_data": has_trade_data,
        "trade_observation_status": trade_observation_status,
        "trade_read_error_count": trade_read_errors,
        "raw_trade_rows": raw_trade_file_rows,
        "trade_rows": len(trade_rows),
        "ml_live_overlay_status": ml_live_overlay_status,
        "ml_factor_enabled": ml_factor_enabled,
        "collect_ml_training_data": collect_ml_training_data,
        "ml_research_use_stable_universe": ml_research_use_stable_universe,
        "latest_24h_trade_count": latest_24h_trade_count if has_trade_data else not_obs,
        "latest_24h_roundtrip_count": latest_24h_roundtrip_count if has_trade_data else not_obs,
        "last_72h_trade_count": last_72h_trade_count if has_trade_data else not_obs,
        "last_72h_roundtrip_count": last_72h_roundtrip_count if has_trade_data else not_obs,
        "open_position_count": len(open_position_rows),
        "effective_open_position_count": effective_open_position_count,
        "dust_residual_position_count": dust_residual_position_count,
        "dust_residual_roundtrip_count": dust_residual_roundtrip_count,
        "dust_threshold_usdt": global_dust_threshold_usdt,
        "summary_trade_count_mismatch_count": len(summary_trade_count_mismatch_rows),
        "summary_trade_count_mismatch_high_issue_count": sum(
            1 for row in summary_trade_count_mismatch_rows
            if str(row.get("diagnosis") or "").startswith("high_issue")
        ),
        "run_summary_invalid": any(
            str(row.get("diagnosis") or "").startswith("high_issue")
            for row in summary_trade_count_mismatch_rows
        ),
        "trade_metrics_rows": len(trade_metrics_rows),
        "fill_metrics_rows": len(fill_metrics_rows),
        "order_lifecycle_rows": len(order_lifecycle_rows),
        "order_lifecycle_trade_metric_fill_count": order_lifecycle_trade_metric_fill_count,
        "order_lifecycle_missing_high_issue": order_lifecycle_missing_high_issue,
        "data_quality_warnings": data_quality_warnings,
        "negative_expectancy_consistency_rows": len(negative_consistency_rows),
        "negative_expectancy_mismatch_count": negative_expectancy_mismatch_count,
        "bnb_risk_recommendation": bnb_risk_summary.get("recommendation", not_obs),
        "bnb_negative_expectancy_closed_cycles": bnb_risk_summary.get("closed_cycles", not_obs),
        "bnb_negative_expectancy_bps": bnb_risk_summary.get("net_expectancy_bps", not_obs),
        "bnb_fast_fail_net_expectancy_bps": bnb_risk_summary.get("fast_fail_net_expectancy_bps", not_obs),
        "bnb_latest_roundtrip_net_bps": bnb_risk_summary.get("latest_roundtrip_net_bps", not_obs),
        "bnb_latest_roundtrip_if_held_current_net_bps": bnb_risk_summary.get("latest_roundtrip_if_held_current_net_bps", not_obs),
        "bnb_protect_alt_short_cycle_guard_active": bnb_risk_summary.get("protect_alt_short_cycle_guard_active", False),
        "config_runtime_consumption_rows": len(config_runtime_consumption_rows),
        "config_runtime_not_consumed_count": config_runtime_not_consumed_count,
        "split_order_runtime_active": False,
        "quant_lab_compliance_rows": len(quant_lab_compliance_rows),
        "quant_lab_permission_audit_rows": len(quant_lab_permission_audit_rows),
        "quant_lab_mode_audit_rows": len(quant_lab_mode_audit_rows),
        "quant_lab_mode": latest_quant_lab_mode_row.get("mode", not_obs),
        "quant_lab_mode_source": latest_quant_lab_mode_row.get("mode_source", not_obs),
        "quant_lab_requested_mode": latest_quant_lab_mode_row.get("quant_lab_requested_mode", latest_quant_lab_mode_row.get("mode", not_obs)),
        "quant_lab_effective_mode": latest_quant_lab_mode_row.get("quant_lab_effective_mode", latest_quant_lab_mode_row.get("mode", not_obs)),
        "enforce_readiness_status": latest_quant_lab_mode_row.get("enforce_readiness_status", not_obs),
        "enforce_blocked_reasons": latest_quant_lab_mode_row.get("enforce_blocked_reasons", not_obs),
        "enforce_blocked_reason": latest_quant_lab_mode_row.get("enforce_blocked_reason", not_obs),
        "contract_version_match": latest_quant_lab_mode_row.get("contract_version_match", not_obs),
        "telemetry_schema_version_match": latest_quant_lab_mode_row.get("telemetry_schema_version_match", not_obs),
        "permission_contract_violation_count": permission_contract_violation_count,
        "stale_permission_count": stale_permission_count,
        "would_block_if_enforced_count": would_block_if_enforced_count,
        "effective_block_count": effective_block_count,
        "quant_lab_cost_usage_rows": len(quant_lab_cost_usage_rows),
        "quant_lab_fallback_rows": len(quant_lab_fallback_rows),
        "quant_lab_shadow_outcome_rows": len(quant_lab_shadow_outcome_rows),
        "quant_lab_shadow_would_block_count": len(quant_lab_shadow_outcome_rows),
        "quant_lab_shadow_executed_count": quant_lab_shadow_executed_count,
        "quant_lab_shadow_profitable_blocked_count": quant_lab_shadow_profitable_blocked_count,
        "quant_lab_shadow_avg_net_bps": fmt_num(quant_lab_shadow_avg_net_bps, 6) if quant_lab_shadow_avg_net_bps is not None else not_obs,
        "quant_lab_shadow_win_rate": fmt_num(quant_lab_shadow_win_rate, 6) if quant_lab_shadow_win_rate is not None else not_obs,
        "quant_lab_shadow_net_pnl_sum_usdt": fmt_num(quant_lab_shadow_net_pnl_sum_usdt, 12) if quant_lab_shadow_net_pnl_sum_usdt is not None else not_obs,
        "quant_lab_request_success_count": quant_lab_request_success_count,
        "quant_lab_request_error_count": quant_lab_request_error_count,
        "quant_lab_actual_fallback_count": len(quant_lab_fallback_rows),
        "quant_lab_fallback_count": len(quant_lab_fallback_rows),
        "live_guard_would_block_count": len(live_guard_would_block_rows),
        "would_block_count": len(live_guard_would_block_rows),
        "live_guard_actual_block_count": 0,
        "would_block_but_profitable_open_positions_count": len(live_guard_profitable_blocked_keys),
        "would_block_strategy_mix": dict(live_guard_would_block_strategy_mix),
        "would_block_symbol_mix": dict(live_guard_would_block_symbol_mix),
        "guard_enforced": False,
        "cost_usage_legacy_rows": len(legacy_cost_rows),
        "cost_usage_current_contract_rows": len(current_contract_cost_rows),
        "cost_usage_latest_24h_rows": len(latest_24h_cost_rows),
        "post_deployment_cost_usage_rows": len(post_deployment_cost_rows),
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
        "readiness_cost_usage_rows": len(post_deployment_cost_rows),
        "readiness_cost_degraded_count": post_deployment_cost_degraded_count,
        "readiness_global_default_cost_count": post_deployment_global_default_cost_count,
        "cost_usage_post_deployment_scope": post_deployment_scope,
        "cost_usage_current_source_snapshot_hash": current_source_hash,
        "post_deployment_cost_usage_start_utc": post_deployment_start_utc,
        "entry_quality_advisory_rows": len(entry_quality_advisory_rows),
        "entry_quality_available": bool(entry_quality_available),
        "entry_quality_status": "available" if entry_quality_available else "quant_lab_entry_quality_unavailable",
        "strategy_advisory_selected_source": advisory_source_health.get("selected_source", not_obs) if 'advisory_source_health' in locals() else not_obs,
        "strategy_advisory_local_row_count": advisory_source_health.get("local_row_count", not_obs) if 'advisory_source_health' in locals() else not_obs,
        "strategy_advisory_api_row_count": advisory_source_health.get("api_row_count", not_obs) if 'advisory_source_health' in locals() else not_obs,
        "strategy_advisory_selected_row_count": advisory_source_health.get("selected_row_count", not_obs) if 'advisory_source_health' in locals() else not_obs,
        "strategy_advisory_source_lag_sec": advisory_source_health.get("advisory_source_lag_sec", not_obs) if 'advisory_source_health' in locals() else not_obs,
        "entry_quality_strategy_advisory_count": len(entry_quality_strategy_rows),
        "entry_quality_would_block_if_enabled_count": entry_quality_would_block_count,
        "entry_quality_would_enter_count": entry_quality_would_enter_count,
        "entry_quality_no_sample_reasons": entry_quality_no_sample_reasons,
        "missed_low_late_chase_loss_count": late_chase_loss_count,
        "late_entry_chase_ready_for_live_guard": late_entry_chase_ready_for_live_guard,
        "pullback_reversal_ready_for_paper": pullback_reversal_ready_for_paper,
        "pullback_reversal_ready_for_live_probe": pullback_reversal_ready_for_live_probe,
        "late_entry_chase_guard_enabled": bool(late_entry_chase_guard_enabled),
        "pullback_reversal_live_enabled": bool(pullback_reversal_live_enabled),
        "risk_on_multi_buy_shadow_rows": len(risk_on_rows),
        "risk_on_multi_buy_latest_selected_symbols": risk_on_latest_selected_symbols,
        "risk_on_multi_buy_source_detail_available": bool(risk_on_source_detail_available),
        "telemetry_contract_version": QUANT_LAB_CONTRACT_VERSION,
        "telemetry_schema_version": QUANT_LAB_SCHEMA_VERSION,
        "rank_exit_sell_count": len(rank_exit_consistency_rows),
        "rank_exit_conflict_count": rank_exit_conflict_count,
        "rank_exit_target_positive_sell_count": rank_exit_target_positive_sell_count,
        "early_exit_case_count": len(early_exit_rows),
        "protect_sideways_normal_entry_count": len(protect_sideways_normal_entry_rows),
        "protect_sideways_normal_entry_avg_net_bps": protect_sideways_avg_net_bps if protect_sideways_avg_net_bps is not None else not_obs,
        "protect_sideways_normal_entry_win_rate": protect_sideways_win_rate if protect_sideways_win_rate is not None else not_obs,
        "protect_sideways_normal_entry_medium_issue": bool(protect_sideways_medium_issue_present),
        "swing_early_exit_audit_rows": len(swing_early_exit_rows),
        "swing_early_exit_count": swing_early_exit_count,
        "swing_post_fix_early_exit_count": swing_post_fix_early_exit_count,
        "swing_historical_or_unknown_early_exit_count": swing_historical_or_unknown_early_exit_count,
        "swing_blocked_by_min_hold_count": swing_blocked_by_min_hold_count,
        "swing_filled_soft_exit_before_min_hold_count": swing_filled_soft_exit_before_min_hold_count,
        "swing_early_exit_atr_trailing_count": swing_early_exit_atr_trailing_count,
        "swing_early_exit_better_to_hold_24h_rate": swing_early_exit_better_24_rate if swing_early_exit_better_24_rate is not None else not_obs,
        "swing_early_exit_medium_issue": bool(swing_early_exit_medium_issue_present),
        "swing_early_exit_historical_or_unknown_issue": bool(swing_early_exit_historical_or_unknown_issue_present),
        "post_min_hold_atr_exit_audit_rows": len(post_min_hold_atr_exit_rows),
        "post_min_hold_atr_exit_count": post_min_hold_atr_exit_count,
        "post_min_hold_atr_better_to_hold_12h_rate": post_min_hold_atr_better_12_rate if post_min_hold_atr_better_12_rate is not None else not_obs,
        "post_min_hold_atr_better_to_hold_12h_count": post_min_hold_atr_better_12_count,
        "post_min_hold_atr_observable_12h_count": len(post_min_hold_atr_better_12_rows),
        "post_min_hold_atr_medium_issue": bool(post_min_hold_atr_medium_issue_present),
        "swing_atr_soft_exit_ready_for_live_guard": swing_atr_soft_exit_readiness.get("ready_for_live_guard", False),
        "swing_atr_soft_exit_readiness_blocking_reasons": swing_atr_soft_exit_readiness.get("blocking_reasons", []),
        "swing_atr_soft_exit_readiness_improvement_bps": swing_atr_soft_exit_readiness.get("improvement_bps", not_obs),
        "swing_atr_soft_exit_shadow_rows": len(swing_atr_soft_exit_shadow_rows),
        "swing_atr_soft_exit_shadow_would_delay_count": sum(
            1 for row in swing_atr_soft_exit_shadow_rows
            if row.get("would_delay_exit_if_enabled") == "true"
        ),
        "bnb_profit_lock_shadow_rows": bnb_profit_lock_shadow_sample_count,
        "bnb_profit_lock_shadow_sample_gate_met": bnb_profit_lock_shadow_sample_gate_met,
        "bnb_profit_lock_shadow_best_policy_mix": bnb_profit_lock_shadow_best_policy_mix,
        "bnb_profit_lock_shadow_latest_best_policy": bnb_profit_lock_shadow_latest.get("best_shadow_exit_policy", not_obs),
        "high_score_blocked_target_count": len(high_score_blocked_rows),
        "high_score_blocked_labelable_target_count": len(high_score_labelable_rows),
        "high_score_blocked_non_entry_management_count": len(high_score_non_entry_management_rows),
        "high_score_blocked_recent_24h_target_count": len(high_score_recent_24h_rows),
        "high_score_block_category_counts": high_score_block_category_counts,
        "high_score_blocked_outcome_count": len(high_score_blocked_outcome_rows),
        "high_score_blocked_pending_count": high_score_pending_count,
        "high_score_blocked_matured_unlabeled_count": high_score_matured_unlabeled_count,
        "alt_impulse_shadow_label_count": len(alt_impulse_shadow_rows),
        "alt_impulse_shadow_duplicate_count": alt_impulse_shadow_duplicate_count,
        "alt_impulse_shadow_entry_px_not_observable_count": alt_impulse_shadow_entry_px_not_observable_count,
        "alt_impulse_shadow_matured_horizon_count": alt_impulse_shadow_matured_horizon_count,
        "alt_impulse_shadow_missing_future_px_count": alt_impulse_shadow_missing_future_px_count,
        "alt_impulse_shadow_ready_for_live_probe": bool(alt_impulse_shadow_readiness.get("ready_for_live_probe")),
        "alt_impulse_shadow_ready_symbols": list(alt_impulse_shadow_readiness.get("ready_symbols") or []),
        "alt_impulse_shadow_readiness_blocking_reasons": list(alt_impulse_shadow_readiness.get("blocking_reasons") or []),
        "alt_impulse_shadow_recent_sample_count": alt_impulse_shadow_readiness.get("recent_sample_count", 0),
        "multi_position_swing_shadow_label_count": len(multi_position_swing_shadow_rows),
        "multi_position_swing_shadow_duplicate_count": multi_position_swing_shadow_duplicate_count,
        "multi_position_swing_shadow_debug_count": len(multi_position_swing_shadow_debug_rows),
        "multi_position_swing_shadow_complete_count": int(multi_position_swing_status_counts.get("complete", 0)),
        "multi_position_swing_shadow_pending_count": int(multi_position_swing_status_counts.get("pending", 0)),
        "multi_position_swing_shadow_not_observable_count": int(multi_position_swing_status_counts.get("not_observable", 0)),
        "protect_sol_exception_shadow_label_count": len(protect_sol_exception_shadow_rows),
        "protect_sol_exception_shadow_heartbeat_count": len(protect_sol_exception_shadow_heartbeat_rows),
        "protect_sol_exception_shadow_no_sample_reasons": dict(
            sorted(
                Counter(
                    flatten_value(first_value(row, ("no_sample_reason", "original_block_reason"), not_obs)) or not_obs
                    for row in protect_sol_exception_shadow_heartbeat_rows
                ).items()
            )
        ),
        "protect_sol_exception_shadow_duplicate_count": protect_sol_exception_shadow_duplicate_count,
        "protect_sol_exception_shadow_complete_count": int(protect_sol_exception_status_counts.get("complete", 0)),
        "protect_sol_exception_shadow_pending_count": int(protect_sol_exception_status_counts.get("pending", 0)),
        "protect_sol_exception_shadow_not_observable_count": int(protect_sol_exception_status_counts.get("not_observable", 0)),
        "protect_sol_exception_shadow_sample_warning_count": protect_sol_exception_sample_warning_count,
        "market_impulse_selection_shadow_rows": len(market_impulse_selection_shadow_rows),
        "factor_contribution_audit_rows": len(factor_contribution_rows),
        "factor_contribution_factor_count": len(factor_contribution_outcomes_by_factor),
        "f3_dominant_count": f3_dominant_count,
        "f3_dominant_avg_4h_net_bps": f3_dominant_avg_4h_net_bps if f3_dominant_avg_4h_net_bps is not None else not_obs,
        "f3_dominant_avg_8h_net_bps": f3_dominant_avg_8h_net_bps if f3_dominant_avg_8h_net_bps is not None else not_obs,
        "f3_dominant_avg_12h_net_bps": f3_dominant_avg_12h_net_bps if f3_dominant_avg_12h_net_bps is not None else not_obs,
        "f3_dominant_avg_24h_net_bps": f3_dominant_avg_24h_net_bps if f3_dominant_avg_24h_net_bps is not None else not_obs,
        "f3_dominant_win_rate_24h": f3_dominant_win_rate_24h if f3_dominant_win_rate_24h is not None else not_obs,
        "f3_dominant_negative_evidence": bool(f3_dominant_negative_evidence),
        "f3_dominant_swing_guard_candidate_count": f3_dominant_swing_guard_candidate_count,
        "f3_dominant_swing_guard_blocked_count": f3_dominant_swing_guard_blocked_count,
        "f3_dominant_swing_guard_still_swing_count": f3_dominant_swing_guard_still_swing_count,
        "probe_rows": len(probe_rows),
        "probe_lifecycle_rows": len(lifecycle_rows),
        "open_probe_watch_rows": len(open_probe_watch_rows),
        "open_probe_active_count": len(open_probe_watch_rows),
        "open_probe_symbols": sorted({row.get("symbol") for row in open_probe_watch_rows if row.get("symbol")}),
        "open_probe_unrealized_net_bps": {"min": min(open_probe_net_values) if open_probe_net_values else not_obs, "max": max(open_probe_net_values) if open_probe_net_values else not_obs, "avg": sum(open_probe_net_values) / len(open_probe_net_values) if open_probe_net_values else not_obs},
        "active_probe_ignore_zero_target_close_count": active_probe_ignore_zero_target_close_total,
        "active_probe_past_time_stop_count": active_probe_past_time_stop_count,
        "active_probe_zero_target_close_issue_count": active_probe_zero_target_close_issue_count,
        "dust_anti_chase_rows": len(dust_rows),
        "btc_leadership_blocked_rows": len(btc_blocked_rows),
        "btc_leadership_blocked_labeler_summary": btc_blocked_labeler_summary,
        "high_issue_count": high_count,
        "medium_issue_count": medium_count,
        "warning_count": warning_count,
        "router_reason_counts": dict(sorted(reason_counts.items())),
        "probe_counts": {field: int(probe_counts[field]) for field in PROBE_COUNT_FIELDS},
        **{field: int(probe_counts[field]) for field in PROBE_COUNT_FIELDS},
        "closed_roundtrip_gross_bps": {"min": min(closed_gross_values) if closed_gross_values else not_obs, "max": max(closed_gross_values) if closed_gross_values else not_obs, "avg": sum(closed_gross_values) / len(closed_gross_values) if closed_gross_values else not_obs},
        "closed_roundtrip_net_bps": {"min": min(closed_net_values) if closed_net_values else not_obs, "max": max(closed_net_values) if closed_net_values else not_obs, "avg": sum(closed_net_values) / len(closed_net_values) if closed_net_values else not_obs},
        "probe_trade_gross_bps": {"min": min(gross_values) if gross_values else not_obs, "max": max(gross_values) if gross_values else not_obs, "avg": sum(gross_values) / len(gross_values) if gross_values else not_obs},
        "probe_trade_net_bps": {"min": min(net_values) if net_values else not_obs, "max": max(net_values) if net_values else not_obs, "avg": sum(net_values) / len(net_values) if net_values else not_obs},
        "open_positions_unrealized_net_bps": {"min": min(open_net_values) if open_net_values else not_obs, "max": max(open_net_values) if open_net_values else not_obs, "avg": sum(open_net_values) / len(open_net_values) if open_net_values else not_obs},
        "missing_paths": sorted(missing_paths),
        "collection_error_count": len(collection_errors),
    }
    write_text("summaries/window_summary.json", json.dumps(window_summary, ensure_ascii=False, indent=2) + "\n")
    enforce_readiness_snapshot = {
        "quant_lab_requested_mode": window_summary.get("quant_lab_requested_mode", not_obs),
        "quant_lab_effective_mode": window_summary.get("quant_lab_effective_mode", not_obs),
        "mode_source": window_summary.get("quant_lab_mode_source", not_obs),
        "status": window_summary.get("enforce_readiness_status", not_obs),
        "blocked_reasons": window_summary.get("enforce_blocked_reasons", not_obs),
        "enforce_blocked_reason": window_summary.get("enforce_blocked_reason", not_obs),
        "contract_version_match": window_summary.get("contract_version_match", not_obs),
        "telemetry_schema_version_match": window_summary.get("telemetry_schema_version_match", not_obs),
        "quant_lab_cost_usage_rows": window_summary.get("post_deployment_cost_usage_rows", window_summary.get("cost_usage_current_contract_rows", window_summary.get("quant_lab_cost_usage_rows", 0))),
        "cost_degraded_count": window_summary.get("post_deployment_cost_degraded_count", window_summary.get("current_contract_cost_degraded_count", window_summary.get("cost_degraded_count", 0))),
        "global_default_cost_count": window_summary.get("post_deployment_global_default_cost_count", window_summary.get("current_contract_global_default_cost_count", window_summary.get("global_default_cost_count", 0))),
        "legacy_global_default_cost_count": window_summary.get("legacy_global_default_cost_count", 0),
        "current_contract_global_default_cost_count": window_summary.get("current_contract_global_default_cost_count", 0),
        "latest_24h_global_default_cost_count": window_summary.get("latest_24h_global_default_cost_count", 0),
        "post_deployment_global_default_cost_count": window_summary.get("post_deployment_global_default_cost_count", 0),
        "cost_usage_legacy_rows": window_summary.get("cost_usage_legacy_rows", 0),
        "cost_usage_current_contract_rows": window_summary.get("cost_usage_current_contract_rows", 0),
        "cost_usage_latest_24h_rows": window_summary.get("cost_usage_latest_24h_rows", 0),
        "post_deployment_cost_usage_rows": window_summary.get("post_deployment_cost_usage_rows", 0),
        "quant_lab_fallback_count": window_summary.get("quant_lab_fallback_count", 0),
        "quant_lab_request_count": quant_lab_request_success_count + quant_lab_request_error_count,
        "summary_trade_count_mismatch_count": len(summary_trade_count_mismatch_rows),
        "telemetry_contract_version": QUANT_LAB_CONTRACT_VERSION,
        "telemetry_schema_version": QUANT_LAB_SCHEMA_VERSION,
    }
    write_text("summaries/enforce_readiness_snapshot.json", json.dumps(enforce_readiness_snapshot, ensure_ascii=False, indent=2) + "\n")

    if trade_observation_status == "no_trades":
        latest_24h_real_trade_text = "no / 0"
        last_72h_real_trade_text = "no / 0"
        closed_roundtrip_gross_net_text = "not_applicable_no_trades"
        gross_net_text = "not_applicable_no_trades"
        probe_lifecycle_text = "not_applicable_no_probe_trade"
        probe_exit_policy_text = "not_applicable_no_probe_trade"
    elif trade_observation_status == "not_observable":
        latest_24h_real_trade_text = not_obs
        last_72h_real_trade_text = not_obs
        closed_roundtrip_gross_net_text = not_obs
        gross_net_text = not_obs
        probe_lifecycle_text = not_obs
        probe_exit_policy_text = not_obs
    else:
        latest_24h_real_trade_text = f"yes / {latest_24h_trade_count}" if latest_24h_trade_count else "no / 0"
        last_72h_real_trade_text = f"yes / {last_72h_trade_count}" if last_72h_trade_count else "no / 0"
        closed_roundtrip_gross_net_text = (
            f"gross={window_summary['closed_roundtrip_gross_bps']}, net={window_summary['closed_roundtrip_net_bps']}"
            if last_72h_roundtrip_count
            else "not_applicable_no_closed_roundtrips"
        )
        if probe_trade_rows:
            gross_net_text = f"gross={window_summary['probe_trade_gross_bps']}, net={window_summary['probe_trade_net_bps']}"
            probe_lifecycle_text = f"rows={len(lifecycle_rows)}" if lifecycle_rows else not_obs
            probe_exit_policy_text = "yes" if probe_exit_count else "no"
        else:
            gross_net_text = "not_applicable_no_probe_trade"
            probe_lifecycle_text = "not_applicable_no_probe_trade"
            probe_exit_policy_text = "not_applicable_no_probe_trade"

    if open_position_rows:
        open_position_text = f"yes / {len(open_position_rows)}"
        account_status_text = "has_effective_position"
        open_pnl_parts = []
        open_net_parts = []
        unprotected_profit = False
        protected_profit = False
        for row in open_position_rows:
            net_bps = as_float(row.get("unrealized_net_bps"))
            if net_bps is None:
                open_pnl_parts.append(f"{row['symbol']}=not_observable")
                open_net_parts.append(f"{row['symbol']}=not_observable")
                continue
            pnl_state = "floating_profit" if net_bps > 0 else ("floating_loss" if net_bps < 0 else "flat")
            open_pnl_parts.append(f"{row['symbol']}={pnl_state}")
            open_net_parts.append(f"{row['symbol']}={row['unrealized_net_bps']}")
            protected = row.get("profit_lock_active") == "true" or row.get("trailing_active") == "true"
            if net_bps > 0 and protected:
                protected_profit = True
            if net_bps > 100 and not protected:
                unprotected_profit = True
        open_pnl_text = ", ".join(open_pnl_parts)
        open_net_bps_text = ", ".join(open_net_parts)
        if unprotected_profit:
            open_stop_protection_text = "no"
        elif protected_profit:
            open_stop_protection_text = "yes"
        else:
            open_stop_protection_text = not_obs
    else:
        open_position_text = "no / 0"
        account_status_text = "flat / dust-only" if dust_residual_position_count else "flat / no observable position"
        open_pnl_text = "not_applicable_no_open_positions"
        open_net_bps_text = "not_applicable_no_open_positions"
        open_stop_protection_text = "not_applicable_no_open_positions"

    if open_probe_watch_rows:
        active_probe_text = f"yes / {len(open_probe_watch_rows)}"
        active_probe_net_text = ", ".join(
            f"{row.get('symbol', not_obs)}={row.get('unrealized_net_bps', not_obs)}"
            for row in open_probe_watch_rows
        )
        active_probe_distance_text = " | ".join(
            f"{row.get('symbol', not_obs)}: {row.get('next_expected_exit_condition', not_obs)}"
            for row in open_probe_watch_rows
        )
        active_probe_zero_target_text = (
            f"yes / {active_probe_ignore_zero_target_close_total}"
            if active_probe_ignore_zero_target_close_total
            else "no / 0"
        )
        active_probe_next_focus_text = (
            "past_time_stop_requires_review"
            if active_probe_past_time_stop_count
            else "watch take-profit / stop-loss / trailing / time-stop thresholds next bundle"
        )
    else:
        active_probe_text = "no / 0"
        active_probe_net_text = "not_applicable_no_active_probe"
        active_probe_distance_text = "not_applicable_no_active_probe"
        active_probe_zero_target_text = "not_applicable_no_active_probe"
        active_probe_next_focus_text = "not_applicable_no_active_probe"

    if protect_sideways_normal_entry_rows:
        protect_sideways_by_symbol_text = "; ".join(
            f"{row.get('symbol')}: count={row.get('count')}, avg_net_bps={row.get('avg_net_bps')}, win_rate={row.get('win_rate')}, avg_hold_minutes={row.get('avg_hold_minutes')}"
            for row in protect_sideways_normal_entry_by_symbol
        )
        protect_sideways_avg_text = fmt_num(protect_sideways_avg_net_bps, 6)
        protect_sideways_win_rate_text = fmt_num(protect_sideways_win_rate, 6)
    else:
        protect_sideways_by_symbol_text = "not_applicable_no_protect_sideways_normal_entries"
        protect_sideways_avg_text = "not_applicable_no_protect_sideways_normal_entries"
        protect_sideways_win_rate_text = "not_applicable_no_protect_sideways_normal_entries"

    if swing_early_exit_by_reason:
        swing_early_exit_by_reason_text = "; ".join(
            f"{row.get('exit_reason', not_obs)}: count={row.get('count', 0)}, early={row.get('early_exit_count', 0)}, "
            f"avg_exit={row.get('avg_net_bps_at_exit', not_obs)}, "
            f"better24_rate={row.get('better_to_hold_24h_rate', not_obs)}"
            for row in swing_early_exit_by_reason
        )
    else:
        swing_early_exit_by_reason_text = "not_applicable_no_swing_hold_roundtrips"
    swing_early_exit_atr_text = (
        f"yes / {swing_early_exit_atr_trailing_count}"
        if swing_early_exit_atr_trailing_count
        else "no / 0"
    )
    swing_early_exit_better_24_text = (
        fmt_num(swing_early_exit_better_24_rate, 6)
        if swing_early_exit_better_24_rate is not None
        else not_obs
    )

    if high_score_recent_24h_rows:
        high_score_recent_labelable_rows = [
            row for row in high_score_recent_24h_rows
            if is_high_score_labelable_reason(high_score_reason_text(row))
        ]
        high_score_symbols_text = ", ".join(sorted({row.get("symbol") or not_obs for row in high_score_recent_24h_rows}))
        high_score_gate_text = ", ".join(
            f"{category}={count}"
            for category, count in Counter(row.get("high_score_block_category") or not_obs for row in high_score_recent_24h_rows).most_common()
        )
        high_score_eth_seen_text = "yes" if any(row.get("symbol") == "ETH/USDT" for row in high_score_recent_24h_rows) else "no"
        high_score_trend_only_text = "yes" if any(row.get("high_score_block_category") == "trend_only" for row in high_score_recent_24h_rows) else "no"
        high_score_alpha6_sell_text = "yes" if any(row.get("high_score_block_category") == "alpha6_sell" or row.get("alpha6_side") == "sell" for row in high_score_recent_24h_rows) else "no"
        high_score_skipped_label_text = "yes" if high_score_recent_labelable_rows else "no / non_entry_management_only"
    else:
        high_score_symbols_text = "none"
        high_score_gate_text = "not_applicable_no_recent_24h_high_score_blocked_targets"
        high_score_eth_seen_text = "no"
        high_score_trend_only_text = "not_applicable_no_recent_24h_high_score_blocked_targets"
        high_score_alpha6_sell_text = "not_applicable_no_recent_24h_high_score_blocked_targets"
        high_score_skipped_label_text = "not_applicable_no_recent_24h_high_score_blocked_targets"
    high_score_non_entry_reason_text = (
        ", ".join(
            f"{reason}={count}"
            for reason, count in Counter(high_score_reason_text(row) or not_obs for row in high_score_non_entry_management_rows).most_common()
        )
        if high_score_non_entry_management_rows
        else "none"
    )

    eth_high_score_outcome_rows = [
        row for row in high_score_blocked_outcome_rows if row.get("symbol") == "ETH/USDT"
    ]

    def avg_net_text(rows, horizon):
        values = [as_float(row.get(f"label_{horizon}h_net_bps")) for row in rows]
        usable = [value for value in values if value is not None]
        return f"{round(sum(usable) / len(usable), 6)}" if usable else not_obs

    def horizon_avg_win_text(rows, *, prefix="label_"):
        parts = []
        for horizon in label_horizons:
            values = [as_float(row.get(f"{prefix}{int(horizon)}h_net_bps")) for row in rows]
            usable = [value for value in values if value is not None]
            avg = round(sum(usable) / len(usable), 6) if usable else not_obs
            win = round(sum(1 for value in usable if value > 0) / len(usable), 6) if usable else not_obs
            parts.append(f"{int(horizon)}h_avg={avg}, {int(horizon)}h_win={win}")
        return ", ".join(parts)

    def aggregate_summary_lines(rows, key_fields, limit=12):
        if not rows:
            return "not_observable_no_rows"
        parts = []
        for row in rows[:limit]:
            key = "/".join(row.get(field) or not_obs for field in key_fields)
            horizon_parts = []
            for horizon in label_horizons:
                h = int(horizon)
                horizon_parts.append(
                    f"{h}h={row.get(f'avg_{h}h_net_bps', not_obs)}"
                    f"/win={row.get(f'win_rate_{h}h', not_obs)}"
                )
            parts.append(f"{key}: count={row.get('count', 0)}, " + ", ".join(horizon_parts))
        return "; ".join(parts)

    def by_horizon_summary_lines(rows):
        if not rows:
            return "not_observable_no_rows"
        return "; ".join(
            f"{row.get('horizon_hours', not_obs)}h: count={row.get('count', 0)}, "
            f"avg={row.get('avg_net_bps', not_obs)}, win={row.get('win_rate', not_obs)}, "
            f"complete={row.get('complete_count', 0)}, pending={row.get('pending_count', 0)}, "
            f"not_observable={row.get('not_observable_count', 0)}"
            for row in rows
        )

    if eth_high_score_outcome_rows:
        eth_high_score_count_text = str(len(eth_high_score_outcome_rows))
        eth_high_score_perf_text = ", ".join(
            f"{horizon}h={avg_net_text(eth_high_score_outcome_rows, horizon)}"
            for horizon in label_horizons
        )
        eth_high_score_relax_gate_text = (
            "diagnostic_only_review_required"
            if any(avg_net_text(eth_high_score_outcome_rows, horizon) != not_obs for horizon in label_horizons)
            else "not_observable_no_matured_labels"
        )
    else:
        eth_high_score_count_text = "0"
        eth_high_score_perf_text = "not_observable_no_eth_samples"
        eth_high_score_relax_gate_text = "not_observable_no_eth_samples"

    def high_score_forward_summary_text():
        if not high_score_blocked_outcomes_by_symbol:
            return "not_observable_no_matured_labels"
        return aggregate_summary_lines(high_score_blocked_outcomes_by_symbol, ["symbol", "skip_reason"])

    high_score_forward_net_bps_text = high_score_forward_summary_text()
    high_score_relax_gate_text = (
        "diagnostic_only_review_required"
        if high_score_blocked_outcome_rows
        else "not_observable_no_matured_labels"
    )

    def alt_impulse_symbol_line(symbol):
        rows = [row for row in alt_impulse_shadow_rows if row.get("symbol") == symbol]
        if not rows:
            return f"{symbol}: count=0, avg_net_bps=not_observable, win_rate=not_observable"
        return f"{symbol}: count={len(rows)}, " + horizon_avg_win_text(rows)

    alt_impulse_future_probe_text = (
        "REGIME_SHADOW_no_live_or_paper_ready"
        if any(row.get("label_status") == "complete" for row in alt_impulse_shadow_rows)
        else ("KEEP_SHADOW_no_live_or_paper_ready" if alt_impulse_shadow_rows else "not_applicable_no_shadow_samples")
    )
    alt_impulse_readiness_ready_text = "yes" if bool(alt_impulse_shadow_readiness.get("ready_for_live_probe")) else "no"
    alt_impulse_readiness_ready_symbols_text = ", ".join(alt_impulse_shadow_readiness.get("ready_symbols") or []) or "none"
    alt_impulse_readiness_blocking_text = ", ".join(alt_impulse_shadow_readiness.get("blocking_reasons") or []) or "none"

    def alt_impulse_readiness_symbol_line(symbol):
        wanted = normalize_symbol_text(symbol)
        for row in alt_impulse_shadow_readiness_by_symbol:
            if normalize_symbol_text(row.get("symbol")) != wanted:
                continue
            return (
                f"{symbol}: ready={row.get('ready_for_live_probe')}, "
                f"samples={row.get('sample_count')}, recent_7d={row.get('recent_sample_count')}, "
                f"24h_avg={row.get('avg_24h_net_bps')}, 24h_win={row.get('win_rate_24h')}, "
                f"48h_avg={row.get('avg_48h_net_bps')}, blocking={row.get('blocking_reasons') or 'none'}"
            )
        return f"{symbol}: ready=false, samples=0, recent_7d=0, blocking=no_symbol_samples"

    def multi_position_by_k_row(k, shadow_mode=MULTI_SHADOW_MODE_ALL):
        key = str(k)
        for row in multi_position_swing_shadow_by_k:
            if str(row.get("k")) == key and flatten_value(row.get("shadow_mode") or MULTI_SHADOW_MODE_ALL) == shadow_mode:
                return row
        return {}

    def multi_position_value(row, key):
        if not row:
            return None
        return as_float(row.get(key))

    def multi_position_top2_vs_top1_text(shadow_mode=MULTI_SHADOW_MODE_ALL):
        k1 = multi_position_by_k_row(1, shadow_mode)
        k2 = multi_position_by_k_row(2, shadow_mode)
        if not k1 or not k2:
            return "not_observable_missing_top1_or_top2"
        for horizon in multi_position_swing_horizons:
            h = int(horizon)
            top1 = multi_position_value(k1, f"avg_{h}h_net_bps")
            top2 = multi_position_value(k2, f"avg_{h}h_net_bps")
            if top1 is not None and top2 is not None:
                verdict = "yes" if top2 > top1 else "no"
                return f"{verdict} / {h}h top1={fmt_num(top1, 6)}, top2={fmt_num(top2, 6)}"
        return "not_observable_no_complete_top1_top2_labels"

    def multi_position_top3_risk_text():
        k2 = multi_position_by_k_row(2, MULTI_SHADOW_MODE_ALL)
        k3 = multi_position_by_k_row(3, MULTI_SHADOW_MODE_ALL)
        if not k2 or not k3:
            return "not_observable_missing_top2_or_top3"
        top2_worst = multi_position_value(k2, "worst_avg")
        top3_worst = multi_position_value(k3, "worst_avg")
        top2_win = multi_position_value(k2, "win_rate")
        top3_win = multi_position_value(k3, "win_rate")
        if top2_worst is None or top3_worst is None:
            return "not_observable_no_complete_top2_top3_worst"
        risk_up = top3_worst < top2_worst or (top2_win is not None and top3_win is not None and top3_win < top2_win)
        return (
            f"{'yes' if risk_up else 'no'} / "
            f"top2_worst_avg={fmt_num(top2_worst, 6)}, top3_worst_avg={fmt_num(top3_worst, 6)}, "
            f"top2_win_rate={fmt_num(top2_win, 6) if top2_win is not None else not_obs}, "
            f"top3_win_rate={fmt_num(top3_win, 6) if top3_win is not None else not_obs}"
        )

    def multi_position_best_combinations_text(limit=5):
        ranked = [
            row for row in multi_position_swing_shadow_rows
            if as_float(row.get("label_24h_portfolio_avg_net_bps")) is not None
        ]
        if not ranked:
            return "not_observable_no_complete_24h_labels"
        ranked.sort(key=lambda row: as_float(row.get("label_24h_portfolio_avg_net_bps")) or -1e18, reverse=True)
        parts = []
        for row in ranked[:limit]:
            parts.append(
                f"mode={row.get('shadow_mode', MULTI_SHADOW_MODE_ALL)} k={row.get('k', not_obs)} symbols={row.get('symbols', not_obs)} "
                f"24h_avg={row.get('label_24h_portfolio_avg_net_bps', not_obs)} "
                f"worst={row.get('label_24h_worst_symbol_net_bps', not_obs)} "
                f"wins={row.get('label_24h_win_count', not_obs)}"
            )
        return "; ".join(parts)

    def multi_position_protect_recovery_observation_text():
        all_k1 = multi_position_by_k_row(1, MULTI_SHADOW_MODE_ALL)
        all_k2 = multi_position_by_k_row(2, MULTI_SHADOW_MODE_ALL)
        pr_k1 = multi_position_by_k_row(1, MULTI_SHADOW_MODE_PROTECT_RECOVERY)
        pr_k2 = multi_position_by_k_row(2, MULTI_SHADOW_MODE_PROTECT_RECOVERY)
        if not all_k1 or not all_k2 or not pr_k2:
            return "not_observable_missing_mode_rows"
        for horizon in multi_position_swing_horizons:
            h = int(horizon)
            all_top1 = multi_position_value(all_k1, f"avg_{h}h_net_bps")
            all_top2 = multi_position_value(all_k2, f"avg_{h}h_net_bps")
            pr_top1 = multi_position_value(pr_k1, f"avg_{h}h_net_bps") if pr_k1 else None
            pr_top2 = multi_position_value(pr_k2, f"avg_{h}h_net_bps")
            if all_top1 is None or all_top2 is None or pr_top2 is None:
                continue
            all_top2_bad = all_top2 < all_top1
            protect_top2_good = (pr_top1 is not None and pr_top2 >= pr_top1) or pr_top2 > all_top2
            verdict = "yes_continue_observing" if all_top2_bad and protect_top2_good else "no_clear_edge_yet"
            return (
                f"{verdict} / {h}h all_top1={fmt_num(all_top1, 6)}, all_top2={fmt_num(all_top2, 6)}, "
                f"protect_recovery_top2={fmt_num(pr_top2, 6)}"
            )
        return "not_observable_no_complete_top2_labels"

    def multi_position_by_k_text():
        if not multi_position_swing_shadow_by_k:
            return "not_observable_no_rows"
        return "; ".join(
            f"mode={row.get('shadow_mode', MULTI_SHADOW_MODE_ALL)} k={row.get('k', not_obs)} count={row.get('count', 0)} "
            f"24h={row.get('avg_24h_net_bps', not_obs)} "
            f"48h={row.get('avg_48h_net_bps', not_obs)} "
            f"72h={row.get('avg_72h_net_bps', not_obs)} "
            f"win_rate={row.get('win_rate', not_obs)} "
            f"worst_avg={row.get('worst_avg', not_obs)}"
            for row in multi_position_swing_shadow_by_k
        )

    def multi_position_by_symbol_text():
        if not multi_position_swing_shadow_by_symbol:
            return "not_observable_no_rows"
        return "; ".join(
            f"mode={row.get('shadow_mode', MULTI_SHADOW_MODE_ALL)} {row.get('symbol', not_obs)} count={row.get('count', 0)} "
            f"24h={row.get('avg_24h_net_bps', not_obs)}/win={row.get('win_rate_24h', not_obs)} "
            f"48h={row.get('avg_48h_net_bps', not_obs)}/win={row.get('win_rate_48h', not_obs)} "
            f"72h={row.get('avg_72h_net_bps', not_obs)}/win={row.get('win_rate_72h', not_obs)}"
            for row in multi_position_swing_shadow_by_symbol
        )

    sol_swing_summary = sol_swing_performance_rows[0] if sol_swing_performance_rows else {}

    def sol_swing_real_profit_text():
        count = as_int(sol_swing_summary.get("real_roundtrip_count", 0))
        avg = as_float(sol_swing_summary.get("real_net_bps_avg"))
        pnl = sol_swing_summary.get("real_net_pnl_usdt", not_obs)
        if count <= 0:
            return "not_observable_no_real_sol_swing_roundtrips"
        if avg is None:
            return f"not_observable / count={count}, net_pnl_usdt={pnl}"
        return f"{'yes' if avg > 0 else 'no'} / count={count}, avg_net_bps={fmt_num(avg, 6)}, net_pnl_usdt={pnl}"

    def sol_swing_shadow_support_text():
        hs_values = [as_float(sol_swing_summary.get(f"high_score_blocked_{h}h_avg")) for h in (24, 48, 72)]
        mp_values = [as_float(sol_swing_summary.get(f"multi_position_shadow_{h}h_avg")) for h in (24, 48, 72)]
        observed = [value for value in hs_values + mp_values if value is not None]
        if not observed:
            return "not_observable_no_sol_shadow_rows"
        support = any(value > 0 for value in observed)
        return (
            f"{'yes' if support else 'no'} / "
            f"high_score_24h={sol_swing_summary.get('high_score_blocked_24h_avg', not_obs)}, "
            f"48h={sol_swing_summary.get('high_score_blocked_48h_avg', not_obs)}, "
            f"72h={sol_swing_summary.get('high_score_blocked_72h_avg', not_obs)}; "
            f"multi_position_24h={sol_swing_summary.get('multi_position_shadow_24h_avg', not_obs)}, "
            f"48h={sol_swing_summary.get('multi_position_shadow_48h_avg', not_obs)}, "
            f"72h={sol_swing_summary.get('multi_position_shadow_72h_avg', not_obs)}"
        )

    sol_swing_continue_observe_text = (
        "yes / diagnostic_only"
        if (
            as_int(sol_swing_summary.get("real_roundtrip_count", 0)) > 0
            or as_int(sol_swing_summary.get("high_score_blocked_count", 0)) > 0
            or sol_swing_summary.get("multi_position_shadow_24h_avg", not_obs) != not_obs
        )
        else "not_observable_no_sol_samples"
    )

    def factor_contribution_summary_text():
        if not factor_contribution_outcomes_by_factor:
            return "not_observable_no_factor_rows"
        return aggregate_summary_lines(factor_contribution_outcomes_by_factor, ["dominant_factor"], limit=8)

    factor_contribution_summary = factor_contribution_summary_text()

    def quant_lab_shadow_profitability_text():
        if not quant_lab_shadow_outcome_rows:
            return "not_observable_no_would_block_shadow_orders"
        if quant_lab_shadow_executed_count <= 0:
            return f"not_observable_no_executed_roundtrips / would_block={len(quant_lab_shadow_outcome_rows)}"
        if quant_lab_shadow_avg_net_bps is None:
            return f"not_observable_missing_net_bps / executed={quant_lab_shadow_executed_count}"
        return (
            f"{'yes' if quant_lab_shadow_avg_net_bps > 0 else 'no'} / "
            f"would_block={len(quant_lab_shadow_outcome_rows)}, executed={quant_lab_shadow_executed_count}, "
            f"avg_net_bps={fmt_num(quant_lab_shadow_avg_net_bps, 6)}, "
            f"win_rate={fmt_num(quant_lab_shadow_win_rate, 6) if quant_lab_shadow_win_rate is not None else not_obs}, "
            f"net_pnl_sum_usdt={fmt_num(quant_lab_shadow_net_pnl_sum_usdt, 12) if quant_lab_shadow_net_pnl_sum_usdt is not None else not_obs}"
        )

    def quant_lab_shadow_enforce_support_text():
        if not quant_lab_shadow_outcome_rows:
            return "not_observable_no_shadow_blocks"
        if quant_lab_shadow_executed_count <= 0 or quant_lab_shadow_avg_net_bps is None:
            return "not_supported_yet_no_completed_outcomes"
        if quant_lab_shadow_avg_net_bps > 0 or quant_lab_shadow_net_pnl_sum_usdt and quant_lab_shadow_net_pnl_sum_usdt > 0:
            return "no_profitable_shadow_blocks_do_not_support_enforce"
        return "potentially_supported_by_nonprofitable_shadow_blocks_continue_monitoring"

    quant_lab_shadow_by_permission_text = (
        aggregate_summary_lines(quant_lab_shadow_outcomes_by_permission, ["permission"])
        if quant_lab_shadow_outcomes_by_permission
        else "not_observable_no_rows"
    )
    advisory_source_health_rows = load_csv_dicts(OUT / "summaries" / "strategy_opportunity_advisory_source_health.csv")
    advisory_source_health = advisory_source_health_rows[-1] if advisory_source_health_rows else {}

    def protect_sol_exception_shadow_text():
        if not protect_sol_exception_shadow_by_horizon:
            if protect_sol_exception_shadow_heartbeat_rows:
                reasons = Counter(
                    flatten_value(first_value(row, ("no_sample_reason", "original_block_reason"), not_obs)) or not_obs
                    for row in protect_sol_exception_shadow_heartbeat_rows
                )
                return "heartbeat_no_samples: " + ", ".join(f"{reason}={count}" for reason, count in sorted(reasons.items()))
            return "not_observable_no_shadow_samples"
        parts = []
        for row in protect_sol_exception_shadow_by_horizon:
            if int(as_float(row.get("horizon_hours")) or 0) not in {24, 48, 72}:
                continue
            parts.append(
                f"{row.get('original_block_reason', not_obs)} {row.get('horizon_hours', not_obs)}h "
                f"unique={row.get('unique_candidate_count', 0)} "
                f"avg={row.get('avg_would_pnl_bps', not_obs)} "
                f"better={row.get('better_than_current_strategy', not_obs)} "
                f"warning={row.get('sample_warning', '') or 'none'}"
            )
        return "; ".join(parts) if parts else "not_observable_no_24h_48h_72h_rows"

    protect_sol_exception_summary_text = protect_sol_exception_shadow_text()
    provenance_status = flatten_value(provenance_meta.get("provenance_status") or not_obs)
    code_provenance_text = flatten_value(provenance_meta.get("code_provenance") or not_obs)
    config_hash_text = flatten_value(provenance_meta.get("config_hash") or not_obs)
    effective_config_hash_text = flatten_value(provenance_meta.get("effective_live_config_hash") or not_obs)
    strategy_hash_text = flatten_value(provenance_meta.get("strategy_hash") or not_obs)
    readme = [
        f"# V5 live follow-up bundle {STAMP}",
        "",
        "This bundle contains read-only, sanitized production evidence for daily live follow-up.",
        "",
        "## Code provenance",
        f"- code provenance ok / degraded: {code_provenance_text}",
        f"- provenance_status: {provenance_status}",
        f"- git_branch: {provenance_meta.get('git_branch', not_obs)}",
        f"- git_commit: {provenance_meta.get('git_commit', not_obs)}",
        f"- git_dirty: {provenance_meta.get('git_dirty', not_obs)}",
        f"- source_snapshot_hash: {provenance_meta.get('source_snapshot_hash', not_obs)}",
        f"- source_tree_file_count: {provenance_meta.get('source_tree_file_count', not_obs)}",
        f"- config hash: {config_hash_text}",
        f"- effective_live_config_hash: {effective_config_hash_text}",
        f"- strategy_version: {provenance_meta.get('strategy_version', not_obs)}",
        f"- strategy hash: {strategy_hash_text}",
        f"- quant_lab_contract_version: {provenance_meta.get('quant_lab_contract_version', not_obs)}",
        "",
        "## ML live overlay",
        f"- ml_live_overlay_status: {window_summary.get('ml_live_overlay_status', not_obs)}",
        f"- ml_factor_enabled: {window_summary.get('ml_factor_enabled', not_obs)}",
        f"- collect_ml_training_data: {window_summary.get('collect_ml_training_data', not_obs)}",
        f"- ml_research_use_stable_universe: {window_summary.get('ml_research_use_stable_universe', not_obs)}",
        "- live_prod status: disabled_in_live_prod means ML overlay and training timers are off; research scripts remain available offline.",
        "",
        "## Quant-lab cost readiness",
        f"- global_default_cost_count_total_72h: {window_summary.get('global_default_cost_count', not_obs)}",
        f"- legacy_global_default_cost_count: {window_summary.get('legacy_global_default_cost_count', not_obs)}",
        f"- current_contract_global_default_cost_count: {window_summary.get('current_contract_global_default_cost_count', not_obs)}",
        f"- latest_24h_global_default_cost_count: {window_summary.get('latest_24h_global_default_cost_count', not_obs)}",
        f"- post_deployment_global_default_cost_count: {window_summary.get('post_deployment_global_default_cost_count', not_obs)}",
        f"- readiness rows: post_deployment={window_summary.get('post_deployment_cost_usage_rows', not_obs)}, scope={window_summary.get('cost_usage_post_deployment_scope', not_obs)}",
        "",
        "## Quant Lab guard observe-only impact",
        f"- guard_enforced: {str(window_summary.get('guard_enforced', False)).lower()}",
        f"- would_block_count: {window_summary.get('would_block_count', not_obs)}",
        f"- would_block_but_profitable_open_positions_count: {window_summary.get('would_block_but_profitable_open_positions_count', not_obs)}",
        f"- would_block_strategy_mix: {window_summary.get('would_block_strategy_mix', not_obs)}",
        f"- would_block_symbol_mix: {window_summary.get('would_block_symbol_mix', not_obs)}",
        "- rule: Quant Lab guard is diagnostic only; it does not change live order decisions.",
        "",
        "## Strategy advisory source health",
        f"- selected_source: {advisory_source_health.get('selected_source', not_obs)}",
        f"- local_row_count: {advisory_source_health.get('local_row_count', not_obs)}",
        f"- api_row_count: {advisory_source_health.get('api_row_count', not_obs)}",
        f"- selected_row_count: {advisory_source_health.get('selected_row_count', not_obs)}",
        f"- latest_local_generated_at: {advisory_source_health.get('latest_local_generated_at', not_obs)}",
        f"- latest_api_generated_at: {advisory_source_health.get('latest_api_generated_at', not_obs)}",
        f"- selected_latest_generated_at: {advisory_source_health.get('selected_latest_generated_at', not_obs)}",
        f"- advisory_source_lag_sec: {advisory_source_health.get('advisory_source_lag_sec', not_obs)}",
        f"- stale_reason: {advisory_source_health.get('stale_reason', not_obs)}",
        f"- warning: {first_observed(advisory_source_health.get('warning'), advisory_source_health.get('freshness_inconsistency_warning'), not_obs)}",
        "",
        "## Entry quality advisory",
        f"- missed_low late_chase_loss_count: {window_summary.get('missed_low_late_chase_loss_count', not_obs)}",
        f"- late_entry_chase ready_for_live_guard: {window_summary.get('late_entry_chase_ready_for_live_guard', not_obs)}",
        f"- pullback_reversal ready_for_paper: {window_summary.get('pullback_reversal_ready_for_paper', not_obs)}",
        f"- pullback_reversal ready_for_live_probe: {window_summary.get('pullback_reversal_ready_for_live_probe', not_obs)}",
        f"- advisory_status: {window_summary.get('entry_quality_status', not_obs)}",
        f"- strategy_advisory_count: {window_summary.get('entry_quality_strategy_advisory_count', not_obs)}",
        f"- would_block_if_enabled_count: {window_summary.get('entry_quality_would_block_if_enabled_count', not_obs)}",
        f"- would_enter_count: {window_summary.get('entry_quality_would_enter_count', not_obs)}",
        f"- no_sample_reasons: {window_summary.get('entry_quality_no_sample_reasons', not_obs)}",
        f"- late_entry_chase_guard_enabled: {str(window_summary.get('late_entry_chase_guard_enabled', False)).lower()}",
        f"- pullback_reversal_live_enabled: {str(window_summary.get('pullback_reversal_live_enabled', False)).lower()}",
        "- live_order_effect: read_only_no_hard_block",
        "- output: summaries/entry_quality_advisory_reader.csv and raw/reports/entry_quality/*",
        "",
        "## Risk-on multi-buy shadow",
        f"- rows: {window_summary.get('risk_on_multi_buy_shadow_rows', not_obs)}",
        f"- latest selected_symbols: {window_summary.get('risk_on_multi_buy_latest_selected_symbols', not_obs)}",
        f"- source_detail_available: {str(window_summary.get('risk_on_multi_buy_source_detail_available', False)).lower()}",
        "- live_order_effect: read_only_no_live_order",
        "- output: summaries/risk_on_multi_buy_shadow.csv",
        "",
        "## Probe 生命周期检查",
        f"- 今天是否有 market_impulse_probe / btc_leadership_probe: market_impulse_probe={bool(market_probe_seen or probe_counts['market_impulse_probe_candidate_count'] or probe_counts['market_impulse_probe_open_count'])}, btc_leadership_probe={bool(btc_seen_in_decision_audit or probe_counts['btc_leadership_probe_candidate_count'] or probe_counts['btc_leadership_probe_open_count'] or probe_counts['btc_leadership_probe_blocked_count'])}",
        f"- latest_24h_trade_count: {window_summary['latest_24h_trade_count']}",
        f"- latest_24h_roundtrip_count: {window_summary['latest_24h_roundtrip_count']}",
        f"- last_72h_trade_count: {window_summary['last_72h_trade_count']}",
        f"- last_72h_roundtrip_count: {window_summary['last_72h_roundtrip_count']}",
        f"- latest_24h 是否真实成交: {latest_24h_real_trade_text}",
        f"- last_72h 是否真实成交: {last_72h_real_trade_text}",
        f"- closed roundtrip gross/net bps: {closed_roundtrip_gross_net_text}",
        f"- early soft exit cases before swing min_hold: {len(early_exit_rows)}",
        f"- probe trade gross/net bps: {gross_net_text}",
        f"- probe lifecycle: {probe_lifecycle_text}",
        f"- 是否按 probe exit policy 退出: {probe_exit_policy_text}",
        f"- 平仓后是否仍有 stale state: {'yes' if stale_state_issues else 'no'}",
        f"- 是否只剩 dust: {'yes' if (dust_only_count or dust_residual_position_count or dust_residual_roundtrip_count) else 'no'}",
        f"- 是否重复生成 exit signal: {'yes' if repeated_exit_issues else 'no'}",
        "",
        "## Open position 检查",
        f"- account status: {account_status_text}",
        f"- 当前是否有持仓: {open_position_text}",
        f"- 持仓是否浮盈/浮亏: {open_pnl_text}",
        f"- unrealized net bps: {open_net_bps_text}",
        f"- 当前 stop 是否足够保护浮盈: {open_stop_protection_text}",
        f"- dust residual ignored: positions={dust_residual_position_count}, roundtrips={dust_residual_roundtrip_count}",
        "",
        "## Active probe watch",
        f"- 当前是否有 active probe: {active_probe_text}",
        f"- 当前浮盈/浮亏 net bps: {active_probe_net_text}",
        f"- 距离 take-profit / stop-loss / trailing / time-stop: {active_probe_distance_text}",
        f"- zero-target close 是否被正确保护: {active_probe_zero_target_text}",
        f"- 下一包重点: {active_probe_next_focus_text}",
        "- output: summaries/open_probe_watch.csv",
        "",
        "## PROTECT Sideways 普通开仓表现",
        f"- sample_count: {len(protect_sideways_normal_entry_rows)}",
        f"- avg_net_bps: {protect_sideways_avg_text}",
        f"- win_rate: {protect_sideways_win_rate_text}",
        f"- by_symbol: {protect_sideways_by_symbol_text}",
        f"- medium issue present: {'yes' if protect_sideways_medium_issue_present else 'no'}",
        "",
        "## Swing early exit audit",
        f"- audit rows: {len(swing_early_exit_rows)}",
        f"- early exit count: {swing_early_exit_count}",
        f"- historical early exits: {swing_historical_or_unknown_early_exit_count}",
        f"- post-fix early exits: {swing_post_fix_early_exit_count}",
        f"- blocked_by_min_hold count: {swing_blocked_by_min_hold_count}",
        f"- filled soft exit before min_hold count: {swing_filled_soft_exit_before_min_hold_count}",
        f"- by reason: {swing_early_exit_by_reason_text}",
        f"- ATR trailing before min_hold: {swing_early_exit_atr_text}",
        f"- better_to_hold_24h_rate: {swing_early_exit_better_24_text}",
        f"- medium issue present: {'yes' if swing_early_exit_medium_issue_present else 'no'}",
        f"- historical/unknown fix-state issue present: {'yes' if swing_early_exit_historical_or_unknown_issue_present else 'no'}",
        "",
        "## Post-min-hold ATR exit audit",
        f"- just-after-min-hold ATR exits: {post_min_hold_atr_exit_count}",
        f"- better if held 6h after exit: {post_min_hold_atr_better_6_text}",
        f"- better_to_hold_12h_rate: {post_min_hold_atr_better_12_text}",
        f"- better_to_hold_24h_rate: {post_min_hold_atr_better_24_text}",
        f"- by_symbol: {post_min_hold_atr_by_symbol_text}",
        f"- medium issue present: {'yes' if post_min_hold_atr_medium_issue_present else 'no'}",
        "- output: summaries/post_min_hold_atr_exit_audit.csv and summaries/post_min_hold_atr_exit_outcomes_by_symbol.csv",
        "",
        "## Swing ATR soft-exit readiness",
        f"- ready_for_live_guard: {str(swing_atr_soft_exit_readiness.get('ready_for_live_guard', False)).lower()}",
        f"- blocking_reasons: {json.dumps(swing_atr_soft_exit_readiness.get('blocking_reasons', []), ensure_ascii=False)}",
        f"- sample_count: {swing_atr_soft_exit_readiness.get('sample_count', 0)}",
        f"- observable_12h_count: {swing_atr_soft_exit_readiness.get('observable_12h_count', 0)}",
        f"- better_to_hold_12h_rate: {fmt_num(swing_atr_soft_exit_readiness.get('better_to_hold_12h_rate'), 6)}",
        f"- avg_realized_net_bps: {fmt_num(swing_atr_soft_exit_readiness.get('avg_realized_net_bps'), 6)}",
        f"- avg_delayed_12h_net_bps: {fmt_num(swing_atr_soft_exit_readiness.get('avg_delayed_12h_net_bps'), 6)}",
        f"- improvement_bps: {fmt_num(swing_atr_soft_exit_readiness.get('improvement_bps'), 6)}",
        f"- ready_symbols: {json.dumps(swing_atr_soft_exit_readiness.get('ready_symbols', []), ensure_ascii=False)}",
        "- interpretation: do not enable the real soft-exit guard until readiness is true and symbol-level evidence is sufficient.",
        "- output: summaries/swing_atr_soft_exit_readiness.json and summaries/swing_atr_soft_exit_readiness_by_symbol.csv",
        "",
        "## BNB profit-lock / ATR trailing shadow",
        f"- sample_count: {bnb_profit_lock_shadow_sample_count}",
        f"- sample_count_gate_met_for_exit_change_review: {str(bnb_profit_lock_shadow_sample_gate_met).lower()}",
        f"- latest actual_exit_net_bps: {bnb_profit_lock_shadow_latest.get('actual_exit_net_bps', not_obs)}",
        f"- latest max_unrealized_bps: {bnb_profit_lock_shadow_latest.get('max_unrealized_bps', not_obs)}",
        f"- latest delayed_exit_6h/12h/24h: {bnb_profit_lock_shadow_latest.get('delayed_exit_6h', not_obs)} / {bnb_profit_lock_shadow_latest.get('delayed_exit_12h', not_obs)} / {bnb_profit_lock_shadow_latest.get('delayed_exit_24h', not_obs)}",
        f"- latest best_shadow_exit_policy: {bnb_profit_lock_shadow_latest.get('best_shadow_exit_policy', not_obs)}",
        f"- best_policy_mix: {json.dumps(bnb_profit_lock_shadow_best_policy_mix, ensure_ascii=False, sort_keys=True)}",
        "- interpretation: this is diagnostic only; do not modify live ATR/profit-lock exit until sample_count >= 10 and follow-up review confirms improvement.",
        "- output: summaries/bnb_profit_lock_shadow.csv",
        "",
        "## PROTECT SOL exception shadow",
        f"- experiment_name: {flatten_value(find_config_value(effective_data, 'protect_sol_exception_experiment_name') or 'protect_sol_exception_v1')}",
        f"- shadow_only: {str(config_bool('protect_sol_exception_enabled_shadow_only', True)).lower()}",
        f"- enable_live_experiment: {str(config_bool('protect_sol_exception_enable_live_experiment', False)).lower()}",
        f"- label_count: {len(protect_sol_exception_shadow_rows)}",
        f"- heartbeat_count: {len(protect_sol_exception_shadow_heartbeat_rows)}",
        f"- by_horizon: {protect_sol_exception_summary_text}",
        f"- factor_weight_candidates: f3={config_string_list('protect_sol_exception_f3_weight_candidates', ['0.20', '0.25'])}, f4={config_string_list('protect_sol_exception_f4_weight_candidates', ['0.25', '0.30'])}",
        "",
        "## Negative expectancy 口径一致性",
        f"- consistency rows: {len(negative_consistency_rows)}",
        f"- mismatch_suspected_count: {negative_expectancy_mismatch_count}",
        f"- high issue present: {'yes' if negative_expectancy_mismatch_count else 'no'}",
        "",
        "## BNB risk summary",
        f"- closed_cycles: {bnb_risk_summary.get('closed_cycles', not_obs)}",
        f"- net_expectancy_bps: {bnb_risk_summary.get('net_expectancy_bps', not_obs)}",
        f"- fast_fail_net_expectancy_bps: {bnb_risk_summary.get('fast_fail_net_expectancy_bps', not_obs)}",
        f"- latest_roundtrip_net_bps: {bnb_risk_summary.get('latest_roundtrip_net_bps', not_obs)}",
        f"- latest_roundtrip_if_held_current_net_bps: {bnb_risk_summary.get('latest_roundtrip_if_held_current_net_bps', not_obs)}",
        f"- protect_alt_short_cycle_guard_active: {str(bnb_risk_summary.get('protect_alt_short_cycle_guard_active', False)).lower()}",
        f"- recommendation: {bnb_risk_summary.get('recommendation', not_obs)}",
        "- interpretation: BNB remains negative expectancy; do not add BNB to protect_recovery multi-position based on one if-held recovery case.",
        "- entry guard stance: high-score BNB with f3-dominant support and weak f4/f5 should stay blocked or shadow-only.",
        "- output: summaries/bnb_risk_summary.json",
        "",
        "## Summary trade metrics check",
        f"- summary_trade_count_mismatch rows: {len(summary_trade_count_mismatch_rows)}",
        f"- high issue present: {'yes' if any(str(row.get('diagnosis') or '').startswith('high_issue') for row in summary_trade_count_mismatch_rows) else 'no'}",
        f"- output: summaries/summary_trade_count_mismatch.csv and reports/summary_trade_count_mismatch.csv",
        f"- trade_metrics rows: {len(trade_metrics_rows)}",
        f"- fill_metrics rows: {len(fill_metrics_rows)}",
        "",
        "## Quant-lab shadow outcome",
        f"- quant-lab would block orders post-trade profitability: {quant_lab_shadow_profitability_text()}",
        f"- supports enabling enforce: {quant_lab_shadow_enforce_support_text()}",
        f"- by_permission: {quant_lab_shadow_by_permission_text}",
        f"- output: summaries/quant_lab_shadow_outcomes.csv and summaries/quant_lab_shadow_outcomes_by_permission.csv",
        "",
        "## Skipped candidate extended forward labels",
        f"- horizons_hours: {','.join(str(int(h)) for h in label_horizons)}",
        f"- by_horizon: {by_horizon_summary_lines(skipped_candidate_outcomes_by_horizon)}",
        f"- by_symbol: {aggregate_summary_lines(skipped_candidate_outcomes_by_symbol, ['symbol', 'skip_reason'])}",
        f"- by_skip_reason: {aggregate_summary_lines(skipped_candidate_outcomes_by_reason, ['skip_reason'])}",
        "",
        "## 配置消费审计",
        f"- audited config keys: {len(config_runtime_consumption_rows)}",
        f"- live config keys not consumed in runtime: {config_runtime_not_consumed_count}",
        "- split_order_runtime_active: false",
        f"- low issue present: {'yes' if config_runtime_not_consumed_count else 'no'}",
        "",
        "## Rank exit 一致性检查",
        f"- rank_exit sell 数量: {len(rank_exit_consistency_rows)}",
        f"- conflict 数量: {rank_exit_conflict_count}",
        f"- 是否存在 target 仍为正但实盘卖出: {'yes' if rank_exit_target_positive_sell_count else 'no'}",
        "",
        "## Alpha6 factor contribution audit",
        f"- factor_contribution_audit_rows: {len(factor_contribution_rows)}",
        f"- outcomes_by_factor: {factor_contribution_summary}",
        f"- f3_dominant_negative_evidence: {'true' if f3_dominant_negative_evidence else 'false'}",
        "",
        "## F3-dominant 风险检查",
        f"- f3_dominant_count: {f3_dominant_count}",
        f"- f3_dominant_swing_guard_candidate_count: {f3_dominant_swing_guard_candidate_count}",
        f"- f3_dominant_swing_guard_blocked_count: {f3_dominant_swing_guard_blocked_count}",
        f"- f3_dominant_still_marked_swing: {'yes' if f3_dominant_swing_guard_still_swing_count else 'no'} ({f3_dominant_swing_guard_still_swing_count})",
        f"- avg_4h_net_bps: {fmt_num(f3_dominant_avg_4h_net_bps, 6)}",
        f"- avg_8h_net_bps: {fmt_num(f3_dominant_avg_8h_net_bps, 6)}",
        f"- avg_12h_net_bps: {fmt_num(f3_dominant_avg_12h_net_bps, 6)}",
        f"- avg_24h_net_bps: {fmt_num(f3_dominant_avg_24h_net_bps, 6)}",
        f"- win_rate_24h: {fmt_num(f3_dominant_win_rate_24h, 6)}",
        f"- f3_dominant_negative_evidence: {'true' if f3_dominant_negative_evidence else 'false'}",
        f"- output: summaries/f3_dominant_swing_guard_cases.csv and summaries/f3_dominant_swing_guard_outcomes.csv",
        f"- action: diagnostic_only_monitor_no_trade_block",
        "",
        "## 高分但未成交目标",
        f"- high-score blocked targets total: {len(high_score_blocked_rows)}",
        f"- labelable high-score blocked targets: {len(high_score_labelable_rows)}",
        f"- non-entry management blocks: {len(high_score_non_entry_management_rows)} ({high_score_non_entry_reason_text})",
        f"- 最近 24h 哪些 symbol 高分但没买: {high_score_symbols_text}",
        f"- ETH 是否出现高分但未成交: {high_score_eth_seen_text}",
        f"- 主要被什么 gate 拦: {high_score_gate_text}",
        f"- 是否是 trend-only: {high_score_trend_only_text}",
        f"- 是否 Alpha6 实际为 sell: {high_score_alpha6_sell_text}",
        f"- 是否建议进入 skipped label: {high_score_skipped_label_text}",
        f"- 这些被挡样本历史 forward net bps: {high_score_forward_net_bps_text}",
        f"- 是否支持放松 gate: {high_score_relax_gate_text}",
        "",
        "## ETH/ALT 高分被挡事后表现",
        f"- ETH 高分被挡样本数: {eth_high_score_count_text}",
        f"- high_score_by_skip_reason: {aggregate_summary_lines(high_score_blocked_outcomes_by_reason, ['skip_reason'])}",
        f"- high_score_by_horizon: {by_horizon_summary_lines(high_score_blocked_outcomes_by_horizon)}",
        f"- ETH extended net bps: {eth_high_score_perf_text}",
        f"- 是否支持放松 gate: {eth_high_score_relax_gate_text}",
        "",
        "## ALT impulse shadow",
        f"- {alt_impulse_symbol_line('ETH/USDT')}",
        f"- {alt_impulse_symbol_line('SOL/USDT')}",
        f"- {alt_impulse_symbol_line('BNB/USDT')}",
        f"- by_skip_reason: {aggregate_summary_lines(alt_impulse_shadow_by_reason, ['skip_reason'])}",
        f"- by_regime: {aggregate_summary_lines(alt_impulse_shadow_by_regime, ['regime_state'])}",
        f"- by_horizon: {by_horizon_summary_lines(alt_impulse_shadow_by_horizon)}",
        f"- 是否支持未来 live probe: {alt_impulse_future_probe_text}",
        "",
        "## ALT impulse readiness",
        f"- ready_for_live_probe: {alt_impulse_readiness_ready_text}",
        f"- ready_symbols: {alt_impulse_readiness_ready_symbols_text}",
        f"- blocking_reasons: {alt_impulse_readiness_blocking_text}",
        f"- sample_count: {alt_impulse_shadow_readiness.get('sample_count', 0)}",
        f"- recent_7d_sample_count: {alt_impulse_shadow_readiness.get('recent_sample_count', 0)}",
        f"- {alt_impulse_readiness_symbol_line('ETH/USDT')}",
        f"- {alt_impulse_readiness_symbol_line('SOL/USDT')}",
        f"- {alt_impulse_readiness_symbol_line('BNB/USDT')}",
        "- default_live_alt_probe: no_until_readiness_true",
        "- output: summaries/alt_impulse_shadow_readiness.json and summaries/alt_impulse_shadow_readiness_by_symbol.csv",
        "",
        "## SOL swing 观察",
        f"- 真实 SOL swing 是否赚钱: {sol_swing_real_profit_text()}",
        f"- shadow 是否支持: {sol_swing_shadow_support_text()}",
        f"- 是否建议继续观察: {sol_swing_continue_observe_text}",
        "- 是否建议启用多币: no / diagnostic_only_default_disabled",
        "",
        "## 多币 swing shadow",
        f"- label_count: {len(multi_position_swing_shadow_rows)}",
        f"- all_candidates top2 是否优于 top1: {multi_position_top2_vs_top1_text(MULTI_SHADOW_MODE_ALL)}",
        f"- protect_recovery_rules top2 是否优于 top1: {multi_position_top2_vs_top1_text(MULTI_SHADOW_MODE_PROTECT_RECOVERY)}",
        f"- all_candidates top2 差但 protect_recovery_rules top2 好: {multi_position_protect_recovery_observation_text()}",
        f"- top3 是否增加风险: {multi_position_top3_risk_text()}",
        f"- 哪些组合表现最好: {multi_position_best_combinations_text()}",
        f"- by_k: {multi_position_by_k_text()}",
        f"- by_symbol: {multi_position_by_symbol_text()}",
        "",
        "## BTC leadership probe 可观测性",
        f"- 逻辑是否出现: {'yes' if btc_config_audit['seen_in_decision_audit'] else 'no'}",
        f"- 配置是否显式: live_prod_yaml={btc_config_audit['present_in_live_prod_yaml']}, effective_config={btc_config_audit['present_in_effective_config']}",
        f"- blocked cases 是否进入 skipped labeler: {'yes' if btc_skip_decisions and not any(row.get('maturity_issue') == 'missing_label_or_outcome' for row in maturity_rows) else ('no' if btc_skip_decisions else not_obs)}",
        f"- 是否需要补配置或 labeler: {'yes' if any(item.get('code') in {'btc_leadership_probe_missing_effective_config', 'btc_leadership_blocked_cases_not_labeled'} for item in issues) else 'no'}",
        "",
        f"High issues: {high_count}",
        f"Medium issues: {medium_count}",
    ]
    write_text("README.md", "\n".join(readme) + "\n")

    issues_payload = {
        "missing_paths": sorted(missing_paths),
        "collection_errors": collection_errors,
        "sanity_failures": [],
        "notes": notes,
        "high_issue_count": high_count,
        "medium_issue_count": medium_count,
        "warning_count": warning_count,
        "issues": issues,
    }
    write_text("summaries/issues_to_fix.json", json.dumps(issues_payload, ensure_ascii=False, indent=2) + "\n")
    return {
        "high_issue_count": high_count,
        "medium_issue_count": medium_count,
        "warning_count": warning_count,
        "roundtrip_warning": roundtrip_warning,
        "summary_trade_count_mismatch_high_issue_count": sum(
            1 for row in summary_trade_count_mismatch_rows
            if str(row.get("diagnosis") or "").startswith("high_issue")
        ),
        "run_summary_invalid": any(
            str(row.get("diagnosis") or "").startswith("high_issue")
            for row in summary_trade_count_mismatch_rows
        ),
        "candidate_snapshot_rows": len(candidate_snapshot_rows),
        "candidate_cost_source_coverage": candidate_cost_source_coverage_value,
        "order_lifecycle_rows": len(order_lifecycle_rows),
        "order_lifecycle_trade_metric_fill_count": order_lifecycle_trade_metric_fill_count,
        "order_lifecycle_missing_high_issue": order_lifecycle_missing_high_issue,
        "data_quality_warnings": data_quality_warnings,
    }


def scan_unredacted_secrets():
    matches = []
    for path in OUT.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(OUT).as_posix()
        if "/.env" in f"/{rel}" or Path(rel).name.startswith(".env"):
            matches.append({"path": rel, "reason": ".env file present"})
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        found = UNREDACTED_SECRET_RE.search(text)
        if found:
            matches.append({"path": rel, "reason": f"unredacted secret-like assignment: {found.group(1)}"})
    return matches


def file_inventory():
    rows = []
    for path in sorted(OUT.rglob("*")):
        if path.is_file():
            rel = path.relative_to(OUT).as_posix()
            data = path.read_bytes()
            rows.append({"path": rel, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    return rows


def hash_text(value):
    if value in (None, "", "not_observable", "not_git"):
        return "not_observable"
    return hashlib.sha256(str(value).encode("utf-8", errors="replace")).hexdigest()


def hash_file(path):
    try:
        if path.is_file():
            return hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception as exc:
        collection_errors.append({"source": str(path), "error": f"hash_file: {exc!r}"})
    return "not_observable"


def iter_snapshot_files(root, rel_paths):
    seen = set()
    excluded_dirs = {
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        "backups",
        "archive",
        "reports",
        "logs",
        "data",
        "models",
    }
    for rel in rel_paths:
        base = root / rel
        if not base.exists():
            continue
        files = [base] if base.is_file() else sorted(path for path in base.rglob("*") if path.is_file())
        for path in files:
            parts = set(path.relative_to(root).parts)
            if parts & excluded_dirs:
                continue
            rel_path = path.relative_to(root).as_posix()
            if rel_path in seen:
                continue
            seen.add(rel_path)
            yield rel_path, path


def combined_files_hash(root, rel_paths):
    digest = hashlib.sha256()
    count = 0
    for rel, path in iter_snapshot_files(root, rel_paths):
        try:
            data = path.read_bytes()
        except Exception as exc:
            collection_errors.append({"source": str(path), "error": f"snapshot_hash: {exc!r}"})
            continue
        digest.update(rel.encode("utf-8", errors="replace"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(data).hexdigest().encode("ascii"))
        digest.update(b"\n")
        count += 1
    if count == 0:
        return "not_observable", 0
    return digest.hexdigest(), count


def read_deployment_version_file(root):
    for rel in DEPLOYMENT_VERSION_PATHS:
        path = root / rel
        if not path.is_file():
            continue
        try:
            text = sanitize_text(path.read_text(encoding="utf-8", errors="replace")).strip()
        except Exception as exc:
            collection_errors.append({"source": str(path), "error": f"deployment_version_read: {exc!r}"})
            text = "not_observable"
        return rel, text[:4000]
    return "not_observable", "not_observable"


def find_nested_value(obj, names):
    if isinstance(obj, dict):
        for name in names:
            if name in obj and obj[name] not in (None, ""):
                return obj[name]
        for value in obj.values():
            found = find_nested_value(value, names)
            if found not in (None, "", "not_observable"):
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = find_nested_value(value, names)
            if found not in (None, "", "not_observable"):
                return found
    return "not_observable"


def find_yaml_scalar(text, names):
    for name in names:
        match = re.search(rf"(?m)^\s*{re.escape(name)}\s*:\s*([^#\n]+)", text or "")
        if match:
            return match.group(1).strip().strip("\"'")
    return "not_observable"


def first_jsonl_nested_value(path, names):
    for row in load_jsonl(path):
        found = find_nested_value(row, names)
        if found not in (None, "", "not_observable"):
            return found
    return "not_observable"


def build_provenance_meta():
    _, inside_out, _ = run_readonly(["git", "rev-parse", "--is-inside-work-tree"])
    is_git = inside_out.splitlines()[0].strip().lower() == "true" if inside_out else False
    _, branch_out, _ = run_readonly(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    _, commit_out, _ = run_readonly(["git", "rev-parse", "HEAD"])
    branch = branch_out.splitlines()[0].strip() if branch_out else "not_git"
    commit = commit_out.splitlines()[0].strip() if commit_out else "not_git"
    if not is_git or branch in ("", "not_git") or commit in ("", "not_git"):
        is_git = False
        branch = "not_git"
        commit = "not_git"

    if is_git:
        _, dirty_out, _ = run_readonly(["git", "status", "--short"])
        git_dirty = bool((dirty_out or "").strip())
        _, remote_out, _ = run_readonly(["git", "remote", "get-url", "origin"])
        provenance_status = "git_dirty_degraded" if git_dirty else "git_clean"
        code_provenance = "degraded" if git_dirty else "ok"
        remote_hash = hash_text(remote_out.splitlines()[0].strip() if remote_out else "")
    else:
        git_dirty = "not_observable"
        remote_hash = "not_observable"
        provenance_status = "not_git_degraded"
        code_provenance = "degraded"

    source_hash, source_count = combined_files_hash(ROOT, SOURCE_SNAPSHOT_PATHS)
    strategy_hash, strategy_count = combined_files_hash(ROOT, STRATEGY_SNAPSHOT_PATHS)
    dependency_hash, dependency_count = combined_files_hash(ROOT, DEPENDENCY_LOCK_PATHS)
    deployment_path, deployment_content = read_deployment_version_file(ROOT)
    live_config_path = ROOT / "configs/live_prod.yaml"
    effective_config_path = OUT / "raw/reports/effective_live_config.json"
    try:
        live_config_text = live_config_path.read_text(encoding="utf-8", errors="replace") if live_config_path.is_file() else ""
    except Exception as exc:
        collection_errors.append({"source": str(live_config_path), "error": f"live_config_read: {exc!r}"})
        live_config_text = ""
    effective_config = load_json(effective_config_path) if effective_config_path.is_file() else None
    strategy_version = find_nested_value(effective_config, ("strategy_version", "quant_lab_strategy_version"))
    if strategy_version == "not_observable":
        strategy_version = find_yaml_scalar(live_config_text, ("strategy_version", "quant_lab_strategy_version"))
    quant_lab_contract_version = find_nested_value(effective_config, ("quant_lab_contract_version", "contract_version"))
    if quant_lab_contract_version == "not_observable":
        quant_lab_contract_version = first_jsonl_nested_value(
            OUT / "raw/reports/quant_lab_usage.jsonl",
            ("contract_version", "quant_lab_contract_version"),
        )
    if quant_lab_contract_version == "not_observable":
        quant_lab_contract_version = QUANT_LAB_CONTRACT_VERSION

    return {
        "provenance_status": provenance_status,
        "code_provenance": code_provenance,
        "git_branch": branch,
        "git_commit": commit,
        "git_dirty": git_dirty,
        "git_remote_url_hash": remote_hash,
        "build_timestamp": NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "python_version": sys.version.replace("\n", " "),
        "dependency_lock_hash": dependency_hash,
        "dependency_lock_file_count": dependency_count,
        "config_hash": hash_file(live_config_path),
        "effective_live_config_hash": hash_file(effective_config_path),
        "schema_version": QUANT_LAB_SCHEMA_VERSION,
        "contract_version": str(quant_lab_contract_version),
        "telemetry_schema_version": QUANT_LAB_SCHEMA_VERSION,
        "telemetry_contract_version": str(quant_lab_contract_version),
        "event_id_generation_version": QUANT_LAB_EVENT_ID_GENERATION_VERSION,
        "trade_export_schema_version": TRADE_EXPORT_SCHEMA_VERSION,
        "summary_metrics_version": SUMMARY_METRICS_VERSION,
        "strategy_version": str(strategy_version),
        "strategy_hash": strategy_hash,
        "strategy_file_count": strategy_count,
        "quant_lab_contract_version": str(quant_lab_contract_version),
        "source_snapshot_hash": source_hash,
        "source_tree_file_count": source_count,
        "deployment_version_file_path": deployment_path,
        "deployment_version_file": deployment_content,
    }


if not ROOT.is_dir():
    fail(f"production root not found: {ROOT}")

if OUT.exists():
    shutil.rmtree(OUT)
OUT.mkdir(parents=True)
for rel in PAYLOAD_DIRS:
    (OUT / rel).mkdir(parents=True, exist_ok=True)

commands.extend([
    f"cd {ROOT}",
    "git rev-parse --abbrev-ref HEAD",
    "git rev-parse HEAD",
    "collect code provenance metadata",
    "copy exact reports state files",
    "copy reports/runs[/prod] last72h lightweight files",
    "copy sanitized log tails last72h",
])

copy_sanitized("configs/live_prod.yaml", "raw/config_live_prod.yaml", required=True)
for src_rel, dest_rel, required in STATE_FILES:
    copy_sanitized(src_rel, dest_rel, required=required)
copy_current_reports()
provenance_meta = build_provenance_meta()
copied_runs, recent_24_decisions = copy_recent_runs()
merged_candidate_snapshot_rows = merge_candidate_snapshot_reports()
copied_logs = copy_logs()
summary_meta = build_summaries(copied_runs, copied_logs, recent_24_decisions, provenance_meta)

sanity = {
    "raw/state/kill_switch.json exists": (OUT / "raw/state/kill_switch.json").is_file(),
    "raw/state/reconcile_status.json exists": (OUT / "raw/state/reconcile_status.json").is_file(),
    "raw/recent_runs has recent24h decision_audit.json": bool(recent_24_decisions),
    "contains raw/state": any((OUT / "raw/state").glob("*.json")),
    "contains raw/recent_runs": any((OUT / "raw/recent_runs").glob("*/decision_audit.json")),
    "contains raw/reports/quant_lab_usage.jsonl": (OUT / "raw/reports/quant_lab_usage.jsonl").is_file(),
    "contains raw/reports/quant_lab_requests.jsonl": (OUT / "raw/reports/quant_lab_requests.jsonl").is_file(),
    "contains summaries/probe_lifecycle_audit.csv": (OUT / "summaries/probe_lifecycle_audit.csv").is_file(),
    "contains summaries/open_probe_watch.csv": (OUT / "summaries/open_probe_watch.csv").is_file(),
    "contains summaries/post_min_hold_atr_exit_audit.csv": (OUT / "summaries/post_min_hold_atr_exit_audit.csv").is_file(),
    "contains summaries/swing_atr_soft_exit_readiness.json": (OUT / "summaries/swing_atr_soft_exit_readiness.json").is_file(),
    "contains summaries/swing_atr_soft_exit_readiness_by_symbol.csv": (OUT / "summaries/swing_atr_soft_exit_readiness_by_symbol.csv").is_file(),
    "contains summaries/swing_atr_soft_exit_shadow.csv": (OUT / "summaries/swing_atr_soft_exit_shadow.csv").is_file(),
    "contains summaries/bnb_risk_summary.json": (OUT / "summaries/bnb_risk_summary.json").is_file(),
    "contains summaries/quant_lab_compliance.csv": (OUT / "summaries/quant_lab_compliance.csv").is_file(),
    "contains summaries/quant_lab_permission_audit.csv": (OUT / "summaries/quant_lab_permission_audit.csv").is_file(),
    "contains summaries/quant_lab_mode_audit.csv": (OUT / "summaries/quant_lab_mode_audit.csv").is_file(),
    "contains summaries/enforce_readiness_snapshot.json": (OUT / "summaries/enforce_readiness_snapshot.json").is_file(),
    "contains summaries/quant_lab_cost_usage.csv": (OUT / "summaries/quant_lab_cost_usage.csv").is_file(),
    "contains summaries/quant_lab_fallbacks.csv": (OUT / "summaries/quant_lab_fallbacks.csv").is_file(),
    "contains summaries/candidate_snapshot.csv": (OUT / "summaries/candidate_snapshot.csv").is_file(),
    "contains summaries/issues_to_fix.json": (OUT / "summaries/issues_to_fix.json").is_file(),
    "provenance_status": provenance_meta.get("provenance_status", "not_observable"),
    "code provenance ok/degraded": provenance_meta.get("code_provenance", "not_observable"),
    "provenance_status explicit": provenance_meta.get("provenance_status") not in (None, "", "ok", "not_observable"),
    "warnings": [],
    "high_issue_count": int(summary_meta.get("high_issue_count", 0)),
    "medium_issue_count": int(summary_meta.get("medium_issue_count", 0)),
    "no .env files": True,
    "no unredacted secret assignments": True,
}
if summary_meta.get("roundtrip_warning"):
    sanity["warnings"].append("trades exist but roundtrip/open trade rows are missing")
if provenance_meta.get("code_provenance") == "degraded":
    sanity["warnings"].append(f"code provenance degraded: {provenance_meta.get('provenance_status')}")
secret_matches = scan_unredacted_secrets()
if secret_matches:
    sanity["no .env files"] = not any(match["reason"] == ".env file present" for match in secret_matches)
    sanity["no unredacted secret assignments"] = not any(match["reason"].startswith("unredacted") for match in secret_matches)
    collection_errors.append({"sanity_secret_scan": secret_matches[:20]})

failure_check_names = [
    "raw/state/kill_switch.json exists",
    "raw/state/reconcile_status.json exists",
    "raw/recent_runs has recent24h decision_audit.json",
    "contains raw/state",
    "contains raw/recent_runs",
    "contains raw/reports/quant_lab_usage.jsonl",
    "contains raw/reports/quant_lab_requests.jsonl",
    "contains summaries/probe_lifecycle_audit.csv",
    "contains summaries/open_probe_watch.csv",
    "contains summaries/post_min_hold_atr_exit_audit.csv",
    "contains summaries/swing_atr_soft_exit_readiness.json",
    "contains summaries/swing_atr_soft_exit_readiness_by_symbol.csv",
    "contains summaries/swing_atr_soft_exit_shadow.csv",
    "contains summaries/bnb_risk_summary.json",
    "contains summaries/quant_lab_compliance.csv",
    "contains summaries/quant_lab_permission_audit.csv",
    "contains summaries/quant_lab_mode_audit.csv",
    "contains summaries/enforce_readiness_snapshot.json",
    "contains summaries/quant_lab_cost_usage.csv",
    "contains summaries/quant_lab_fallbacks.csv",
    "contains summaries/candidate_snapshot.csv",
    "contains summaries/issues_to_fix.json",
    "provenance_status explicit",
    "no .env files",
    "no unredacted secret assignments",
]
failed = [name for name in failure_check_names if not sanity.get(name)]
issues_path = OUT / "summaries/issues_to_fix.json"
issues_data = json.loads(issues_path.read_text(encoding="utf-8"))
issues_data["collection_errors"] = collection_errors
issues_data["sanity_checks"] = sanity
issues_data["sanity_failures"] = failed
issues_data["high_issue_count"] = int(summary_meta.get("high_issue_count", 0))
issues_data["medium_issue_count"] = int(summary_meta.get("medium_issue_count", 0))
issues_path.write_text(json.dumps(issues_data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

commands_path = write_text("commands.log", "\n".join(commands) + "\n")
inventory_before_manifest = file_inventory()
manifest = {
    "host": socket.gethostname(),
    "cwd": str(ROOT),
    "provenance_status": provenance_meta.get("provenance_status", "not_observable"),
    "code_provenance": provenance_meta.get("code_provenance", "not_observable"),
    "git_branch": provenance_meta.get("git_branch", "not_git"),
    "git_commit": provenance_meta.get("git_commit", "not_git"),
    "git_dirty": provenance_meta.get("git_dirty", "not_observable"),
    "git_remote_url_hash": provenance_meta.get("git_remote_url_hash", "not_observable"),
    "build_timestamp": provenance_meta.get("build_timestamp", NOW.strftime("%Y-%m-%dT%H:%M:%SZ")),
    "python_version": provenance_meta.get("python_version", sys.version.replace("\n", " ")),
    "dependency_lock_hash": provenance_meta.get("dependency_lock_hash", "not_observable"),
    "dependency_lock_file_count": provenance_meta.get("dependency_lock_file_count", 0),
    "config_hash": provenance_meta.get("config_hash", "not_observable"),
    "effective_live_config_hash": provenance_meta.get("effective_live_config_hash", "not_observable"),
    "schema_version": provenance_meta.get("schema_version", QUANT_LAB_SCHEMA_VERSION),
    "contract_version": provenance_meta.get("contract_version", provenance_meta.get("quant_lab_contract_version", QUANT_LAB_CONTRACT_VERSION)),
    "telemetry_schema_version": provenance_meta.get("telemetry_schema_version", QUANT_LAB_SCHEMA_VERSION),
    "telemetry_contract_version": provenance_meta.get("telemetry_contract_version", provenance_meta.get("contract_version", QUANT_LAB_CONTRACT_VERSION)),
    "event_id_generation_version": provenance_meta.get("event_id_generation_version", QUANT_LAB_EVENT_ID_GENERATION_VERSION),
    "trade_export_schema_version": provenance_meta.get("trade_export_schema_version", TRADE_EXPORT_SCHEMA_VERSION),
    "summary_metrics_version": provenance_meta.get("summary_metrics_version", SUMMARY_METRICS_VERSION),
    "run_summary_invalid": bool(summary_meta.get("run_summary_invalid", False)),
    "summary_trade_count_mismatch_high_issue_count": int(
        summary_meta.get("summary_trade_count_mismatch_high_issue_count", 0) or 0
    ),
    "candidate_snapshot_rows": int(summary_meta.get("candidate_snapshot_rows", 0) or 0),
    "candidate_cost_source_coverage": summary_meta.get("candidate_cost_source_coverage", 0.0),
    "order_lifecycle_rows": int(summary_meta.get("order_lifecycle_rows", 0) or 0),
    "order_lifecycle_trade_metric_fill_count": int(
        summary_meta.get("order_lifecycle_trade_metric_fill_count", 0) or 0
    ),
    "order_lifecycle_missing_high_issue": bool(summary_meta.get("order_lifecycle_missing_high_issue", False)),
    "data_quality_warnings": summary_meta.get("data_quality_warnings", []),
    "strategy_version": provenance_meta.get("strategy_version", "not_observable"),
    "strategy_hash": provenance_meta.get("strategy_hash", "not_observable"),
    "strategy_file_count": provenance_meta.get("strategy_file_count", 0),
    "quant_lab_contract_version": provenance_meta.get("quant_lab_contract_version", "not_observable"),
    "source_snapshot_hash": provenance_meta.get("source_snapshot_hash", "not_observable"),
    "source_tree_file_count": provenance_meta.get("source_tree_file_count", 0),
    "deployment_version_file_path": provenance_meta.get("deployment_version_file_path", "not_observable"),
    "deployment_version_file": provenance_meta.get("deployment_version_file", "not_observable"),
    "sampling_end_utc": NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "last_72h_start_utc": WINDOW_72H_START.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "last_72h_end_utc": WINDOW_72H_END.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "remote_root": str(ROOT),
    "bundle_path": str(TAR),
    "sha256_path": str(SHA_PATH),
    "missing_paths": sorted(missing_paths),
    "missing_optional_files": sorted(missing_paths),
    "notes": notes,
    "collection_errors": collection_errors,
    "sanity_checks": sanity,
    "recent_24h_decision_audits": recent_24_decisions,
    "files_in_bundle": [row["path"] for row in inventory_before_manifest],
    "file_inventory": inventory_before_manifest,
    "copied_sources": copied_sources,
}
write_text("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")

inventory = file_inventory()
file_list_text = "\n".join(row["path"] for row in inventory) + "\n"
write_text("file_list.txt", file_list_text)

if failed:
    print("SANITY_CHECKS=" + json.dumps(sanity, ensure_ascii=False), file=sys.stderr)
    fail("sanity check failed: " + ", ".join(failed), code=5)

if TAR.exists():
    TAR.unlink()
with tarfile.open(TAR, "w:gz") as tf:
    tf.add(OUT, arcname=BUNDLE_STEM)
sha = hashlib.sha256(TAR.read_bytes()).hexdigest()
SHA_PATH.write_text(f"{sha}  {TAR.name}\n", encoding="utf-8")
size = TAR.stat().st_size

print(f"BUNDLE_PATH={TAR}")
print(f"SHA256_PATH={SHA_PATH}")
print(f"SHA256={sha}")
print(f"SIZE_BYTES={size}")
print("SANITY_CHECKS=" + json.dumps(sanity, ensure_ascii=False, sort_keys=True))
print(f"HIGH_ISSUES={int(summary_meta.get('high_issue_count', 0))}")
print(f"MEDIUM_ISSUES={int(summary_meta.get('medium_issue_count', 0))}")
print("MISSING_PATHS=" + json.dumps(sorted(missing_paths), ensure_ascii=False))
print(f"FILE_COUNT={len(file_inventory())}")
print("FILE_LIST_BEGIN")
for row in file_inventory():
    print(row["path"])
print("FILE_LIST_END")
PY
