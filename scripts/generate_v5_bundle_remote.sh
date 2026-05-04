#!/usr/bin/env bash
set -euo pipefail

ROOT="${V5_REMOTE_ROOT:-/home/ubuntu/clawd/v5-prod}"

python3 - "$ROOT" <<'PY'
import csv
import datetime as dt
import fnmatch
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tarfile
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
RUN_FILES = ("decision_audit.json", "trades.csv", "equity.jsonl", "summary.json")
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
]
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


def run_readonly(cmd):
    commands.append(cmd)
    try:
        proc = subprocess.run(
            cmd,
            shell=True,
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


def build_summaries(copied_runs, copied_logs, recent_24_decisions):
    not_obs = "not_observable"

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

    config_dust_usdt_ignore = config_number("dust_usdt_ignore") or 0.0
    config_min_trade_value_usdt = config_number("min_trade_value_usdt") or 0.0
    global_dust_threshold_usdt = max(config_dust_usdt_ignore, 1.0, 0.1 * config_min_trade_value_usdt)

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

    def first_observed(*values):
        for value in values:
            if value not in (None, "", not_obs):
                return flatten_value(value)
        return not_obs

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

    def router_trade_reason(item, intent):
        reason = flatten_value(item.get("reason"))
        source_reason = flatten_value(item.get("source_reason"))
        if intent == "CLOSE_LONG" and source_reason:
            return source_reason
        return reason or source_reason or not_obs

    def router_trade_probe_type(item, reason):
        return first_observed(probe_type_of(item), probe_type_from_reason(reason), probe_type_from_reason(item.get("source_reason")))

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
    dust_residual_roundtrip_rows = []
    high_score_blocked_rows = []
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
        audit_text = safe_json(audit)
        if "btc_leadership_probe" in audit_text:
            btc_seen_in_decision_audit = True
        if "market_impulse_probe" in audit_text:
            market_probe_seen = True
        audit_ts = run_ts(run_id, audit)
        audit_regime = flatten_value(first_value(audit, ("regime", "market_regime"), not_obs))
        audit_level = flatten_value(first_value(audit, ("current_level", "risk_level"), not_obs))
        counts = audit.get("counts") if isinstance(audit.get("counts"), dict) else {}
        for field in PROBE_COUNT_FIELDS:
            probe_counts[field] += as_int(counts.get(field))
        for item in audit.get("target_execution_explain") or []:
            if not isinstance(item, dict):
                continue
            high_score_blocked = str(item.get("high_score_but_not_executed", "")).strip().lower() == "true"
            if item.get("high_score_but_not_executed") is True:
                high_score_blocked = True
            if high_score_blocked:
                audit_high_score_but_not_executed_count += 1
            router_action = flatten_value(item.get("router_action")).lower()
            if not high_score_blocked or router_action != "skip":
                continue
            symbol = flatten_value(item.get("symbol")) or not_obs
            high_score_blocked_rows.append({
                "ts_utc": audit_ts,
                "run_id": run_id,
                "symbol": symbol,
                "final_score": first_observed(item.get("final_score")),
                "selected_rank": first_observed(item.get("selected_rank")),
                "target_w": first_observed(item.get("target_w")),
                "router_action": first_observed(item.get("router_action")),
                "router_reason": first_observed(first_value(item, ("router_reason", "blocked_reason"), not_obs)),
                "high_score_block_category": first_observed(item.get("high_score_block_category")),
                "trend_score": first_observed(item.get("trend_score")),
                "trend_side": first_observed(item.get("trend_side")),
                "alpha6_score": first_observed(item.get("alpha6_score")),
                "alpha6_side": first_observed(item.get("alpha6_side")),
                "f4_volume_expansion": first_observed(item.get("f4_volume_expansion")),
                "f5_rsi_trend_confirm": first_observed(item.get("f5_rsi_trend_confirm")),
                "current_level": first_observed(first_value(item, ("current_level",), audit_level)),
                "regime": first_observed(first_value(item, ("regime",), audit_regime)),
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
                "raw_json": raw_json,
            }
            router_rows.append(row)

            router_intent = normalize_trade_intent(item)
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
                    "raw_json": raw_json,
                })

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
                for idx, item in enumerate(reader):
                    raw_trade_events.append(build_trade_event(trade_path, run_id, idx, item))
        except Exception as exc:
            trade_read_errors += 1
            collection_errors.append({"source": str(trade_path), "error": f"trade_csv: {exc!r}"})

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
    high_score_outcome_fields = [
        "ts_utc",
        "run_id",
        "symbol",
        "intended_side",
        "skip_reason",
        "high_score_block_category",
        "final_score",
        "target_w",
        "trend_score",
        "trend_side",
        "alpha6_score",
        "alpha6_side",
        "f4_volume_expansion",
        "f5_rsi_trend_confirm",
        "entry_px",
        "rt_cost_bps",
        "current_level",
        "regime",
        "label_4h_gross_bps",
        "label_4h_net_bps",
        "label_4h_would_have_won_net",
        "label_4h_status",
        "label_8h_gross_bps",
        "label_8h_net_bps",
        "label_8h_would_have_won_net",
        "label_8h_status",
        "label_12h_gross_bps",
        "label_12h_net_bps",
        "label_12h_would_have_won_net",
        "label_12h_status",
        "label_24h_gross_bps",
        "label_24h_net_bps",
        "label_24h_would_have_won_net",
        "label_24h_status",
        "label_status",
        "label_not_observable_reason",
    ]

    def truthy(value):
        return str(value or "").strip().lower() in {"1", "true", "yes", "y"}

    def is_high_score_blocked_outcome_source(row):
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

    high_score_blocked_outcome_rows = []
    for row in high_score_outcome_by_key.values():
        high_score_blocked_outcome_rows.append({
            field: first_observed(first_value(row, (field,), not_obs))
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
            for horizon in (4, 8, 12, 24):
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
        "whitelist_positive_4h_count",
        "regime",
        "current_level",
        "rt_cost_bps",
        "label_4h_net_bps",
        "label_8h_net_bps",
        "label_12h_net_bps",
        "label_24h_net_bps",
        "label_status",
    ]
    alt_impulse_shadow_rows = []
    for row in alt_impulse_shadow_label_rows:
        alt_impulse_shadow_rows.append({
            field: first_observed(first_value(row, (field,), not_obs))
            for field in alt_impulse_shadow_fields
        })
    alt_impulse_shadow_by_symbol = aggregate_high_score_outcomes(
        alt_impulse_shadow_rows,
        ["symbol", "skip_reason"],
    )
    alt_impulse_shadow_by_reason = aggregate_high_score_outcomes(
        alt_impulse_shadow_rows,
        ["skip_reason"],
    )

    high_score_pending_count = 0
    high_score_matured_unlabeled_count = 0
    if audit_high_score_but_not_executed_count and not high_score_blocked_rows:
        add_issue(
            "medium",
            "high_score_blocked_targets_summary_missing",
            "Decision audit contains high_score_but_not_executed=true but high_score_blocked_targets.csv would be empty.",
            {"audit_high_score_but_not_executed_count": audit_high_score_but_not_executed_count},
        )

    for row in high_score_blocked_rows:
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
        outcome = outcome_index.get(key) or outcome_loose_index.get(loose_key)
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
        pnl_mismatch = (rt_net_pnl - neg_net_pnl) if rt_net_pnl is not None and neg_net_pnl is not None else None
        bps_mismatch = (rt_weighted_bps - neg_net_bps) if rt_weighted_bps is not None and neg_net_bps is not None else None
        mismatch_suspected = bool(rt_net_pnl is not None and neg_net_pnl is not None and rt_net_pnl > 0 and neg_net_pnl < 0 and abs(rt_net_pnl - neg_net_pnl) > 0.05)
        if mismatch_suspected:
            diagnosis = "high_issue_negative_expectancy_roundtrip_mismatch"
            add_issue(
                "high",
                "negative_expectancy_roundtrip_mismatch",
                "Roundtrip summary shows positive net PnL while negative expectancy state shows negative net PnL for the same symbol.",
                {
                    "symbol": symbol,
                    "roundtrip_net_pnl_sum_usdt": fmt_num(rt_net_pnl, 12),
                    "negexp_net_pnl_sum_usdt": fmt_num(neg_net_pnl, 12),
                    "pnl_mismatch_usdt": fmt_num(pnl_mismatch, 12),
                    "roundtrip_closed_count": int(rt["count"]),
                    "negexp_closed_cycles": fmt_num(neg_closed_cycles, 0),
                },
            )
        elif not neg:
            diagnosis = "not_observable_negative_expectancy_symbol_missing"
        elif rt["count"] == 0:
            diagnosis = "not_applicable_no_closed_roundtrips"
        elif rt_net_pnl is None or neg_net_pnl is None:
            diagnosis = "not_observable_pnl"
        else:
            diagnosis = "ok"
        negative_consistency_rows.append({
            "symbol": symbol,
            "roundtrip_closed_count": int(rt["count"]),
            "roundtrip_net_pnl_sum_usdt": fmt_num(rt_net_pnl, 12),
            "roundtrip_net_bps_weighted": fmt_num(rt_weighted_bps, 4),
            "negexp_closed_cycles": fmt_num(neg_closed_cycles, 0),
            "negexp_net_pnl_sum_usdt": fmt_num(neg_net_pnl, 12),
            "negexp_net_expectancy_bps": fmt_num(neg_net_bps, 4),
            "pnl_mismatch_usdt": fmt_num(pnl_mismatch, 12),
            "bps_mismatch": fmt_num(bps_mismatch, 4),
            "mismatch_suspected": str(mismatch_suspected).lower(),
            "diagnosis": diagnosis,
        })

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

    write_csv(
        "summaries/router_decisions.csv",
        router_rows,
        ["run_id", "audit_timestamp", "index", "symbol", "action", "reason", "source_reason", "stage", "side", "drift", "deadband", "raw_json"],
    )
    write_csv(
        "summaries/trades_roundtrips.csv",
        trade_rows,
        ["run_id", "source_file", "row_number", "timestamp", "symbol", "side", "qty", "price", "entry_ts", "entry_px", "exit_ts", "exit_px", "entry_reason", "exit_reason", "probe_type", "roundtrip_status", "gross_pnl_usdt", "fee_total_usdt", "net_pnl_usdt", "gross_bps", "net_bps", "hold_minutes", "remaining_value_usdt", "dust_threshold_usdt", "raw_json"],
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
        ["ts_utc", "run_id", "symbol", "skip_reason", "entry_px", "age_hours", "label_4h_net_bps", "label_8h_net_bps", "label_12h_net_bps", "label_24h_net_bps", "label_status", "not_observable_reason", "alpha6_score", "f4_volume_expansion", "f5_rsi_trend_confirm", "rolling_high", "breakout_met", "net_expectancy_bps", "closed_cycles"],
    )
    write_csv(
        "summaries/negative_expectancy_consistency.csv",
        negative_consistency_rows,
        ["symbol", "roundtrip_closed_count", "roundtrip_net_pnl_sum_usdt", "roundtrip_net_bps_weighted", "negexp_closed_cycles", "negexp_net_pnl_sum_usdt", "negexp_net_expectancy_bps", "pnl_mismatch_usdt", "bps_mismatch", "mismatch_suspected", "diagnosis"],
    )
    write_csv(
        "summaries/high_score_blocked_targets.csv",
        high_score_blocked_rows,
        ["ts_utc", "run_id", "symbol", "final_score", "selected_rank", "target_w", "router_action", "router_reason", "high_score_block_category", "trend_score", "trend_side", "alpha6_score", "alpha6_side", "f4_volume_expansion", "f5_rsi_trend_confirm", "current_level", "regime"],
    )
    write_csv(
        "summaries/high_score_blocked_outcomes.csv",
        high_score_blocked_outcome_rows,
        high_score_outcome_fields,
    )
    write_csv(
        "summaries/high_score_blocked_outcomes_by_symbol.csv",
        high_score_blocked_outcomes_by_symbol,
        ["symbol", "skip_reason", "count", "avg_4h_net_bps", "avg_8h_net_bps", "avg_12h_net_bps", "avg_24h_net_bps", "win_rate_4h", "win_rate_8h", "win_rate_12h", "win_rate_24h"],
    )
    write_csv(
        "summaries/high_score_blocked_outcomes_by_reason.csv",
        high_score_blocked_outcomes_by_reason,
        ["skip_reason", "count", "avg_4h_net_bps", "avg_8h_net_bps", "avg_12h_net_bps", "avg_24h_net_bps", "win_rate_4h", "win_rate_8h", "win_rate_12h", "win_rate_24h"],
    )
    write_csv(
        "summaries/alt_impulse_shadow_outcomes.csv",
        alt_impulse_shadow_rows,
        alt_impulse_shadow_fields,
    )
    write_csv(
        "summaries/alt_impulse_shadow_outcomes_by_symbol.csv",
        alt_impulse_shadow_by_symbol,
        ["symbol", "skip_reason", "count", "avg_4h_net_bps", "avg_8h_net_bps", "avg_12h_net_bps", "avg_24h_net_bps", "win_rate_4h", "win_rate_8h", "win_rate_12h", "win_rate_24h"],
    )
    write_csv(
        "summaries/alt_impulse_shadow_outcomes_by_reason.csv",
        alt_impulse_shadow_by_reason,
        ["skip_reason", "count", "avg_4h_net_bps", "avg_8h_net_bps", "avg_12h_net_bps", "avg_24h_net_bps", "win_rate_4h", "win_rate_8h", "win_rate_12h", "win_rate_24h"],
    )

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
    dust_residual_position_count = len(dust_residual_position_keys)
    dust_residual_roundtrip_count = len(dust_residual_roundtrip_rows)
    effective_open_position_count = len(open_position_rows)
    negative_expectancy_mismatch_count = sum(1 for row in negative_consistency_rows if row.get("mismatch_suspected") == "true")
    high_score_block_category_counts = dict(sorted(Counter(row.get("high_score_block_category") or not_obs for row in high_score_blocked_rows).items()))
    high_score_recent_24h_rows = [
        row for row in high_score_blocked_rows
        if parse_dt_utc(row.get("ts_utc")) is not None and parse_dt_utc(row.get("ts_utc")).timestamp() >= RECENT_24H
    ]

    window_summary = {
        "sampled_at_utc": NOW.isoformat(),
        "window_hours": 72,
        "remote_root": str(ROOT),
        "run_count": len(copied_runs),
        "recent_24h_decision_audit_count": len(recent_24_decisions),
        "log_file_count": len(copied_logs),
        "router_decision_rows": len(router_rows),
        "has_trade_data": has_trade_data,
        "trade_observation_status": trade_observation_status,
        "trade_read_error_count": trade_read_errors,
        "raw_trade_rows": raw_trade_file_rows,
        "trade_rows": len(trade_rows),
        "latest_24h_trade_count": latest_24h_trade_count if has_trade_data else not_obs,
        "latest_24h_roundtrip_count": latest_24h_roundtrip_count if has_trade_data else not_obs,
        "last_72h_trade_count": last_72h_trade_count if has_trade_data else not_obs,
        "last_72h_roundtrip_count": last_72h_roundtrip_count if has_trade_data else not_obs,
        "open_position_count": len(open_position_rows),
        "effective_open_position_count": effective_open_position_count,
        "dust_residual_position_count": dust_residual_position_count,
        "dust_residual_roundtrip_count": dust_residual_roundtrip_count,
        "dust_threshold_usdt": global_dust_threshold_usdt,
        "negative_expectancy_consistency_rows": len(negative_consistency_rows),
        "negative_expectancy_mismatch_count": negative_expectancy_mismatch_count,
        "high_score_blocked_target_count": len(high_score_blocked_rows),
        "high_score_blocked_recent_24h_target_count": len(high_score_recent_24h_rows),
        "high_score_block_category_counts": high_score_block_category_counts,
        "high_score_blocked_outcome_count": len(high_score_blocked_outcome_rows),
        "high_score_blocked_pending_count": high_score_pending_count,
        "high_score_blocked_matured_unlabeled_count": high_score_matured_unlabeled_count,
        "alt_impulse_shadow_label_count": len(alt_impulse_shadow_rows),
        "alt_impulse_shadow_duplicate_count": alt_impulse_shadow_duplicate_count,
        "probe_rows": len(probe_rows),
        "probe_lifecycle_rows": len(lifecycle_rows),
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

    if high_score_recent_24h_rows:
        high_score_symbols_text = ", ".join(sorted({row.get("symbol") or not_obs for row in high_score_recent_24h_rows}))
        high_score_gate_text = ", ".join(
            f"{category}={count}"
            for category, count in Counter(row.get("high_score_block_category") or not_obs for row in high_score_recent_24h_rows).most_common()
        )
        high_score_eth_seen_text = "yes" if any(row.get("symbol") == "ETH/USDT" for row in high_score_recent_24h_rows) else "no"
        high_score_trend_only_text = "yes" if any(row.get("high_score_block_category") == "trend_only" for row in high_score_recent_24h_rows) else "no"
        high_score_alpha6_sell_text = "yes" if any(row.get("high_score_block_category") == "alpha6_sell" or row.get("alpha6_side") == "sell" for row in high_score_recent_24h_rows) else "no"
        high_score_skipped_label_text = "yes"
    else:
        high_score_symbols_text = "none"
        high_score_gate_text = "not_applicable_no_recent_24h_high_score_blocked_targets"
        high_score_eth_seen_text = "no"
        high_score_trend_only_text = "not_applicable_no_recent_24h_high_score_blocked_targets"
        high_score_alpha6_sell_text = "not_applicable_no_recent_24h_high_score_blocked_targets"
        high_score_skipped_label_text = "not_applicable_no_recent_24h_high_score_blocked_targets"

    eth_high_score_outcome_rows = [
        row for row in high_score_blocked_outcome_rows if row.get("symbol") == "ETH/USDT"
    ]

    def avg_net_text(rows, horizon):
        values = [as_float(row.get(f"label_{horizon}h_net_bps")) for row in rows]
        usable = [value for value in values if value is not None]
        return f"{round(sum(usable) / len(usable), 6)}" if usable else not_obs

    if eth_high_score_outcome_rows:
        eth_high_score_count_text = str(len(eth_high_score_outcome_rows))
        eth_high_score_perf_text = ", ".join(
            f"{horizon}h={avg_net_text(eth_high_score_outcome_rows, horizon)}"
            for horizon in (4, 8, 12, 24)
        )
        eth_high_score_relax_gate_text = (
            "diagnostic_only_review_required"
            if any(avg_net_text(eth_high_score_outcome_rows, horizon) != not_obs for horizon in (4, 8, 12, 24))
            else "not_observable_no_matured_labels"
        )
    else:
        eth_high_score_count_text = "0"
        eth_high_score_perf_text = "not_observable_no_eth_samples"
        eth_high_score_relax_gate_text = "not_observable_no_eth_samples"

    def high_score_forward_summary_text():
        if not high_score_blocked_outcomes_by_symbol:
            return "not_observable_no_matured_labels"
        parts = []
        for row in high_score_blocked_outcomes_by_symbol[:12]:
            symbol = row.get("symbol") or not_obs
            reason = row.get("skip_reason") or not_obs
            parts.append(
                f"{symbol}/{reason}: "
                f"4h={row.get('avg_4h_net_bps', not_obs)}, "
                f"8h={row.get('avg_8h_net_bps', not_obs)}, "
                f"12h={row.get('avg_12h_net_bps', not_obs)}, "
                f"24h={row.get('avg_24h_net_bps', not_obs)}"
            )
        return "; ".join(parts)

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
        avg_parts = []
        win_parts = []
        for horizon in (4, 8, 12, 24):
            values = [as_float(row.get(f"label_{horizon}h_net_bps")) for row in rows]
            usable = [value for value in values if value is not None]
            avg = round(sum(usable) / len(usable), 6) if usable else not_obs
            win = round(sum(1 for value in usable if value > 0) / len(usable), 6) if usable else not_obs
            avg_parts.append(f"{horizon}h={avg}")
            win_parts.append(f"{horizon}h={win}")
        return f"{symbol}: count={len(rows)}, avg_net_bps " + ", ".join(avg_parts) + "; win_rate " + ", ".join(win_parts)

    alt_impulse_future_probe_text = (
        "diagnostic_only_review_required"
        if any(row.get("label_status") == "complete" for row in alt_impulse_shadow_rows)
        else ("not_observable_no_matured_labels" if alt_impulse_shadow_rows else "not_applicable_no_shadow_samples")
    )

    readme = [
        f"# V5 live follow-up bundle {STAMP}",
        "",
        "This bundle contains read-only, sanitized production evidence for daily live follow-up.",
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
        "## Negative expectancy 口径检查",
        f"- consistency rows: {len(negative_consistency_rows)}",
        f"- mismatch_suspected_count: {negative_expectancy_mismatch_count}",
        f"- high issue present: {'yes' if negative_expectancy_mismatch_count else 'no'}",
        "",
        "## 高分但未成交目标",
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
        f"- ETH 4h/8h/12h/24h net bps: {eth_high_score_perf_text}",
        f"- 是否支持放松 gate: {eth_high_score_relax_gate_text}",
        "",
        "## ALT impulse shadow",
        f"- ETH/USDT: {alt_impulse_symbol_line('ETH/USDT')}",
        f"- SOL/USDT: {alt_impulse_symbol_line('SOL/USDT')}",
        f"- BNB/USDT: {alt_impulse_symbol_line('BNB/USDT')}",
        f"- 是否支持未来 live probe: {alt_impulse_future_probe_text}",
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
    "copy exact reports state files",
    "copy reports/runs[/prod] last72h lightweight files",
    "copy sanitized log tails last72h",
])
_, branch, _ = run_readonly("git rev-parse --abbrev-ref HEAD 2>/dev/null || echo not_git")
_, commit, _ = run_readonly("git rev-parse HEAD 2>/dev/null || echo not_git")

copy_sanitized("configs/live_prod.yaml", "raw/config_live_prod.yaml", required=True)
for src_rel, dest_rel, required in STATE_FILES:
    copy_sanitized(src_rel, dest_rel, required=required)
copy_current_reports()
copied_runs, recent_24_decisions = copy_recent_runs()
copied_logs = copy_logs()
summary_meta = build_summaries(copied_runs, copied_logs, recent_24_decisions)

sanity = {
    "raw/state/kill_switch.json exists": (OUT / "raw/state/kill_switch.json").is_file(),
    "raw/state/reconcile_status.json exists": (OUT / "raw/state/reconcile_status.json").is_file(),
    "raw/recent_runs has recent24h decision_audit.json": bool(recent_24_decisions),
    "contains raw/state": any((OUT / "raw/state").glob("*.json")),
    "contains raw/recent_runs": any((OUT / "raw/recent_runs").glob("*/decision_audit.json")),
    "contains summaries/probe_lifecycle_audit.csv": (OUT / "summaries/probe_lifecycle_audit.csv").is_file(),
    "contains summaries/issues_to_fix.json": (OUT / "summaries/issues_to_fix.json").is_file(),
    "warnings": [],
    "high_issue_count": int(summary_meta.get("high_issue_count", 0)),
    "medium_issue_count": int(summary_meta.get("medium_issue_count", 0)),
    "no .env files": True,
    "no unredacted secret assignments": True,
}
if summary_meta.get("roundtrip_warning"):
    sanity["warnings"].append("trades exist but roundtrip/open trade rows are missing")
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
    "contains summaries/probe_lifecycle_audit.csv",
    "contains summaries/issues_to_fix.json",
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
    "git_branch": branch.splitlines()[0] if branch else "not_git",
    "git_commit": commit.splitlines()[0] if commit else "not_git",
    "sampling_end_utc": NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
