from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SUPPORTED_SYMBOLS = {"BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"}
LABEL_HORIZONS = (4, 8, 12, 24)
LABEL_TIME_FIELDS = (
    "ts_utc",
    "timestamp",
    "ts",
    "decision_ts",
    "entry_ts",
    "candidate_ts",
    "bar_ts",
    "signal_ts",
    "window_start_ts",
)
LABEL_TIME_MS_FIELDS = (
    "entry_ts_ms",
    "ts_ms",
    "timestamp_ms",
    "decision_ts_ms",
    "candidate_ts_ms",
)
LABEL_NEAREST_MAX_SKEW_MS = 10 * 60 * 1000
LABEL_SAME_RUN_BAR_START_MAX_SKEW_MS = 75 * 60 * 1000
LABEL_BY_RUN_SYMBOL_KEY = ("__label_by_run_symbol__", "", "")
LABEL_BY_SYMBOL_KEY = ("__label_by_symbol__", "", "")
CONFLICT_FIELDS = (
    "run_id",
    "ts_utc",
    "symbol",
    "final_score",
    "alpha6_score",
    "alpha6_side",
    "f3_vol_adj_ret",
    "f4_volume_expansion",
    "f5_rsi_trend_confirm",
    "expected_edge_bps",
    "required_edge_bps",
    "cost_gate_verified",
    "final_decision",
    "block_reason",
    "no_signal_reason",
    "negative_expectancy_net_bps",
    "negative_expectancy_fast_fail_net_bps",
    "future_4h_net_bps",
    "future_8h_net_bps",
    "future_12h_net_bps",
    "future_24h_net_bps",
    "max_future_net_bps",
    "best_future_horizon_hours",
    "material_profit_flag",
    "label_join_attempted",
    "label_join_key",
    "label_join_match_type",
    "label_join_time_skew_sec",
    "label_join_failure_reason",
    "label_4h_status",
    "label_8h_status",
    "label_12h_status",
    "label_24h_status",
    "any_label_complete",
    "all_labels_complete",
    "label_status",
    "missed_profit_flag",
)


def normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("-", "/")
    if text.endswith("/USDT"):
        return text
    if text.endswith("USDT") and "/" not in text and len(text) > 4:
        return f"{text[:-4]}/USDT"
    return text


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "not_observable", "pending"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on", "passed", "pass"}


def first_observed(*values: Any, default: str = "not_observable") -> Any:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"none", "null", "nan", "not_observable"}:
            return value
    return default


def label_status_for_future(value: Any) -> str:
    return "complete" if as_float(value) is not None else "pending"


def aggregate_label_status(statuses: Iterable[str]) -> str:
    status_list = [str(status or "").strip() for status in statuses]
    completed = [status for status in status_list if status == "complete"]
    if completed and len(completed) == len(status_list):
        return "complete"
    if completed:
        return "partial_complete"
    return "pending"


def best_future_net_bps(futures: Mapping[int, Any]) -> tuple[Any, Any, bool]:
    observed: list[tuple[int, float]] = []
    for horizon, value in futures.items():
        parsed = as_float(value)
        if parsed is not None:
            observed.append((int(horizon), parsed))
    if not observed:
        return "not_observable", "not_observable", False
    best_horizon, best_value = max(observed, key=lambda item: item[1])
    return best_value, best_horizon, best_value >= 50.0


def _time_value_ms(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "not_observable", "pending"}:
        return None
    try:
        if text.replace(".", "", 1).isdigit():
            raw = float(text)
            if raw <= 0:
                return None
            if raw < 10_000_000_000:
                raw *= 1000.0
            return int(raw)
    except Exception:
        pass
    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        dt_value = datetime.fromisoformat(normalized)
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        return int(dt_value.astimezone(timezone.utc).timestamp() * 1000.0)
    except Exception:
        return None


def label_time_candidates_ms(row: Mapping[str, Any]) -> list[int]:
    values: list[int] = []
    for field in LABEL_TIME_FIELDS:
        parsed = _time_value_ms(row.get(field))
        if parsed is not None:
            values.append(parsed)
    for field in LABEL_TIME_MS_FIELDS:
        parsed = _time_value_ms(row.get(field))
        if parsed is not None:
            values.append(parsed)
    out: list[int] = []
    seen: set[int] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _iso_from_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def label_join_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    times = label_time_candidates_ms(row)
    return (
        str(row.get("run_id") or "").strip(),
        normalize_symbol(row.get("symbol")),
        _iso_from_ms(times[0]) if times else "",
    )


def label_join_keys(row: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    run_id = str(row.get("run_id") or "").strip()
    symbol = normalize_symbol(row.get("symbol"))
    if not run_id or not symbol:
        return []
    return [(run_id, symbol, _iso_from_ms(value)) for value in label_time_candidates_ms(row)]


def label_future_value(row: Mapping[str, Any], horizon: int) -> Any:
    return first_observed(
        row.get(f"future_{horizon}h_net_bps"),
        row.get(f"label_{horizon}h_net_bps"),
        row.get(f"label_{horizon}h_after_cost_bps"),
        row.get(f"paper_pnl_bps_{horizon}h"),
        default="not_observable",
    )


def _observed_label_count(row: Mapping[str, Any]) -> int:
    return sum(1 for horizon in LABEL_HORIZONS if as_float(label_future_value(row, horizon)) is not None)


def _merge_label_row(existing: Mapping[str, Any], row: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for field, value in row.items():
        if value in (None, ""):
            continue
        if merged.get(field) in (None, "", "not_observable"):
            merged[field] = value
    return merged


def build_label_index(rows: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str, str], Any]:
    index: dict[tuple[str, str, str], Any] = {}
    by_run_symbol: dict[tuple[str, str], list[tuple[int, dict[str, Any]]]] = {}
    by_symbol: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for row in rows:
        keys = label_join_keys(row)
        if not keys:
            continue
        observed_count = _observed_label_count(row)
        if observed_count <= 0:
            continue
        payload = dict(row)
        for key in keys:
            existing = index.get(key)
            if existing is None:
                index[key] = payload
            elif observed_count > _observed_label_count(existing):
                index[key] = _merge_label_row(existing, payload)
        run_symbol = (keys[0][0], keys[0][1])
        for time_ms in label_time_candidates_ms(row):
            by_run_symbol.setdefault(run_symbol, []).append((time_ms, payload))
            by_symbol.setdefault(keys[0][1], []).append((time_ms, payload))
    index[LABEL_BY_RUN_SYMBOL_KEY] = by_run_symbol
    index[LABEL_BY_SYMBOL_KEY] = by_symbol
    return index


def label_row_for(
    row: Mapping[str, Any],
    label_index: Mapping[tuple[str, str, str], Any],
) -> Mapping[str, Any]:
    label_row, _diagnostics = label_join_diagnostics(row, label_index)
    return label_row


def label_join_diagnostics(
    row: Mapping[str, Any],
    label_index: Mapping[tuple[str, str, str], Any],
) -> tuple[Mapping[str, Any], dict[str, Any]]:
    run_id = str(row.get("run_id") or "").strip()
    raw_symbol = str(row.get("symbol") or "").strip()
    symbol = normalize_symbol(raw_symbol)
    times = label_time_candidates_ms(row)
    keys = label_join_keys(row)
    primary_key = keys[0] if keys else (run_id, symbol, _iso_from_ms(times[0]) if times else "")
    diagnostics: dict[str, Any] = {
        "label_join_attempted": "true" if run_id and symbol and times else "false",
        "label_join_key": "|".join(str(part) for part in primary_key),
        "label_join_match_type": "none",
        "label_join_time_skew_sec": "not_observable",
        "label_join_failure_reason": "",
    }
    if not run_id:
        diagnostics["label_join_failure_reason"] = "missing_run_id"
        return {}, diagnostics
    if not symbol:
        diagnostics["label_join_failure_reason"] = "missing_symbol"
        return {}, diagnostics
    if not times:
        diagnostics["label_join_failure_reason"] = "missing_ts"
        return {}, diagnostics
    for key in label_join_keys(row):
        label_row = label_index.get(key)
        if isinstance(label_row, Mapping):
            diagnostics["label_join_match_type"] = "exact"
            diagnostics["label_join_time_skew_sec"] = 0
            return label_row, diagnostics
    by_run_symbol = label_index.get(LABEL_BY_RUN_SYMBOL_KEY)
    same_run_candidates: list[tuple[int, Mapping[str, Any]]] = []
    if isinstance(by_run_symbol, Mapping):
        same_run_candidates.extend(by_run_symbol.get((run_id, symbol), []))
    best_same_run = _nearest_label_candidate(times, same_run_candidates)
    if best_same_run is not None:
        skew, candidate_row = best_same_run
        if skew <= LABEL_NEAREST_MAX_SKEW_MS:
            diagnostics["label_join_match_type"] = "nearest_same_run_symbol"
            diagnostics["label_join_time_skew_sec"] = round(skew / 1000.0, 3)
            return candidate_row, diagnostics
        if skew <= LABEL_SAME_RUN_BAR_START_MAX_SKEW_MS:
            diagnostics["label_join_match_type"] = "same_run_symbol_bar_start_drift"
            diagnostics["label_join_time_skew_sec"] = round(skew / 1000.0, 3)
            return candidate_row, diagnostics
        diagnostics["label_join_failure_reason"] = "nearest_label_too_far"
        diagnostics["label_join_time_skew_sec"] = round(skew / 1000.0, 3)
        return {}, diagnostics
    # V5 run_id can drift by one hour around UTC/local run folders while labels
    # keep the rounded signal timestamp. Fall back to symbol+nearby timestamp so
    # research diagnostics do not stay pending when the sample is otherwise the
    # same closed-bar candidate.
    by_symbol = label_index.get(LABEL_BY_SYMBOL_KEY)
    symbol_candidates: list[tuple[int, Mapping[str, Any]]] = []
    if isinstance(by_symbol, Mapping):
        symbol_candidates.extend(by_symbol.get(symbol, []))
    best_symbol = _nearest_label_candidate(times, symbol_candidates)
    if best_symbol is not None:
        skew, candidate_row = best_symbol
        if skew <= LABEL_NEAREST_MAX_SKEW_MS:
            diagnostics["label_join_match_type"] = "nearest_symbol_only"
            diagnostics["label_join_time_skew_sec"] = round(skew / 1000.0, 3)
            return candidate_row, diagnostics
        diagnostics["label_join_failure_reason"] = "nearest_label_too_far"
        diagnostics["label_join_time_skew_sec"] = round(skew / 1000.0, 3)
        return {}, diagnostics
    if raw_symbol and raw_symbol.upper().replace("-", "/") != symbol:
        diagnostics["label_join_failure_reason"] = "symbol_normalization_mismatch"
    else:
        diagnostics["label_join_failure_reason"] = "no_label_same_run_symbol"
    return {}, diagnostics


def _nearest_label_candidate(
    times: Sequence[int],
    candidates: Sequence[tuple[int, Mapping[str, Any]]],
) -> tuple[int, Mapping[str, Any]] | None:
    if not times or not candidates:
        return None
    best: tuple[int, Mapping[str, Any]] | None = None
    for candidate_time, candidate_row in candidates:
        skew = min(abs(candidate_time - value) for value in times)
        if best is None or skew < best[0] or (
            skew == best[0] and _observed_label_count(candidate_row) > _observed_label_count(best[1])
        ):
            best = (skew, candidate_row)
    return best


def future_value_for_horizon(
    row: Mapping[str, Any],
    label_row: Mapping[str, Any],
    horizon: int,
    future_net_bps: Mapping[int, Any] | Mapping[str, Any],
) -> Any:
    return first_observed(
        future_net_bps.get(horizon),
        future_net_bps.get(str(horizon)),
        label_future_value(row, horizon),
        label_future_value(label_row, horizon),
        default="not_observable",
    )


def is_final_score_alpha6_conflict_candidate(
    row: Mapping[str, Any],
    *,
    symbols: Iterable[str] = SUPPORTED_SYMBOLS,
) -> bool:
    allowed = {normalize_symbol(symbol) for symbol in symbols}
    if normalize_symbol(row.get("symbol")) not in allowed:
        return False
    if str(row.get("alpha6_side") or "").strip().lower() != "buy":
        return False
    alpha6_score = as_float(row.get("alpha6_score"))
    if alpha6_score is None or alpha6_score < 0.9:
        return False
    expected = as_float(row.get("expected_edge_bps"))
    required = as_float(row.get("required_edge_bps"))
    if expected is None or required is None or expected <= required:
        return False
    if not truthy(row.get("cost_gate_verified")):
        return False
    final_score = as_float(row.get("final_score"))
    final_decision = str(row.get("final_decision") or "").strip().lower()
    return (final_score is not None and final_score < 0.0) or final_decision in {"no_order", "blocked"}


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            return [dict(row) for row in csv.DictReader(fh)]
    except Exception:
        return []


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
    except Exception:
        return rows
    return rows


def _decision_audit_candidate_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    run_id = str(data.get("run_id") or path.parent.name) if isinstance(data, dict) else path.parent.name
    rows: list[dict[str, Any]] = []

    def collect(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("symbol") and (
                "alpha6_score" in value
                or "final_score" in value
                or "final_decision" in value
                or "expected_edge_bps" in value
            ):
                row = dict(value)
                row.setdefault("run_id", run_id)
                rows.append(row)
                return
            for item in value.values():
                collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)

    collect(data)
    return rows


def load_report_input_rows(root: Path) -> list[dict[str, Any]]:
    """Load candidate-like rows from the local bundle/root without changing live state."""
    root = Path(root)
    rows: list[dict[str, Any]] = []
    csv_paths = [
        root / "candidate_snapshot.csv",
        root / "reports" / "candidate_snapshot.csv",
        root / "raw" / "reports" / "candidate_snapshot.csv",
    ]
    csv_paths.extend(sorted((root / "raw" / "recent_runs").glob("*/candidate_snapshot.csv")))
    csv_paths.extend(sorted((root / "reports" / "runs" / "prod").glob("*/candidate_snapshot.csv")))
    for path in csv_paths:
        for row in _read_csv(path):
            row.setdefault("source_path", str(path))
            rows.append(row)
    for path in (
        root / "skipped_candidate_labels.jsonl",
        root / "reports" / "skipped_candidate_labels.jsonl",
        root / "raw" / "reports" / "skipped_candidate_labels.jsonl",
    ):
        for row in _read_jsonl(path):
            row.setdefault("source_path", str(path))
            rows.append(row)
    for path in sorted((root / "reports" / "runs" / "prod").glob("*/decision_audit.json")):
        rows.extend(_decision_audit_candidate_rows(path))
    return _dedupe_rows(rows)


def _dedupe_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str]] = set()
    by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    out: list[dict[str, Any]] = []
    for row in rows:
        key = (
            str(row.get("run_id") or ""),
            normalize_symbol(row.get("symbol")),
            str(first_observed(row.get("ts_utc"), row.get("timestamp"), row.get("ts"), default="")),
            str(first_observed(row.get("strategy_candidate"), row.get("entry_reason"), default="")),
        )
        if key in seen:
            existing = by_key.get(key)
            if existing is not None:
                for field, value in row.items():
                    if value in (None, ""):
                        continue
                    if existing.get(field) in (None, "", "not_observable"):
                        existing[field] = value
            continue
        seen.add(key)
        item = dict(row)
        by_key[key] = item
        out.append(item)
    return out


def build_conflict_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    future_net_bps: Mapping[int, Any] | None = None,
    negative_expectancy_stats: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    row_list = [dict(row) for row in rows]
    label_index = build_label_index(row_list)
    out: list[dict[str, Any]] = []
    future_net_bps = future_net_bps or {}
    negative_expectancy_stats = negative_expectancy_stats or {}
    for row in row_list:
        if not is_final_score_alpha6_conflict_candidate(row):
            continue
        symbol = normalize_symbol(row.get("symbol"))
        label_row, label_join = label_join_diagnostics(row, label_index)
        futures = {
            h: future_value_for_horizon(row, label_row, h, future_net_bps)
            for h in LABEL_HORIZONS
        }
        label_statuses = {h: label_status_for_future(futures[h]) for h in LABEL_HORIZONS}
        any_label_complete = any(status == "complete" for status in label_statuses.values())
        all_labels_complete = all(status == "complete" for status in label_statuses.values())
        max_future, best_horizon, material_profit = best_future_net_bps(futures)
        neg = negative_expectancy_stats.get(symbol)
        if isinstance(neg, Mapping):
            neg_net = first_observed(neg.get("net_expectancy_bps"), neg.get("negexp_net_expectancy_bps"))
            neg_fast = first_observed(
                neg.get("fast_fail_net_expectancy_bps"),
                neg.get("negexp_fast_fail_net_expectancy_bps"),
            )
        else:
            neg_net = "not_observable"
            neg_fast = "not_observable"
        out.append(
            {
                "run_id": first_observed(row.get("run_id")),
                "ts_utc": first_observed(row.get("ts_utc"), row.get("timestamp"), row.get("ts")),
                "symbol": symbol,
                "final_score": first_observed(row.get("final_score")),
                "alpha6_score": first_observed(row.get("alpha6_score")),
                "alpha6_side": first_observed(row.get("alpha6_side")),
                "f3_vol_adj_ret": first_observed(row.get("f3_vol_adj_ret"), row.get("f3")),
                "f4_volume_expansion": first_observed(row.get("f4_volume_expansion"), row.get("f4")),
                "f5_rsi_trend_confirm": first_observed(row.get("f5_rsi_trend_confirm"), row.get("f5")),
                "expected_edge_bps": first_observed(row.get("expected_edge_bps")),
                "required_edge_bps": first_observed(row.get("required_edge_bps")),
                "cost_gate_verified": first_observed(row.get("cost_gate_verified")),
                "final_decision": first_observed(row.get("final_decision")),
                "block_reason": first_observed(row.get("block_reason")),
                "no_signal_reason": first_observed(row.get("no_signal_reason")),
                "negative_expectancy_net_bps": neg_net,
                "negative_expectancy_fast_fail_net_bps": neg_fast,
                "future_4h_net_bps": futures[4],
                "future_8h_net_bps": futures[8],
                "future_12h_net_bps": futures[12],
                "future_24h_net_bps": futures[24],
                "max_future_net_bps": max_future,
                "best_future_horizon_hours": best_horizon,
                "material_profit_flag": str(material_profit).lower(),
                "label_join_attempted": label_join["label_join_attempted"],
                "label_join_key": label_join["label_join_key"],
                "label_join_match_type": label_join["label_join_match_type"],
                "label_join_time_skew_sec": label_join["label_join_time_skew_sec"],
                "label_join_failure_reason": label_join["label_join_failure_reason"],
                "label_4h_status": label_statuses[4],
                "label_8h_status": label_statuses[8],
                "label_12h_status": label_statuses[12],
                "label_24h_status": label_statuses[24],
                "any_label_complete": str(any_label_complete).lower(),
                "all_labels_complete": str(all_labels_complete).lower(),
                "label_status": aggregate_label_status(label_statuses.values()),
                "missed_profit_flag": str(material_profit).lower(),
            }
        )
    out.sort(key=lambda item: (str(item.get("ts_utc") or ""), str(item.get("symbol") or "")))
    return out


def write_conflict_report(root: Path, output_path: Path) -> list[dict[str, Any]]:
    rows = build_conflict_rows(load_report_input_rows(root))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CONFLICT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return rows
