from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SUPPORTED_SYMBOLS = {"BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"}
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
    out: list[dict[str, Any]] = []
    for row in rows:
        key = (
            str(row.get("run_id") or ""),
            normalize_symbol(row.get("symbol")),
            str(first_observed(row.get("ts_utc"), row.get("timestamp"), row.get("ts"), default="")),
            str(first_observed(row.get("strategy_candidate"), row.get("entry_reason"), default="")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(row))
    return out


def build_conflict_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    future_net_bps: Mapping[int, Any] | None = None,
    negative_expectancy_stats: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    future_net_bps = future_net_bps or {}
    negative_expectancy_stats = negative_expectancy_stats or {}
    for row in rows:
        if not is_final_score_alpha6_conflict_candidate(row):
            continue
        symbol = normalize_symbol(row.get("symbol"))
        futures = {h: first_observed(future_net_bps.get(h), default="not_observable") for h in (4, 8, 12, 24)}
        label_statuses = {h: label_status_for_future(futures[h]) for h in (4, 8, 12, 24)}
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
