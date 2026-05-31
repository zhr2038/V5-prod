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
    "f1",
    "f2",
    "f3",
    "f4",
    "f5",
    "expected_edge_bps",
    "required_edge_bps",
    "final_decision",
    "block_reason",
    "no_signal_reason",
    "negative_expectancy_stats",
    "future_4h_net_bps",
    "future_8h_net_bps",
    "future_12h_net_bps",
    "future_24h_net_bps",
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
        observed = [as_float(value) for value in futures.values()]
        observed = [value for value in observed if value is not None]
        out.append(
            {
                "run_id": first_observed(row.get("run_id")),
                "ts_utc": first_observed(row.get("ts_utc"), row.get("timestamp"), row.get("ts")),
                "symbol": symbol,
                "final_score": first_observed(row.get("final_score")),
                "alpha6_score": first_observed(row.get("alpha6_score")),
                "alpha6_side": first_observed(row.get("alpha6_side")),
                "f1": first_observed(row.get("f1"), row.get("f1_mom_5d")),
                "f2": first_observed(row.get("f2"), row.get("f2_mom_20d")),
                "f3": first_observed(row.get("f3"), row.get("f3_vol_adj_ret")),
                "f4": first_observed(row.get("f4"), row.get("f4_volume_expansion")),
                "f5": first_observed(row.get("f5"), row.get("f5_rsi_trend_confirm")),
                "expected_edge_bps": first_observed(row.get("expected_edge_bps")),
                "required_edge_bps": first_observed(row.get("required_edge_bps")),
                "final_decision": first_observed(row.get("final_decision")),
                "block_reason": first_observed(row.get("block_reason")),
                "no_signal_reason": first_observed(row.get("no_signal_reason")),
                "negative_expectancy_stats": first_observed(negative_expectancy_stats.get(symbol)),
                "future_4h_net_bps": futures[4],
                "future_8h_net_bps": futures[8],
                "future_12h_net_bps": futures[12],
                "future_24h_net_bps": futures[24],
                "missed_profit_flag": str(bool(observed and max(observed) > 0.0)).lower(),
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
