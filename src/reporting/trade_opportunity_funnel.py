from __future__ import annotations

import csv
import json
import os
import tempfile
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


TRADE_OPPORTUNITY_FUNNEL_SCHEMA_VERSION = "v5.trade_opportunity_funnel.v1"
TRADE_OPPORTUNITY_FUNNEL_FIELDS = (
    "schema_version",
    "run_id",
    "ts_utc",
    "execution_mode",
    "stage_order",
    "stage",
    "input_count",
    "output_count",
    "dropped_count",
    "conversion_rate",
    "entry_output_count",
    "exit_output_count",
    "primary_blocker",
    "blocker_mix",
    "count_source",
    "live_order_effect",
)

STAGES = (
    (10, "market_data"),
    (20, "candidate_scoring"),
    (30, "signal_selection"),
    (40, "risk_target"),
    (50, "local_order_generation"),
    (60, "order_arbitration"),
    (70, "live_preflight"),
    (80, "quant_lab_guard"),
    (90, "exchange_submit"),
    (100, "exchange_fill"),
)

SELECTION_REASON_VOCABULARY = frozenset(
    {
        "below_rank_cutoff",
        "portfolio_limit",
        "duplicate_symbol",
        "correlation_limit",
        "lower_score_same_symbol",
        "invalid_candidate",
        "no_budget",
        "other",
    }
)


def record_order_stage(
    audit: Any,
    stage: str,
    orders: Iterable[Any],
    *,
    blockers: Mapping[str, Any] | None = None,
    applied: bool = True,
) -> None:
    counts = order_stage_counts(orders)
    funnel = dict(getattr(audit, "trade_funnel", {}) or {})
    funnel[stage] = {
        **counts,
        "blockers": _positive_counts(blockers or {}),
        "applied": bool(applied),
    }
    audit.trade_funnel = funnel


def order_stage_counts(orders: Iterable[Any]) -> dict[str, int]:
    materialized = list(orders or [])
    entry = 0
    exit_count = 0
    for order in materialized:
        side = str(_value(order, "side") or "").strip().lower()
        intent = str(_value(order, "intent") or "").strip().upper()
        if side == "sell" or intent in {"CLOSE_LONG", "EXIT", "REDUCE"}:
            exit_count += 1
        else:
            entry += 1
    return {"total": len(materialized), "entry": entry, "exit": exit_count}


def blocker_counts_from_decisions(
    decisions: Iterable[Mapping[str, Any]],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in decisions or []:
        action = str(row.get("action") or row.get("router_action") or "").lower()
        if action not in {"blocked", "skip", "filtered"}:
            continue
        reason = str(
            row.get("reason")
            or row.get("blocked_reason")
            or row.get("block_reason")
            or row.get("reject_reason")
            or "unspecified"
        ).strip()
        counts[reason] += 1
    return dict(counts)


def write_trade_opportunity_funnel(
    *,
    run_dir: str | Path,
    reports_dir: str | Path | None,
    audit: Any,
    lifecycle_rows: Iterable[Mapping[str, Any]],
    execution_mode: str,
    ts_utc: str | None = None,
) -> list[dict[str, Any]]:
    rows = build_trade_opportunity_funnel(
        audit=audit,
        lifecycle_rows=lifecycle_rows,
        execution_mode=execution_mode,
        ts_utc=ts_utc,
    )
    run_path = Path(run_dir)
    _write_csv_atomic(run_path / "trade_opportunity_funnel.csv", rows)
    if reports_dir is not None:
        report_path = Path(reports_dir) / "trade_opportunity_funnel.csv"
        existing = _read_csv(report_path)
        keyed = {
            (str(row.get("run_id") or ""), str(row.get("stage") or "")): row
            for row in existing
        }
        for row in rows:
            keyed[(str(row["run_id"]), str(row["stage"]))] = row
        _write_csv_atomic(report_path, list(keyed.values())[-20_000:])
    return rows


def build_trade_opportunity_funnel(
    *,
    audit: Any,
    lifecycle_rows: Iterable[Mapping[str, Any]],
    execution_mode: str,
    ts_utc: str | None = None,
) -> list[dict[str, Any]]:
    funnel = dict(getattr(audit, "trade_funnel", {}) or {})
    counts = dict(getattr(audit, "counts", {}) or {})
    universe = _int(counts.get("universe"))
    market_available = _int(funnel.get("market_data_available", universe))
    scored = _int(counts.get("scored"))
    selected = _int(counts.get("selected"))
    risk_output = _nonzero_count(getattr(audit, "targets_post_risk", {}) or {})

    local = _stage_counts(funnel, "local_order_generation")
    arbitration = _stage_counts(funnel, "order_arbitration", fallback=local)
    preflight = _stage_counts(funnel, "live_preflight", fallback=arbitration)
    guard = _stage_counts(funnel, "quant_lab_guard", fallback=preflight)
    lifecycle = list(lifecycle_rows or [])
    submitted_entry, submitted_exit, filled_entry, filled_exit = _lifecycle_counts(
        lifecycle,
        execution_mode=execution_mode,
    )
    stage_values = {
        "market_data": (universe, market_available, 0, 0, {}, "market_data_snapshot"),
        "candidate_scoring": (
            market_available,
            scored,
            scored,
            0,
            _audit_blockers(audit, "candidate_scoring"),
            "decision_audit.counts",
        ),
        "signal_selection": (
            scored,
            selected,
            selected,
            0,
            _audit_blockers(audit, "signal_selection"),
            "decision_audit.counts",
        ),
        "risk_target": (
            selected,
            risk_output,
            risk_output,
            0,
            _audit_blockers(audit, "risk_target"),
            "decision_audit.targets_post_risk",
        ),
        "local_order_generation": (
            risk_output,
            local["entry"],
            local["entry"],
            local["exit"],
            _stage_blockers(funnel, "local_order_generation", audit),
            "v5_order_intents",
        ),
        "order_arbitration": (
            local["entry"],
            arbitration["entry"],
            arbitration["entry"],
            arbitration["exit"],
            _stage_blockers(funnel, "order_arbitration", audit),
            "order_arbitrator",
        ),
        "live_preflight": (
            arbitration["entry"],
            preflight["entry"],
            preflight["entry"],
            preflight["exit"],
            _stage_blockers(funnel, "live_preflight", audit),
            "live_preflight",
        ),
        "quant_lab_guard": (
            preflight["entry"],
            guard["entry"],
            guard["entry"],
            guard["exit"],
            _stage_blockers(funnel, "quant_lab_guard", audit),
            "quant_lab_guard",
        ),
        "exchange_submit": (
            guard["entry"],
            submitted_entry,
            submitted_entry,
            submitted_exit,
            _lifecycle_blockers(lifecycle, submitted=False),
            "order_lifecycle",
        ),
        "exchange_fill": (
            submitted_entry,
            filled_entry,
            filled_entry,
            filled_exit,
            _lifecycle_blockers(lifecycle, submitted=True),
            "order_lifecycle",
        ),
    }
    generated_at = ts_utc or datetime.now(UTC).isoformat()
    output: list[dict[str, Any]] = []
    for order, stage in STAGES:
        input_count, output_count, entry_count, exit_count, blockers, source = stage_values[
            stage
        ]
        dropped = max(input_count - output_count, 0)
        normalized_blockers = _blockers_for_stage_loss(blockers, dropped=dropped)
        output.append(
            {
                "schema_version": TRADE_OPPORTUNITY_FUNNEL_SCHEMA_VERSION,
                "run_id": str(getattr(audit, "run_id", "") or ""),
                "ts_utc": generated_at,
                "execution_mode": str(execution_mode or "unknown").lower(),
                "stage_order": order,
                "stage": stage,
                "input_count": input_count,
                "output_count": output_count,
                "dropped_count": dropped,
                "conversion_rate": output_count / input_count if input_count else None,
                "entry_output_count": entry_count,
                "exit_output_count": exit_count,
                "primary_blocker": _primary_blocker(normalized_blockers),
                "blocker_mix": json.dumps(normalized_blockers, sort_keys=True),
                "count_source": source,
                "live_order_effect": "read_only_observability",
            }
        )
    return output


def _audit_blockers(audit: Any, stage: str) -> dict[str, int]:
    counts = dict(getattr(audit, "counts", {}) or {})
    rejects = dict(getattr(audit, "rejects", {}) or {})
    if stage == "risk_target":
        tokens = (
            "risk_off_suppressed",
            "target_zero_after_",
            "risk_block",
            "risk_reject",
        )
    elif stage == "signal_selection":
        decisions = (
            dict(getattr(audit, "trade_funnel", {}) or {}).get(
                "selection_decisions"
            )
            or []
        )
        decision_counts: Counter[str] = Counter()
        for row in decisions:
            if not isinstance(row, Mapping):
                continue
            reason = str(row.get("reason") or "other").strip().lower()
            decision_counts[
                reason if reason in SELECTION_REASON_VOCABULARY else "other"
            ] += 1
        if decision_counts:
            return dict(decision_counts)
        tokens = ("no_signal", "not_selected", "rank_filtered", "selection_block")
    elif stage == "candidate_scoring":
        tokens = (
            "market_data_missing",
            "no_closed_bar",
            "not_scored",
            "scoring_error",
            "provider_error",
        )
    else:
        tokens = (
            "protect_entry",
            "negative_expectancy",
            "cooldown",
            "min_notional",
            "spread_gate",
            "cost_edge",
            "confirmation",
        )
    output = {
        key: _int(value)
        for key, value in {**rejects, **counts}.items()
        if any(token in str(key).lower() for token in tokens) and _int(value) > 0
    }
    return output


def _stage_blockers(funnel: Mapping[str, Any], stage: str, audit: Any) -> dict[str, int]:
    row = funnel.get(stage)
    if isinstance(row, Mapping) and "blockers" in row:
        explicit = row.get("blockers")
        return _positive_counts(explicit if isinstance(explicit, Mapping) else {})
    return _positive_counts(_audit_blockers(audit, stage))


def _stage_counts(
    funnel: Mapping[str, Any],
    stage: str,
    *,
    fallback: Mapping[str, int] | None = None,
) -> dict[str, int]:
    row = funnel.get(stage)
    if not isinstance(row, Mapping):
        return dict(fallback or {"total": 0, "entry": 0, "exit": 0})
    return {
        "total": _int(row.get("total")),
        "entry": _int(row.get("entry")),
        "exit": _int(row.get("exit")),
    }


def _lifecycle_counts(
    rows: Iterable[Mapping[str, Any]],
    *,
    execution_mode: str,
) -> tuple[int, int, int, int]:
    if str(execution_mode or "").lower() != "live":
        return 0, 0, 0, 0
    submitted_entry = submitted_exit = filled_entry = filled_exit = 0
    for row in rows:
        is_exit = _is_exit(row)
        submitted = bool(
            str(row.get("exchange_order_id") or "").strip()
            or str(row.get("submit_ts") or "").strip()
            or str(row.get("order_state") or "").upper()
            not in {"", "DECISION", "LOCAL"}
        )
        filled = _float(row.get("filled_qty")) > 0 or _int(row.get("fill_count")) > 0
        if submitted:
            if is_exit:
                submitted_exit += 1
            else:
                submitted_entry += 1
        if filled:
            if is_exit:
                filled_exit += 1
            else:
                filled_entry += 1
    return submitted_entry, submitted_exit, filled_entry, filled_exit


def _lifecycle_blockers(
    rows: Iterable[Mapping[str, Any]],
    *,
    submitted: bool,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        if _is_exit(row):
            continue
        has_submit = bool(str(row.get("submit_ts") or "").strip())
        has_fill = _float(row.get("filled_qty")) > 0 or _int(row.get("fill_count")) > 0
        if (submitted and has_submit and not has_fill) or (
            not submitted and not has_submit
        ):
            reason = str(
                row.get("last_error_code")
                or row.get("last_error_msg")
                or row.get("order_state")
                or "not_submitted"
            ).strip()
            counts[reason] += 1
    return dict(counts)


def _is_exit(row: Mapping[str, Any]) -> bool:
    return str(row.get("side") or "").lower() == "sell" or str(
        row.get("intent") or ""
    ).upper() in {"CLOSE_LONG", "EXIT", "REDUCE"}


def _nonzero_count(values: Mapping[str, Any]) -> int:
    return sum(abs(_float(value)) > 1e-12 for value in values.values())


def _positive_counts(values: Mapping[str, Any]) -> dict[str, int]:
    return {
        str(key): _int(value)
        for key, value in values.items()
        if _int(value) > 0
    }


def _blockers_for_stage_loss(
    values: Mapping[str, Any], *, dropped: int
) -> dict[str, int]:
    """Report only reasons that explain an actual loss at this funnel stage."""

    if dropped <= 0:
        return {}
    blockers = _positive_counts(values)
    attributed = sum(blockers.values())
    if attributed < dropped:
        blockers["other"] = blockers.get("other", 0) + dropped - attributed
    return blockers


def _primary_blocker(values: Mapping[str, int]) -> str:
    if not values:
        return ""
    return max(values.items(), key=lambda item: (item[1], item[0]))[0]


def _value(obj: Any, field: str) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(field)
    return getattr(obj, field, None)


def _int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return number if number == number else 0.0


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle) if row]
    except (OSError, csv.Error):
        return []


def _write_csv_atomic(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=list(TRADE_OPPORTUNITY_FUNNEL_FIELDS),
                extrasaction="ignore",
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise
