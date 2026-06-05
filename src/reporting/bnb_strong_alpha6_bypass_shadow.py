from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.reporting.final_score_alpha6_conflict import (
    aggregate_label_status,
    as_float,
    best_future_net_bps,
    build_label_index,
    first_observed,
    label_status_for_future,
    future_value_for_horizon,
    label_row_for,
    LABEL_HORIZONS,
    normalize_symbol,
    truthy,
)


STRATEGY_ID = "BNB_STRONG_ALPHA6_BYPASS_SHADOW_V1"
BYPASS_SHADOW_FIELDS = (
    "run_id",
    "ts_utc",
    "strategy_id",
    "symbol",
    "would_bypass",
    "alpha6_score",
    "f3",
    "f4",
    "f5",
    "expected_edge_bps",
    "required_edge_bps",
    "final_score",
    "final_decision",
    "block_reason",
    "no_signal_reason",
    "negative_expectancy_blocked",
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
    "outcome",
    "live_order_effect",
)


def is_bnb_strong_alpha6_bypass_candidate(row: Mapping[str, Any]) -> bool:
    if normalize_symbol(row.get("symbol")) != "BNB/USDT":
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
    f3 = as_float(first_observed(row.get("f3"), row.get("f3_vol_adj_ret")))
    f4 = as_float(first_observed(row.get("f4"), row.get("f4_volume_expansion")))
    return bool((f4 is not None and f4 >= 1.0) or (f3 is not None and f3 >= 10.0))


def shadow_outcome(values: Iterable[Any]) -> str:
    max_future, _best_horizon, material_profit = best_future_net_bps({idx: value for idx, value in enumerate(values)})
    if as_float(max_future) is not None:
        return "material_profit_shadow" if material_profit else "non_material_profit_shadow"
    if any(str(value or "").strip().lower() == "pending" for value in values):
        return "pending"
    return "not_observable"


def build_bnb_strong_alpha6_bypass_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    future_net_bps: Mapping[int, Any] | None = None,
) -> list[dict[str, Any]]:
    row_list = [dict(row) for row in rows]
    label_index = build_label_index(row_list)
    out: list[dict[str, Any]] = []
    future_net_bps = future_net_bps or {}
    for row in row_list:
        if not is_bnb_strong_alpha6_bypass_candidate(row):
            continue
        label_row = label_row_for(row, label_index)
        futures = {
            h: future_value_for_horizon(row, label_row, h, future_net_bps)
            for h in LABEL_HORIZONS
        }
        label_statuses = {h: label_status_for_future(futures[h]) for h in LABEL_HORIZONS}
        any_label_complete = any(status == "complete" for status in label_statuses.values())
        all_labels_complete = all(status == "complete" for status in label_statuses.values())
        max_future, best_horizon, material_profit = best_future_net_bps(futures)
        block_text = " ".join(
            str(value or "").strip().lower()
            for value in (row.get("block_reason"), row.get("no_signal_reason"), row.get("final_decision"))
        )
        out.append(
            {
                "run_id": first_observed(row.get("run_id")),
                "ts_utc": first_observed(row.get("ts_utc"), row.get("timestamp"), row.get("ts")),
                "strategy_id": STRATEGY_ID,
                "symbol": "BNB/USDT",
                "would_bypass": "true",
                "alpha6_score": first_observed(row.get("alpha6_score")),
                "f3": first_observed(row.get("f3"), row.get("f3_vol_adj_ret")),
                "f4": first_observed(row.get("f4"), row.get("f4_volume_expansion")),
                "f5": first_observed(row.get("f5"), row.get("f5_rsi_trend_confirm")),
                "expected_edge_bps": first_observed(row.get("expected_edge_bps")),
                "required_edge_bps": first_observed(row.get("required_edge_bps")),
                "final_score": first_observed(row.get("final_score")),
                "final_decision": first_observed(row.get("final_decision")),
                "block_reason": first_observed(row.get("block_reason")),
                "no_signal_reason": first_observed(row.get("no_signal_reason")),
                "negative_expectancy_blocked": str("negative_expectancy" in block_text).lower(),
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
                "outcome": shadow_outcome([futures[4], futures[8], futures[12], futures[24]]),
                "live_order_effect": "read_only_no_live_order",
            }
        )
    out.sort(key=lambda item: (str(item.get("ts_utc") or ""), str(item.get("run_id") or "")))
    return out


def write_bnb_strong_alpha6_bypass_report(rows: Iterable[Mapping[str, Any]], output_path: Path) -> list[dict[str, Any]]:
    report_rows = build_bnb_strong_alpha6_bypass_rows(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=BYPASS_SHADOW_FIELDS)
        writer.writeheader()
        writer.writerows(report_rows)
    return report_rows
