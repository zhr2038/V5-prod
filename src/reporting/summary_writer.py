from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from src.reporting.metrics import (
    compute_equity_metrics,
    compute_trade_metrics,
    read_equity_jsonl,
    read_trades_csv,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_run_dir(run_dir: str | Path) -> Path:
    resolved = Path(run_dir)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


def _negative_expectancy_summary_fields(rd: Path) -> Dict[str, int]:
    decision_audit_path = rd / "decision_audit.json"
    if not decision_audit_path.exists():
        return {
            "negative_expectancy_penalty_count": 0,
            "negative_expectancy_cooldown_count": 0,
            "negative_expectancy_open_block_count": 0,
            "negative_expectancy_fast_fail_open_block_count": 0,
            "protect_negative_expectancy_short_cycle_block_count": 0,
            "negative_expectancy_probation_release_count": 0,
        }

    try:
        payload = json.loads(decision_audit_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    counts = payload.get("counts", {}) if isinstance(payload, dict) else {}

    return {
        "negative_expectancy_penalty_count": int(counts.get("negative_expectancy_score_penalty", 0) or 0),
        "negative_expectancy_cooldown_count": int(counts.get("negative_expectancy_cooldown", 0) or 0),
        "negative_expectancy_open_block_count": int(counts.get("negative_expectancy_open_block", 0) or 0),
        "negative_expectancy_fast_fail_open_block_count": int(
            counts.get("negative_expectancy_fast_fail_open_block", 0) or 0
        ),
        "protect_negative_expectancy_short_cycle_block_count": int(
            counts.get("protect_negative_expectancy_short_cycle_block_count", 0) or 0
        ),
        "negative_expectancy_probation_release_count": 0,
    }


def _quant_lab_summary_fields(rd: Path) -> Dict[str, Any]:
    decision_audit_path = rd / "decision_audit.json"
    empty = {
        "enabled": False,
        "permission": None,
        "final_permission": None,
        "permission_decision": None,
        "effective_decision": None,
        "cost_model_version": None,
        "gate_version": None,
        "fallback_used": False,
        "cost_request_count": 0,
        "cost_fallback_count": 0,
        "filtered_by_cost_count": 0,
        "filtered_by_permission_count": 0,
    }
    if not decision_audit_path.exists():
        return {"quant_lab": empty}

    try:
        payload = json.loads(decision_audit_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    ql = payload.get("quant_lab", {}) if isinstance(payload, dict) else {}
    if not isinstance(ql, dict) or not ql:
        return {"quant_lab": empty}

    permission = ql.get("permission") if isinstance(ql.get("permission"), dict) else {}
    filtered_orders = ql.get("filtered_orders") if isinstance(ql.get("filtered_orders"), list) else []
    cost_estimates = ql.get("cost_estimates") if isinstance(ql.get("cost_estimates"), list) else []
    orders_filtered = [row for row in filtered_orders if isinstance(row, dict) and row.get("filtered")]
    permission_value = ql.get("permission") if isinstance(ql.get("permission"), str) else permission.get("decision")
    final_permission = (
        ql.get("final_permission")
        or ql.get("effective_decision")
        or permission.get("effective_decision")
        or permission_value
    )
    summary = {
        "enabled": bool(ql.get("enabled", True)),
        "permission": permission_value,
        "final_permission": final_permission,
        "permission_decision": permission_value,
        "effective_decision": final_permission,
        "cost_model_version": ql.get("cost_model_version"),
        "gate_version": ql.get("gate_version"),
        "fallback_used": bool(ql.get("fallback_used") or permission.get("fallback_used")),
        "fail_policy": ql.get("fail_policy") or permission.get("fail_policy"),
        "cost_request_count": int(ql.get("cost_request_count", len(cost_estimates)) or 0),
        "cost_fallback_count": int(
            ql.get(
                "cost_fallback_count",
                len([row for row in cost_estimates if isinstance(row, dict) and row.get("fallback_used")]),
            )
            or 0
        ),
        "filtered_by_cost_count": int(ql.get("filtered_by_cost_count", 0) or 0),
        "filtered_by_permission_count": int(ql.get("filtered_by_permission_count", len(orders_filtered)) or 0),
        "orders_filtered": len(orders_filtered) or int(
            ql.get("filtered_by_cost_count", 0) or 0
        ) + int(ql.get("filtered_by_permission_count", 0) or 0),
        "buy_orders_filtered": len(
            [row for row in orders_filtered if str(row.get("side", "")).lower() == "buy"]
        ),
        "cost_estimate_count": len(cost_estimates) or int(ql.get("cost_request_count", 0) or 0),
        "legacy_cost_fallback_count": len(
            [row for row in cost_estimates if isinstance(row, dict) and row.get("fallback_used")]
        ),
    }
    return {"quant_lab": summary}


def write_summary(
    run_dir: str,
    window_start_ts: int | None = None,
    window_end_ts: int | None = None,
) -> Dict[str, Any]:
    rd = _resolve_run_dir(run_dir)
    eq_rows = read_equity_jsonl(str(rd / "equity.jsonl"))
    trades = read_trades_csv(str(rd / "trades.csv"))

    avg_equity = None
    if eq_rows:
        xs = [float(r.get("equity") or 0.0) for r in eq_rows]
        avg_equity = sum(xs) / len(xs) if xs else None

    eqm = compute_equity_metrics(eq_rows)
    tm = compute_trade_metrics(trades, avg_equity=avg_equity)
    
    # 确定窗口时间：优先使用传入的窗口，否则使用equity.jsonl范围
    equity_first_ts = eq_rows[0].get("ts") if eq_rows else None
    equity_last_ts = eq_rows[-1].get("ts") if eq_rows else None
    
    start_ts = equity_first_ts
    end_ts = equity_last_ts
    
    # 若main传了窗口，则覆盖（窗口语义优先）
    if window_start_ts is not None and window_end_ts is not None:
        start_ts = window_start_ts
        end_ts = window_end_ts

    summ: Dict[str, Any] = {
        "run_id": rd.name,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "avg_equity": avg_equity,
        **eqm,
        **tm,
        **_negative_expectancy_summary_fields(rd),
        **_quant_lab_summary_fields(rd),
    }

    (rd / "summary.json").write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")
    return summ


def refresh_summary_metrics(run_dir: str) -> Dict[str, Any]:
    """Recompute trade/equity metrics from current trades.csv + equity.jsonl.

    Used for live finalize: fills/trades may arrive after the initial summary was written.
    This function patches summary.json in-place while preserving unrelated fields (e.g. budget).
    """

    rd = _resolve_run_dir(run_dir)
    p = rd / "summary.json"
    if not p.exists():
        # create from scratch
        return write_summary(run_dir)

    summ = json.loads(p.read_text(encoding="utf-8"))

    eq_rows = read_equity_jsonl(str(rd / "equity.jsonl"))
    trades = read_trades_csv(str(rd / "trades.csv"))

    avg_equity = None
    if eq_rows:
        xs = [float(r.get("equity") or 0.0) for r in eq_rows]
        avg_equity = sum(xs) / len(xs) if xs else None

    eqm = compute_equity_metrics(eq_rows)
    tm = compute_trade_metrics(trades, avg_equity=avg_equity)

    # patch
    summ["avg_equity"] = avg_equity
    for k, v in {**eqm, **tm}.items():
        summ[k] = v
    for k, v in _negative_expectancy_summary_fields(rd).items():
        summ[k] = v
    for k, v in _quant_lab_summary_fields(rd).items():
        summ[k] = v

    p.write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")
    return summ


def attach_budget(run_dir: str, budget: Dict[str, Any]) -> Dict[str, Any]:
    """Patch run_dir/summary.json with a top-level 'budget' field."""
    rd = _resolve_run_dir(run_dir)
    p = rd / "summary.json"
    if not p.exists():
        raise FileNotFoundError(str(p))
    summ = json.loads(p.read_text(encoding="utf-8"))
    summ["budget"] = budget
    p.write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")
    return summ


def attach_exit_signals(run_dir: str, exit_signals: list[dict[str, Any]]) -> Dict[str, Any]:
    """Patch run_dir/summary.json with a top-level 'exit_signals' field."""
    rd = _resolve_run_dir(run_dir)
    p = rd / "summary.json"
    if not p.exists():
        raise FileNotFoundError(str(p))
    summ = json.loads(p.read_text(encoding="utf-8"))
    summ["exit_signals"] = exit_signals
    p.write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")
    return summ

