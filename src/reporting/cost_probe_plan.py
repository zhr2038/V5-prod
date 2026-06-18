from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


COST_PROBE_PLAN_FIELDS = [
    "generated_at",
    "symbol",
    "plan_status",
    "blocked_reasons",
    "dry_run",
    "live_enabled",
    "no_order_submitted",
    "max_notional_usdt",
    "order_style",
    "exit_policy",
    "max_open_seconds",
    "roundtrip_limit",
    "entry_intent",
    "exit_intent",
    "live_order_effect",
]


def build_cost_probe_dry_run_plan(
    cfg: Any,
    *,
    generated_at: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build a read-only cost-probe plan without touching exchange state."""

    execution = getattr(cfg, "execution", cfg)
    generated = (generated_at or datetime.now(UTC)).astimezone(UTC)
    symbols = _cost_probe_symbols(execution)
    blockers = _cost_probe_blockers(execution)
    max_orders = max(_int_attr(execution, "cost_probe_max_orders_per_day", 0), 0)
    roundtrip_limit = max(
        _int_attr(execution, "cost_probe_max_roundtrips_per_symbol_per_day", 0),
        0,
    )
    candidate_symbols = symbols[:max_orders] if max_orders > 0 else []
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        symbol_blockers = list(blockers)
        if max_orders <= 0:
            symbol_blockers.append("cost_probe_max_orders_per_day_zero")
        elif symbol not in candidate_symbols:
            symbol_blockers.append("daily_order_limit_exceeded")
        if roundtrip_limit <= 0:
            symbol_blockers.append("roundtrip_limit_zero")
        if _bool_attr(execution, "cost_probe_use_exchange_min_notional", True):
            symbol_blockers.append("exchange_min_notional_check_pending")
        plan_status = "planned" if not symbol_blockers else "blocked"
        rows.append(
            {
                "generated_at": _iso(generated),
                "symbol": symbol,
                "plan_status": plan_status,
                "blocked_reasons": ";".join(sorted(set(symbol_blockers))),
                "dry_run": _bool_attr(execution, "cost_probe_dry_run", True),
                "live_enabled": _bool_attr(execution, "cost_probe_live_enabled", False),
                "no_order_submitted": True,
                "max_notional_usdt": _float_attr(
                    execution,
                    "cost_probe_max_notional_usdt",
                    0.0,
                ),
                "order_style": str(
                    getattr(execution, "cost_probe_order_style", "marketable_limit_ioc")
                    or "marketable_limit_ioc"
                ),
                "exit_policy": str(
                    getattr(execution, "cost_probe_exit_policy", "immediate_flat")
                    or "immediate_flat"
                ),
                "max_open_seconds": _int_attr(execution, "cost_probe_max_open_seconds", 60),
                "roundtrip_limit": roundtrip_limit,
                "entry_intent": "DRY_RUN_ENTRY_ONLY_NO_ORDER",
                "exit_intent": "DRY_RUN_IMMEDIATE_FLAT_NO_ORDER",
                "live_order_effect": "none_read_only_dry_run_plan",
            }
        )

    planned_rows = [row for row in rows if row["plan_status"] == "planned"]
    summary = {
        "generated_at": _iso(generated),
        "state": _summary_state(execution, blockers, planned_rows),
        "dry_run": _bool_attr(execution, "cost_probe_dry_run", True),
        "live_enabled": _bool_attr(execution, "cost_probe_live_enabled", False),
        "no_order_submitted": True,
        "symbols_configured": symbols,
        "planned_symbols": [str(row["symbol"]) for row in planned_rows],
        "blocked_symbols": [
            str(row["symbol"]) for row in rows if row["plan_status"] == "blocked"
        ],
        "plan_rows": len(rows),
        "planned_rows": len(planned_rows),
        "blocked_reasons": sorted(
            {reason for row in rows for reason in str(row["blocked_reasons"]).split(";") if reason}
        ),
        "max_notional_usdt": _float_attr(execution, "cost_probe_max_notional_usdt", 0.0),
        "max_orders_per_day": max_orders,
        "max_roundtrips_per_symbol_per_day": roundtrip_limit,
        "live_order_effect": "none_read_only_dry_run_plan",
    }
    return rows, summary


def write_cost_probe_dry_run_outputs(
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
    *,
    plan_path: str | Path = "reports/cost_probe_plan.csv",
    summary_path: str | Path = "reports/cost_probe_summary.json",
) -> tuple[Path, Path]:
    plan = Path(plan_path)
    summary_file = Path(summary_path)
    plan.parent.mkdir(parents=True, exist_ok=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    with plan.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COST_PROBE_PLAN_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in COST_PROBE_PLAN_FIELDS})
    summary_file.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return plan, summary_file


def _cost_probe_blockers(execution: Any) -> list[str]:
    blockers: list[str] = []
    if not _bool_attr(execution, "cost_bootstrap_enabled", False):
        blockers.append("cost_bootstrap_enabled_false")
    if not _bool_attr(execution, "cost_probe_enabled", False):
        blockers.append("cost_probe_enabled_false")
    if not _bool_attr(execution, "cost_probe_dry_run", True):
        blockers.append("cost_probe_dry_run_false")
    if _bool_attr(execution, "cost_probe_live_enabled", False):
        blockers.append("cost_probe_live_enabled_true")
    if _float_attr(execution, "cost_probe_max_notional_usdt", 0.0) <= 0:
        blockers.append("cost_probe_max_notional_usdt_zero")
    return blockers


def _summary_state(
    execution: Any,
    blockers: list[str],
    planned_rows: list[dict[str, Any]],
) -> str:
    if _bool_attr(execution, "cost_probe_live_enabled", False):
        return "LIVE_ENABLED_BLOCKED_BY_DRY_RUN_PLANNER"
    if blockers:
        return "DISABLED"
    if planned_rows:
        return "DRY_RUN_PLAN_READY"
    return "NO_PLAN_ROWS"


def _cost_probe_symbols(execution: Any) -> list[str]:
    raw_symbols = getattr(execution, "cost_probe_symbols", None) or []
    symbols: list[str] = []
    for raw in raw_symbols:
        symbol = str(raw or "").strip().upper().replace("-", "/")
        if not symbol:
            continue
        if "/" not in symbol and symbol.endswith("USDT"):
            symbol = f"{symbol[:-4]}/USDT"
        if symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _bool_attr(obj: Any, name: str, default: bool) -> bool:
    value = getattr(obj, name, default)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _int_attr(obj: Any, name: str, default: int) -> int:
    try:
        return int(float(getattr(obj, name, default) or 0))
    except (TypeError, ValueError):
        return default


def _float_attr(obj: Any, name: str, default: float) -> float:
    try:
        return float(getattr(obj, name, default) or 0.0)
    except (TypeError, ValueError):
        return default


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
