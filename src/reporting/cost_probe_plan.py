from __future__ import annotations

import csv
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.execution.fill_store import (
    derive_position_store_path,
    derive_runtime_named_json_path,
)


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
    "orders_per_roundtrip",
    "daily_order_used_count",
    "available_order_slots",
    "entry_intent",
    "exit_intent",
    "live_order_effect",
]
COST_PROBE_ORDER_FIELDS = [
    "generated_at",
    "symbol",
    "leg",
    "side",
    "intent",
    "order_status",
    "dry_run",
    "live_enabled",
    "no_order_submitted",
    "notional_usdt",
    "order_style",
    "blocked_reasons",
    "live_order_effect",
]
COST_PROBE_ROUNDTRIP_FIELDS = [
    "generated_at",
    "symbol",
    "roundtrip_status",
    "entry_order_status",
    "exit_order_status",
    "max_open_seconds",
    "blocked_reasons",
    "no_order_submitted",
    "live_order_effect",
]
RUNTIME_COST_GUARD_FIELDS = [
    "generated_at",
    "guard_name",
    "status",
    "reason",
    "path",
    "observed_value",
]
COST_DISAGREEMENT_FIELDS = [
    "generated_at",
    "symbol",
    "status",
    "v5_cost_bps",
    "quant_lab_cost_bps",
    "diff_bps",
    "reason",
    "live_order_effect",
]
P3_MANUAL_PROBE_ALLOWED_SYMBOLS = ("BTC/USDT", "ETH/USDT")
P3_MANUAL_PROBE_MAX_NOTIONAL_USDT = 5.0
P3_MANUAL_PROBE_MAX_OPEN_SECONDS = 60
P3_POST_PROBE_REQUIRED_EVIDENCE = [
    "v5_cost_probe_orders_entry_exit_submitted_for_authorized_symbol",
    "v5_cost_probe_roundtrip_closed_and_flat_for_authorized_symbol",
    "quant_lab_bootstrap_probe_available_observed",
    "quant_lab_trusted_live_coverage_remains_false_until_strategy_or_private_fills",
    "quant_lab_live_actual_or_mixed_coverage_not_promoted_by_cost_probe_only",
]


class CostProbeEngine:
    """Read-only cost-probe dry-run engine.

    The engine evaluates local runtime gates and writes probe artifacts, but it
    never calls exchange or live execution code.
    """

    def __init__(
        self,
        cfg: Any,
        *,
        reports_dir: str | Path = "reports",
        generated_at: datetime | None = None,
        project_root: str | Path | None = None,
    ) -> None:
        self.cfg = cfg
        self.execution = getattr(cfg, "execution", cfg)
        self.reports_dir = Path(reports_dir)
        self.generated_at = (generated_at or datetime.now(UTC)).astimezone(UTC)
        self.project_root = Path(project_root or Path.cwd()).resolve()
        self.order_store_path = _resolve_project_path(
            getattr(self.execution, "order_store_path", "reports/orders.sqlite"),
            project_root=self.project_root,
        )
        self.position_store_path = derive_position_store_path(self.order_store_path)
        self.kill_switch_path = _runtime_json_path(
            getattr(self.execution, "kill_switch_path", None),
            order_store_path=self.order_store_path,
            base_name="kill_switch",
            legacy_default="reports/kill_switch.json",
            project_root=self.project_root,
        )
        self.reconcile_status_path = _runtime_json_path(
            getattr(self.execution, "reconcile_status_path", None),
            order_store_path=self.order_store_path,
            base_name="reconcile_status",
            legacy_default="reports/reconcile_status.json",
            project_root=self.project_root,
        )

    def build(self) -> dict[str, Any]:
        guard_rows, runtime_blockers = self.evaluate_runtime_guards()
        (
            history_guard_rows,
            history_blockers,
            symbol_runtime_blockers,
            history_summary,
        ) = self.evaluate_cost_probe_history()
        guard_rows.extend(history_guard_rows)
        runtime_blockers = [*runtime_blockers, *history_blockers]
        plan_rows, summary = build_cost_probe_dry_run_plan(
            self.cfg,
            generated_at=self.generated_at,
            runtime_blockers=runtime_blockers,
            symbol_runtime_blockers=symbol_runtime_blockers,
            daily_order_used_count=int(history_summary["daily_order_used_count"]),
        )
        order_rows = _cost_probe_order_rows(plan_rows)
        roundtrip_rows = _cost_probe_roundtrip_rows(plan_rows)
        disagreement_rows = _cost_disagreement_rows(plan_rows, generated_at=self.generated_at)
        summary = {
            **summary,
            "engine": "CostProbeEngine",
            "runtime_blockers": sorted(set(runtime_blockers)),
            "symbol_runtime_blockers": symbol_runtime_blockers,
            **history_summary,
            "order_rows": len(order_rows),
            "roundtrip_rows": len(roundtrip_rows),
            "guard_rows": len(guard_rows),
            "disagreement_rows": len(disagreement_rows),
            "order_store_path": str(self.order_store_path),
            "position_store_path": str(self.position_store_path),
            "kill_switch_path": str(self.kill_switch_path),
            "reconcile_status_path": str(self.reconcile_status_path),
        }
        p3_preflight = build_cost_probe_p3_preflight(plan_rows, summary, guard_rows)
        return {
            "plan_rows": plan_rows,
            "summary": summary,
            "order_rows": order_rows,
            "roundtrip_rows": roundtrip_rows,
            "guard_rows": guard_rows,
            "disagreement_rows": disagreement_rows,
            "p3_preflight": p3_preflight,
        }

    def write(self) -> dict[str, Path]:
        payload = self.build()
        return write_cost_probe_dry_run_outputs(
            payload["plan_rows"],
            payload["summary"],
            order_rows=payload["order_rows"],
            roundtrip_rows=payload["roundtrip_rows"],
            guard_rows=payload["guard_rows"],
            disagreement_rows=payload["disagreement_rows"],
            p3_preflight=payload["p3_preflight"],
            plan_path=self.reports_dir / "cost_probe_plan.csv",
            summary_path=self.reports_dir / "cost_probe_summary.json",
            orders_path=self.reports_dir / "cost_probe_orders.csv",
            roundtrips_path=self.reports_dir / "cost_probe_roundtrips.csv",
            runtime_guard_path=self.reports_dir / "runtime_cost_guard.csv",
            disagreement_path=self.reports_dir / "cost_disagreement.csv",
            p3_preflight_path=self.reports_dir / "cost_probe_p3_preflight.json",
        )

    def evaluate_runtime_guards(self) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        blockers: list[str] = []
        if _bool_attr(self.execution, "cost_probe_respect_kill_switch", True):
            status, reason, observed = _kill_switch_guard(self.kill_switch_path)
            rows.append(
                self._guard_row(
                    "kill_switch_clean",
                    status=status,
                    reason=reason,
                    path=self.kill_switch_path,
                    observed_value=observed,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        if _bool_attr(self.execution, "cost_probe_require_reconcile_clean", True):
            status, reason, observed = _reconcile_guard(
                self.reconcile_status_path,
                max_age_sec=_int_attr(self.execution, "max_status_age_sec", 180),
                generated_at=self.generated_at,
            )
            rows.append(
                self._guard_row(
                    "reconcile_clean",
                    status=status,
                    reason=reason,
                    path=self.reconcile_status_path,
                    observed_value=observed,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        if _bool_attr(self.execution, "cost_probe_disable_if_order_store_dirty", True):
            status, reason, observed = _order_store_guard(self.order_store_path)
            rows.append(
                self._guard_row(
                    "order_store_clean",
                    status=status,
                    reason=reason,
                    path=self.order_store_path,
                    observed_value=observed,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        if _bool_attr(self.execution, "cost_probe_require_no_existing_position", True):
            status, reason, observed = _position_store_guard(self.position_store_path)
            rows.append(
                self._guard_row(
                    "no_existing_position",
                    status=status,
                    reason=reason,
                    path=self.position_store_path,
                    observed_value=observed,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        if _bool_attr(self.execution, "cost_probe_disable_if_position_state_dirty", True):
            position_dirty = any(
                row["guard_name"] == "no_existing_position" and row["status"] != "PASS"
                for row in rows
            )
            status = "BLOCK" if position_dirty else "PASS"
            reason = "position_state_clean" if status == "PASS" else "position_state_dirty"
            rows.append(
                self._guard_row(
                    "position_state_clean",
                    status=status,
                    reason=reason,
                    path=self.position_store_path,
                    observed_value=reason,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        return rows, sorted(set(blockers))

    def evaluate_cost_probe_history(
        self,
    ) -> tuple[list[dict[str, Any]], list[str], dict[str, list[str]], dict[str, Any]]:
        orders_path = self.reports_dir / "cost_probe_orders.csv"
        roundtrips_path = self.reports_dir / "cost_probe_roundtrips.csv"
        orders = _read_csv_rows(orders_path)
        roundtrips = _read_csv_rows(roundtrips_path)
        symbols = _cost_probe_symbols(self.execution)
        daily_orders = [
            row
            for row in orders
            if _probe_order_submitted(row)
            and _same_utc_day(_row_datetime(row), self.generated_at)
        ]
        daily_roundtrips = [
            row
            for row in roundtrips
            if _probe_roundtrip_active(row)
            and _same_utc_day(_row_datetime(row), self.generated_at)
        ]
        daily_order_used_count = len(daily_orders)
        max_orders = max(_int_attr(self.execution, "cost_probe_max_orders_per_day", 0), 0)
        roundtrip_limit = max(
            _int_attr(self.execution, "cost_probe_max_roundtrips_per_symbol_per_day", 0),
            0,
        )
        cooldown_minutes = max(
            _int_attr(self.execution, "cost_probe_cooldown_minutes", 0),
            0,
        )
        max_daily_loss = _float_attr(self.execution, "cost_probe_max_daily_loss_usdt", 0.0)
        daily_loss = _daily_roundtrip_loss_usdt(daily_roundtrips)
        rows: list[dict[str, Any]] = []
        blockers: list[str] = []
        symbol_blockers: dict[str, list[str]] = {symbol: [] for symbol in symbols}

        order_status = "PASS"
        order_reason = "daily_order_budget_available"
        if max_orders > 0 and daily_order_used_count >= max_orders:
            order_status = "BLOCK"
            order_reason = "daily_order_limit_exhausted"
            blockers.append(order_reason)
        rows.append(
            self._guard_row(
                "cost_probe_daily_order_budget",
                status=order_status,
                reason=order_reason,
                path=orders_path,
                observed_value=f"used={daily_order_used_count};max={max_orders}",
            )
        )

        loss_status = "PASS"
        loss_reason = "daily_loss_budget_available"
        if max_daily_loss > 0 and daily_loss >= max_daily_loss:
            loss_status = "BLOCK"
            loss_reason = "daily_loss_limit_reached"
            blockers.append(loss_reason)
        rows.append(
            self._guard_row(
                "cost_probe_daily_loss_budget",
                status=loss_status,
                reason=loss_reason,
                path=roundtrips_path,
                observed_value=f"loss_usdt={daily_loss:.8f};max={max_daily_loss:.8f}",
            )
        )

        for symbol in symbols:
            symbol_roundtrips = [
                row
                for row in daily_roundtrips
                if _normalize_cost_probe_symbol(row.get("symbol")) == symbol
            ]
            roundtrip_status = "PASS"
            roundtrip_reason = "roundtrip_budget_available"
            if roundtrip_limit > 0 and len(symbol_roundtrips) >= roundtrip_limit:
                roundtrip_status = "BLOCK"
                roundtrip_reason = "roundtrip_limit_reached"
                symbol_blockers[symbol].append(roundtrip_reason)
            rows.append(
                self._guard_row(
                    "cost_probe_symbol_roundtrip_budget",
                    status=roundtrip_status,
                    reason=roundtrip_reason,
                    path=roundtrips_path,
                    observed_value=(
                        f"symbol={symbol};used={len(symbol_roundtrips)};max={roundtrip_limit}"
                    ),
                )
            )

            latest_symbol_probe = _latest_probe_datetime(symbol, orders, roundtrips)
            cooldown_status = "PASS"
            cooldown_reason = "cooldown_clear"
            cooldown_observed = f"symbol={symbol};cooldown_minutes={cooldown_minutes}"
            if latest_symbol_probe is not None and cooldown_minutes > 0:
                cooldown_until = latest_symbol_probe + timedelta(minutes=cooldown_minutes)
                if cooldown_until > self.generated_at:
                    remain_sec = max((cooldown_until - self.generated_at).total_seconds(), 0.0)
                    cooldown_status = "BLOCK"
                    cooldown_reason = "cost_probe_cooldown_active"
                    cooldown_observed = (
                        f"symbol={symbol};remaining_sec={remain_sec:.1f};"
                        f"latest={_iso(latest_symbol_probe)}"
                    )
                    symbol_blockers[symbol].append(cooldown_reason)
            rows.append(
                self._guard_row(
                    "cost_probe_symbol_cooldown",
                    status=cooldown_status,
                    reason=cooldown_reason,
                    path=orders_path,
                    observed_value=cooldown_observed,
                )
            )

        symbol_blockers = {
            symbol: sorted(set(reasons))
            for symbol, reasons in symbol_blockers.items()
            if reasons
        }
        history_summary = {
            "daily_order_used_count": daily_order_used_count,
            "daily_roundtrip_used_count": len(daily_roundtrips),
            "daily_loss_usdt": daily_loss,
            "cost_probe_orders_path": str(orders_path),
            "cost_probe_roundtrips_path": str(roundtrips_path),
        }
        return rows, sorted(set(blockers)), symbol_blockers, history_summary

    def _guard_row(
        self,
        guard_name: str,
        *,
        status: str,
        reason: str,
        path: Path,
        observed_value: str,
    ) -> dict[str, Any]:
        return {
            "generated_at": _iso(self.generated_at),
            "guard_name": guard_name,
            "status": status,
            "reason": reason,
            "path": str(path),
            "observed_value": observed_value,
        }


def build_cost_probe_dry_run_plan(
    cfg: Any,
    *,
    generated_at: datetime | None = None,
    runtime_blockers: list[str] | None = None,
    symbol_runtime_blockers: dict[str, list[str]] | None = None,
    daily_order_used_count: int = 0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build a read-only cost-probe plan without touching exchange state."""

    execution = getattr(cfg, "execution", cfg)
    generated = (generated_at or datetime.now(UTC)).astimezone(UTC)
    symbols = _cost_probe_symbols(execution)
    blockers = [*_cost_probe_blockers(execution), *(runtime_blockers or [])]
    max_orders = max(_int_attr(execution, "cost_probe_max_orders_per_day", 0), 0)
    used_order_count = max(int(daily_order_used_count or 0), 0)
    orders_per_roundtrip = 2
    available_order_slots = max(max_orders - used_order_count, 0)
    roundtrip_limit = max(
        _int_attr(execution, "cost_probe_max_roundtrips_per_symbol_per_day", 0),
        0,
    )
    symbol_blockers_map = symbol_runtime_blockers or {}
    budget_candidate_limit = available_order_slots // orders_per_roundtrip
    budget_pool = [symbol for symbol in symbols if not symbol_blockers_map.get(symbol)]
    candidate_symbols = budget_pool[:budget_candidate_limit] if max_orders > 0 else []
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        symbol_blockers = [*blockers, *symbol_blockers_map.get(symbol, [])]
        if max_orders <= 0:
            symbol_blockers.append("cost_probe_max_orders_per_day_zero")
        elif available_order_slots < orders_per_roundtrip or symbol not in candidate_symbols:
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
                "orders_per_roundtrip": orders_per_roundtrip,
                "daily_order_used_count": used_order_count,
                "available_order_slots": available_order_slots,
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
        "daily_order_used_count": used_order_count,
        "available_order_slots": available_order_slots,
        "orders_per_roundtrip": orders_per_roundtrip,
        "max_roundtrips_per_symbol_per_day": roundtrip_limit,
        "live_order_effect": "none_read_only_dry_run_plan",
    }
    return rows, summary


def build_cost_probe_p3_preflight(
    plan_rows: list[dict[str, Any]],
    summary: dict[str, Any],
    guard_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a read-only authorization preflight for manual P3 live probes."""

    planned_symbols = [str(symbol) for symbol in summary.get("planned_symbols") or []]
    if not planned_symbols:
        planned_symbols = [
            str(row.get("symbol"))
            for row in plan_rows
            if str(row.get("plan_status") or "") == "planned" and row.get("symbol")
        ]
    manual_probe_symbol = planned_symbols[0] if len(planned_symbols) == 1 else ""
    selected_plan_rows = [
        row
        for row in plan_rows
        if str(row.get("symbol") or "") == manual_probe_symbol
        and str(row.get("plan_status") or "") == "planned"
    ]
    selected_plan = selected_plan_rows[0] if len(selected_plan_rows) == 1 else {}
    exit_policy = str(selected_plan.get("exit_policy") or "")
    max_open_seconds = _int_value(selected_plan.get("max_open_seconds"))
    guard_failures = [
        {
            "guard_name": str(row.get("guard_name") or ""),
            "reason": str(row.get("reason") or ""),
        }
        for row in (guard_rows or [])
        if str(row.get("status") or "") != "PASS"
    ]
    blockers: list[str] = []
    if summary.get("state") != "DRY_RUN_PLAN_READY":
        blockers.append("dry_run_plan_not_ready")
    if summary.get("runtime_blockers"):
        blockers.append("runtime_blockers_present")
    if guard_failures:
        blockers.append("runtime_guard_failures_present")
    if len(planned_symbols) != 1:
        blockers.append("single_symbol_plan_required")
    elif manual_probe_symbol not in P3_MANUAL_PROBE_ALLOWED_SYMBOLS:
        blockers.append("manual_probe_symbol_not_allowed")
    if not bool(summary.get("dry_run")):
        blockers.append("dry_run_true_required")
    if bool(summary.get("live_enabled")):
        blockers.append("live_enabled_must_remain_false_for_preflight")
    if not bool(summary.get("no_order_submitted", True)):
        blockers.append("no_order_submitted_required")
    orders_per_roundtrip = _int_value(summary.get("orders_per_roundtrip"))
    available_order_slots = _int_value(summary.get("available_order_slots"))
    max_notional_usdt = _float_value(summary.get("max_notional_usdt"))
    if orders_per_roundtrip != 2:
        blockers.append("orders_per_roundtrip_must_be_two")
    if available_order_slots < max(orders_per_roundtrip, 2):
        blockers.append("insufficient_available_order_slots")
    if max_notional_usdt <= 0:
        blockers.append("max_notional_usdt_positive_required")
    if max_notional_usdt > P3_MANUAL_PROBE_MAX_NOTIONAL_USDT:
        blockers.append("max_notional_exceeds_p3_manual_limit")
    if manual_probe_symbol and exit_policy != "immediate_flat":
        blockers.append("immediate_flat_exit_policy_required")
    if manual_probe_symbol and (
        max_open_seconds <= 0 or max_open_seconds > P3_MANUAL_PROBE_MAX_OPEN_SECONDS
    ):
        blockers.append("max_open_seconds_exceeds_p3_manual_limit")
    if _int_value(summary.get("daily_order_used_count")) > 0:
        blockers.append("daily_probe_order_history_present")
    if _int_value(summary.get("daily_roundtrip_used_count")) > 0:
        blockers.append("daily_probe_roundtrip_history_present")
    if _float_value(summary.get("daily_loss_usdt")) > 0:
        blockers.append("daily_probe_loss_present")

    blockers = sorted(set(blockers))
    return {
        "generated_at": str(summary.get("generated_at") or ""),
        "state": "READY_FOR_MANUAL_AUTHORIZATION" if not blockers else "NOT_READY",
        "ready_to_request_manual_live_probe": not blockers,
        "manual_authorization_required": True,
        "approved_live_order_execution": False,
        "live_order_effect": "none_preflight_only_no_order",
        "manual_probe_symbol": manual_probe_symbol,
        "manual_allowed_symbols": list(P3_MANUAL_PROBE_ALLOWED_SYMBOLS),
        "manual_max_notional_usdt": P3_MANUAL_PROBE_MAX_NOTIONAL_USDT,
        "manual_required_exit_policy": "immediate_flat",
        "manual_max_open_seconds": P3_MANUAL_PROBE_MAX_OPEN_SECONDS,
        "planned_symbols": planned_symbols,
        "blockers": blockers,
        "runtime_blockers": summary.get("runtime_blockers") or [],
        "symbol_runtime_blockers": summary.get("symbol_runtime_blockers") or {},
        "guard_failures": guard_failures,
        "dry_run_plan_state": summary.get("state"),
        "dry_run": bool(summary.get("dry_run")),
        "live_enabled": bool(summary.get("live_enabled")),
        "no_order_submitted": bool(summary.get("no_order_submitted", True)),
        "max_notional_usdt": max_notional_usdt,
        "max_orders_per_day": _int_value(summary.get("max_orders_per_day")),
        "orders_per_roundtrip": orders_per_roundtrip,
        "exit_policy": exit_policy,
        "max_open_seconds": max_open_seconds,
        "daily_order_used_count": _int_value(summary.get("daily_order_used_count")),
        "available_order_slots": available_order_slots,
        "daily_roundtrip_used_count": _int_value(
            summary.get("daily_roundtrip_used_count")
        ),
        "daily_loss_usdt": _float_value(summary.get("daily_loss_usdt")),
        "next_action": (
            "request_explicit_operator_authorization_for_one_symbol_live_probe"
            if not blockers
            else "fix_preflight_blockers_before_requesting_live_probe"
        ),
        "post_probe_required_evidence": P3_POST_PROBE_REQUIRED_EVIDENCE,
    }


def write_cost_probe_dry_run_outputs(
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
    *,
    order_rows: list[dict[str, Any]] | None = None,
    roundtrip_rows: list[dict[str, Any]] | None = None,
    guard_rows: list[dict[str, Any]] | None = None,
    disagreement_rows: list[dict[str, Any]] | None = None,
    p3_preflight: dict[str, Any] | None = None,
    plan_path: str | Path = "reports/cost_probe_plan.csv",
    summary_path: str | Path = "reports/cost_probe_summary.json",
    orders_path: str | Path = "reports/cost_probe_orders.csv",
    roundtrips_path: str | Path = "reports/cost_probe_roundtrips.csv",
    runtime_guard_path: str | Path = "reports/runtime_cost_guard.csv",
    disagreement_path: str | Path = "reports/cost_disagreement.csv",
    p3_preflight_path: str | Path = "reports/cost_probe_p3_preflight.json",
) -> dict[str, Path]:
    plan = Path(plan_path)
    summary_file = Path(summary_path)
    orders = Path(orders_path)
    roundtrips = Path(roundtrips_path)
    runtime_guard = Path(runtime_guard_path)
    disagreement = Path(disagreement_path)
    p3_preflight_file = Path(p3_preflight_path)
    plan.parent.mkdir(parents=True, exist_ok=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    _write_csv(plan, COST_PROBE_PLAN_FIELDS, rows)
    _write_csv(orders, COST_PROBE_ORDER_FIELDS, order_rows or [])
    _write_csv(roundtrips, COST_PROBE_ROUNDTRIP_FIELDS, roundtrip_rows or [])
    _write_csv(runtime_guard, RUNTIME_COST_GUARD_FIELDS, guard_rows or [])
    _write_csv(disagreement, COST_DISAGREEMENT_FIELDS, disagreement_rows or [])
    summary_file.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    preflight = p3_preflight or build_cost_probe_p3_preflight(
        rows,
        summary,
        guard_rows or [],
    )
    p3_preflight_file.parent.mkdir(parents=True, exist_ok=True)
    p3_preflight_file.write_text(
        json.dumps(preflight, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        "plan_path": plan,
        "summary_path": summary_file,
        "orders_path": orders,
        "roundtrips_path": roundtrips,
        "runtime_guard_path": runtime_guard,
        "disagreement_path": disagreement,
        "p3_preflight_path": p3_preflight_file,
    }


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


def _cost_probe_order_rows(plan_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in plan_rows:
        for leg, side, intent in (
            ("entry", "buy", "DRY_RUN_ENTRY_ONLY_NO_ORDER"),
            ("exit", "sell", "DRY_RUN_IMMEDIATE_FLAT_NO_ORDER"),
        ):
            rows.append(
                {
                    "generated_at": row.get("generated_at"),
                    "symbol": row.get("symbol"),
                    "leg": leg,
                    "side": side,
                    "intent": intent,
                    "order_status": "not_submitted",
                    "dry_run": row.get("dry_run"),
                    "live_enabled": row.get("live_enabled"),
                    "no_order_submitted": True,
                    "notional_usdt": row.get("max_notional_usdt"),
                    "order_style": row.get("order_style"),
                    "blocked_reasons": row.get("blocked_reasons"),
                    "live_order_effect": row.get("live_order_effect"),
                }
            )
    return rows


def _cost_probe_roundtrip_rows(plan_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "generated_at": row.get("generated_at"),
            "symbol": row.get("symbol"),
            "roundtrip_status": (
                "dry_run_ready" if row.get("plan_status") == "planned" else "blocked"
            ),
            "entry_order_status": "not_submitted",
            "exit_order_status": "not_submitted",
            "max_open_seconds": row.get("max_open_seconds"),
            "blocked_reasons": row.get("blocked_reasons"),
            "no_order_submitted": True,
            "live_order_effect": row.get("live_order_effect"),
        }
        for row in plan_rows
    ]


def _cost_disagreement_rows(
    plan_rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
) -> list[dict[str, Any]]:
    symbols = [str(row.get("symbol") or "") for row in plan_rows if row.get("symbol")]
    return [
        {
            "generated_at": _iso(generated_at),
            "symbol": ",".join(symbols),
            "status": "not_evaluated",
            "v5_cost_bps": "",
            "quant_lab_cost_bps": "",
            "diff_bps": "",
            "reason": "no_live_probe_roundtrip_no_cost_disagreement",
            "live_order_effect": "none_read_only_dry_run_plan",
        }
    ]


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _kill_switch_guard(path: Path) -> tuple[str, str, str]:
    payload = _read_json(path)
    if payload is None:
        return "BLOCK", "kill_switch_status_missing", "missing"
    enabled = _json_bool(payload.get("enabled"))
    if "kill_switch" in payload and isinstance(payload.get("kill_switch"), dict):
        enabled = _json_bool(payload["kill_switch"].get("enabled"))
    if enabled:
        return "BLOCK", "kill_switch_enabled", "enabled"
    return "PASS", "kill_switch_clean", "disabled"


def _reconcile_guard(
    path: Path,
    *,
    max_age_sec: int,
    generated_at: datetime,
) -> tuple[str, str, str]:
    payload = _read_json(path)
    if payload is None:
        return "BLOCK", "reconcile_status_missing", "missing"
    ok = _json_bool(payload.get("ok"))
    reason = str(payload.get("reason") or "")
    ts_ms = _int_value(payload.get("generated_ts_ms") or payload.get("ts_ms"))
    if not ok:
        return "BLOCK", f"reconcile_not_ok:{reason or 'unknown'}", json.dumps(payload, ensure_ascii=False)
    if ts_ms > 0:
        age_sec = max(generated_at.timestamp() - ts_ms / 1000.0, 0.0)
        if age_sec > max_age_sec:
            return "BLOCK", "reconcile_status_stale", f"age_sec={age_sec:.1f}"
    return "PASS", "reconcile_clean", "ok"


def _order_store_guard(path: Path) -> tuple[str, str, str]:
    if not path.exists():
        return "PASS", "order_store_missing_assumed_clean", "missing"
    try:
        with sqlite3.connect(str(path), timeout=2.0) as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT COUNT(*) FROM orders
                WHERE state IN ('NEW','SENT','ACK','OPEN','PARTIAL','UNKNOWN')
                """
            )
            open_count = int(cur.fetchone()[0] or 0)
    except Exception as exc:
        return "BLOCK", "order_store_unreadable", str(exc)
    if open_count > 0:
        return "BLOCK", "order_store_open_orders", f"open_orders={open_count}"
    return "PASS", "order_store_clean", "open_orders=0"


def _position_store_guard(path: Path) -> tuple[str, str, str]:
    if not path.exists():
        return "PASS", "position_store_missing_assumed_flat", "missing"
    try:
        with sqlite3.connect(str(path), timeout=2.0) as con:
            cur = con.cursor()
            cur.execute("SELECT symbol, qty FROM positions WHERE qty > 0")
            rows = [(str(symbol), float(qty or 0.0)) for symbol, qty in cur.fetchall()]
    except Exception as exc:
        return "BLOCK", "position_store_unreadable", str(exc)
    if rows:
        return "BLOCK", "existing_position_present", json.dumps(rows, ensure_ascii=False)
    return "PASS", "position_store_flat", "positions=0"


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except (OSError, csv.Error):
        return []


def _row_datetime(row: dict[str, Any]) -> datetime | None:
    for key in (
        "generated_at",
        "filled_at",
        "closed_at",
        "completed_at",
        "exit_at",
        "updated_at",
        "created_at",
        "timestamp",
        "ts",
    ):
        parsed = _parse_datetime(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.replace(".", "", 1).isdigit():
            try:
                numeric = float(raw)
            except ValueError:
                return None
            if numeric > 10_000_000_000:
                numeric /= 1000.0
            return datetime.fromtimestamp(numeric, tz=UTC)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _same_utc_day(value: datetime | None, ref: datetime) -> bool:
    if value is None:
        return False
    return value.astimezone(UTC).date() == ref.astimezone(UTC).date()


def _normalize_cost_probe_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper().replace("-", "/")
    if "/" not in symbol and symbol.endswith("USDT"):
        symbol = f"{symbol[:-4]}/USDT"
    return symbol


def _probe_order_submitted(row: dict[str, Any]) -> bool:
    if _json_bool(row.get("no_order_submitted")):
        return False
    status = str(row.get("order_status") or row.get("status") or "").strip().lower()
    if status in {"", "not_submitted", "blocked", "dry_run_ready"}:
        return False
    if str(row.get("live_order_effect") or "").startswith("none_read_only"):
        return False
    if str(row.get("intent") or "").upper().startswith("DRY_RUN_"):
        return False
    return True


def _probe_roundtrip_active(row: dict[str, Any]) -> bool:
    if _json_bool(row.get("no_order_submitted")):
        return False
    status = str(row.get("roundtrip_status") or row.get("status") or "").strip().lower()
    if status in {"", "blocked", "dry_run_ready", "not_submitted", "planned"}:
        return False
    if str(row.get("live_order_effect") or "").startswith("none_read_only"):
        return False
    return True


def _latest_probe_datetime(
    symbol: str,
    orders: list[dict[str, Any]],
    roundtrips: list[dict[str, Any]],
) -> datetime | None:
    timestamps: list[datetime] = []
    for row in orders:
        if _normalize_cost_probe_symbol(row.get("symbol")) != symbol:
            continue
        if not _probe_order_submitted(row):
            continue
        parsed = _row_datetime(row)
        if parsed is not None:
            timestamps.append(parsed)
    for row in roundtrips:
        if _normalize_cost_probe_symbol(row.get("symbol")) != symbol:
            continue
        if not _probe_roundtrip_active(row):
            continue
        parsed = _row_datetime(row)
        if parsed is not None:
            timestamps.append(parsed)
    return max(timestamps) if timestamps else None


def _daily_roundtrip_loss_usdt(roundtrips: list[dict[str, Any]]) -> float:
    loss = 0.0
    for row in roundtrips:
        pnl = _float_value(
            row.get("net_pnl_usdt")
            or row.get("realized_pnl_usdt")
            or row.get("pnl_usdt")
            or 0.0
        )
        if pnl < 0:
            loss += abs(pnl)
    return loss


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _runtime_json_path(
    raw_path: str | None,
    *,
    order_store_path: Path,
    base_name: str,
    legacy_default: str,
    project_root: Path,
) -> Path:
    raw = str(raw_path or "").strip()
    if not raw or Path(raw).as_posix() == Path(legacy_default).as_posix():
        return _resolve_project_path(
            derive_runtime_named_json_path(order_store_path, base_name),
            project_root=project_root,
        )
    return _resolve_project_path(raw, project_root=project_root)


def _resolve_project_path(raw_path: str | Path, *, project_root: Path) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path


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


def _json_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _int_value(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


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


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
