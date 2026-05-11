from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from src.core.models import Order
from src.execution.quant_lab_client import (
    QuantLabClient,
    QuantLabResponse,
    append_jsonl,
    sanitize_quant_lab_obj,
    summarize_response,
)


DECISION_ALLOW = "ALLOW"
DECISION_SELL_ONLY = "SELL_ONLY"
DECISION_ABORT = "ABORT"
DECISION_UNKNOWN = "UNKNOWN"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_quant_lab_decision(value: Any) -> str:
    raw = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    if raw in {"ALLOW", "ALLOWED", "OK", "LIVE_ALLOWED", "TRADE_ALLOWED"}:
        return DECISION_ALLOW
    if raw in {"SELL_ONLY", "SELLONLY", "SELL", "LIQUIDATE_ONLY", "QUARANTINE"}:
        return DECISION_SELL_ONLY
    if raw in {"ABORT", "DENY", "DENIED", "BLOCK", "BLOCKED", "HALT", "KILL", "STOP"}:
        return DECISION_ABORT
    return DECISION_UNKNOWN


def decision_from_payload(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return DECISION_UNKNOWN
    keys = ("decision", "permission", "action", "mode", "risk_mode", "status")
    for key in keys:
        decision = normalize_quant_lab_decision(payload.get(key))
        if decision != DECISION_UNKNOWN:
            return decision
    for nested_key in ("data", "result", "payload"):
        nested = payload.get(nested_key)
        if isinstance(nested, Mapping):
            decision = decision_from_payload(nested)
            if decision != DECISION_UNKNOWN:
                return decision
    return DECISION_UNKNOWN


def fallback_decision(fail_policy: str) -> str:
    policy = str(fail_policy or "sell_only").strip().lower()
    if policy == "allow":
        return DECISION_ALLOW
    if policy == "abort":
        return DECISION_ABORT
    return DECISION_SELL_ONLY


def _first_number(payload: Any, keys: Iterable[str]) -> Optional[float]:
    if isinstance(payload, Mapping):
        for key in keys:
            if key in payload and payload.get(key) not in (None, ""):
                try:
                    return float(payload.get(key))
                except (TypeError, ValueError):
                    pass
        for nested_key in ("data", "result", "payload"):
            nested = payload.get(nested_key)
            value = _first_number(nested, keys)
            if value is not None:
                return value
    return None


def _first_string(payload: Any, keys: Iterable[str]) -> Optional[str]:
    if isinstance(payload, Mapping):
        for key in keys:
            if key in payload and payload.get(key) not in (None, ""):
                return str(payload.get(key))
        for nested_key in ("data", "result", "payload"):
            nested = payload.get(nested_key)
            value = _first_string(nested, keys)
            if value:
                return value
    return None


def _order_alpha_id(order: Order, default_alpha_id: str) -> str:
    meta = dict(getattr(order, "meta", None) or {})
    for key in ("alpha_id", "strategy", "entry_reason", "reason", "probe_type"):
        value = str(meta.get(key) or "").strip()
        if value:
            return value
    return str(default_alpha_id or "v5")


def _filter_reason(decision: str, event_source: str, fallback_used: bool) -> str:
    if fallback_used:
        return f"quant_lab_{event_source}_fallback_{decision.lower()}"
    return f"quant_lab_{event_source}_{decision.lower()}"


@dataclass
class QuantLabGuard:
    client: QuantLabClient
    fail_policy: str = "sell_only"
    usage_log_path: str | Path = "reports/quant_lab_usage.jsonl"
    run_id: Optional[str] = None
    default_alpha_id: str = "v5"
    strategy: str = "v5"
    strategy_version: str = "v1"
    cost_regime: str = "UNKNOWN"
    cost_quantile: str = "p75"
    gate_check_enabled: bool = False
    phase: str = "live"
    permission_response: Optional[QuantLabResponse] = None
    health_response: Optional[QuantLabResponse] = None
    permission_decision: str = DECISION_UNKNOWN
    effective_decision: str = DECISION_UNKNOWN
    fallback_used: bool = False
    events: List[Dict[str, Any]] = field(default_factory=list)
    cost_estimates: List[Dict[str, Any]] = field(default_factory=list)
    filtered_orders: List[Dict[str, Any]] = field(default_factory=list)

    def _emit_usage(self, payload: Mapping[str, Any]) -> None:
        row = {
            "ts": utc_now_iso(),
            "run_id": self.run_id,
            "phase": self.phase,
            **dict(payload),
        }
        safe = sanitize_quant_lab_obj(row)
        self.events.append(dict(safe))
        append_jsonl(self.usage_log_path, safe)

    def refresh_permission(self, *, include_health: bool = True) -> str:
        if include_health:
            self.health_response = self.client.health()
            self._emit_usage(
                {
                    "event_type": "health",
                    "endpoint": "/v1/health",
                    "ok": bool(self.health_response.ok),
                    "fallback_used": False,
                    "response": summarize_response(self.health_response.data),
                    "error": self.health_response.error,
                }
            )

        response = self.client.live_permission(strategy=self.strategy, version=self.strategy_version)
        self.permission_response = response
        raw_decision = decision_from_payload(response.data)
        if response.ok and raw_decision != DECISION_UNKNOWN:
            self.permission_decision = raw_decision
            self.effective_decision = raw_decision
            self.fallback_used = False
        else:
            self.permission_decision = raw_decision
            self.effective_decision = fallback_decision(self.fail_policy)
            self.fallback_used = True

        self._emit_usage(
            {
                "event_type": "permission",
                "endpoint": "/v1/risk/live-permission",
                "ok": bool(response.ok),
                "quant_lab_decision": self.permission_decision,
                "effective_decision": self.effective_decision,
                "fail_policy": str(self.fail_policy or "sell_only").lower(),
                "strategy": str(self.strategy or ""),
                "version": str(self.strategy_version or ""),
                "fallback_used": bool(self.fallback_used),
                "response": summarize_response(response.data),
                "error": response.error,
            }
        )
        return self.effective_decision

    def _estimate_order_cost(self, order: Order) -> Dict[str, Any]:
        alpha_id = _order_alpha_id(order, self.default_alpha_id)
        response = self.client.cost_estimate(
            symbol=str(getattr(order, "symbol", "") or ""),
            side=str(getattr(order, "side", "") or ""),
            notional_usdt=float(getattr(order, "notional_usdt", 0.0) or 0.0),
            regime=str(self.cost_regime or "UNKNOWN"),
            quantile=str(self.cost_quantile or "p75"),
            signal_price=float(getattr(order, "signal_price", 0.0) or 0.0),
            alpha_id=alpha_id,
        )
        cost_decision = decision_from_payload(response.data)
        fallback = not bool(response.ok)
        row = {
            "event_type": "cost_estimate",
            "endpoint": "/v1/costs/estimate",
            "ok": bool(response.ok),
            "symbol": str(getattr(order, "symbol", "") or ""),
            "side": str(getattr(order, "side", "") or ""),
            "intent": str(getattr(order, "intent", "") or ""),
            "notional_usdt": float(getattr(order, "notional_usdt", 0.0) or 0.0),
            "alpha_id": alpha_id,
            "quant_lab_decision": cost_decision,
            "fallback_used": fallback,
            "cost_bps": _first_number(
                response.data,
                ("cost_bps", "total_bps", "total_cost_bps", "estimated_cost_bps", "roundtrip_cost_bps", "cost_bucket_bps"),
            ),
            "cost_usdt": _first_number(response.data, ("cost_usdt", "estimated_cost_usdt", "total_cost_usdt")),
            "cost_source": _first_string(response.data, ("source", "cost_source", "bucket_source")),
            "response": summarize_response(response.data),
            "error": response.error,
        }
        safe_row = sanitize_quant_lab_obj(row)
        self.cost_estimates.append(dict(safe_row))
        self._emit_usage(safe_row)
        return dict(safe_row)

    def _gate_order(self, order: Order) -> Dict[str, Any]:
        alpha_id = _order_alpha_id(order, self.default_alpha_id)
        response = self.client.gate_decision(alpha_id)
        gate_decision = decision_from_payload(response.data)
        fallback = not bool(response.ok)
        row = {
            "event_type": "gate_decision",
            "endpoint": f"/v1/gates/decision/{alpha_id}",
            "ok": bool(response.ok),
            "symbol": str(getattr(order, "symbol", "") or ""),
            "side": str(getattr(order, "side", "") or ""),
            "intent": str(getattr(order, "intent", "") or ""),
            "alpha_id": alpha_id,
            "quant_lab_decision": gate_decision,
            "fallback_used": fallback,
            "response": summarize_response(response.data),
            "error": response.error,
        }
        self._emit_usage(row)
        return dict(sanitize_quant_lab_obj(row))

    @staticmethod
    def _is_buy(order: Order) -> bool:
        return str(getattr(order, "side", "") or "").strip().lower() == "buy"

    def filter_orders(self, orders: Iterable[Order]) -> Tuple[List[Order], Dict[str, Any]]:
        source_orders = list(orders or [])
        kept: List[Order] = []
        before = len(source_orders)

        if self.effective_decision == DECISION_UNKNOWN:
            self.effective_decision = fallback_decision(self.fail_policy)
            self.fallback_used = True

        for order in source_orders:
            cost_row = self._estimate_order_cost(order)
            gate_row: Optional[Dict[str, Any]] = None
            decision = self.effective_decision
            event_source = "permission"
            fallback_used = bool(self.fallback_used)

            cost_decision = normalize_quant_lab_decision(cost_row.get("quant_lab_decision"))
            if cost_row.get("fallback_used"):
                decision = fallback_decision(self.fail_policy)
                event_source = "cost"
                fallback_used = True
            elif cost_decision in {DECISION_SELL_ONLY, DECISION_ABORT}:
                decision = cost_decision
                event_source = "cost"

            if self.gate_check_enabled:
                gate_row = self._gate_order(order)
                gate_decision = normalize_quant_lab_decision(gate_row.get("quant_lab_decision"))
                if gate_row.get("fallback_used"):
                    decision = fallback_decision(self.fail_policy)
                    event_source = "gate"
                    fallback_used = True
                elif gate_decision in {DECISION_SELL_ONLY, DECISION_ABORT}:
                    decision = gate_decision
                    event_source = "gate"

            filtered = False
            reason = ""
            if decision == DECISION_ABORT:
                filtered = True
                reason = _filter_reason(DECISION_ABORT, event_source, fallback_used)
            elif decision == DECISION_SELL_ONLY and self._is_buy(order):
                filtered = True
                reason = _filter_reason(DECISION_SELL_ONLY, event_source, fallback_used)

            meta = dict(getattr(order, "meta", None) or {})
            meta["quant_lab"] = {
                "permission_decision": self.permission_decision,
                "effective_decision": self.effective_decision,
                "order_decision": decision,
                "filter_source": event_source,
                "fallback_used": fallback_used,
                "filtered": filtered,
                "filter_reason": reason,
                "cost": cost_row,
                "gate": gate_row,
            }
            order.meta = sanitize_quant_lab_obj(meta)

            filter_row = {
                "event_type": "order_filter",
                "symbol": str(getattr(order, "symbol", "") or ""),
                "side": str(getattr(order, "side", "") or ""),
                "intent": str(getattr(order, "intent", "") or ""),
                "notional_usdt": float(getattr(order, "notional_usdt", 0.0) or 0.0),
                "permission_decision": self.permission_decision,
                "effective_decision": self.effective_decision,
                "order_decision": decision,
                "filter_source": event_source,
                "fallback_used": fallback_used,
                "filtered": filtered,
                "filter_reason": reason,
                "cost_bps": cost_row.get("cost_bps"),
                "cost_source": cost_row.get("cost_source"),
            }
            self.filtered_orders.append(dict(sanitize_quant_lab_obj(filter_row)))
            self._emit_usage(filter_row)
            if not filtered:
                kept.append(order)

        summary = {
            "enabled": True,
            "run_id": self.run_id,
            "permission_decision": self.permission_decision,
            "effective_decision": self.effective_decision,
            "fail_policy": str(self.fail_policy or "sell_only").lower(),
            "fallback_used": bool(self.fallback_used),
            "orders_before": before,
            "orders_after": len(kept),
            "orders_filtered": before - len(kept),
            "buy_orders_filtered": len(
                [row for row in self.filtered_orders if row.get("filtered") and str(row.get("side")).lower() == "buy"]
            ),
            "abort_orders_filtered": len(
                [row for row in self.filtered_orders if str(row.get("order_decision")) == DECISION_ABORT and row.get("filtered")]
            ),
            "cost_estimate_count": len(self.cost_estimates),
            "cost_fallback_count": len([row for row in self.cost_estimates if row.get("fallback_used")]),
            "gate_check_enabled": bool(self.gate_check_enabled),
        }
        self._emit_usage({"event_type": "run_summary", **summary})
        return kept, sanitize_quant_lab_obj(summary)

    def audit_payload(self) -> Dict[str, Any]:
        return sanitize_quant_lab_obj(
            {
                "enabled": True,
                "run_id": self.run_id,
                "permission": {
                    "decision": self.permission_decision,
                    "effective_decision": self.effective_decision,
                    "fallback_used": bool(self.fallback_used),
                    "fail_policy": str(self.fail_policy or "sell_only").lower(),
                    "strategy": str(self.strategy or ""),
                    "version": str(self.strategy_version or ""),
                    "response": self.permission_response.summary() if self.permission_response else None,
                },
                "health": self.health_response.summary() if self.health_response else None,
                "cost_estimates": self.cost_estimates,
                "filtered_orders": self.filtered_orders,
                "events_tail": self.events[-50:],
            }
        )
