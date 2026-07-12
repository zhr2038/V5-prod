from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.paper_runtime.contracts import PaperRule


class PaperRuleInterpreter:
    """Evaluate a closed rule vocabulary; never executes source text or imports."""

    def evaluate(
        self,
        rule: PaperRule,
        context: Mapping[str, Any],
        *,
        history: Sequence[Mapping[str, Any]] = (),
    ) -> bool:
        operator = rule.operator
        if operator == "all":
            return all(
                self.evaluate(child, context, history=history)
                for child in rule.children
            )
        if operator == "any":
            return any(
                self.evaluate(child, context, history=history)
                for child in rule.children
            )
        if operator == "not":
            return not self.evaluate(rule.children[0], context, history=history)
        if operator == "consecutive":
            count = int(rule.periods or 0)
            samples = [*history, context][-count:]
            return len(samples) == count and all(
                self.evaluate(rule.children[0], sample, history=[])
                for sample in samples
            )
        if operator == "regime_in":
            actual = str(context.get(rule.field or "") or "").upper()
            return actual in {str(value).upper() for value in rule.values}
        if operator == "max_holding_bars":
            return _number(context.get("holding_bars")) >= _number(rule.value)
        if operator == "take_profit":
            return _number(context.get("net_pnl_bps")) >= _number(rule.value)
        if operator == "stop_loss":
            return _number(context.get("net_pnl_bps")) <= _number(rule.value)
        if operator == "trailing_exit":
            peak = _number(context.get("peak_pnl_bps"))
            current = _number(context.get("net_pnl_bps"))
            return peak > 0 and peak - current >= abs(_number(rule.value))
        if operator == "signal_invalid":
            if rule.field is None:
                return False
            return _number(context.get(rule.field)) <= _number(rule.value)
        if operator in {"crosses_above", "crosses_below"}:
            if not history:
                return False
            previous = history[-1]
            previous_left = _number(previous.get(rule.field or ""))
            current_left = _number(context.get(rule.field or ""))
            previous_right = _right_value(rule, previous)
            current_right = _right_value(rule, context)
            if operator == "crosses_above":
                return previous_left <= previous_right and current_left > current_right
            return previous_left >= previous_right and current_left < current_right
        left = _number(context.get(rule.field or ""))
        right = _right_value(rule, context)
        if operator in {
            "gt",
            "rank_gte",
            "quantile_gte",
            "momentum_gt",
            "return_gt",
            "volatility_gt",
            "volume_zscore_gt",
        }:
            return left > right if operator == "gt" else left >= right
        if operator in {
            "lt",
            "rank_lte",
            "quantile_lte",
            "momentum_lt",
            "return_lt",
            "volatility_lt",
            "volume_zscore_lt",
        }:
            return left < right if operator == "lt" else left <= right
        if operator == "gte":
            return left >= right
        if operator == "lte":
            return left <= right
        raise ValueError(f"unsupported_operator:{operator}")

    def match_reason(
        self,
        rule: PaperRule,
        context: Mapping[str, Any],
        *,
        history: Sequence[Mapping[str, Any]] = (),
    ) -> str:
        """Return the closed-vocabulary branch that caused a true rule."""
        if not self.evaluate(rule, context, history=history):
            return ""
        if rule.operator == "all":
            reasons = [
                self.match_reason(child, context, history=history)
                for child in rule.children
            ]
            return "all:" + "+".join(reason for reason in reasons if reason)
        if rule.operator == "any":
            for child in rule.children:
                reason = self.match_reason(child, context, history=history)
                if reason:
                    return reason
            return "any"
        if rule.operator == "not":
            return f"not:{rule.children[0].operator}"
        if rule.operator == "consecutive":
            return f"consecutive:{rule.children[0].operator}"
        return rule.operator


def _right_value(rule: PaperRule, context: Mapping[str, Any]) -> float:
    if rule.reference_field:
        return _number(context.get(rule.reference_field))
    return _number(rule.value)


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")
