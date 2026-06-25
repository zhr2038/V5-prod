from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from src.factor_factory.operators import ALLOWED_OPERATORS, apply_operator


class FactorExpressionError(ValueError):
    pass


def collect_features(expression: dict[str, Any]) -> set[str]:
    features: set[str] = set()

    def visit(node: Any) -> None:
        if not isinstance(node, dict):
            raise FactorExpressionError("expression nodes must be objects")
        if "feature" in node:
            name = str(node.get("feature") or "").strip()
            if not name:
                raise FactorExpressionError("feature name cannot be empty")
            features.add(name)
            return
        if "const" in node:
            return
        if "op" in node:
            op = str(node.get("op") or "").strip().upper()
            if op not in ALLOWED_OPERATORS:
                raise FactorExpressionError(f"Unsupported operator: {op}")
            args = node.get("args")
            if not isinstance(args, list):
                raise FactorExpressionError(f"{op} args must be a list")
            for arg in args:
                visit(arg)
            return
        raise FactorExpressionError("expression node must contain feature, const, or op")

    visit(expression)
    return features


def expression_depth(expression: dict[str, Any]) -> int:
    if "feature" in expression or "const" in expression:
        return 1
    args = expression.get("args")
    if not isinstance(args, list) or not args:
        return 1
    return 1 + max(expression_depth(arg) for arg in args)


class FactorExpressionExecutor:
    def __init__(self, allowed_features: Iterable[str] | None = None):
        self.allowed_features = {str(name) for name in allowed_features} if allowed_features is not None else None

    def validate(self, expression: dict[str, Any], frame: pd.DataFrame | None = None) -> None:
        features = collect_features(expression)
        if self.allowed_features is not None:
            unknown = sorted(features - self.allowed_features)
            if unknown:
                raise FactorExpressionError(f"Unknown primitive features: {unknown}")
        if frame is not None:
            missing = sorted(name for name in features if name not in frame.columns)
            if missing:
                raise FactorExpressionError(f"Missing primitive features: {missing}")

    def evaluate(self, expression: dict[str, Any], frame: pd.DataFrame) -> pd.Series:
        if frame.empty:
            raise FactorExpressionError("primitive frame cannot be empty")
        self.validate(expression, frame)
        index = frame.index

        def eval_node(node: Any) -> Any:
            if "feature" in node:
                return pd.Series(frame[str(node["feature"])], index=index, dtype="float64")
            if "const" in node:
                return node["const"]
            op = str(node.get("op") or "").strip().upper()
            args = [eval_node(arg) for arg in node.get("args") or []]
            return apply_operator(op, args, index)

        out = eval_node(expression)
        if not isinstance(out, pd.Series):
            out = pd.Series([out] * len(frame), index=index, dtype="float64")
        return out.replace([float("inf"), float("-inf")], pd.NA).astype("float64")
