from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


class FactorOperatorError(ValueError):
    pass


def _as_series(value: Any, index: pd.Index) -> pd.Series:
    if isinstance(value, pd.Series):
        return value.reset_index(drop=True).reindex(range(len(index))).set_axis(index)
    return pd.Series([value] * len(index), index=index, dtype="float64")


def _as_positive_int(value: Any, *, name: str, allow_zero: bool = False) -> int:
    try:
        out = int(value)
    except Exception as exc:
        raise FactorOperatorError(f"{name} must be an integer") from exc
    if allow_zero:
        if out < 0:
            raise FactorOperatorError(f"{name} cannot be negative")
    elif out <= 0:
        raise FactorOperatorError(f"{name} must be positive")
    return out


def _safe_div(lhs: pd.Series, rhs: pd.Series) -> pd.Series:
    denom = rhs.replace([np.inf, -np.inf], np.nan)
    out = lhs / denom.where(denom.abs() > 1e-12)
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def apply_operator(op: str, args: list[Any], index: pd.Index) -> pd.Series:
    name = str(op or "").strip().upper()

    if name in {"ADD", "SUB", "MUL", "SAFE_DIV"}:
        if len(args) != 2:
            raise FactorOperatorError(f"{name} expects 2 arguments")
        lhs = _as_series(args[0], index)
        rhs = _as_series(args[1], index)
        if name == "ADD":
            return lhs + rhs
        if name == "SUB":
            return lhs - rhs
        if name == "MUL":
            return lhs * rhs
        return _safe_div(lhs, rhs)

    if name in {"NEG", "ABS", "SIGN"}:
        if len(args) != 1:
            raise FactorOperatorError(f"{name} expects 1 argument")
        value = _as_series(args[0], index)
        if name == "NEG":
            return -value
        if name == "ABS":
            return value.abs()
        return np.sign(value).astype("float64")

    if name == "CLIP":
        if len(args) != 3:
            raise FactorOperatorError("CLIP expects 3 arguments")
        value = _as_series(args[0], index)
        low = float(args[1])
        high = float(args[2])
        if low > high:
            raise FactorOperatorError("CLIP low cannot be greater than high")
        return value.clip(lower=low, upper=high)

    if name == "DELAY":
        if len(args) != 2:
            raise FactorOperatorError("DELAY expects 2 arguments")
        bars = _as_positive_int(args[1], name="DELAY bars", allow_zero=True)
        return _as_series(args[0], index).shift(bars)

    if name == "DELTA":
        if len(args) != 2:
            raise FactorOperatorError("DELTA expects 2 arguments")
        bars = _as_positive_int(args[1], name="DELTA bars")
        value = _as_series(args[0], index)
        return value - value.shift(bars)

    if name in {"ROLL_MEAN", "ROLL_STD", "ROLL_MIN", "ROLL_MAX", "ROLL_ZSCORE", "EMA", "DECAY_LINEAR"}:
        if len(args) != 2:
            raise FactorOperatorError(f"{name} expects 2 arguments")
        value = _as_series(args[0], index)
        bars = _as_positive_int(args[1], name=f"{name} bars")
        if name == "ROLL_MEAN":
            return value.rolling(bars, min_periods=bars).mean()
        if name == "ROLL_STD":
            return value.rolling(bars, min_periods=bars).std()
        if name == "ROLL_MIN":
            return value.rolling(bars, min_periods=bars).min()
        if name == "ROLL_MAX":
            return value.rolling(bars, min_periods=bars).max()
        if name == "ROLL_ZSCORE":
            mean = value.rolling(bars, min_periods=bars).mean()
            std = value.rolling(bars, min_periods=bars).std()
            return _safe_div(value - mean, std)
        if name == "EMA":
            return value.ewm(span=bars, adjust=False, min_periods=bars).mean()

        weights = np.arange(1, bars + 1, dtype=float)
        weights = weights / weights.sum()
        return value.rolling(bars, min_periods=bars).apply(
            lambda xs: float(np.dot(np.asarray(xs, dtype=float), weights)),
            raw=True,
        )

    raise FactorOperatorError(f"Unsupported operator: {op}")


ALLOWED_OPERATORS = frozenset(
    {
        "ADD",
        "SUB",
        "MUL",
        "SAFE_DIV",
        "NEG",
        "ABS",
        "SIGN",
        "CLIP",
        "DELAY",
        "DELTA",
        "ROLL_MEAN",
        "ROLL_STD",
        "ROLL_MIN",
        "ROLL_MAX",
        "ROLL_ZSCORE",
        "EMA",
        "DECAY_LINEAR",
    }
)
