from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd

from src.factor_factory.expression import FactorExpressionExecutor, collect_features
from src.factor_factory.graph import order_specs_by_dependencies
from src.factor_factory.models import FactorSnapshot, FactorSpec
from src.utils.time import timeframe_seconds


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float, np.integer, np.floating)):
        raw = float(value)
        dt = datetime.fromtimestamp(raw / 1000.0 if raw > 10_000_000_000 else raw, tz=timezone.utc)
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _latest_event_time(frame: pd.DataFrame, explicit: Any = None) -> datetime:
    dt = _coerce_datetime(explicit)
    if dt is not None:
        return dt
    for column in ("event_time", "timestamp", "ts"):
        if column in frame.columns and len(frame[column]) > 0:
            dt = _coerce_datetime(frame[column].iloc[-1])
            if dt is not None:
                return dt
    return datetime.now(timezone.utc)


class FactorExecutor:
    def __init__(self, expression_executor: FactorExpressionExecutor | None = None):
        self.expression_executor = expression_executor or FactorExpressionExecutor()

    def execute(
        self,
        specs: Iterable[FactorSpec],
        primitive_frame: pd.DataFrame,
        *,
        symbol: str,
        timeframe: str | None = None,
        event_time: Any = None,
        data_version: str,
        calculated_at: datetime | None = None,
        strict: bool = True,
    ) -> list[FactorSnapshot]:
        frame = primitive_frame.copy()
        ordered_specs = order_specs_by_dependencies(specs)
        snapshots: list[FactorSnapshot] = []
        calculated = calculated_at or datetime.now(timezone.utc)

        for spec in ordered_specs:
            quality_flags: list[str] = []
            if len(frame) < spec.required_bars:
                quality_flags.append("insufficient_history")

            missing = sorted(name for name in collect_features(spec.expression) if name not in frame.columns)
            if missing:
                if strict:
                    raise ValueError(f"{spec.factor_id}@{spec.version} missing primitive features: {missing}")
                quality_flags.extend(f"missing_input:{name}" for name in missing)
                series = pd.Series([np.nan] * len(frame), index=frame.index, dtype="float64")
            else:
                series = self.expression_executor.evaluate(spec.expression, frame)

            raw_value = series.iloc[-1]
            try:
                value = float(raw_value)
            except Exception:
                value = float("nan")
            if not np.isfinite(value):
                quality_flags.append("non_finite_value")
                value = 0.0

            effective_timeframe = timeframe or spec.timeframe
            event_dt = _latest_event_time(frame, explicit=event_time)
            available_dt = event_dt + timedelta(
                seconds=timeframe_seconds(effective_timeframe) * int(spec.availability_lag_bars)
            )
            snapshot = FactorSnapshot(
                factor_id=spec.factor_id,
                factor_version=spec.version,
                symbol=str(symbol),
                timeframe=effective_timeframe,
                value=value,
                event_time=event_dt,
                available_time=available_dt,
                calculated_at=calculated,
                data_version=str(data_version),
                quality_flags=quality_flags,
            )
            snapshots.append(snapshot)
            frame[spec.factor_id] = series

        return snapshots
