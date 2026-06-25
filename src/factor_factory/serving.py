from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from src.factor_factory.models import FactorSnapshot


def latest_snapshot_map(snapshots: Iterable[FactorSnapshot]) -> dict[str, dict[str, FactorSnapshot]]:
    out: dict[str, dict[str, FactorSnapshot]] = defaultdict(dict)
    for snapshot in snapshots or []:
        current = out[snapshot.symbol].get(snapshot.factor_id)
        if current is None or snapshot.available_time >= current.available_time:
            out[snapshot.symbol][snapshot.factor_id] = snapshot
    return {symbol: dict(bucket) for symbol, bucket in out.items()}


def ensemble_score(snapshots: Iterable[FactorSnapshot], *, weights: dict[str, float]) -> float:
    total_weight = 0.0
    total = 0.0
    for snapshot in snapshots or []:
        if snapshot.quality_flags:
            continue
        weight = float(weights.get(snapshot.factor_id, 0.0))
        if weight == 0.0:
            continue
        total += weight * float(snapshot.value)
        total_weight += abs(weight)
    if total_weight <= 1e-12:
        return 0.0
    return float(total / total_weight)
