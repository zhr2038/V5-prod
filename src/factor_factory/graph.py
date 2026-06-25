from __future__ import annotations

from collections import defaultdict, deque
from typing import Iterable

from src.factor_factory.models import FactorSpec


def order_specs_by_dependencies(specs: Iterable[FactorSpec]) -> list[FactorSpec]:
    spec_list = list(specs or [])
    by_id = {spec.factor_id: spec for spec in spec_list}
    incoming: dict[str, set[str]] = {spec.factor_id: set() for spec in spec_list}
    outgoing: dict[str, set[str]] = defaultdict(set)

    for spec in spec_list:
        for dep in spec.inputs:
            if dep in by_id:
                incoming[spec.factor_id].add(dep)
                outgoing[dep].add(spec.factor_id)

    ready = deque(sorted(fid for fid, deps in incoming.items() if not deps))
    ordered: list[FactorSpec] = []
    while ready:
        fid = ready.popleft()
        ordered.append(by_id[fid])
        for child in sorted(outgoing.get(fid, set())):
            incoming[child].discard(fid)
            if not incoming[child]:
                ready.append(child)

    if len(ordered) != len(spec_list):
        cycle_ids = sorted(fid for fid, deps in incoming.items() if deps)
        raise ValueError(f"factor dependency cycle detected: {cycle_ids}")
    return ordered
