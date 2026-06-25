from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from src.factor_factory.expression import collect_features
from src.factor_factory.models import FactorSpec, FactorStatus


ONLINE_STATUSES = frozenset({FactorStatus.SHADOW, FactorStatus.PRODUCTION})


class FactorRegistry:
    def __init__(self, specs: Iterable[FactorSpec] | None = None):
        self._specs: dict[tuple[str, str], FactorSpec] = {}
        self._versions_by_id: dict[str, list[str]] = defaultdict(list)
        for spec in specs or []:
            self.register(spec)

    def register(self, spec: FactorSpec, *, replace: bool = False) -> FactorSpec:
        expression_inputs = collect_features(spec.expression)
        missing_declared = sorted(expression_inputs - set(spec.inputs))
        if missing_declared:
            raise ValueError(f"{spec.factor_id} inputs missing expression features: {missing_declared}")

        key = (spec.factor_id, spec.version)
        if key in self._specs and not replace:
            raise ValueError(f"factor already registered: {spec.factor_id}@{spec.version}")
        self._specs[key] = spec
        versions = self._versions_by_id[spec.factor_id]
        if spec.version not in versions:
            versions.append(spec.version)
            versions.sort()
        return spec

    def get(self, factor_id: str, version: str | None = None) -> FactorSpec:
        if version is None:
            versions = self._versions_by_id.get(str(factor_id), [])
            if not versions:
                raise KeyError(f"unknown factor_id: {factor_id}")
            version = versions[-1]
        key = (str(factor_id), str(version))
        if key not in self._specs:
            raise KeyError(f"unknown factor: {factor_id}@{version}")
        return self._specs[key]

    def list(
        self,
        *,
        timeframe: str | None = None,
        statuses: Iterable[FactorStatus | str] | None = None,
    ) -> list[FactorSpec]:
        status_set = {FactorStatus(s) for s in statuses} if statuses is not None else None
        out = []
        for spec in self._specs.values():
            if timeframe is not None and spec.timeframe != timeframe:
                continue
            if status_set is not None and spec.status not in status_set:
                continue
            out.append(spec)
        return sorted(out, key=lambda spec: (spec.factor_id, spec.version))

    def get_online_factors(self, timeframe: str, *, statuses: Iterable[FactorStatus | str] | None = None) -> list[FactorSpec]:
        return self.list(timeframe=timeframe, statuses=statuses or ONLINE_STATUSES)

    def required_bars(
        self,
        *,
        timeframe: str | None = None,
        statuses: Iterable[FactorStatus | str] | None = None,
    ) -> int:
        specs = self.list(timeframe=timeframe, statuses=statuses)
        if not specs:
            return 0
        return max(spec.required_bars for spec in specs)
