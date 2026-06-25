from __future__ import annotations

from src.factor_factory.models import FactorSpec, FactorStatus


ALLOWED_TRANSITIONS: dict[FactorStatus, set[FactorStatus]] = {
    FactorStatus.CANDIDATE: {FactorStatus.VALIDATED, FactorStatus.RETIRED},
    FactorStatus.VALIDATED: {FactorStatus.SHADOW, FactorStatus.RETIRED},
    FactorStatus.SHADOW: {FactorStatus.PRODUCTION, FactorStatus.RETIRED},
    FactorStatus.PRODUCTION: {FactorStatus.RETIRED},
    FactorStatus.RETIRED: set(),
}


def transition_factor(spec: FactorSpec, new_status: FactorStatus | str) -> FactorSpec:
    target = FactorStatus(new_status)
    allowed = ALLOWED_TRANSITIONS[spec.status]
    if target not in allowed:
        raise ValueError(f"invalid factor status transition: {spec.status.value} -> {target.value}")
    return spec.model_copy(update={"status": target})
